use crate::config::Config;
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
};
use jupnet_sdk::{
    account::Account,
    clock::Slot,
    message::{
        SanitizedMessage::{Batched, Legacy},
        VersionedMessage,
    },
    pubkey::Pubkey,
    signature::Keypair,
};
use quic_geyser_block_builder::block_builder::start_block_building_thread;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    plugin_error::QuicGeyserError,
    types::{
        block_meta::{BlockMeta, SlotStatus as QuicSlotStatus},
        slot_identifier::SlotIdentifier,
        transaction::{BatchedStepMeta, Transaction, TransactionMeta},
    },
};
use quic_geyser_server::quic_server::QuicServer;

#[derive(Debug, Default)]
pub struct QuicGeyserPlugin {
    quic_server: Option<QuicServer>,
    block_builder_channel: Option<tokio::sync::mpsc::UnboundedSender<ChannelMessage>>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl GeyserPlugin for QuicGeyserPlugin {
    fn name(&self) -> &'static str {
        "quic_geyser_plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        log::info!("loading quic_geyser plugin");
        let config = match Config::load_from_file(config_file) {
            Ok(config) => config,
            Err(e) => {
                log::error!("Error loading config file: {}", e);
                return Err(e);
            }
        };
        let compression_type = config.quic_plugin.compression_parameters.compression_type;
        let enable_block_builder = config.quic_plugin.enable_block_builder;
        let build_blocks_with_accounts = config.quic_plugin.build_blocks_with_accounts;
        log::info!("Quic plugin config correctly loaded");
        jupnet_logger::setup_with_default(&config.quic_plugin.log_level);

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        let runtime = builder.enable_all().build().unwrap();

        let quic_server =
            QuicServer::new(config.quic_plugin, Keypair::new(), &runtime).map_err(|_| {
                GeyserPluginError::Custom(Box::new(QuicGeyserError::ErrorConfiguringServer))
            })?;
        if enable_block_builder {
            // disable block building for now
            let (sx, rx) = tokio::sync::mpsc::unbounded_channel();
            start_block_building_thread(
                rx,
                quic_server.data_channel_sender.clone(),
                compression_type,
                build_blocks_with_accounts,
            );
            self.block_builder_channel = Some(sx);
        }

        self.quic_server = Some(quic_server);
        self.runtime = Some(runtime);
        log::info!("geyser plugin loaded ok ()");
        Ok(())
    }

    fn on_unload(&mut self) {
        self.quic_server = None;
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        self.runtime = None;
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: Slot,
        is_startup: bool,
    ) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };

        if !quic_server.quic_plugin_config.allow_accounts
            || (is_startup && !quic_server.quic_plugin_config.allow_accounts_at_startup)
        {
            return Ok(());
        }
        let ReplicaAccountInfoVersions::V0_0_3(account_info) = account;
        let owner = Pubkey::try_from(account_info.owner)
            .map_err(|e| GeyserPluginError::Custom(format!("Invalid owner pubkey: {e}").into()))?;
        let pubkey = Pubkey::try_from(account_info.pubkey).map_err(|e| {
            GeyserPluginError::Custom(format!("Invalid account pubkey: {e}").into())
        })?;
        let account = Account {
            lamports: account_info.lamports,
            data: account_info.data.to_vec(),
            owner,
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
        };

        let channel_message = ChannelMessage::Account(
            AccountData {
                pubkey,
                account,
                write_version: account_info.write_version,
            },
            slot,
            is_startup,
        );

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(channel_message.clone());
        }

        quic_server.send_message(channel_message).map_err(|e| {
            log::error!("Error sending account message: {}", e);
            GeyserPluginError::Custom(Box::new(e))
        })?;
        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        // Todo
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };
        let quic_slot_status = match status {
            SlotStatus::Processed => QuicSlotStatus::Processed,
            SlotStatus::Rooted => QuicSlotStatus::Finalized,
            SlotStatus::Confirmed => QuicSlotStatus::Confirmed,
            SlotStatus::FirstShredReceived => QuicSlotStatus::FirstShredReceived,
            SlotStatus::Completed => QuicSlotStatus::LastShredReceived,
            SlotStatus::CreatedBank => {
                return Ok(());
            }
            SlotStatus::Dead(_) => QuicSlotStatus::Dead,
        };
        let slot_message = ChannelMessage::Slot(slot, parent.unwrap_or_default(), quic_slot_status);

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(slot_message.clone());
        }

        quic_server
            .send_message(slot_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: Slot,
    ) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };
        let ReplicaTransactionInfoVersions::V0_0_3(jupiter_transaction) = transaction;

        let message = jupiter_transaction.transaction.message().clone();
        let batched_steps_meta = match &message {
            Legacy(_) => None,
            Batched(batched_msg) => {
                let all_signatures = jupiter_transaction.transaction.signatures();
                let num_batch_signers = batched_msg.batched_message.batch_signer_indexes.len();
                let step_signatures = &all_signatures[num_batch_signers..];

                let mut sig_offset = 0;
                Some(
                    batched_msg
                        .batched_message
                        .steps
                        .iter()
                        .zip(jupiter_transaction.batch_step_metas.iter())
                        .map(|(step, step_meta)| {
                            let num_inner_signers = step.inner_signer_indexes.len();
                            let sigs = step_signatures[sig_offset..sig_offset + num_inner_signers]
                                .to_vec();
                            sig_offset += num_inner_signers;
                            BatchedStepMeta {
                                signatures: sigs,
                                meta: TransactionMeta {
                                    error: step_meta.status.as_ref().err().cloned(),
                                    fee: step_meta.fee,
                                    pre_balances: Some(step_meta.pre_balances.clone()),
                                    post_balances: Some(step_meta.post_balances.clone()),
                                    inner_instructions: step_meta.inner_instructions.clone(),
                                    log_messages: step_meta.log_messages.clone(),
                                    rewards: step_meta.rewards.clone(),
                                    return_data: step_meta.return_data.clone(),
                                    compute_units_consumed: Some(step_meta.compute_units_consumed),
                                },
                            }
                        })
                        .collect(),
                )
            }
        };

        let status_meta = jupiter_transaction.transaction_status_meta;
        let versioned_message = match &message {
            Legacy(message) => VersionedMessage::Legacy((*message.message).clone()),
            Batched(message) => VersionedMessage::Batched((*message.batched_message).clone()),
        };

        let transaction = Transaction {
            slot_identifier: SlotIdentifier { slot },
            signatures: jupiter_transaction.transaction.signatures().to_vec(),
            message: Some(versioned_message),
            is_vote: jupiter_transaction.is_vote,
            transaction_meta: TransactionMeta {
                error: match &status_meta.status {
                    Ok(_) => None,
                    Err(e) => Some(e.clone()),
                },
                fee: status_meta.fee,
                pre_balances: Some(status_meta.pre_balances.clone()),
                post_balances: Some(status_meta.post_balances.clone()),
                inner_instructions: status_meta.inner_instructions.clone(),
                log_messages: status_meta.log_messages.clone(),
                rewards: status_meta.rewards.clone(),
                return_data: status_meta.return_data.clone(),
                compute_units_consumed: Some(status_meta.compute_units_consumed),
            },
            index: jupiter_transaction.index as u64,
            is_batched_transaction: matches!(message, Batched(_)),
            batched_steps_meta,
        };

        let transaction_message = ChannelMessage::Transaction(Box::new(transaction));

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(transaction_message.clone());
        }

        quic_server
            .send_message(transaction_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        // Not required
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };

        let ReplicaBlockInfoVersions::V0_0_4(blockinfo) = blockinfo;

        let block_meta = BlockMeta {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            blockhash: blockinfo.blockhash.to_string(),
            rewards: blockinfo.rewards.rewards.to_vec(),
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
            entries_count: blockinfo.entry_count,
            block_time: blockinfo.block_time.unwrap_or_default() as u64,
        };

        let block_meta_message = ChannelMessage::BlockMeta(block_meta);

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(block_meta_message.clone());
        }

        quic_server
            .send_message(block_meta_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = QuicGeyserPlugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
