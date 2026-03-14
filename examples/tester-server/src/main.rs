use clap::Parser;
use cli::Args;
use itertools::Itertools;
use jupnet_sdk::{
    account::Account,
    hash::Hash,
    instruction::CompiledInstruction,
    message::{Message, MessageHeader},
    pubkey::Pubkey,
    signature::{Keypair, TypedSignature},
};
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    config::{CompressionParameters, ConfigQuicPlugin, QuicParameters},
    net::parse_host_port,
    types::{
        slot_identifier::SlotIdentifier,
        transaction::{Transaction, TransactionMeta},
    },
};
use quic_geyser_server::quic_server::QuicServer;
use rand::{thread_rng, Rng};
use std::time::Duration;

pub mod cli;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config = ConfigQuicPlugin {
        address: parse_host_port(format!("[::]:{}", args.port).as_str()).unwrap(),
        log_level: "info".to_string(),
        quic_parameters: QuicParameters {
            max_number_of_streams_per_client: args.number_of_streams,
            ..Default::default()
        },
        compression_parameters: CompressionParameters {
            compression_type: quic_geyser_common::compression::CompressionType::None,
        },
        number_of_retries: 100,
        allow_accounts: true,
        allow_accounts_at_startup: false,
        enable_block_builder: false,
        build_blocks_with_accounts: false,
    };
    let quic_server = QuicServer::new(config, Keypair::new(), &runtime).unwrap();
    // to avoid errors
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut slot = 4;
    let mut write_version = 1;
    let mut rand = thread_rng();
    let datas = (0..args.number_of_random_accounts)
        .map(|_| {
            let size: usize =
                rand.gen_range(args.min_account_data_size..args.max_account_data_size);
            let data = (0..size).map(|_| rand.gen::<u8>()).collect_vec();
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: rand.gen(),
                    data,
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version,
            }
        })
        .collect_vec();

    // Generate a few program ids for test transactions
    let test_program_ids: Vec<Pubkey> = (0..3).map(|_| Pubkey::new_unique()).collect();
    if args.transactions_per_slot > 0 {
        log::info!(
            "Emitting {} transactions per slot with program ids: {:?}",
            args.transactions_per_slot,
            test_program_ids
        );
    }

    let sleep_time_in_nanos = 1_000_000_000 / (args.accounts_per_second / 1000) as u64;
    loop {
        slot += 1;
        quic_server
            .send_message(ChannelMessage::Slot(
                slot,
                slot - 1,
                quic_geyser_common::types::block_meta::SlotStatus::FirstShredReceived,
            ))
            .unwrap();
        quic_server
            .send_message(ChannelMessage::Slot(
                slot,
                slot - 1,
                quic_geyser_common::types::block_meta::SlotStatus::LastShredReceived,
            ))
            .unwrap();
        quic_server
            .send_message(ChannelMessage::Slot(
                slot,
                slot - 1,
                quic_geyser_common::types::block_meta::SlotStatus::Processed,
            ))
            .unwrap();
        quic_server
            .send_message(ChannelMessage::Slot(
                slot - 1,
                slot - 2,
                quic_geyser_common::types::block_meta::SlotStatus::Confirmed,
            ))
            .unwrap();
        quic_server
            .send_message(ChannelMessage::Slot(
                slot - 2,
                slot - 3,
                quic_geyser_common::types::block_meta::SlotStatus::Finalized,
            ))
            .unwrap();

        // Emit test transactions
        for i in 0..args.transactions_per_slot {
            let program_id = test_program_ids[i as usize % test_program_ids.len()];
            let signer = Pubkey::new_unique();
            let tx = Transaction {
                slot_identifier: SlotIdentifier { slot },
                signatures: vec![TypedSignature::new_unique()],
                message: Some(jupnet_sdk::message::VersionedMessage::Legacy(Message {
                    header: MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    },
                    account_keys: vec![signer, program_id],
                    recent_blockhash: Hash::new_unique(),
                    instructions: vec![CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0],
                        data: vec![rand.gen(), rand.gen(), rand.gen(), rand.gen()],
                    }],
                })),
                is_vote: false,
                transaction_meta: TransactionMeta {
                    error: None,
                    fee: 5000,
                    pre_balances: Some(vec![1_000_000, 0]),
                    post_balances: Some(vec![995_000, 0]),
                    inner_instructions: None,
                    log_messages: Some(vec![format!("Program {program_id} invoke [1]")]),
                    rewards: None,
                    return_data: None,
                    compute_units_consumed: Some(rand.gen_range(1000..200_000)),
                },
                index: i as u64,
                batched_steps_meta: None,
            };
            quic_server
                .send_message(ChannelMessage::Transaction(Box::new(tx)))
                .unwrap();
        }

        for i in 1..args.accounts_per_second + 1 {
            write_version += 1;
            let data_index = 0;
            let mut account = datas.get(data_index).unwrap().clone();
            account.write_version = write_version;
            let channel_message = ChannelMessage::Account(account, slot, false);
            quic_server.send_message(channel_message).unwrap();
            if i % 1000 == 0 {
                tokio::time::sleep(Duration::from_nanos(sleep_time_in_nanos)).await;
            }
        }
    }
}
