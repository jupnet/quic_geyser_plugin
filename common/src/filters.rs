use std::collections::HashSet;

use jupnet_sdk::{pubkey::Pubkey, signature::TypedSignature};
use serde::{Deserialize, Serialize};

use crate::channel_message::ChannelMessage;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct TransactionDetails {
    pub original_message: bool,
    pub logs: bool,
    pub inner_instructions: bool,
    pub rewards: bool,
    pub pre_post_balances: bool,
    pub return_data: bool,
    pub batched_steps: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TransactionFilter {
    /// Filter by specific signature. If None, matches any signature.
    pub signature: Option<TypedSignature>,
    /// Filter by account referenced in the transaction. If None, matches any account.
    pub filter_by_account: Option<Pubkey>,
    /// What representation to send to the client.
    pub output_type: TransactionDetails,
    /// When true, also forward account updates for accounts owned by the program.
    pub merge_accounts: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Filter {
    Account(AccountFilter),
    // will excude vote accounts and stake accounts by default
    AccountsAll,
    Slot,
    BlockMeta,
    FilterTransaction(TransactionFilter),
    BlockAll,
    DeletedAccounts,
    AccountsExcluding(AccountFilter),
}

impl Filter {
    pub fn allows(&self, message: &ChannelMessage) -> bool {
        match &self {
            Filter::Account(account) => account.allows(message),
            Filter::AccountsAll => match message {
                ChannelMessage::Account(account, _, _init) => {
                    account.account.owner != jupnet_program::vote::program::ID // does not belong to vote program
                        && account.account.owner != jupnet_program::stake::program::ID
                    // does not belong to stake program
                }
                _ => false,
            },
            Filter::Slot => matches!(message, ChannelMessage::Slot(..)),
            Filter::BlockMeta => matches!(message, ChannelMessage::BlockMeta(..)),
            Filter::FilterTransaction(tf) => match message {
                ChannelMessage::Transaction(transaction) => {
                    if let Some(ref sig) = tf.signature {
                        if transaction.signatures[0] != *sig {
                            return false;
                        }
                    }
                    if let Some(ref account) = tf.filter_by_account {
                        if !transaction.references_account(account) {
                            return false;
                        }
                    }
                    true
                }
                ChannelMessage::Account(account, _, _) => {
                    if tf.merge_accounts {
                        if let Some(ref filter_by_account) = tf.filter_by_account {
                            account.account.owner == *filter_by_account
                        } else {
                            true
                        }
                    } else {
                        false
                    }
                }
                _ => false,
            },
            Filter::BlockAll => matches!(message, ChannelMessage::Block(_)),
            Filter::DeletedAccounts => match message {
                ChannelMessage::Account(account, _, _) => account.account.lamports == 0,
                _ => false,
            },
            Filter::AccountsExcluding(account) => !account.allows(message),
        }
    }

    /// Transform the channel message based on the filter type.
    /// Strips fields from the transaction based on TransactionDetails options.
    /// Should only be called after `allows()` returns true.
    pub fn transform(&self, message: ChannelMessage) -> ChannelMessage {
        match self {
            Filter::FilterTransaction(tf) => {
                if let ChannelMessage::Transaction(tx) = message {
                    let details = &tf.output_type;
                    let meta = &tx.transaction_meta;
                    let stripped = crate::types::transaction::Transaction {
                        slot_identifier: tx.slot_identifier,
                        signatures: tx.signatures,
                        message: if details.original_message {
                            tx.message
                        } else {
                            None
                        },
                        is_vote: tx.is_vote,
                        transaction_meta: crate::types::transaction::TransactionMeta {
                            error: meta.error.clone(),
                            fee: meta.fee,
                            pre_balances: if details.pre_post_balances {
                                meta.pre_balances.clone()
                            } else {
                                None
                            },
                            post_balances: if details.pre_post_balances {
                                meta.post_balances.clone()
                            } else {
                                None
                            },
                            inner_instructions: if details.inner_instructions {
                                meta.inner_instructions.clone()
                            } else {
                                None
                            },
                            log_messages: if details.logs {
                                meta.log_messages.clone()
                            } else {
                                None
                            },
                            rewards: if details.rewards {
                                meta.rewards.clone()
                            } else {
                                None
                            },
                            return_data: if details.return_data {
                                meta.return_data.clone()
                            } else {
                                None
                            },
                            compute_units_consumed: meta.compute_units_consumed,
                        },
                        index: tx.index,
                        is_batched_transaction: tx.is_batched_transaction,
                        batched_steps_meta: if details.batched_steps {
                            tx.batched_steps_meta
                        } else {
                            None
                        },
                    };
                    ChannelMessage::Transaction(Box::new(stripped))
                } else {
                    // Account messages (from merge_accounts) pass through unchanged
                    message
                }
            }
            // All other filters pass the message through unchanged
            _ => message,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum MemcmpFilterData {
    Bytes(Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MemcmpFilter {
    pub offset: u64,
    pub data: MemcmpFilterData,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum AccountFilterType {
    Datasize(u64),
    Memcmp(MemcmpFilter),
}

// setting owner to 11111111111111111111111111111111 will subscribe to all the accounts
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct AccountFilter {
    pub owner: Option<Pubkey>,
    pub accounts: Option<HashSet<Pubkey>>,
    pub filters: Option<Vec<AccountFilterType>>,
}

impl AccountFilter {
    pub fn allows(&self, message: &ChannelMessage) -> bool {
        if let ChannelMessage::Account(account, _, _init) = message {
            if let Some(owner) = self.owner {
                if owner == account.account.owner {
                    // to do move the filtering somewhere else because here we need to decode the account data
                    // but cannot be avoided for now, this will lag the client is abusing this filter
                    // lagged clients will be dropped
                    if let Some(filters) = &self.filters {
                        return filters.iter().all(|filter| match filter {
                            AccountFilterType::Datasize(data_length) => {
                                account.account.data.len() == *data_length as usize
                            }
                            AccountFilterType::Memcmp(memcmp) => {
                                let offset = memcmp.offset as usize;
                                if offset > account.account.data.len() {
                                    false
                                } else {
                                    let MemcmpFilterData::Bytes(bytes) = &memcmp.data;
                                    if account.account.data[offset..].len() < bytes.len() {
                                        false
                                    } else {
                                        account.account.data[offset..offset + bytes.len()]
                                            == bytes[..]
                                    }
                                }
                            }
                        });
                    }
                    return true;
                }
            }
            if let Some(accounts) = &self.accounts {
                return accounts.contains(&account.pubkey);
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use jupnet_sdk::{
        account::Account as JupnetAccount,
        hash::Hash,
        message::{Message, MessageHeader},
        pubkey::Pubkey,
        signature::TypedSignature,
    };

    use crate::{
        channel_message::{AccountData, ChannelMessage},
        filters::{
            AccountFilter, AccountFilterType, Filter, MemcmpFilter, TransactionDetails,
            TransactionFilter,
        },
        types::{
            slot_identifier::SlotIdentifier,
            transaction::{Transaction, TransactionMeta},
        },
    };

    fn make_tx(program_ids: &[Pubkey]) -> Box<Transaction> {
        let signer = Pubkey::new_unique();
        let mut account_keys = vec![signer];
        account_keys.extend_from_slice(program_ids);

        let instructions = program_ids
            .iter()
            .enumerate()
            .map(|(i, _)| jupnet_sdk::instruction::CompiledInstruction {
                program_id_index: (i + 1) as u8,
                accounts: vec![0],
                data: vec![],
            })
            .collect();

        Box::new(Transaction {
            slot_identifier: SlotIdentifier { slot: 42 },
            signatures: vec![TypedSignature::new_unique()],
            message: Some(jupnet_sdk::message::VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: program_ids.len() as u8,
                },
                account_keys,
                recent_blockhash: Hash::new_unique(),
                instructions,
            })),
            is_vote: false,
            transaction_meta: TransactionMeta {
                error: None,
                fee: 5000,
                pre_balances: Some(vec![100000]),
                post_balances: Some(vec![95000]),
                inner_instructions: None,
                log_messages: Some(vec!["log1".to_string()]),
                rewards: None,
                return_data: None,
                compute_units_consumed: Some(12345),
            },
            index: 0,
            is_batched_transaction: false,
            batched_steps_meta: None,
        })
    }

    /// All details enabled
    fn all_details() -> TransactionDetails {
        TransactionDetails {
            original_message: true,
            logs: true,
            inner_instructions: true,
            rewards: true,
            pre_post_balances: true,
            return_data: true,
            batched_steps: true,
        }
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_filter_transaction_by_program() {
        let program_a = Pubkey::new_unique();
        let program_b = Pubkey::new_unique();
        let program_c = Pubkey::new_unique();

        let tx = make_tx(&[program_a, program_b]);
        let msg = ChannelMessage::Transaction(tx);

        let f_a = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_a),
            output_type: all_details(),
            merge_accounts: false,
        });
        let f_b = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_b),
            output_type: all_details(),
            merge_accounts: false,
        });
        let f_c = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_c),
            output_type: all_details(),
            merge_accounts: false,
        });

        assert_eq!(f_a.allows(&msg), true);
        assert_eq!(f_b.allows(&msg), true);
        assert_eq!(f_c.allows(&msg), false);

        // All details enabled should pass through the transaction unchanged
        let transformed = f_a.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::Transaction(_)));
        assert_eq!(transformed, msg);

        // non-transaction messages should not match
        let slot_msg = ChannelMessage::Slot(1, 0, crate::types::block_meta::SlotStatus::Processed);
        assert_eq!(f_a.allows(&slot_msg), false);
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_filter_transaction_strips_fields() {
        let program_a = Pubkey::new_unique();

        let tx = make_tx(&[program_a]);
        let sig = tx.signatures[0].clone();
        let msg = ChannelMessage::Transaction(tx);

        // Default details (all false) — minimal output
        let f_minimal = Filter::FilterTransaction(TransactionFilter {
            signature: Some(sig.clone()),
            filter_by_account: None,
            output_type: TransactionDetails::default(),
            merge_accounts: false,
        });
        let f_wrong_sig = Filter::FilterTransaction(TransactionFilter {
            signature: Some(TypedSignature::new_unique()),
            filter_by_account: None,
            output_type: TransactionDetails::default(),
            merge_accounts: false,
        });
        assert_eq!(f_minimal.allows(&msg), true);
        assert_eq!(f_wrong_sig.allows(&msg), false);

        let transformed = f_minimal.transform(msg.clone());
        match &transformed {
            ChannelMessage::Transaction(tx) => {
                assert_eq!(tx.signatures[0], sig);
                assert_eq!(tx.transaction_meta.error, None);
                assert_eq!(tx.transaction_meta.fee, 5000);
                assert_eq!(tx.slot_identifier.slot, 42);
                assert_eq!(tx.transaction_meta.compute_units_consumed, Some(12345));
                // stripped fields
                assert!(tx.message.is_none());
                assert!(tx.transaction_meta.pre_balances.is_none());
                assert!(tx.transaction_meta.post_balances.is_none());
                assert!(tx.transaction_meta.inner_instructions.is_none());
                assert!(tx.transaction_meta.log_messages.is_none());
                assert!(tx.transaction_meta.rewards.is_none());
                assert!(tx.transaction_meta.return_data.is_none());
            }
            _ => panic!("expected Transaction"),
        }

        // With original_message enabled
        let f_with_msg = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_a),
            output_type: TransactionDetails {
                original_message: true,
                ..Default::default()
            },
            merge_accounts: false,
        });
        let transformed = f_with_msg.transform(msg.clone());
        match &transformed {
            ChannelMessage::Transaction(tx) => {
                assert!(tx.message.is_some());
                assert!(tx.transaction_meta.log_messages.is_none());
            }
            _ => panic!("expected Transaction"),
        }

        // With logs enabled
        let f_with_logs = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_a),
            output_type: TransactionDetails {
                logs: true,
                ..Default::default()
            },
            merge_accounts: false,
        });
        let transformed = f_with_logs.transform(msg.clone());
        match &transformed {
            ChannelMessage::Transaction(tx) => {
                assert!(tx.message.is_none());
                assert!(tx.transaction_meta.log_messages.is_some());
                assert_eq!(
                    tx.transaction_meta.log_messages.as_ref().unwrap()[0],
                    "log1"
                );
            }
            _ => panic!("expected Transaction"),
        }

        // With pre_post_balances enabled
        let f_with_balances = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: None,
            output_type: TransactionDetails {
                pre_post_balances: true,
                ..Default::default()
            },
            merge_accounts: false,
        });
        let transformed = f_with_balances.transform(msg.clone());
        match &transformed {
            ChannelMessage::Transaction(tx) => {
                assert_eq!(tx.transaction_meta.pre_balances, Some(vec![100000]));
                assert_eq!(tx.transaction_meta.post_balances, Some(vec![95000]));
            }
            _ => panic!("expected Transaction"),
        }
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_filter_transaction_full_passthrough() {
        let program_a = Pubkey::new_unique();
        let tx = make_tx(&[program_a]);
        let sig = tx.signatures[0].clone();
        let msg = ChannelMessage::Transaction(tx);

        // All details enabled = full passthrough
        let f = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: None,
            output_type: all_details(),
            merge_accounts: false,
        });
        assert_eq!(f.allows(&msg), true);
        let transformed = f.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::Transaction(_)));

        // By signature, all details
        let f = Filter::FilterTransaction(TransactionFilter {
            signature: Some(sig),
            filter_by_account: None,
            output_type: all_details(),
            merge_accounts: false,
        });
        assert_eq!(f.allows(&msg), true);
        let transformed = f.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::Transaction(_)));
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_filter_transaction_merge_accounts() {
        let program_a = Pubkey::new_unique();
        let other_owner = Pubkey::new_unique();

        let tx = make_tx(&[program_a]);
        let tx_msg = ChannelMessage::Transaction(tx);

        // Account owned by program_a
        let account_match = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: JupnetAccount {
                    lamports: 1,
                    data: vec![],
                    owner: program_a,
                    executable: false,
                    rent_epoch: 0,
                },
                write_version: 0,
            },
            0,
            false,
        );

        // Account owned by a different program
        let account_miss = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: JupnetAccount {
                    lamports: 1,
                    data: vec![],
                    owner: other_owner,
                    executable: false,
                    rent_epoch: 0,
                },
                write_version: 0,
            },
            0,
            false,
        );

        // Without merge_accounts: only transactions match
        let f_no_merge = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_a),
            output_type: all_details(),
            merge_accounts: false,
        });
        assert_eq!(f_no_merge.allows(&tx_msg), true);
        assert_eq!(f_no_merge.allows(&account_match), false);
        assert_eq!(f_no_merge.allows(&account_miss), false);

        // With merge_accounts: transactions + accounts owned by program match
        let f_merge = Filter::FilterTransaction(TransactionFilter {
            signature: None,
            filter_by_account: Some(program_a),
            output_type: all_details(),
            merge_accounts: true,
        });
        assert_eq!(f_merge.allows(&tx_msg), true);
        assert_eq!(f_merge.allows(&account_match), true);
        assert_eq!(f_merge.allows(&account_miss), false);

        // Account messages pass through transform unchanged
        let transformed = f_merge.transform(account_match.clone());
        assert!(matches!(transformed, ChannelMessage::Account(..)));
    }

    // asserts are more readable like this
    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_accounts_filter() {
        let owner = Pubkey::new_unique();

        let owner_2 = Pubkey::new_unique();

        let jupnet_accoung_1 = JupnetAccount {
            lamports: 1,
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            owner,
            executable: false,
            rent_epoch: 100,
        };
        let jupnet_accoung_2 = JupnetAccount {
            lamports: 2,
            data: vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            owner,
            executable: false,
            rent_epoch: 100,
        };
        let jupnet_accoung_3 = JupnetAccount {
            lamports: 3,
            data: vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            owner: owner_2,
            executable: false,
            rent_epoch: 100,
        };
        let jupnet_accoung_4 = JupnetAccount {
            lamports: 3,
            data: vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
            owner: owner_2,
            executable: false,
            rent_epoch: 100,
        };

        let msg_0 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: jupnet_accoung_1.clone(),
                write_version: 0,
            },
            0,
            true,
        );

        let msg_1 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: jupnet_accoung_1.clone(),
                write_version: 0,
            },
            0,
            false,
        );

        let msg_2 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: jupnet_accoung_2.clone(),
                write_version: 0,
            },
            0,
            false,
        );

        let msg_3 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: jupnet_accoung_3.clone(),
                write_version: 0,
            },
            0,
            false,
        );
        let msg_4 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: jupnet_accoung_4.clone(),
                write_version: 0,
            },
            0,
            false,
        );

        let f1 = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filters: None,
        };

        assert_eq!(f1.allows(&msg_0), true);
        assert_eq!(f1.allows(&msg_1), true);
        assert_eq!(f1.allows(&msg_2), true);
        assert_eq!(f1.allows(&msg_3), false);
        assert_eq!(f1.allows(&msg_4), false);

        let f2 = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filters: Some(vec![AccountFilterType::Datasize(9)]),
        };
        assert_eq!(f2.allows(&msg_0), false);
        assert_eq!(f2.allows(&msg_1), false);
        assert_eq!(f2.allows(&msg_2), false);
        assert_eq!(f2.allows(&msg_3), false);
        assert_eq!(f2.allows(&msg_4), false);

        let f3 = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filters: Some(vec![AccountFilterType::Datasize(10)]),
        };
        assert_eq!(f3.allows(&msg_0), true);
        assert_eq!(f3.allows(&msg_1), true);
        assert_eq!(f3.allows(&msg_2), true);
        assert_eq!(f3.allows(&msg_3), false);
        assert_eq!(f3.allows(&msg_4), false);

        let f4: AccountFilter = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filters: Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                offset: 2,
                data: crate::filters::MemcmpFilterData::Bytes(vec![3, 4, 5]),
            })]),
        };
        assert_eq!(f4.allows(&msg_0), true);
        assert_eq!(f4.allows(&msg_1), true);
        assert_eq!(f4.allows(&msg_2), false);
        assert_eq!(f4.allows(&msg_3), false);
        assert_eq!(f4.allows(&msg_4), false);

        let f5: AccountFilter = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filters: Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                offset: 2,
                data: crate::filters::MemcmpFilterData::Bytes(vec![13, 14, 15]),
            })]),
        };
        assert_eq!(f5.allows(&msg_0), false);
        assert_eq!(f5.allows(&msg_1), false);
        assert_eq!(f5.allows(&msg_2), true);
        assert_eq!(f5.allows(&msg_3), false);
        assert_eq!(f5.allows(&msg_4), false);

        let f6: AccountFilter = AccountFilter {
            owner: Some(owner_2),
            accounts: None,
            filters: Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                offset: 2,
                data: crate::filters::MemcmpFilterData::Bytes(vec![13, 14, 15]),
            })]),
        };
        assert_eq!(f6.allows(&msg_0), false);
        assert_eq!(f6.allows(&msg_1), false);
        assert_eq!(f6.allows(&msg_2), false);
        assert_eq!(f6.allows(&msg_3), true);
        assert_eq!(f6.allows(&msg_4), true);

        let f7: AccountFilter = AccountFilter {
            owner: Some(owner_2),
            accounts: None,
            filters: Some(vec![
                AccountFilterType::Datasize(10),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: crate::filters::MemcmpFilterData::Bytes(vec![13, 14, 15]),
                }),
            ]),
        };
        assert_eq!(f7.allows(&msg_0), false);
        assert_eq!(f7.allows(&msg_1), false);
        assert_eq!(f7.allows(&msg_2), false);
        assert_eq!(f7.allows(&msg_3), true);
        assert_eq!(f7.allows(&msg_4), false);

        let f8: AccountFilter = AccountFilter {
            owner: Some(owner_2),
            accounts: None,
            filters: Some(vec![
                AccountFilterType::Datasize(11),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: crate::filters::MemcmpFilterData::Bytes(vec![13, 14, 15]),
                }),
            ]),
        };
        assert_eq!(f8.allows(&msg_0), false);
        assert_eq!(f8.allows(&msg_1), false);
        assert_eq!(f8.allows(&msg_2), false);
        assert_eq!(f8.allows(&msg_3), false);
        assert_eq!(f8.allows(&msg_4), true);
    }
}
