use std::collections::HashSet;

use jupnet_sdk::{pubkey::Pubkey, signature::TypedSignature};
use serde::{Deserialize, Serialize};

use crate::channel_message::ChannelMessage;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Filter {
    Account(AccountFilter),
    // will excude vote accounts and stake accounts by default
    AccountsAll,
    Slot,
    BlockMeta,
    Transaction(TypedSignature),
    TransactionsAll,
    BlockAll,
    DeletedAccounts,
    AccountsExcluding(AccountFilter),
    TransactionStatus(TypedSignature),
    TransactionStatusAll,
    TransactionAllProgram(Pubkey),
    TransactionStatusByAccount(Pubkey),
    /// Lightweight transaction notification filtered by program.
    /// Sends signature, status, CU consumed, and optionally the message.
    /// Bool flag controls whether the message is included.
    TransactionNotifyByProgram(Pubkey, bool),
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
            Filter::Transaction(signature) => {
                match message {
                    ChannelMessage::Transaction(transaction) => {
                        // just check the first signature
                        transaction.signatures[0] == *signature
                    }
                    _ => false,
                }
            }
            Filter::TransactionsAll => matches!(message, ChannelMessage::Transaction(_)),
            Filter::BlockAll => matches!(message, ChannelMessage::Block(_)),
            Filter::DeletedAccounts => match message {
                ChannelMessage::Account(account, _, _) => account.account.lamports == 0,
                _ => false,
            },
            Filter::AccountsExcluding(account) => !account.allows(message),
            Filter::TransactionStatus(signature) => match message {
                ChannelMessage::Transaction(transaction) => {
                    transaction.signatures[0] == *signature
                }
                _ => false,
            },
            Filter::TransactionStatusAll => {
                matches!(message, ChannelMessage::Transaction(_))
            }
            Filter::TransactionAllProgram(program_id) => match message {
                ChannelMessage::Transaction(transaction) => {
                    transaction.references_program(program_id)
                }
                _ => false,
            },
            Filter::TransactionStatusByAccount(program_id) => match message {
                ChannelMessage::Transaction(transaction) => {
                    transaction.references_program(program_id)
                }
                _ => false,
            },
            Filter::TransactionNotifyByProgram(program_id, _) => match message {
                ChannelMessage::Transaction(transaction) => {
                    transaction.references_program(program_id)
                }
                _ => false,
            },
        }
    }

    /// Transform the channel message based on the filter type.
    /// Some filters send a lighter representation instead of the full message.
    /// Should only be called after `allows()` returns true.
    pub fn transform(&self, message: ChannelMessage) -> ChannelMessage {
        match self {
            Filter::TransactionStatus(_) | Filter::TransactionStatusAll
            | Filter::TransactionStatusByAccount(_) => {
                if let ChannelMessage::Transaction(tx) = &message {
                    ChannelMessage::TransactionStatus(
                        crate::types::transaction::TransactionStatus {
                            slot_identifier: tx.slot_identifier,
                            signatures: tx.signatures.clone(),
                            error: tx.transaction_meta.error.clone(),
                            fee: tx.transaction_meta.fee,
                        },
                    )
                } else {
                    message
                }
            }
            Filter::TransactionNotifyByProgram(_, include_message) => {
                if let ChannelMessage::Transaction(tx) = &message {
                    ChannelMessage::TransactionNotify(Box::new(
                        tx.to_notify(*include_message),
                    ))
                } else {
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
        filters::{AccountFilter, AccountFilterType, Filter, MemcmpFilter},
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
            message: jupnet_sdk::message::VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: program_ids.len() as u8,
                },
                account_keys,
                recent_blockhash: Hash::new_unique(),
                instructions,
            }),
            is_vote: false,
            transaction_meta: TransactionMeta {
                error: None,
                fee: 5000,
                pre_balances: vec![100000],
                post_balances: vec![95000],
                inner_instructions: None,
                log_messages: None,
                rewards: None,
                return_data: None,
                compute_units_consumed: Some(12345),
            },
            index: 0,
            batched_steps_meta: None,
        })
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_transaction_all_program_filter() {
        let program_a = Pubkey::new_unique();
        let program_b = Pubkey::new_unique();
        let program_c = Pubkey::new_unique();

        let tx = make_tx(&[program_a, program_b]);
        let msg = ChannelMessage::Transaction(tx);

        let f_a = Filter::TransactionAllProgram(program_a);
        let f_b = Filter::TransactionAllProgram(program_b);
        let f_c = Filter::TransactionAllProgram(program_c);

        assert_eq!(f_a.allows(&msg), true);
        assert_eq!(f_b.allows(&msg), true);
        assert_eq!(f_c.allows(&msg), false);

        // transform should pass through the full transaction unchanged
        let transformed = f_a.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::Transaction(_)));
        assert_eq!(transformed, msg);

        // non-transaction messages should not match
        let slot_msg = ChannelMessage::Slot(1, 0, crate::types::block_meta::SlotStatus::Processed);
        assert_eq!(f_a.allows(&slot_msg), false);
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_transaction_status_filters() {
        let program_a = Pubkey::new_unique();
        let program_b = Pubkey::new_unique();

        let tx = make_tx(&[program_a]);
        let sig = tx.signatures[0].clone();
        let msg = ChannelMessage::Transaction(tx);

        // TransactionStatus by signature
        let f_sig = Filter::TransactionStatus(sig.clone());
        let f_wrong_sig = Filter::TransactionStatus(TypedSignature::new_unique());
        assert_eq!(f_sig.allows(&msg), true);
        assert_eq!(f_wrong_sig.allows(&msg), false);

        let transformed = f_sig.transform(msg.clone());
        match &transformed {
            ChannelMessage::TransactionStatus(status) => {
                assert_eq!(status.signatures[0], sig);
                assert_eq!(status.error, None);
                assert_eq!(status.fee, 5000);
                assert_eq!(status.slot_identifier.slot, 42);
            }
            _ => panic!("expected TransactionStatus"),
        }

        // TransactionStatusAll
        let f_all = Filter::TransactionStatusAll;
        assert_eq!(f_all.allows(&msg), true);
        let transformed = f_all.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::TransactionStatus(_)));

        // TransactionStatusByAccount
        let f_by_acc = Filter::TransactionStatusByAccount(program_a);
        let f_by_acc_miss = Filter::TransactionStatusByAccount(program_b);
        assert_eq!(f_by_acc.allows(&msg), true);
        assert_eq!(f_by_acc_miss.allows(&msg), false);

        let transformed = f_by_acc.transform(msg.clone());
        match &transformed {
            ChannelMessage::TransactionStatus(status) => {
                assert_eq!(status.slot_identifier.slot, 42);
                assert_eq!(status.fee, 5000);
            }
            _ => panic!("expected TransactionStatus"),
        }
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_transaction_notify_by_program() {
        let program_a = Pubkey::new_unique();
        let program_b = Pubkey::new_unique();

        let tx = make_tx(&[program_a]);
        let sig = tx.signatures[0].clone();
        let msg = ChannelMessage::Transaction(tx);

        // without message
        let f_no_msg = Filter::TransactionNotifyByProgram(program_a, false);
        let f_with_msg = Filter::TransactionNotifyByProgram(program_a, true);
        let f_miss = Filter::TransactionNotifyByProgram(program_b, false);

        assert_eq!(f_no_msg.allows(&msg), true);
        assert_eq!(f_with_msg.allows(&msg), true);
        assert_eq!(f_miss.allows(&msg), false);

        // transform without message
        let transformed = f_no_msg.transform(msg.clone());
        match &transformed {
            ChannelMessage::TransactionNotify(notify) => {
                assert_eq!(notify.signature, sig);
                assert_eq!(notify.error, None);
                assert_eq!(notify.compute_units_consumed, Some(12345));
                assert_eq!(notify.slot_identifier.slot, 42);
                assert!(notify.message.is_none());
            }
            _ => panic!("expected TransactionNotify"),
        }

        // transform with message
        let transformed = f_with_msg.transform(msg.clone());
        match &transformed {
            ChannelMessage::TransactionNotify(notify) => {
                assert_eq!(notify.signature, sig);
                assert!(notify.message.is_some());
            }
            _ => panic!("expected TransactionNotify"),
        }
    }

    #[allow(clippy::bool_assert_comparison)]
    #[tokio::test]
    async fn test_passthrough_filters_do_not_transform() {
        let program_a = Pubkey::new_unique();
        let tx = make_tx(&[program_a]);
        let sig = tx.signatures[0].clone();
        let msg = ChannelMessage::Transaction(tx);

        // TransactionsAll should pass through full transaction
        let f = Filter::TransactionsAll;
        assert_eq!(f.allows(&msg), true);
        let transformed = f.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::Transaction(_)));

        // Transaction(sig) should pass through full transaction
        let f = Filter::Transaction(sig);
        assert_eq!(f.allows(&msg), true);
        let transformed = f.transform(msg.clone());
        assert!(matches!(transformed, ChannelMessage::Transaction(_)));
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
