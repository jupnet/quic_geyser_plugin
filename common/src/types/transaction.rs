use jupnet_sdk::{
    message::VersionedMessage, pubkey::Pubkey, signature::TypedSignature,
    transaction::TransactionError, transaction_context::TransactionReturnData,
};
use jupnet_transaction_status::{InnerInstructions, Rewards};
use serde::{Deserialize, Serialize};

use super::slot_identifier::SlotIdentifier;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct TransactionMeta {
    pub error: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub rewards: Option<Rewards>,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct Transaction {
    pub slot_identifier: SlotIdentifier,
    pub signatures: Vec<TypedSignature>,
    pub message: VersionedMessage,
    pub is_vote: bool,
    pub transaction_meta: TransactionMeta,
    pub index: u64,
    pub batched_steps_meta: Option<Vec<TransactionMeta>>,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct TransactionStatus {
    pub slot_identifier: SlotIdentifier,
    pub signatures: Vec<TypedSignature>,
    pub error: Option<TransactionError>,
    pub fee: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct TransactionNotify {
    pub slot_identifier: SlotIdentifier,
    pub signature: TypedSignature,
    pub error: Option<TransactionError>,
    pub compute_units_consumed: Option<u64>,
    pub message: Option<VersionedMessage>,
}

impl Transaction {
    /// Returns true if the given program is referenced in this transaction's account keys.
    pub fn references_program(&self, program_id: &Pubkey) -> bool {
        self.message.static_account_keys().any(|k| k == program_id)
    }

    /// Create a lightweight TransactionNotify from this transaction.
    pub fn to_notify(&self, include_message: bool) -> TransactionNotify {
        TransactionNotify {
            slot_identifier: self.slot_identifier,
            signature: self.signatures[0].clone(),
            error: self.transaction_meta.error.clone(),
            compute_units_consumed: self.transaction_meta.compute_units_consumed,
            message: if include_message {
                Some(self.message.clone())
            } else {
                None
            },
        }
    }
}