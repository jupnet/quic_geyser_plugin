use jupnet_sdk::{
    message::VersionedMessage, signature::TypedSignature, transaction::TransactionError,
    transaction_context::TransactionReturnData,
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
    pub transasction_meta: TransactionMeta,
    pub index: u64,
    pub batched_steps_meta: Option<Vec<TransactionMeta>>,
}
