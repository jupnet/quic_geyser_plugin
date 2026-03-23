use jupnet_sdk::{
    message::VersionedMessage, pubkey::Pubkey, signature::TypedSignature,
    transaction::TransactionError, transaction_context::TransactionReturnData,
};
use jupnet_transaction_status::{InnerInstructions, Rewards};
use serde::{Deserialize, Serialize};

use super::slot_identifier::SlotIdentifier;
use crate::compression::{compress_with_dict, decompress_with_dict, CompressionError};
use crate::dictionary::{DictionaryId, DictionaryType, FieldCompressionMeta};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct TransactionMeta {
    pub error: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Option<Vec<u64>>,
    pub post_balances: Option<Vec<u64>>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub rewards: Option<Rewards>,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct BatchedStepMeta {
    pub signatures: Vec<TypedSignature>,
    pub meta: TransactionMeta,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct Transaction {
    pub slot_identifier: SlotIdentifier,
    pub signatures: Vec<TypedSignature>,
    pub message: Option<VersionedMessage>,
    pub is_vote: bool,
    pub transaction_meta: TransactionMeta,
    pub index: u64,
    pub is_batched_transaction: bool,
    pub batched_steps_meta: Option<Vec<BatchedStepMeta>>,
}

impl Transaction {
    /// Returns true if the given program is referenced in this transaction's account keys.
    pub fn references_account(&self, account: &Pubkey) -> bool {
        match &self.message {
            Some(msg) => msg.static_account_keys().any(|k| k == account),
            None => false,
        }
    }
}

/// Transaction metadata with dictionary-compressed log_messages field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct CompressedTransactionMeta {
    pub error: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Option<Vec<u64>>,
    pub post_balances: Option<Vec<u64>>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    /// Compressed log messages (bincode-serialized Vec<String> then LZ4 with dictionary)
    pub log_messages_compressed: Option<Vec<u8>>,
    /// Compression metadata for log_messages
    pub log_messages_compression: Option<FieldCompressionMeta>,
    pub rewards: Option<Rewards>,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

/// A transaction with dictionary-compressed message and log_messages fields
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct CompressedTransaction {
    pub slot_identifier: SlotIdentifier,
    pub signatures: Vec<TypedSignature>,
    /// Compressed VersionedMessage (bincode-serialized then LZ4 with dictionary)
    pub message_compressed: Option<Vec<u8>>,
    /// Compression metadata for the message field
    pub message_compression: Option<FieldCompressionMeta>,
    pub is_vote: bool,
    pub transaction_meta: CompressedTransactionMeta,
    pub index: u64,
    pub is_batched_transaction: bool,
    pub batched_steps_meta: Option<Vec<BatchedStepMeta>>,
}

impl CompressedTransaction {
    /// Create a compressed transaction from a regular transaction
    ///
    /// # Arguments
    /// * `tx` - The transaction to compress
    /// * `log_dict` - Optional dictionary for log_messages compression (dict bytes, dict id)
    /// * `msg_dict` - Optional dictionary for message compression (dict bytes, dict id)
    pub fn from_transaction(
        tx: &Transaction,
        log_dict: Option<(&[u8], DictionaryId)>,
        msg_dict: Option<(&[u8], DictionaryId)>,
    ) -> Result<Self, CompressionError> {
        // Compress log_messages if present
        let (log_messages_compressed, log_messages_compression) =
            if let Some(ref logs) = tx.transaction_meta.log_messages {
                let serialized = bincode::serialize(logs)
                    .map_err(|e| CompressionError::SerializationError(e.to_string()))?;
                let original_size = serialized.len() as u32;

                match log_dict {
                    Some((dict, dict_id)) => {
                        let compressed = compress_with_dict(&serialized, Some(dict))?;
                        (
                            Some(compressed),
                            Some(FieldCompressionMeta::with_dictionary(dict_id, original_size)),
                        )
                    }
                    None => {
                        // Store uncompressed serialized data when no dictionary
                        (
                            Some(serialized),
                            Some(FieldCompressionMeta::uncompressed(original_size)),
                        )
                    }
                }
            } else {
                (None, None)
            };

        // Compress message if present
        let (message_compressed, message_compression) = if let Some(ref msg) = tx.message {
            let serialized = bincode::serialize(msg)
                .map_err(|e| CompressionError::SerializationError(e.to_string()))?;
            let original_size = serialized.len() as u32;

            match msg_dict {
                Some((dict, dict_id)) => {
                    let compressed = compress_with_dict(&serialized, Some(dict))?;
                    (
                        Some(compressed),
                        Some(FieldCompressionMeta::with_dictionary(dict_id, original_size)),
                    )
                }
                None => {
                    // Store uncompressed serialized data when no dictionary
                    (
                        Some(serialized),
                        Some(FieldCompressionMeta::uncompressed(original_size)),
                    )
                }
            }
        } else {
            (None, None)
        };

        Ok(Self {
            slot_identifier: tx.slot_identifier,
            signatures: tx.signatures.clone(),
            message_compressed,
            message_compression,
            is_vote: tx.is_vote,
            transaction_meta: CompressedTransactionMeta {
                error: tx.transaction_meta.error.clone(),
                fee: tx.transaction_meta.fee,
                pre_balances: tx.transaction_meta.pre_balances.clone(),
                post_balances: tx.transaction_meta.post_balances.clone(),
                inner_instructions: tx.transaction_meta.inner_instructions.clone(),
                log_messages_compressed,
                log_messages_compression,
                rewards: tx.transaction_meta.rewards.clone(),
                return_data: tx.transaction_meta.return_data.clone(),
                compute_units_consumed: tx.transaction_meta.compute_units_consumed,
            },
            index: tx.index,
            is_batched_transaction: tx.is_batched_transaction,
            batched_steps_meta: tx.batched_steps_meta.clone(),
        })
    }

    /// Decompress into a regular Transaction using the provided dictionaries
    ///
    /// # Arguments
    /// * `log_dict` - Dictionary for log_messages decompression (if compressed with dictionary)
    /// * `msg_dict` - Dictionary for message decompression (if compressed with dictionary)
    pub fn decompress(
        &self,
        log_dict: Option<&[u8]>,
        msg_dict: Option<&[u8]>,
    ) -> Result<Transaction, CompressionError> {
        // Decompress log_messages if present
        let log_messages = if let Some(ref compressed) = self.transaction_meta.log_messages_compressed
        {
            let uses_dict = self
                .transaction_meta
                .log_messages_compression
                .as_ref()
                .map(|c| c.uses_dictionary())
                .unwrap_or(false);

            let decompressed = if uses_dict {
                decompress_with_dict(compressed, log_dict)?
            } else {
                // Data was stored uncompressed (just serialized)
                compressed.clone()
            };

            let logs: Vec<String> = bincode::deserialize(&decompressed)
                .map_err(|e| CompressionError::SerializationError(e.to_string()))?;
            Some(logs)
        } else {
            None
        };

        // Decompress message if present
        let message = if let Some(ref compressed) = self.message_compressed {
            let uses_dict = self
                .message_compression
                .as_ref()
                .map(|c| c.uses_dictionary())
                .unwrap_or(false);

            let decompressed = if uses_dict {
                decompress_with_dict(compressed, msg_dict)?
            } else {
                // Data was stored uncompressed (just serialized)
                compressed.clone()
            };

            let msg: VersionedMessage = bincode::deserialize(&decompressed)
                .map_err(|e| CompressionError::SerializationError(e.to_string()))?;
            Some(msg)
        } else {
            None
        };

        Ok(Transaction {
            slot_identifier: self.slot_identifier,
            signatures: self.signatures.clone(),
            message,
            is_vote: self.is_vote,
            transaction_meta: TransactionMeta {
                error: self.transaction_meta.error.clone(),
                fee: self.transaction_meta.fee,
                pre_balances: self.transaction_meta.pre_balances.clone(),
                post_balances: self.transaction_meta.post_balances.clone(),
                inner_instructions: self.transaction_meta.inner_instructions.clone(),
                log_messages,
                rewards: self.transaction_meta.rewards.clone(),
                return_data: self.transaction_meta.return_data.clone(),
                compute_units_consumed: self.transaction_meta.compute_units_consumed,
            },
            index: self.index,
            is_batched_transaction: self.is_batched_transaction,
            batched_steps_meta: self.batched_steps_meta.clone(),
        })
    }

    /// Get the dictionaries required to decompress this transaction
    ///
    /// Returns a list of (DictionaryType, DictionaryId) pairs for dictionaries
    /// that are needed but not yet available.
    pub fn required_dictionaries(&self) -> Vec<(DictionaryType, DictionaryId)> {
        let mut required = Vec::new();

        if let Some(ref meta) = self.transaction_meta.log_messages_compression {
            if meta.uses_dictionary() {
                required.push((DictionaryType::LogMessages, meta.dictionary_id));
            }
        }

        if let Some(ref meta) = self.message_compression {
            if meta.uses_dictionary() {
                required.push((DictionaryType::TransactionMessage, meta.dictionary_id));
            }
        }

        required
    }
}
