use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use quic_geyser_common::dictionary::{CompressionDictionary, DictionaryId, DictionaryType};
use quic_geyser_common::types::transaction::{CompressedTransaction, Transaction};

/// Timeout for pending decompressions waiting for dictionaries
const PENDING_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of pending decompressions to queue
const MAX_PENDING_QUEUE: usize = 1000;

/// A compressed transaction waiting for dictionaries
struct PendingDecompression {
    compressed: CompressedTransaction,
    needed_log_dict: Option<DictionaryId>,
    needed_msg_dict: Option<DictionaryId>,
    created_at: Instant,
}

/// Stores dictionaries received from the server and manages decompression
pub struct ClientDictionaryStore {
    /// Current dictionaries by type
    current: RwLock<HashMap<DictionaryType, Arc<CompressionDictionary>>>,
    /// Historical dictionaries indexed by (type, id)
    historical: RwLock<HashMap<(DictionaryType, DictionaryId), Arc<CompressionDictionary>>>,
    /// Pending decompressions waiting for dictionaries
    pending: RwLock<Vec<PendingDecompression>>,
}

impl ClientDictionaryStore {
    pub fn new() -> Self {
        Self {
            current: RwLock::new(HashMap::new()),
            historical: RwLock::new(HashMap::new()),
            pending: RwLock::new(Vec::new()),
        }
    }

    /// Store a new dictionary, returns true if this is a new version
    pub async fn store(&self, dict: CompressionDictionary) -> bool {
        let dict = Arc::new(dict);
        let dict_type = dict.dict_type;
        let dict_id = dict.id;

        // Check if this is a new dictionary
        let is_new = {
            let current = self.current.read().await;
            current
                .get(&dict_type)
                .map(|d| d.id != dict_id)
                .unwrap_or(true)
        };

        // Update current
        {
            let mut current = self.current.write().await;
            current.insert(dict_type, dict.clone());
        }

        // Add to historical
        let dict_size = dict.data.len();
        {
            let mut historical = self.historical.write().await;
            historical.insert((dict_type, dict_id), dict);

            // Prune old historical entries (keep last 10 per type)
            Self::prune_historical(&mut historical, dict_type);
        }

        if is_new {
            log::debug!(
                "Stored new {:?} dictionary id={} size={}",
                dict_type,
                dict_id,
                dict_size
            );
        }

        is_new
    }

    fn prune_historical(
        historical: &mut HashMap<(DictionaryType, DictionaryId), Arc<CompressionDictionary>>,
        dict_type: DictionaryType,
    ) {
        let mut type_entries: Vec<_> = historical
            .iter()
            .filter(|((t, _), _)| *t == dict_type)
            .map(|((_, id), _)| *id)
            .collect();

        if type_entries.len() > 10 {
            type_entries.sort();
            let to_remove = type_entries.len() - 10;
            // Remove oldest (lowest IDs)
            for id in type_entries.into_iter().take(to_remove) {
                historical.remove(&(dict_type, id));
            }
        }
    }

    /// Get dictionary by type and ID
    pub async fn get(
        &self,
        dict_type: DictionaryType,
        id: DictionaryId,
    ) -> Option<Arc<CompressionDictionary>> {
        // Check current first
        if let Some(current) = self.current.read().await.get(&dict_type) {
            if current.id == id {
                return Some(current.clone());
            }
        }

        // Check historical
        self.historical.read().await.get(&(dict_type, id)).cloned()
    }

    /// Get the current dictionary for a type
    pub async fn get_current(
        &self,
        dict_type: DictionaryType,
    ) -> Option<Arc<CompressionDictionary>> {
        self.current.read().await.get(&dict_type).cloned()
    }

    /// Try to decompress a transaction, may return None if dictionaries are missing
    ///
    /// If dictionaries are missing, the transaction is queued for later decompression.
    /// Returns Ok(Some(tx)) if decompression succeeded, Ok(None) if queued,
    /// or Err with the list of missing dictionaries.
    pub async fn decompress_or_queue(
        &self,
        compressed: CompressedTransaction,
    ) -> Result<Option<Transaction>, Vec<(DictionaryType, DictionaryId)>> {
        let required = compressed.required_dictionaries();

        // Try to get all required dictionaries
        let mut log_dict: Option<Arc<CompressionDictionary>> = None;
        let mut msg_dict: Option<Arc<CompressionDictionary>> = None;
        let mut missing = Vec::new();

        for (dict_type, dict_id) in &required {
            if let Some(dict) = self.get(*dict_type, *dict_id).await {
                match dict_type {
                    DictionaryType::LogMessages => log_dict = Some(dict),
                    DictionaryType::TransactionMessage => msg_dict = Some(dict),
                }
            } else {
                missing.push((*dict_type, *dict_id));
            }
        }

        if !missing.is_empty() {
            // Queue for later
            let mut pending = self.pending.write().await;
            if pending.len() < MAX_PENDING_QUEUE {
                pending.push(PendingDecompression {
                    compressed,
                    needed_log_dict: missing
                        .iter()
                        .find(|(t, _)| *t == DictionaryType::LogMessages)
                        .map(|(_, id)| *id),
                    needed_msg_dict: missing
                        .iter()
                        .find(|(t, _)| *t == DictionaryType::TransactionMessage)
                        .map(|(_, id)| *id),
                    created_at: Instant::now(),
                });
                log::debug!("Queued transaction for decompression, waiting for {:?}", missing);
            } else {
                log::warn!(
                    "Pending decompression queue full, dropping transaction"
                );
            }
            return Err(missing);
        }

        // Decompress
        match compressed.decompress(
            log_dict.as_ref().map(|d| d.data.as_slice()),
            msg_dict.as_ref().map(|d| d.data.as_slice()),
        ) {
            Ok(tx) => Ok(Some(tx)),
            Err(e) => {
                log::warn!("Decompression failed: {:?}", e);
                Ok(None)
            }
        }
    }

    /// Process pending decompressions after receiving new dictionary
    /// Returns successfully decompressed transactions
    pub async fn process_pending(&self) -> Vec<Transaction> {
        let now = Instant::now();

        // Take all entries from pending
        let entries: Vec<PendingDecompression> = {
            let mut pending = self.pending.write().await;
            pending.drain(..).collect()
        };

        let mut completed = Vec::new();
        let mut still_pending = Vec::new();

        for entry in entries {
            // Skip expired entries
            if now.duration_since(entry.created_at) >= PENDING_TIMEOUT {
                continue;
            }

            let log_dict = if let Some(id) = entry.needed_log_dict {
                self.get(DictionaryType::LogMessages, id).await
            } else {
                None
            };

            let msg_dict = if let Some(id) = entry.needed_msg_dict {
                self.get(DictionaryType::TransactionMessage, id).await
            } else {
                None
            };

            let log_missing = entry.needed_log_dict.is_some() && log_dict.is_none();
            let msg_missing = entry.needed_msg_dict.is_some() && msg_dict.is_none();

            if log_missing || msg_missing {
                // Still missing dictionaries, re-queue
                still_pending.push(entry);
                continue;
            }

            // Try to decompress
            match entry.compressed.decompress(
                log_dict.as_ref().map(|d| d.data.as_slice()),
                msg_dict.as_ref().map(|d| d.data.as_slice()),
            ) {
                Ok(tx) => {
                    completed.push(tx);
                }
                Err(e) => {
                    log::warn!("Failed to decompress pending transaction: {:?}", e);
                }
            }
        }

        // Re-add entries that are still pending
        if !still_pending.is_empty() {
            let mut pending = self.pending.write().await;
            pending.extend(still_pending);
        }

        completed
    }

    /// Get the number of pending decompressions
    pub async fn pending_count(&self) -> usize {
        self.pending.read().await.len()
    }

    /// Cleanup expired pending entries
    pub async fn cleanup_expired(&self) {
        let mut pending = self.pending.write().await;
        let now = Instant::now();
        let before = pending.len();
        pending.retain(|p| now.duration_since(p.created_at) < PENDING_TIMEOUT);
        let after = pending.len();
        if before != after {
            log::debug!(
                "Cleaned up {} expired pending decompressions",
                before - after
            );
        }
    }
}

impl Default for ClientDictionaryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quic_geyser_common::types::slot_identifier::SlotIdentifier;
    use quic_geyser_common::types::transaction::CompressedTransactionMeta;

    fn create_test_dict(id: DictionaryId, dict_type: DictionaryType) -> CompressionDictionary {
        CompressionDictionary::new(id, dict_type, vec![1, 2, 3, 4])
    }

    #[tokio::test]
    async fn test_store_creation() {
        let store = ClientDictionaryStore::new();
        assert!(store.get_current(DictionaryType::LogMessages).await.is_none());
        assert!(store
            .get_current(DictionaryType::TransactionMessage)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let store = ClientDictionaryStore::new();

        let dict = create_test_dict(1, DictionaryType::LogMessages);
        let is_new = store.store(dict).await;
        assert!(is_new);

        let retrieved = store.get(DictionaryType::LogMessages, 1).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, 1);
    }

    #[tokio::test]
    async fn test_historical_retrieval() {
        let store = ClientDictionaryStore::new();

        // Store first dictionary
        let dict1 = create_test_dict(1, DictionaryType::LogMessages);
        store.store(dict1).await;

        // Store second dictionary (becomes current)
        let dict2 = create_test_dict(2, DictionaryType::LogMessages);
        store.store(dict2).await;

        // Current should be dict2
        let current = store.get_current(DictionaryType::LogMessages).await;
        assert_eq!(current.unwrap().id, 2);

        // Should still be able to get dict1
        let historical = store.get(DictionaryType::LogMessages, 1).await;
        assert!(historical.is_some());
        assert_eq!(historical.unwrap().id, 1);
    }

    #[tokio::test]
    async fn test_decompress_without_dictionary() {
        let store = ClientDictionaryStore::new();

        // Create a compressed transaction that doesn't use dictionaries
        let compressed = CompressedTransaction {
            slot_identifier: SlotIdentifier { slot: 1 },
            signatures: vec![],
            message_compressed: None,
            message_compression: None,
            is_vote: false,
            transaction_meta: CompressedTransactionMeta {
                error: None,
                fee: 5000,
                pre_balances: None,
                post_balances: None,
                inner_instructions: None,
                log_messages_compressed: None,
                log_messages_compression: None,
                rewards: None,
                return_data: None,
                compute_units_consumed: Some(100),
            },
            index: 0,
            is_batched_transaction: false,
            batched_steps_meta: None,
        };

        let result = store.decompress_or_queue(compressed).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }
}
