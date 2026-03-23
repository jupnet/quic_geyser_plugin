use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use quic_geyser_common::compression::train_dictionary;
use quic_geyser_common::config::DictionaryCompressionConfig;
use quic_geyser_common::dictionary::{CompressionDictionary, DictionaryType};
use quic_geyser_common::types::transaction::Transaction;

/// Accumulates training samples and produces dictionaries
pub struct DictionaryTrainer {
    /// Samples for log_messages training (serialized Vec<String>)
    log_samples: RwLock<VecDeque<Vec<u8>>>,
    /// Samples for transaction message training (serialized VersionedMessage)
    msg_samples: RwLock<VecDeque<Vec<u8>>>,
    /// Configuration
    config: DictionaryCompressionConfig,
    /// Next dictionary ID to assign
    next_dict_id: AtomicU64,
}

impl DictionaryTrainer {
    pub fn new(config: DictionaryCompressionConfig) -> Self {
        Self {
            log_samples: RwLock::new(VecDeque::with_capacity(config.training_window_size)),
            msg_samples: RwLock::new(VecDeque::with_capacity(config.training_window_size)),
            config,
            next_dict_id: AtomicU64::new(1), // Start at 1, 0 is reserved for "no dictionary"
        }
    }

    /// Add a transaction's fields as training samples
    pub async fn add_sample(&self, tx: &Transaction) {
        // Extract log messages if present
        if let Some(ref logs) = tx.transaction_meta.log_messages {
            if let Ok(serialized) = bincode::serialize(logs) {
                if !serialized.is_empty() {
                    let mut samples = self.log_samples.write().await;
                    samples.push_back(serialized);
                    while samples.len() > self.config.training_window_size {
                        samples.pop_front();
                    }
                }
            }
        }

        // Extract message if present
        if let Some(ref msg) = tx.message {
            if let Ok(serialized) = bincode::serialize(msg) {
                if !serialized.is_empty() {
                    let mut samples = self.msg_samples.write().await;
                    samples.push_back(serialized);
                    while samples.len() > self.config.training_window_size {
                        samples.pop_front();
                    }
                }
            }
        }
    }

    /// Train and return new dictionaries if enough samples exist
    /// Returns (log_dict, msg_dict) - either may be None
    pub async fn train_dictionaries(
        &self,
    ) -> (Option<CompressionDictionary>, Option<CompressionDictionary>) {
        let log_dict = self.train_single_dictionary(DictionaryType::LogMessages).await;
        let msg_dict = self
            .train_single_dictionary(DictionaryType::TransactionMessage)
            .await;
        (log_dict, msg_dict)
    }

    async fn train_single_dictionary(
        &self,
        dict_type: DictionaryType,
    ) -> Option<CompressionDictionary> {
        let samples = match dict_type {
            DictionaryType::LogMessages => self.log_samples.read().await,
            DictionaryType::TransactionMessage => self.msg_samples.read().await,
        };

        if samples.len() < self.config.min_samples_for_training {
            log::debug!(
                "Not enough samples for {:?} dictionary: {} < {}",
                dict_type,
                samples.len(),
                self.config.min_samples_for_training
            );
            return None;
        }

        // Convert to slice references for training
        let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();

        // Train dictionary using zstd
        match train_dictionary(&sample_refs, self.config.max_dictionary_size) {
            Ok(dict_bytes) => {
                let id = self.next_dict_id.fetch_add(1, Ordering::SeqCst);
                log::info!(
                    "Trained {:?} dictionary id={} with {} samples, size={} bytes",
                    dict_type,
                    id,
                    samples.len(),
                    dict_bytes.len()
                );
                Some(CompressionDictionary::new(id, dict_type, dict_bytes))
            }
            Err(e) => {
                log::warn!("Failed to train {:?} dictionary: {:?}", dict_type, e);
                None
            }
        }
    }

    /// Get the current sample counts
    pub async fn sample_counts(&self) -> (usize, usize) {
        let log_count = self.log_samples.read().await.len();
        let msg_count = self.msg_samples.read().await.len();
        (log_count, msg_count)
    }

    /// Clear all samples (useful for testing)
    pub async fn clear_samples(&self) {
        self.log_samples.write().await.clear();
        self.msg_samples.write().await.clear();
    }
}

/// Thread-safe wrapper for the trainer
pub type SharedDictionaryTrainer = Arc<DictionaryTrainer>;

#[cfg(test)]
mod tests {
    use super::*;
    use quic_geyser_common::types::slot_identifier::SlotIdentifier;
    use quic_geyser_common::types::transaction::TransactionMeta;

    fn create_test_transaction(logs: Option<Vec<String>>) -> Transaction {
        Transaction {
            slot_identifier: SlotIdentifier { slot: 1 },
            signatures: vec![],
            message: None,
            is_vote: false,
            transaction_meta: TransactionMeta {
                error: None,
                fee: 5000,
                pre_balances: None,
                post_balances: None,
                inner_instructions: None,
                log_messages: logs,
                rewards: None,
                return_data: None,
                compute_units_consumed: Some(100),
            },
            index: 0,
            is_batched_transaction: false,
            batched_steps_meta: None,
        }
    }

    #[tokio::test]
    async fn test_trainer_creation() {
        let config = DictionaryCompressionConfig {
            enabled: true,
            update_interval_secs: 30,
            min_samples_for_training: 10,
            max_dictionary_size: 4096,
            training_window_size: 100,
        };

        let trainer = DictionaryTrainer::new(config);
        let (log_count, msg_count) = trainer.sample_counts().await;
        assert_eq!(log_count, 0);
        assert_eq!(msg_count, 0);
    }

    #[tokio::test]
    async fn test_sample_collection() {
        let config = DictionaryCompressionConfig {
            enabled: true,
            update_interval_secs: 30,
            min_samples_for_training: 10,
            max_dictionary_size: 4096,
            training_window_size: 100,
        };

        let trainer = DictionaryTrainer::new(config);

        // Add samples
        for i in 0..20 {
            let tx = create_test_transaction(Some(vec![
                format!("Program log: Transfer {} lamports", i * 100),
                format!("Program log: Success"),
            ]));
            trainer.add_sample(&tx).await;
        }

        let (log_count, msg_count) = trainer.sample_counts().await;
        assert_eq!(log_count, 20);
        assert_eq!(msg_count, 0); // No messages in our test transactions
    }

    #[tokio::test]
    async fn test_training_window_limit() {
        let config = DictionaryCompressionConfig {
            enabled: true,
            update_interval_secs: 30,
            min_samples_for_training: 10,
            max_dictionary_size: 4096,
            training_window_size: 10, // Small window for testing
        };

        let trainer = DictionaryTrainer::new(config);

        // Add more samples than window size
        for i in 0..20 {
            let tx = create_test_transaction(Some(vec![format!("Log {}", i)]));
            trainer.add_sample(&tx).await;
        }

        let (log_count, _) = trainer.sample_counts().await;
        assert_eq!(log_count, 10); // Should be limited to window size
    }

    #[tokio::test]
    async fn test_dictionary_training() {
        let config = DictionaryCompressionConfig {
            enabled: true,
            update_interval_secs: 30,
            min_samples_for_training: 50, // Lower threshold for testing
            max_dictionary_size: 4096,
            training_window_size: 200,
        };

        let trainer = DictionaryTrainer::new(config);

        // Add enough samples for training
        for i in 0..100 {
            let tx = create_test_transaction(Some(vec![
                format!("Program log: Transfer {} lamports from account A to account B", i * 100),
                format!("Program log: Invoke instruction {} on program XYZ", i),
                format!("Program log: Success with {} compute units", i * 10),
            ]));
            trainer.add_sample(&tx).await;
        }

        let (log_dict, msg_dict) = trainer.train_dictionaries().await;

        // Should have log dictionary
        assert!(log_dict.is_some());
        let dict = log_dict.unwrap();
        assert!(dict.id > 0);
        assert_eq!(dict.dict_type, DictionaryType::LogMessages);
        assert!(!dict.data.is_empty());

        // Should not have message dictionary (no messages in samples)
        assert!(msg_dict.is_none());
    }
}
