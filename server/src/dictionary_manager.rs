use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

use quic_geyser_common::config::DictionaryCompressionConfig;
use quic_geyser_common::dictionary::{CompressionDictionary, DictionaryId, DictionaryType};
use quic_geyser_common::types::transaction::Transaction;

use crate::dictionary_trainer::DictionaryTrainer;

/// Maximum number of historical dictionaries to keep per type
const MAX_HISTORICAL_PER_TYPE: usize = 10;

/// Maximum age of historical dictionaries in seconds
const MAX_HISTORICAL_AGE_SECS: u64 = 300;

/// Manages dictionaries and their distribution to clients
pub struct DictionaryManager {
    /// Current active dictionaries by type
    current_dictionaries: RwLock<HashMap<DictionaryType, Arc<CompressionDictionary>>>,
    /// Historical dictionaries for clients that may have old references
    /// Key: (DictionaryType, DictionaryId)
    historical_dictionaries:
        RwLock<HashMap<(DictionaryType, DictionaryId), Arc<CompressionDictionary>>>,
    /// Channel to broadcast new dictionaries to all connections
    dictionary_broadcast: broadcast::Sender<Arc<CompressionDictionary>>,
    /// Trainer instance
    trainer: DictionaryTrainer,
    /// Config
    config: DictionaryCompressionConfig,
}

impl DictionaryManager {
    /// Create a new dictionary manager
    pub fn new(config: DictionaryCompressionConfig) -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            current_dictionaries: RwLock::new(HashMap::new()),
            historical_dictionaries: RwLock::new(HashMap::new()),
            dictionary_broadcast: tx,
            trainer: DictionaryTrainer::new(config.clone()),
            config,
        }
    }

    /// Start the periodic training loop
    /// Returns a JoinHandle that can be used to stop the loop
    pub fn start_training_loop(self: Arc<Self>) -> JoinHandle<()> {
        let interval = Duration::from_secs(self.config.update_interval_secs);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            // Skip the first immediate tick
            interval_timer.tick().await;

            loop {
                interval_timer.tick().await;

                if !self.config.enabled {
                    continue;
                }

                // Train new dictionaries
                let (log_dict, msg_dict) = self.trainer.train_dictionaries().await;

                // Update and broadcast log dictionary
                if let Some(dict) = log_dict {
                    self.update_dictionary(dict).await;
                }

                // Update and broadcast message dictionary
                if let Some(dict) = msg_dict {
                    self.update_dictionary(dict).await;
                }

                // Prune old historical dictionaries
                self.prune_historical().await;
            }
        })
    }

    /// Update the current dictionary and broadcast to clients
    async fn update_dictionary(&self, dict: CompressionDictionary) {
        let dict = Arc::new(dict);
        let dict_type = dict.dict_type;
        let dict_id = dict.id;

        // Update current dictionary
        {
            let mut current = self.current_dictionaries.write().await;
            current.insert(dict_type, dict.clone());
        }

        // Add to historical
        {
            let mut historical = self.historical_dictionaries.write().await;
            historical.insert((dict_type, dict_id), dict.clone());
        }

        // Broadcast to all subscribers
        // It's okay if there are no receivers
        let _ = self.dictionary_broadcast.send(dict);
    }

    /// Get the current dictionary for a type
    pub async fn get_current(&self, dict_type: DictionaryType) -> Option<Arc<CompressionDictionary>> {
        self.current_dictionaries.read().await.get(&dict_type).cloned()
    }

    /// Get a specific historical dictionary
    pub async fn get_by_id(
        &self,
        dict_type: DictionaryType,
        id: DictionaryId,
    ) -> Option<Arc<CompressionDictionary>> {
        // First check if it's the current dictionary
        if let Some(current) = self.current_dictionaries.read().await.get(&dict_type) {
            if current.id == id {
                return Some(current.clone());
            }
        }

        // Check historical
        self.historical_dictionaries
            .read()
            .await
            .get(&(dict_type, id))
            .cloned()
    }

    /// Subscribe to dictionary updates
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<CompressionDictionary>> {
        self.dictionary_broadcast.subscribe()
    }

    /// Record a transaction for training
    pub async fn record_transaction(&self, tx: &Transaction) {
        if self.config.enabled {
            self.trainer.add_sample(tx).await;
        }
    }

    /// Get sample counts from the trainer
    pub async fn sample_counts(&self) -> (usize, usize) {
        self.trainer.sample_counts().await
    }

    /// Prune old historical dictionaries
    async fn prune_historical(&self) {
        let mut historical = self.historical_dictionaries.write().await;

        // Get current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Remove expired dictionaries
        historical.retain(|_, dict| now.saturating_sub(dict.created_at) < MAX_HISTORICAL_AGE_SECS);

        // Count per type and remove excess
        let mut log_count = 0usize;
        let mut msg_count = 0usize;
        let mut to_remove = Vec::new();

        // Collect counts and identify dictionaries to remove
        let mut entries: Vec<_> = historical.iter().collect();
        entries.sort_by(|a, b| b.1.id.cmp(&a.1.id)); // Sort by ID descending (newest first)

        for ((dict_type, dict_id), _) in entries {
            match dict_type {
                DictionaryType::LogMessages => {
                    log_count += 1;
                    if log_count > MAX_HISTORICAL_PER_TYPE {
                        to_remove.push((*dict_type, *dict_id));
                    }
                }
                DictionaryType::TransactionMessage => {
                    msg_count += 1;
                    if msg_count > MAX_HISTORICAL_PER_TYPE {
                        to_remove.push((*dict_type, *dict_id));
                    }
                }
            }
        }

        for key in to_remove {
            historical.remove(&key);
        }
    }

    /// Check if dictionary compression is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get both current dictionaries (for use in compression)
    pub async fn get_current_dictionaries(
        &self,
    ) -> (
        Option<Arc<CompressionDictionary>>,
        Option<Arc<CompressionDictionary>>,
    ) {
        let current = self.current_dictionaries.read().await;
        (
            current.get(&DictionaryType::LogMessages).cloned(),
            current.get(&DictionaryType::TransactionMessage).cloned(),
        )
    }
}

/// Thread-safe wrapper for the manager
pub type SharedDictionaryManager = Arc<DictionaryManager>;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> DictionaryCompressionConfig {
        DictionaryCompressionConfig {
            enabled: true,
            update_interval_secs: 1, // Short interval for testing
            min_samples_for_training: 10,
            max_dictionary_size: 4096,
            training_window_size: 100,
        }
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let manager = DictionaryManager::new(test_config());

        assert!(manager.get_current(DictionaryType::LogMessages).await.is_none());
        assert!(manager.get_current(DictionaryType::TransactionMessage).await.is_none());
    }

    #[tokio::test]
    async fn test_manual_dictionary_update() {
        let manager = DictionaryManager::new(test_config());

        let dict = CompressionDictionary::new(1, DictionaryType::LogMessages, vec![1, 2, 3, 4]);

        manager.update_dictionary(dict).await;

        let current = manager.get_current(DictionaryType::LogMessages).await;
        assert!(current.is_some());
        assert_eq!(current.unwrap().id, 1);
    }

    #[tokio::test]
    async fn test_historical_lookup() {
        let manager = DictionaryManager::new(test_config());

        // Add first dictionary
        let dict1 = CompressionDictionary::new(1, DictionaryType::LogMessages, vec![1, 2, 3, 4]);
        manager.update_dictionary(dict1).await;

        // Add second dictionary (becomes current)
        let dict2 = CompressionDictionary::new(2, DictionaryType::LogMessages, vec![5, 6, 7, 8]);
        manager.update_dictionary(dict2).await;

        // Current should be dict2
        let current = manager.get_current(DictionaryType::LogMessages).await;
        assert_eq!(current.unwrap().id, 2);

        // Should still be able to get dict1 from historical
        let historical = manager.get_by_id(DictionaryType::LogMessages, 1).await;
        assert!(historical.is_some());
        assert_eq!(historical.unwrap().data, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_subscription() {
        let manager = DictionaryManager::new(test_config());
        let mut subscriber = manager.subscribe();

        let dict = CompressionDictionary::new(1, DictionaryType::LogMessages, vec![1, 2, 3, 4]);
        manager.update_dictionary(dict).await;

        let received = subscriber.recv().await.unwrap();
        assert_eq!(received.id, 1);
    }
}
