use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a dictionary, monotonically increasing
pub type DictionaryId = u64;

/// The type of field this dictionary is trained for
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(C)]
pub enum DictionaryType {
    /// For Transaction.transaction_meta.log_messages
    LogMessages,
    /// For Transaction.message (VersionedMessage)
    TransactionMessage,
}

/// A trained dictionary with its metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompressionDictionary {
    /// Unique ID for this dictionary version
    pub id: DictionaryId,
    /// What type of data this dictionary compresses
    pub dict_type: DictionaryType,
    /// The raw dictionary bytes (max 64KB for LZ4)
    pub data: Vec<u8>,
    /// Unix timestamp when this dictionary was created
    pub created_at: u64,
}

impl CompressionDictionary {
    /// Maximum dictionary size in bytes (LZ4 limit)
    pub const MAX_DICTIONARY_SIZE: usize = 65536;

    /// Create a new dictionary with the current timestamp
    pub fn new(id: DictionaryId, dict_type: DictionaryType, data: Vec<u8>) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            id,
            dict_type,
            data,
            created_at,
        }
    }

    /// Check if this dictionary has expired based on max age
    pub fn is_expired(&self, max_age_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        now.saturating_sub(self.created_at) > max_age_secs
    }
}

/// Metadata about compression applied to a field
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldCompressionMeta {
    /// The dictionary ID used for compression (0 if no dictionary/no compression)
    pub dictionary_id: DictionaryId,
    /// Original uncompressed size in bytes
    pub original_size: u32,
}

impl FieldCompressionMeta {
    /// Special dictionary ID indicating no dictionary was used
    pub const NO_DICTIONARY: DictionaryId = 0;

    /// Create metadata for uncompressed data
    pub fn uncompressed(original_size: u32) -> Self {
        Self {
            dictionary_id: Self::NO_DICTIONARY,
            original_size,
        }
    }

    /// Create metadata for dictionary-compressed data
    pub fn with_dictionary(dictionary_id: DictionaryId, original_size: u32) -> Self {
        Self {
            dictionary_id,
            original_size,
        }
    }

    /// Check if this field was compressed with a dictionary
    pub fn uses_dictionary(&self) -> bool {
        self.dictionary_id != Self::NO_DICTIONARY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_creation() {
        let dict = CompressionDictionary::new(
            1,
            DictionaryType::LogMessages,
            vec![1, 2, 3, 4],
        );

        assert_eq!(dict.id, 1);
        assert_eq!(dict.dict_type, DictionaryType::LogMessages);
        assert_eq!(dict.data, vec![1, 2, 3, 4]);
        assert!(dict.created_at > 0);
    }

    #[test]
    fn test_dictionary_expiration() {
        let mut dict = CompressionDictionary::new(
            1,
            DictionaryType::TransactionMessage,
            vec![],
        );

        // Not expired with large max age
        assert!(!dict.is_expired(3600));

        // Simulate old dictionary
        dict.created_at = 0;
        assert!(dict.is_expired(3600));
    }

    #[test]
    fn test_field_compression_meta() {
        let uncompressed = FieldCompressionMeta::uncompressed(1000);
        assert!(!uncompressed.uses_dictionary());
        assert_eq!(uncompressed.original_size, 1000);

        let compressed = FieldCompressionMeta::with_dictionary(42, 1000);
        assert!(compressed.uses_dictionary());
        assert_eq!(compressed.dictionary_id, 42);
    }
}
