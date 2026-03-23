use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for compression/decompression operations
#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Compression failed: {0}")]
    CompressionFailed(String),
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),
    #[error("Dictionary training failed: {0}")]
    TrainingFailed(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub enum CompressionType {
    None,
    Lz4Fast(i32),
    Lz4(i32),
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Lz4Fast(8)
    }
}

impl CompressionType {
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return vec![];
        }

        match self {
            CompressionType::None => data.to_vec(),
            CompressionType::Lz4Fast(speed) => {
                lz4::block::compress(data, Some(lz4::block::CompressionMode::FAST(*speed)), true)
                    .expect("Compression should work")
            }
            CompressionType::Lz4(compression) => lz4::block::compress(
                data,
                Some(lz4::block::CompressionMode::HIGHCOMPRESSION(*compression)),
                true,
            )
            .expect("compression should work"),
        }
    }
}

/// Compress data using LZ4 with an optional dictionary
///
/// Uses lz4_flex block format with size prepended. The output can be
/// decompressed with `decompress_with_dict` using the same dictionary.
pub fn compress_with_dict(data: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>, CompressionError> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    match dict {
        Some(dict_bytes) if dict_bytes.len() >= 4 => {
            // Use lz4_flex block compression with dictionary
            // Dictionary must be at least 4 bytes (minimum match length)
            Ok(lz4_flex::block::compress_prepend_size_with_dict(
                data, dict_bytes,
            ))
        }
        _ => {
            // Use simple block compression without dictionary
            Ok(lz4_flex::compress_prepend_size(data))
        }
    }
}

/// Decompress data using LZ4 with an optional dictionary
///
/// The input must be in the same format as produced by `compress_with_dict`.
/// If the data was compressed with a dictionary, the same dictionary must be provided.
pub fn decompress_with_dict(data: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>, CompressionError> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    match dict {
        Some(dict_bytes) if dict_bytes.len() >= 4 => {
            // Use lz4_flex block decompression with dictionary
            lz4_flex::block::decompress_size_prepended_with_dict(data, dict_bytes)
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
        }
        _ => {
            // Use simple block decompression without dictionary
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
        }
    }
}

/// Train a compression dictionary from sample data using zstd's dictionary builder
///
/// The resulting dictionary can be used with LZ4 compression for better compression
/// ratios on similar data. The dictionary captures common byte sequences found
/// across the samples.
///
/// # Arguments
/// * `samples` - Slices of data to train on (more samples = better dictionary)
/// * `max_size` - Maximum dictionary size in bytes (up to 64KB for LZ4)
///
/// # Returns
/// The trained dictionary bytes, or an error if training fails
pub fn train_dictionary(samples: &[&[u8]], max_size: usize) -> Result<Vec<u8>, CompressionError> {
    if samples.is_empty() {
        return Err(CompressionError::TrainingFailed(
            "No samples provided".to_string(),
        ));
    }

    // Clamp max_size to LZ4's limit
    let max_size = max_size.min(65536);

    // Use zstd's dictionary training - it produces high-quality dictionaries
    // that work well with LZ4 too
    zstd::dict::from_samples(samples, max_size)
        .map_err(|e| CompressionError::TrainingFailed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_without_dict() {
        let original = b"Hello, world! This is some test data for compression.";

        let compressed = compress_with_dict(original, None).unwrap();
        let decompressed = decompress_with_dict(&compressed, None).unwrap();

        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_with_dict() {
        // Create a dictionary from sample data
        // zstd requires substantial sample data for training
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for i in 0..100 {
            samples.push(format!("Program log: Transfer {} lamports from account A to account B", i * 100).into_bytes());
            samples.push(format!("Program log: Invoke [{}] instruction on program XYZ", i).into_bytes());
            samples.push(format!("Program log: Success with {} compute units consumed", i * 10).into_bytes());
            samples.push(format!("Program log: Error: insufficient funds for transfer of {} lamports", i * 50).into_bytes());
        }
        let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();

        let dict = train_dictionary(&sample_refs, 4096).unwrap();
        assert!(!dict.is_empty());

        let data = b"Program log: Transfer 150 lamports from account A to account B";
        let compressed = compress_with_dict(data, Some(&dict)).unwrap();
        let decompressed = decompress_with_dict(&compressed, Some(&dict)).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_empty_data() {
        let empty: &[u8] = &[];

        let compressed = compress_with_dict(empty, None).unwrap();
        assert!(compressed.is_empty());

        let decompressed = decompress_with_dict(&compressed, None).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_dictionary_training() {
        // zstd requires substantial sample data for training
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for i in 0..100 {
            samples.push(format!("Transaction {} successful with signature ABC{}", i, i).into_bytes());
            samples.push(format!("Transaction {} failed: insufficient funds for {} lamports", i, i * 100).into_bytes());
            samples.push(format!("Transaction {} pending confirmation slot {}", i, i + 1000).into_bytes());
        }
        let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();

        let dict = train_dictionary(&sample_refs, 4096).unwrap();
        assert!(!dict.is_empty());
        assert!(dict.len() <= 4096);
    }

    #[test]
    fn test_empty_samples_error() {
        let samples: Vec<&[u8]> = vec![];
        let result = train_dictionary(&samples, 1024);
        assert!(result.is_err());
    }
}
