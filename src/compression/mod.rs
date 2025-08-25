//! Compression utilities for ClickHouse

use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::io::{self, Write};

/// Compression methods supported by ClickHouse
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    /// No compression
    None,
    /// LZ4 compression
    LZ4,
    /// ZSTD compression
    ZSTD,
    /// GZIP compression
    GZIP,
    /// BZIP2 compression
    BZIP2,
    /// XZ compression
    XZ,
}

impl CompressionMethod {
    /// Get the compression method name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionMethod::None => "none",
            CompressionMethod::LZ4 => "lz4",
            CompressionMethod::ZSTD => "zstd",
            CompressionMethod::GZIP => "gzip",
            CompressionMethod::BZIP2 => "bzip2",
            CompressionMethod::XZ => "xz",
        }
    }

    /// Get the compression method from a string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" => Some(CompressionMethod::None),
            "lz4" => Some(CompressionMethod::LZ4),
            "zstd" => Some(CompressionMethod::ZSTD),
            "gzip" => Some(CompressionMethod::GZIP),
            "bzip2" => Some(CompressionMethod::BZIP2),
            "xz" => Some(CompressionMethod::XZ),
            _ => None,
        }
    }

    /// Check if compression is enabled
    pub fn is_enabled(&self) -> bool {
        *self != CompressionMethod::None
    }
}

/// Compression level (0-9, where 0 is fastest and 9 is best compression)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompressionLevel(pub u8);

impl CompressionLevel {
    /// Create a new compression level
    pub fn new(level: u8) -> Result<Self> {
        if level <= 9 {
            Ok(CompressionLevel(level))
        } else {
            Err(Error::Configuration(
                "Compression level must be between 0 and 9".to_string(),
            ))
        }
    }

    /// Get the compression level value
    pub fn value(&self) -> u8 {
        self.0
    }

    /// Fastest compression (level 0)
    pub fn fastest() -> Self {
        CompressionLevel(0)
    }

    /// Best compression (level 9)
    pub fn best() -> Self {
        CompressionLevel(9)
    }

    /// Default compression (level 3)
    pub fn default() -> Self {
        CompressionLevel(3)
    }
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::default()
    }
}

/// Compression trait for different compression methods
pub trait Compressor {
    /// Compress data
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>>;

    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Get the compression method
    fn method(&self) -> CompressionMethod;
}

/// No compression implementation
pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, data: &[u8], _level: CompressionLevel) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn method(&self) -> CompressionMethod {
        CompressionMethod::None
    }
}

/// LZ4 compression implementation
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut encoder = lz4::EncoderBuilder::new()
            .level(level.value() as u32)
            .build(&mut compressed)?;

        encoder.write_all(data)?;
        let (_, result) = encoder.finish();
        result?;

        Ok(compressed)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut decoder = lz4::Decoder::new(data)?;

        io::copy(&mut decoder, &mut decompressed)?;
        Ok(decompressed)
    }

    fn method(&self) -> CompressionMethod {
        CompressionMethod::LZ4
    }
}

/// ZSTD compression implementation
pub struct ZstdCompressor;

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        let compressed = zstd::bulk::compress(data, level.value() as i32)?;
        Ok(compressed)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Try to decompress with a reasonable buffer size
        let mut buffer = Vec::new();
        let mut stream = zstd::stream::Decoder::new(data)?;
        std::io::copy(&mut stream, &mut buffer)?;
        Ok(buffer)
    }

    fn method(&self) -> CompressionMethod {
        CompressionMethod::ZSTD
    }
}

/// Compression manager for handling different compression methods
pub struct CompressionManager {
    /// Current compression method
    method: CompressionMethod,
    /// Current compression level
    level: CompressionLevel,
    /// Compression threshold (minimum size to compress)
    threshold: usize,
    /// Compressor instance
    compressor: Box<dyn Compressor>,
}

impl CompressionManager {
    /// Create a new compression manager
    pub fn new(method: CompressionMethod, level: CompressionLevel, threshold: usize) -> Result<Self> {
        let compressor: Box<dyn Compressor> = match method {
            CompressionMethod::None => Box::new(NoCompressor),
            CompressionMethod::LZ4 => Box::new(Lz4Compressor),
            CompressionMethod::ZSTD => Box::new(ZstdCompressor),
            CompressionMethod::GZIP => {
                return Err(Error::Unsupported("GZIP compression not yet implemented".to_string()));
            }
            CompressionMethod::BZIP2 => {
                return Err(Error::Unsupported("BZIP2 compression not yet implemented".to_string()));
            }
            CompressionMethod::XZ => {
                return Err(Error::Unsupported("XZ compression not yet implemented".to_string()));
            }
        };

        Ok(Self {
            method,
            level,
            threshold,
            compressor,
        })
    }

    /// Create a new compression manager with default settings
    pub fn default() -> Result<Self> {
        Self::new(
            CompressionMethod::LZ4,
            CompressionLevel::default(),
            1024, // 1KB threshold
        )
    }

    /// Compress data if it meets the threshold
    pub fn compress_if_needed(&self, data: &[u8]) -> Result<CompressedData> {
        if data.len() < self.threshold || !self.method.is_enabled() {
            return Ok(CompressedData {
                data: data.to_vec(),
                method: CompressionMethod::None,
                original_size: data.len(),
                compressed_size: data.len(),
            });
        }

        let compressed = self.compressor.compress(data, self.level)?;
        let compressed_size = compressed.len();
        let compression_ratio = compressed_size as f64 / data.len() as f64;

        // Only use compression if it actually reduces size
        if compression_ratio >= 1.0 {
            return Ok(CompressedData {
                data: data.to_vec(),
                method: CompressionMethod::None,
                original_size: data.len(),
                compressed_size: data.len(),
            });
        }

        Ok(CompressedData {
            data: compressed,
            method: self.method,
            original_size: data.len(),
            compressed_size,
        })
    }

    /// Decompress data
    pub fn decompress(&self, data: &CompressedData) -> Result<Vec<u8>> {
        if data.method == CompressionMethod::None {
            return Ok(data.data.clone());
        }

        self.compressor.decompress(&data.data)
    }

    /// Get the current compression method
    pub fn method(&self) -> CompressionMethod {
        self.method
    }

    /// Get the current compression level
    pub fn level(&self) -> CompressionLevel {
        self.level
    }

    /// Get the compression threshold
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Set the compression method
    pub fn set_method(&mut self, method: CompressionMethod) -> Result<()> {
        let compressor: Box<dyn Compressor> = match method {
            CompressionMethod::None => Box::new(NoCompressor),
            CompressionMethod::LZ4 => Box::new(Lz4Compressor),
            CompressionMethod::ZSTD => Box::new(ZstdCompressor),
            CompressionMethod::GZIP => {
                return Err(Error::Unsupported("GZIP compression not yet implemented".to_string()));
            }
            CompressionMethod::BZIP2 => {
                return Err(Error::Unsupported("BZIP2 compression not yet implemented".to_string()));
            }
            CompressionMethod::XZ => {
                return Err(Error::Unsupported("XZ compression not yet implemented".to_string()));
            }
        };

        self.method = method;
        self.compressor = compressor;
        Ok(())
    }

    /// Set the compression level
    pub fn set_level(&mut self, level: CompressionLevel) -> Result<()> {
        self.level = level;
        Ok(())
    }

    /// Set the compression threshold
    pub fn set_threshold(&mut self, threshold: usize) -> Result<()> {
        self.threshold = threshold;
        Ok(())
    }
}

/// Compressed data structure
#[derive(Debug, Clone)]
pub struct CompressedData {
    /// The compressed or uncompressed data
    pub data: Vec<u8>,
    /// The compression method used
    pub method: CompressionMethod,
    /// Original data size
    pub original_size: usize,
    /// Compressed data size
    pub compressed_size: usize,
}

impl CompressedData {
    /// Create new compressed data
    pub fn new(data: Vec<u8>, method: CompressionMethod, original_size: usize) -> Self {
        let compressed_size = data.len();
        Self {
            data,
            method,
            original_size,
            compressed_size,
        }
    }

    /// Get the compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            0.0
        } else {
            self.compressed_size as f64 / self.original_size as f64
        }
    }

    /// Get the space savings percentage
    pub fn space_savings(&self) -> f64 {
        if self.original_size == 0 {
            0.0
        } else {
            (1.0 - self.compression_ratio()) * 100.0
        }
    }

    /// Check if the data is compressed
    pub fn is_compressed(&self) -> bool {
        self.method != CompressionMethod::None
    }

    /// Get the data as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get the data as a vector
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }
}

/// Compression utilities
pub mod utils {
    use super::*;

    /// Compress data with the specified method and level
    pub fn compress(
        data: &[u8],
        method: CompressionMethod,
        level: CompressionLevel,
    ) -> Result<Vec<u8>> {
        let manager = CompressionManager::new(method, level, 0)?;
        let compressed = manager.compress_if_needed(data)?;
        Ok(compressed.data)
    }

    /// Decompress data with the specified method
    pub fn decompress(data: &[u8], method: CompressionMethod) -> Result<Vec<u8>> {
        let manager = CompressionManager::new(method, CompressionLevel::default(), 0)?;
        let compressed_data = CompressedData::new(data.to_vec(), method, data.len());
        manager.decompress(&compressed_data)
    }

    /// Get compression statistics
    pub fn get_compression_stats(
        data: &[u8],
        method: CompressionMethod,
        level: CompressionLevel,
    ) -> Result<CompressionStats> {
        let manager = CompressionManager::new(method, level, 0)?;
        let compressed = manager.compress_if_needed(data)?;

        Ok(CompressionStats {
            original_size: compressed.original_size,
            compressed_size: compressed.compressed_size,
            method: compressed.method,
            compression_ratio: compressed.compression_ratio(),
            space_savings: compressed.space_savings(),
        })
    }
}

/// Compression statistics
#[derive(Debug, Clone)]
pub struct CompressionStats {
    /// Original data size
    pub original_size: usize,
    /// Compressed data size
    pub compressed_size: usize,
    /// Compression method used
    pub method: CompressionMethod,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Space savings percentage
    pub space_savings: f64,
}

impl CompressionStats {
    /// Create new compression stats
    pub fn new(
        original_size: usize,
        compressed_size: usize,
        method: CompressionMethod,
    ) -> Self {
        let compression_ratio = if original_size == 0 {
            0.0
        } else {
            compressed_size as f64 / original_size as f64
        };

        let space_savings = (1.0 - compression_ratio) * 100.0;

        Self {
            original_size,
            compressed_size,
            method,
            compression_ratio,
            space_savings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_method_conversion() {
        assert_eq!(CompressionMethod::from_str("lz4"), Some(CompressionMethod::LZ4));
        assert_eq!(CompressionMethod::from_str("ZSTD"), Some(CompressionMethod::ZSTD));
        assert_eq!(CompressionMethod::from_str("none"), Some(CompressionMethod::None));
        assert_eq!(CompressionMethod::from_str("unknown"), None);
    }

    #[test]
    fn test_compression_level() {
        assert!(CompressionLevel::new(5).is_ok());
        assert!(CompressionLevel::new(10).is_err());
        assert_eq!(CompressionLevel::fastest().value(), 0);
        assert_eq!(CompressionLevel::best().value(), 9);
        assert_eq!(CompressionLevel::default().value(), 3);
    }

    #[test]
    fn test_no_compressor() {
        let compressor = NoCompressor;
        let data = b"Hello, World!";
        let compressed = compressor.compress(data, CompressionLevel::default()).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data, &decompressed[..]);
        assert_eq!(compressor.method(), CompressionMethod::None);
    }

    #[test]
    fn test_lz4_compressor() {
        let compressor = Lz4Compressor;
        let data = b"Hello, World! This is a test string for compression.";
        let compressed = compressor.compress(data, CompressionLevel::default()).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data, &decompressed[..]);
        assert_eq!(compressor.method(), CompressionMethod::LZ4);
    }

    #[test]
    fn test_zstd_compressor() {
        let compressor = ZstdCompressor;
        let data = b"Hello, World! This is a test string for compression.";
        let compressed = compressor.compress(data, CompressionLevel::default()).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data, &decompressed[..]);
        assert_eq!(compressor.method(), CompressionMethod::ZSTD);
    }

    #[test]
    fn test_compression_manager() {
        let manager = CompressionManager::default().unwrap();
        assert_eq!(manager.method(), CompressionMethod::LZ4);
        assert_eq!(manager.level().value(), 3);
        assert_eq!(manager.threshold(), 1024);
    }

    #[test]
    fn test_compressed_data() {
        let data = CompressedData::new(
            b"compressed".to_vec(),
            CompressionMethod::LZ4,
            100,
        );

        assert_eq!(data.compression_ratio(), 0.1);
        assert_eq!(data.space_savings(), 90.0);
        assert!(data.is_compressed());
    }
}
