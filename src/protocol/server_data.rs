//! Server Data message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use crate::types::Block;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Server Data message for receiving data blocks
#[derive(Debug, Clone)]
pub struct ServerData {
    /// Block of data
    pub block: Block,
    /// Block info
    pub block_info: Option<BlockInfo>,
    /// Compression method (optional)
    pub compression_method: Option<String>,
    /// Compression level (optional)
    pub compression_level: Option<u8>,
}

/// Block information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    /// Is overflows
    pub is_overflows: bool,
    /// Bucket number
    pub bucket_num: i32,
    /// Is bucket number
    pub is_bucket_number: bool,
}

impl BlockInfo {
    /// Create a new BlockInfo
    pub fn new() -> Self {
        Self {
            is_overflows: false,
            bucket_num: -1,
            is_bucket_number: false,
        }
    }

    /// Set overflows flag
    pub fn with_overflows(mut self, is_overflows: bool) -> Self {
        self.is_overflows = is_overflows;
        self
    }

    /// Set bucket number
    pub fn with_bucket_num(mut self, bucket_num: i32) -> Self {
        self.bucket_num = bucket_num;
        self.is_bucket_number = true;
        self
    }

    /// Check if overflows
    pub fn is_overflows(&self) -> bool {
        self.is_overflows
    }

    /// Get bucket number
    pub fn bucket_num(&self) -> Option<i32> {
        if self.is_bucket_number {
            Some(self.bucket_num)
        } else {
            None
        }
    }

    /// Check if has bucket number
    pub fn has_bucket_number(&self) -> bool {
        self.is_bucket_number
    }
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerData {
    /// Create a new Server Data message
    pub fn new(block: Block) -> Self {
        Self {
            block,
            block_info: None,
            compression_method: None,
            compression_level: None,
        }
    }

    /// Set block info
    pub fn with_block_info(mut self, block_info: BlockInfo) -> Self {
        self.block_info = Some(block_info);
        self
    }

    /// Set compression method
    pub fn with_compression_method(mut self, method: impl Into<String>) -> Self {
        self.compression_method = Some(method.into());
        self
    }

    /// Set compression level
    pub fn with_compression_level(mut self, level: u8) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Get the data block
    pub fn block(&self) -> &Block {
        &self.block
    }

    /// Get the block info
    pub fn block_info(&self) -> Option<&BlockInfo> {
        self.block_info.as_ref()
    }

    /// Get the compression method
    pub fn compression_method(&self) -> Option<&str> {
        self.compression_method.as_deref()
    }

    /// Get the compression level
    pub fn compression_level(&self) -> Option<u8> {
        self.compression_level
    }

    /// Check if compression is enabled
    pub fn is_compressed(&self) -> bool {
        self.compression_method.is_some()
    }

    /// Get the number of rows in the block
    pub fn row_count(&self) -> usize {
        self.block.row_count()
    }

    /// Get the number of columns in the block
    pub fn column_count(&self) -> usize {
        self.block.column_count()
    }

    /// Check if the block has overflows
    pub fn has_overflows(&self) -> bool {
        self.block_info
            .as_ref()
            .map(|info| info.is_overflows())
            .unwrap_or(false)
    }

    /// Get the bucket number if available
    pub fn bucket_number(&self) -> Option<i32> {
        self.block_info
            .as_ref()
            .and_then(|info| info.bucket_num())
    }
}

impl Packet for ServerData {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerData
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write block info
        if let Some(ref block_info) = self.block_info {
            buf.put_u8(if block_info.is_overflows { 1 } else { 0 });
            buf.put_i32_le(block_info.bucket_num);
            buf.put_u8(if block_info.is_bucket_number { 1 } else { 0 });
        } else {
            buf.put_u8(0); // No overflows
            buf.put_i32_le(-1); // No bucket number
            buf.put_u8(0); // No bucket number flag
        }

        // Write compression method
        if let Some(ref compression_method) = self.compression_method {
            buf.put_u64_le(compression_method.len() as u64);
            buf.extend_from_slice(compression_method.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        // Write compression level
        if let Some(compression_level) = self.compression_level {
            buf.put_u8(compression_level);
        } else {
            buf.put_u8(0);
        }

        // Write block (simplified for now)
        // For now, just write a placeholder for the block
        buf.put_u64_le(0); // Block size placeholder

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read block info
        let is_overflows = buf.get_u8() != 0;
        let bucket_num = buf.get_i32_le();
        let is_bucket_number = buf.get_u8() != 0;

        let block_info = if is_overflows || is_bucket_number {
            Some(BlockInfo {
                is_overflows,
                bucket_num,
                is_bucket_number,
            })
        } else {
            None
        };

        // Read compression method
        let compression_method_len = buf.get_u64_le() as usize;
        let compression_method = if compression_method_len > 0 {
            if buf.remaining() < compression_method_len {
                return Err(Error::Protocol("Insufficient data for compression method".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(compression_method_len)).to_string())
        } else {
            None
        };

        // Read compression level
        let compression_level = if buf.remaining() > 0 {
            let level = buf.get_u8();
            if level > 0 {
                Some(level)
            } else {
                None
            }
        } else {
            None
        };

        // Read block (simplified for now)
        let _block_size = buf.get_u64_le(); // Skip block size for now
        let block = Block::default(); // Placeholder

        Ok(Self {
            block,
            block_info,
            compression_method,
            compression_level,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Block, Column, ColumnData};

    fn create_test_block() -> Block {
        let mut block = Block::new();
        let column = Column::new(
            "test_column".to_string(),
            ColumnData::UInt8(vec![1, 2, 3, 4, 5]),
        );
        block.add_column(column);
        block
    }

    #[test]
    fn test_server_data_new() {
        let block = create_test_block();
        let data = ServerData::new(block.clone());
        assert_eq!(data.block().row_count(), 5);
        assert_eq!(data.block().column_count(), 1);
        assert!(data.block_info().is_none());
        assert!(data.compression_method().is_none());
        assert!(data.compression_level().is_none());
        assert!(!data.is_compressed());
        assert!(!data.has_overflows());
        assert!(data.bucket_number().is_none());
    }

    #[test]
    fn test_server_data_with_block_info() {
        let block = create_test_block();
        let block_info = BlockInfo::new()
            .with_overflows(true)
            .with_bucket_num(42);
        let data = ServerData::new(block).with_block_info(block_info);
        
        assert!(data.block_info().is_some());
        assert!(data.has_overflows());
        assert_eq!(data.bucket_number(), Some(42));
    }

    #[test]
    fn test_server_data_with_compression_method() {
        let block = create_test_block();
        let data = ServerData::new(block).with_compression_method("lz4");
        assert_eq!(data.compression_method(), Some("lz4"));
        assert!(data.is_compressed());
    }

    #[test]
    fn test_server_data_with_compression_level() {
        let block = create_test_block();
        let data = ServerData::new(block).with_compression_level(5);
        assert_eq!(data.compression_level(), Some(5));
    }

    #[test]
    fn test_block_info_new() {
        let info = BlockInfo::new();
        assert!(!info.is_overflows());
        assert_eq!(info.bucket_num(), None);
        assert!(!info.has_bucket_number());
    }

    #[test]
    fn test_block_info_with_overflows() {
        let info = BlockInfo::new().with_overflows(true);
        assert!(info.is_overflows());
    }

    #[test]
    fn test_block_info_with_bucket_num() {
        let info = BlockInfo::new().with_bucket_num(42);
        assert_eq!(info.bucket_num(), Some(42));
        assert!(info.has_bucket_number());
    }

    #[test]
    fn test_server_data_packet_type() {
        let block = create_test_block();
        let data = ServerData::new(block);
        assert_eq!(data.packet_type(), PacketType::ServerData);
    }

    #[test]
    fn test_server_data_serialize_deserialize() {
        let block = create_test_block();
        let block_info = BlockInfo::new()
            .with_overflows(true)
            .with_bucket_num(42);
        let original = ServerData::new(block)
            .with_block_info(block_info)
            .with_compression_method("lz4")
            .with_compression_level(5);

        let mut buf = BytesMut::new();
        original.serialize(&mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = ServerData::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.block_info.as_ref().unwrap().is_overflows, 
                   deserialized.block_info.as_ref().unwrap().is_overflows);
        assert_eq!(original.block_info.as_ref().unwrap().bucket_num, 
                   deserialized.block_info.as_ref().unwrap().bucket_num);
        assert_eq!(original.block_info.as_ref().unwrap().is_bucket_number, 
                   deserialized.block_info.as_ref().unwrap().is_bucket_number);
        assert_eq!(original.compression_method, deserialized.compression_method);
        assert_eq!(original.compression_level, deserialized.compression_level);
    }
}
