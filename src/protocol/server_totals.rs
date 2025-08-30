//! Server totals packet implementation

use crate::error::{Error, Result};
use crate::protocol::{Packet, PacketType};
use crate::types::Block;
use bytes::{Buf, BufMut, BytesMut};

/// Block information
#[derive(Debug, Clone, PartialEq)]
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
}

/// Server totals packet
/// 
/// This packet contains aggregated totals for query results,
/// typically used for GROUP BY queries with totals.
#[derive(Debug, Clone)]
pub struct ServerTotals {
    /// Block containing the totals data
    pub block: Block,
    /// Block information (compression, etc.)
    pub block_info: Option<BlockInfo>,
}

impl ServerTotals {
    /// Create a new server totals packet
    pub fn new(block: Block) -> Self {
        Self {
            block,
            block_info: None,
        }
    }

    /// Create a new server totals packet with block info
    pub fn with_block_info(block: Block, block_info: BlockInfo) -> Self {
        Self {
            block,
            block_info: Some(block_info),
        }
    }

    /// Get the totals block
    pub fn block(&self) -> &Block {
        &self.block
    }

    /// Get the block info
    pub fn block_info(&self) -> Option<&BlockInfo> {
        self.block_info.as_ref()
    }

    /// Set the block info
    pub fn set_block_info(&mut self, block_info: BlockInfo) {
        self.block_info = Some(block_info);
    }

    /// Check if the packet has block info
    pub fn has_block_info(&self) -> bool {
        self.block_info.is_some()
    }
}

impl Packet for ServerTotals {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerTotals
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize block info flag
        buf.put_u8(if self.block_info.is_some() { 1 } else { 0 });
        
        // Serialize block info if present
        if let Some(ref block_info) = self.block_info {
            // Simplified block info serialization
            buf.put_u8(if block_info.is_overflows { 1 } else { 0 });
            buf.put_i32_le(block_info.bucket_num);
            buf.put_u8(if block_info.is_bucket_number { 1 } else { 0 });
        }
        
        // Serialize the block (simplified for now)
        buf.put_u64_le(0); // Block size placeholder
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 1 {
            return Err(Error::Protocol("Insufficient data for ServerTotals packet".to_string()));
        }

        // Read block info flag
        let has_block_info = buf.get_u8() == 1;
        
        // Read block info if present
        let block_info = if has_block_info {
            let is_overflows = buf.get_u8() != 0;
            let bucket_num = buf.get_i32_le();
            let is_bucket_number = buf.get_u8() != 0;
            Some(BlockInfo {
                is_overflows,
                bucket_num,
                is_bucket_number,
            })
        } else {
            None
        };
        
        // Read the block (simplified for now)
        let _block_size = buf.get_u64_le(); // Skip block size for now
        let block = Block::default(); // Placeholder
        
        Ok(ServerTotals {
            block,
            block_info,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Column, ColumnData};
    use bytes::BytesMut;

    fn create_test_block() -> Block {
        let mut block = Block::new();
        let column = Column::new(
            "total_count".to_string(),
            "UInt64".to_string(),
            ColumnData::UInt64(vec![1000, 2000, 3000]),
        );
        block.add_column("total_count".to_string(), column);
        block
    }

    fn create_test_block_info() -> BlockInfo {
        BlockInfo::new()
            .with_overflows(true)
            .with_bucket_num(42)
    }

    #[test]
    fn test_server_totals_new() {
        let block = create_test_block();
        let totals = ServerTotals::new(block.clone());
        
        assert_eq!(totals.block.row_count(), block.row_count());
        assert_eq!(totals.block.column_count(), block.column_count());
        assert_eq!(totals.block_info, None);
        assert_eq!(totals.packet_type(), PacketType::ServerTotals);
    }

    #[test]
    fn test_server_totals_with_block_info() {
        let block = create_test_block();
        let block_info = create_test_block_info();
        let totals = ServerTotals::with_block_info(block.clone(), block_info.clone());
        
        assert_eq!(totals.block.row_count(), block.row_count());
        assert_eq!(totals.block.column_count(), block.column_count());
        assert_eq!(totals.block_info, Some(block_info));
    }

    #[test]
    fn test_server_totals_get_set() {
        let mut totals = ServerTotals::new(create_test_block());
        let block_info = create_test_block_info();
        
        assert!(!totals.has_block_info());
        totals.set_block_info(block_info.clone());
        assert!(totals.has_block_info());
        assert_eq!(totals.block_info(), Some(&block_info));
    }

    #[test]
    fn test_server_totals_serialize_deserialize_empty() {
        let totals = ServerTotals::new(create_test_block());
        let mut buf = BytesMut::new();
        
        totals.serialize(&mut buf).unwrap();
        let deserialized = ServerTotals::deserialize(&mut buf).unwrap();
        
        assert_eq!(totals.block_info, deserialized.block_info);
        assert_eq!(totals.block.row_count(), deserialized.block.row_count());
        assert_eq!(totals.block.column_count(), deserialized.block.column_count());
    }

    #[test]
    fn test_server_totals_serialize_deserialize_with_block_info() {
        let block = create_test_block();
        let block_info = create_test_block_info();
        let totals = ServerTotals::with_block_info(block, block_info);
        let mut buf = BytesMut::new();
        
        totals.serialize(&mut buf).unwrap();
        let deserialized = ServerTotals::deserialize(&mut buf).unwrap();
        
        assert_eq!(totals.block_info, deserialized.block_info);
        assert_eq!(totals.block.row_count(), deserialized.block.row_count());
        assert_eq!(totals.block.column_count(), deserialized.block.column_count());
    }

    #[test]
    fn test_server_totals_insufficient_data() {
        let mut buf = BytesMut::new();
        let result = ServerTotals::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }
}
