//! Client Data message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use crate::types::Block;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Client Data message for sending data blocks
#[derive(Debug, Clone)]
pub struct ClientData {
    /// Block of data
    pub block: Block,
    /// Table name (optional)
    pub table_name: Option<String>,
    /// Database name (optional)
    pub database_name: Option<String>,
    /// Compression method (optional)
    pub compression_method: Option<String>,
    /// Compression level (optional)
    pub compression_level: Option<u8>,
}

impl ClientData {
    /// Create a new Client Data message
    pub fn new(block: Block) -> Self {
        Self {
            block,
            table_name: None,
            database_name: None,
            compression_method: None,
            compression_level: None,
        }
    }

    /// Set table name
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Set database name
    pub fn with_database_name(mut self, database_name: impl Into<String>) -> Self {
        self.database_name = Some(database_name.into());
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

    /// Get the table name
    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    /// Get the database name
    pub fn database_name(&self) -> Option<&str> {
        self.database_name.as_deref()
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
}

impl Packet for ClientData {
    fn packet_type(&self) -> PacketType {
        PacketType::ClientData
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write table name
        if let Some(ref table_name) = self.table_name {
            buf.put_u64_le(table_name.len() as u64);
            buf.extend_from_slice(table_name.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        // Write database name
        if let Some(ref database_name) = self.database_name {
            buf.put_u64_le(database_name.len() as u64);
            buf.extend_from_slice(database_name.as_bytes());
        } else {
            buf.put_u64_le(0);
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
        // Read table name
        let table_name_len = buf.get_u64_le() as usize;
        let table_name = if table_name_len > 0 {
            if buf.remaining() < table_name_len {
                return Err(Error::Protocol("Insufficient data for table name".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(table_name_len)).to_string())
        } else {
            None
        };

        // Read database name
        let database_name_len = buf.get_u64_le() as usize;
        let database_name = if database_name_len > 0 {
            if buf.remaining() < database_name_len {
                return Err(Error::Protocol("Insufficient data for database name".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(database_name_len)).to_string())
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
            table_name,
            database_name,
            compression_method,
            compression_level,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Packet;
    use crate::types::{Block, Column, ColumnData, Value};

    fn create_test_block() -> Block {
        let mut block = Block::new();
        let column = Column::new(
            "test_column".to_string(),
            "UInt8".to_string(),
            ColumnData::UInt8(vec![1, 2, 3, 4, 5]),
        );
        block.add_column("test_column", column);
        block
    }

    #[test]
    fn test_client_data_new() {
        let block = create_test_block();
        let data = ClientData::new(block.clone());
        assert_eq!(data.block().row_count(), 5);
        assert_eq!(data.block().column_count(), 1);
        assert!(data.table_name().is_none());
        assert!(data.database_name().is_none());
        assert!(data.compression_method().is_none());
        assert!(data.compression_level().is_none());
        assert!(!data.is_compressed());
    }

    #[test]
    fn test_client_data_with_table_name() {
        let block = create_test_block();
        let data = ClientData::new(block).with_table_name("test_table");
        assert_eq!(data.table_name(), Some("test_table"));
    }

    #[test]
    fn test_client_data_with_database_name() {
        let block = create_test_block();
        let data = ClientData::new(block).with_database_name("test_db");
        assert_eq!(data.database_name(), Some("test_db"));
    }

    #[test]
    fn test_client_data_with_compression_method() {
        let block = create_test_block();
        let data = ClientData::new(block).with_compression_method("lz4");
        assert_eq!(data.compression_method(), Some("lz4"));
        assert!(data.is_compressed());
    }

    #[test]
    fn test_client_data_with_compression_level() {
        let block = create_test_block();
        let data = ClientData::new(block).with_compression_level(5);
        assert_eq!(data.compression_level(), Some(5));
    }

    #[test]
    fn test_client_data_packet_type() {
        let block = create_test_block();
        let data = ClientData::new(block);
        assert_eq!(data.packet_type(), PacketType::ClientData);
    }

    #[test]
    fn test_client_data_serialize_deserialize() {
        let block = create_test_block();
        let original = ClientData::new(block)
            .with_table_name("test_table")
            .with_database_name("test_db")
            .with_compression_method("lz4")
            .with_compression_level(5);

        let mut buf = BytesMut::new();
        Packet::serialize(&original, &mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = <ClientData as Packet>::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.table_name, deserialized.table_name);
        assert_eq!(original.database_name, deserialized.database_name);
        assert_eq!(original.compression_method, deserialized.compression_method);
        assert_eq!(original.compression_level, deserialized.compression_level);
    }
}
