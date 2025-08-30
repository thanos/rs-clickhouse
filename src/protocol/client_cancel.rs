//! Client cancel packet implementation

use crate::error::{Error, Result};
use crate::protocol::{Packet, PacketType};
use bytes::{Buf, BufMut, BytesMut};

/// Client cancel packet
/// 
/// This packet is sent by the client to cancel a running query.
/// It includes the query ID to identify which query to cancel.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientCancel {
    /// Query ID to cancel
    pub query_id: String,
}

impl ClientCancel {
    /// Create a new client cancel packet
    pub fn new(query_id: String) -> Self {
        Self { query_id }
    }

    /// Get the query ID
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Set the query ID
    pub fn set_query_id(&mut self, query_id: String) {
        self.query_id = query_id;
    }
}

impl Packet for ClientCancel {
    fn packet_type(&self) -> PacketType {
        PacketType::ClientCancel
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize the query ID as a string
        let query_id_bytes = self.query_id.as_bytes();
        buf.put_u64_le(query_id_bytes.len() as u64);
        buf.extend_from_slice(query_id_bytes);
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for ClientCancel packet".to_string()));
        }

        // Read the query ID length
        let query_id_len = buf.get_u64_le() as usize;
        
        // Validate query ID length
        if query_id_len > buf.len() {
            return Err(Error::Protocol(format!(
                "Invalid query ID length: {} (available: {})",
                query_id_len,
                buf.len()
            )));
        }

        // Read the query ID
        let query_id_bytes = buf.copy_to_bytes(query_id_len);
        let query_id = String::from_utf8(query_id_bytes.to_vec())
            .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in query ID: {}", e)))?;

        Ok(ClientCancel { query_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_client_cancel_new() {
        let query_id = "test_query_123".to_string();
        let cancel = ClientCancel::new(query_id.clone());
        assert_eq!(cancel.query_id, query_id);
        assert_eq!(cancel.packet_type(), PacketType::ClientCancel);
    }

    #[test]
    fn test_client_cancel_get_set() {
        let mut cancel = ClientCancel::new("old_id".to_string());
        assert_eq!(cancel.query_id(), "old_id");
        
        cancel.set_query_id("new_id".to_string());
        assert_eq!(cancel.query_id(), "new_id");
    }

    #[test]
    fn test_client_cancel_serialize() {
        let query_id = "test_query_456".to_string();
        let cancel = ClientCancel::new(query_id.clone());
        let mut buf = BytesMut::new();
        
        cancel.serialize(&mut buf).unwrap();
        
        // Should contain length (8 bytes) + query ID (14 bytes)
        assert_eq!(buf.len(), 22);
        assert_eq!(buf[0..8], [14, 0, 0, 0, 0, 0, 0, 0]); // length = 14
        assert_eq!(&buf[8..22], b"test_query_456");
    }

    #[test]
    fn test_client_cancel_serialize_empty() {
        let cancel = ClientCancel::new("".to_string());
        let mut buf = BytesMut::new();
        
        cancel.serialize(&mut buf).unwrap();
        
        // Should contain length (8 bytes) + no query ID
        assert_eq!(buf.len(), 8);
        assert_eq!(buf[0..8], [0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_client_cancel_deserialize() {
        let mut buf = BytesMut::new();
        let query_id = "test_query_789".to_string();
        let query_id_bytes = query_id.as_bytes();
        
        buf.put_u64_le(query_id_bytes.len() as u64);
        buf.extend_from_slice(query_id_bytes);
        
        let cancel = ClientCancel::deserialize(&mut buf).unwrap();
        assert_eq!(cancel.query_id, query_id);
    }

    #[test]
    fn test_client_cancel_deserialize_empty() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(0); // length = 0
        
        let cancel = ClientCancel::deserialize(&mut buf).unwrap();
        assert_eq!(cancel.query_id, "");
    }

    #[test]
    fn test_client_cancel_deserialize_insufficient_data() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // Only 4 bytes, need 8
        
        let result = ClientCancel::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }

    #[test]
    fn test_client_cancel_deserialize_invalid_length() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1000); // Length > available data
        
        let result = ClientCancel::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }

    #[test]
    fn test_client_cancel_round_trip() {
        let original = ClientCancel::new("test_query_round_trip".to_string());
        let mut buf = BytesMut::new();
        
        original.serialize(&mut buf).unwrap();
        let deserialized = ClientCancel::deserialize(&mut buf).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_client_cancel_round_trip_empty() {
        let original = ClientCancel::new("".to_string());
        let mut buf = BytesMut::new();
        
        original.serialize(&mut buf).unwrap();
        let deserialized = ClientCancel::deserialize(&mut buf).unwrap();
        
        assert_eq!(original, deserialized);
    }
}
