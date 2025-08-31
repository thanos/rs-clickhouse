//! Client ping packet implementation

use crate::error::{Error, Result};
use crate::protocol::{Packet, PacketType};
use bytes::{Buf, BufMut, BytesMut};

/// Client ping packet
/// 
/// This packet is sent by the client to keep the connection alive
/// and check if the server is still responsive.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientPing {
    /// Optional custom ping data (can be empty)
    pub data: Vec<u8>,
}

impl ClientPing {
    /// Create a new client ping packet
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Create a new client ping packet with custom data
    pub fn with_data(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Get the ping data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Set the ping data
    pub fn set_data(&mut self, data: Vec<u8>) {
        self.data = data;
    }
}

impl Default for ClientPing {
    fn default() -> Self {
        Self::new()
    }
}

impl Packet for ClientPing {
    fn packet_type(&self) -> PacketType {
        PacketType::ClientPing
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize the ping data length
        buf.put_u64_le(self.data.len() as u64);
        
        // Serialize the ping data
        if !self.data.is_empty() {
            buf.extend_from_slice(&self.data);
        }
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for ClientPing packet".to_string()));
        }

        // Read the ping data length
        let data_len = buf.get_u64_le() as usize;
        
        // Validate data length
        if data_len > buf.len() {
            return Err(Error::Protocol(format!(
                "Invalid ping data length: {} (available: {})",
                data_len,
                buf.len()
            )));
        }

        // Read the ping data
        let data = if data_len > 0 {
            buf.copy_to_bytes(data_len).to_vec()
        } else {
            Vec::new()
        };

        Ok(ClientPing { data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_client_ping_new() {
        let ping = ClientPing::new();
        assert_eq!(ping.data.len(), 0);
        assert_eq!(ping.packet_type(), PacketType::ClientPing);
    }

    #[test]
    fn test_client_ping_with_data() {
        let data = b"ping_data".to_vec();
        let ping = ClientPing::with_data(data.clone());
        assert_eq!(ping.data, data);
    }

    #[test]
    fn test_client_ping_default() {
        let ping = ClientPing::default();
        assert_eq!(ping.data.len(), 0);
    }

    #[test]
    fn test_client_ping_set_data() {
        let mut ping = ClientPing::new();
        let data = b"new_data".to_vec();
        ping.set_data(data.clone());
        assert_eq!(ping.data, data);
    }

    #[test]
    fn test_client_ping_serialize_empty() {
        let ping = ClientPing::new();
        let mut buf = BytesMut::new();
        ping.serialize(&mut buf).unwrap();
        
        // Should contain length (8 bytes) + no data
        assert_eq!(buf.len(), 8);
        assert_eq!(buf[0..8], [0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_client_ping_serialize_with_data() {
        let data = b"ping_data".to_vec();
        let ping = ClientPing::with_data(data.clone());
        let mut buf = BytesMut::new();
        ping.serialize(&mut buf).unwrap();
        
        // Should contain length (8 bytes) + data (9 bytes)
        assert_eq!(buf.len(), 17);
        assert_eq!(buf[0..8], [9, 0, 0, 0, 0, 0, 0, 0]); // length = 9
        assert_eq!(&buf[8..17], b"ping_data");
    }

    #[test]
    fn test_client_ping_deserialize_empty() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(0); // length = 0
        
        let ping = ClientPing::deserialize(&mut buf).unwrap();
        assert_eq!(ping.data.len(), 0);
    }

    #[test]
    fn test_client_ping_deserialize_with_data() {
        let mut buf = BytesMut::new();
        let data = b"ping_data".to_vec();
        buf.put_u64_le(data.len() as u64);
        buf.extend_from_slice(&data);
        
        let ping = ClientPing::deserialize(&mut buf).unwrap();
        assert_eq!(ping.data, data);
    }

    #[test]
    fn test_client_ping_deserialize_insufficient_data() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // Only 4 bytes, need 8
        
        let result = ClientPing::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }

    #[test]
    fn test_client_ping_deserialize_invalid_length() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1000); // Length > available data
        
        let result = ClientPing::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }

    #[test]
    fn test_client_ping_round_trip() {
        let original = ClientPing::with_data(b"test_ping".to_vec());
        let mut buf = BytesMut::new();
        
        original.serialize(&mut buf).unwrap();
        let deserialized = ClientPing::deserialize(&mut buf).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_client_ping_round_trip_empty() {
        let original = ClientPing::new();
        let mut buf = BytesMut::new();
        
        original.serialize(&mut buf).unwrap();
        let deserialized = ClientPing::deserialize(&mut buf).unwrap();
        
        assert_eq!(original, deserialized);
    }
}
