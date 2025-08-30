//! Server profile info packet implementation

use crate::error::{Error, Result};
use crate::protocol::{Packet, PacketType};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

/// Profile event information
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileEvent {
    /// Event type
    pub event_type: String,
    /// Event count
    pub count: u64,
    /// Event duration in nanoseconds
    pub duration_ns: u64,
}

impl ProfileEvent {
    /// Create a new profile event
    pub fn new(event_type: String, count: u64, duration_ns: u64) -> Self {
        Self {
            event_type,
            count,
            duration_ns,
        }
    }
}

/// Server profile info packet
/// 
/// This packet contains profiling information about query execution,
/// including various performance metrics and event counts.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerProfileInfo {
    /// Query execution time in nanoseconds
    pub execution_time_ns: u64,
    /// Number of rows processed
    pub rows_read: u64,
    /// Number of bytes read
    pub bytes_read: u64,
    /// Number of rows written
    pub rows_written: u64,
    /// Number of bytes written
    pub bytes_written: u64,
    /// Profile events
    pub profile_events: HashMap<String, ProfileEvent>,
}

impl ServerProfileInfo {
    /// Create a new server profile info packet
    pub fn new(
        execution_time_ns: u64,
        rows_read: u64,
        bytes_read: u64,
        rows_written: u64,
        bytes_written: u64,
    ) -> Self {
        Self {
            execution_time_ns,
            rows_read,
            bytes_read,
            rows_written,
            bytes_written,
            profile_events: HashMap::new(),
        }
    }

    /// Add a profile event
    pub fn add_profile_event(&mut self, event: ProfileEvent) {
        self.profile_events.insert(event.event_type.clone(), event);
    }

    /// Get execution time in milliseconds
    pub fn execution_time_ms(&self) -> f64 {
        self.execution_time_ns as f64 / 1_000_000.0
    }

    /// Get execution time in seconds
    pub fn execution_time_s(&self) -> f64 {
        self.execution_time_ns as f64 / 1_000_000_000.0
    }

    /// Get read throughput in MB/s
    pub fn read_throughput_mbps(&self) -> f64 {
        if self.execution_time_s() > 0.0 {
            (self.bytes_read as f64 / 1_048_576.0) / self.execution_time_s()
        } else {
            0.0
        }
    }

    /// Get write throughput in MB/s
    pub fn write_throughput_mbps(&self) -> f64 {
        if self.execution_time_s() > 0.0 {
            (self.bytes_written as f64 / 1_048_576.0) / self.execution_time_s()
        } else {
            0.0
        }
    }
}

impl Packet for ServerProfileInfo {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerProfileInfo
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize basic metrics
        buf.put_u64_le(self.execution_time_ns);
        buf.put_u64_le(self.rows_read);
        buf.put_u64_le(self.bytes_read);
        buf.put_u64_le(self.rows_written);
        buf.put_u64_le(self.bytes_written);
        
        // Serialize profile events count
        buf.put_u64_le(self.profile_events.len() as u64);
        
        // Serialize each profile event
        for (event_type, event) in &self.profile_events {
            // Event type length and data
            let event_type_bytes = event_type.as_bytes();
            buf.put_u64_le(event_type_bytes.len() as u64);
            buf.extend_from_slice(event_type_bytes);
            
            // Event count and duration
            buf.put_u64_le(event.count);
            buf.put_u64_le(event.duration_ns);
        }
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 40 {
            return Err(Error::Protocol("Insufficient data for ServerProfileInfo packet".to_string()));
        }

        // Read basic metrics
        let execution_time_ns = buf.get_u64_le();
        let rows_read = buf.get_u64_le();
        let bytes_read = buf.get_u64_le();
        let rows_written = buf.get_u64_le();
        let bytes_written = buf.get_u64_le();
        
        // Read profile events count
        let events_count = buf.get_u64_le() as usize;
        
        // Read profile events
        let mut profile_events = HashMap::new();
        for _ in 0..events_count {
            if buf.len() < 8 {
                return Err(Error::Protocol("Insufficient data for profile event".to_string()));
            }
            
            // Read event type length
            let event_type_len = buf.get_u64_le() as usize;
            if event_type_len > buf.len() {
                return Err(Error::Protocol("Invalid event type length".to_string()));
            }
            
            // Read event type
            let event_type_bytes = buf.copy_to_bytes(event_type_len);
            let event_type = String::from_utf8(event_type_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in event type: {}", e)))?;
            
            if buf.len() < 16 {
                return Err(Error::Protocol("Insufficient data for event metrics".to_string()));
            }
            
            // Read event count and duration
            let count = buf.get_u64_le();
            let duration_ns = buf.get_u64_le();
            
            let event = ProfileEvent::new(event_type.clone(), count, duration_ns);
            profile_events.insert(event_type, event);
        }
        
        Ok(ServerProfileInfo {
            execution_time_ns,
            rows_read,
            bytes_read,
            rows_written,
            bytes_written,
            profile_events,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BytesMut, BufMut};

    #[test]
    fn test_profile_event_new() {
        let event = ProfileEvent::new("test_event".to_string(), 100, 1000000);
        assert_eq!(event.event_type, "test_event");
        assert_eq!(event.count, 100);
        assert_eq!(event.duration_ns, 1000000);
    }

    #[test]
    fn test_server_profile_info_new() {
        let profile = ServerProfileInfo::new(1000000, 1000, 1024, 500, 512);
        assert_eq!(profile.execution_time_ns, 1000000);
        assert_eq!(profile.rows_read, 1000);
        assert_eq!(profile.bytes_read, 1024);
        assert_eq!(profile.rows_written, 500);
        assert_eq!(profile.bytes_written, 512);
        assert_eq!(profile.profile_events.len(), 0);
    }

    #[test]
    fn test_server_profile_info_add_event() {
        let mut profile = ServerProfileInfo::new(1000000, 1000, 1024, 500, 512);
        let event = ProfileEvent::new("test_event".to_string(), 100, 1000000);
        
        profile.add_profile_event(event.clone());
        assert_eq!(profile.profile_events.len(), 1);
        assert_eq!(profile.profile_events.get("test_event"), Some(&event));
    }

    #[test]
    fn test_server_profile_info_time_conversions() {
        let profile = ServerProfileInfo::new(1_000_000_000, 1000, 1024, 500, 512);
        assert_eq!(profile.execution_time_ms(), 1000.0);
        assert_eq!(profile.execution_time_s(), 1.0);
    }

    #[test]
    fn test_server_profile_info_throughput() {
        let profile = ServerProfileInfo::new(1_000_000_000, 1000, 1048576, 500, 512);
        assert_eq!(profile.read_throughput_mbps(), 1.0);
        assert_eq!(profile.write_throughput_mbps(), 0.00048828125);
    }

    #[test]
    fn test_server_profile_info_serialize_empty() {
        let profile = ServerProfileInfo::new(1000000, 1000, 1024, 500, 512);
        let mut buf = BytesMut::new();
        
        profile.serialize(&mut buf).unwrap();
        
        // Should contain: 5 u64 metrics (40 bytes) + events count (8 bytes) = 48 bytes
        assert_eq!(buf.len(), 48);
    }

    #[test]
    fn test_server_profile_info_serialize_with_events() {
        let mut profile = ServerProfileInfo::new(1000000, 1000, 1024, 500, 512);
        let event = ProfileEvent::new("test_event".to_string(), 100, 1000000);
        profile.add_profile_event(event);
        
        let mut buf = BytesMut::new();
        profile.serialize(&mut buf).unwrap();
        

        
        // Should contain: 5 u64 metrics (40 bytes) + events count (8 bytes) + 
        // event type length (8) + event type (10) + count (8) + duration (8) = 82 bytes
        assert_eq!(buf.len(), 82);
    }

    #[test]
    fn test_server_profile_info_deserialize_empty() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1000000); // execution_time_ns
        buf.put_u64_le(1000);     // rows_read
        buf.put_u64_le(1024);     // bytes_read
        buf.put_u64_le(500);      // rows_written
        buf.put_u64_le(512);      // bytes_written
        buf.put_u64_le(0);        // events_count
        
        let profile = ServerProfileInfo::deserialize(&mut buf).unwrap();
        assert_eq!(profile.execution_time_ns, 1000000);
        assert_eq!(profile.rows_read, 1000);
        assert_eq!(profile.bytes_read, 1024);
        assert_eq!(profile.rows_written, 500);
        assert_eq!(profile.bytes_written, 512);
        assert_eq!(profile.profile_events.len(), 0);
    }

    #[test]
    fn test_server_profile_info_deserialize_with_events() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1000000); // execution_time_ns
        buf.put_u64_le(1000);     // rows_read
        buf.put_u64_le(1024);     // bytes_read
        buf.put_u64_le(500);      // rows_written
        buf.put_u64_le(512);      // bytes_written
        buf.put_u64_le(1);        // events_count
        
        // Event: type length (8) + type (10) + count (8) + duration (8)
        buf.put_u64_le(10);       // event type length
        buf.extend_from_slice(b"test_event"); // event type
        buf.put_u64_le(100);      // count
        buf.put_u64_le(1000000);  // duration
        
        let profile = ServerProfileInfo::deserialize(&mut buf).unwrap();
        assert_eq!(profile.profile_events.len(), 1);
        let event = profile.profile_events.get("test_event").unwrap();
        assert_eq!(event.count, 100);
        assert_eq!(event.duration_ns, 1000000);
    }

    #[test]
    fn test_server_profile_info_round_trip() {
        let mut original = ServerProfileInfo::new(1000000, 1000, 1024, 500, 512);
        let event = ProfileEvent::new("test_event".to_string(), 100, 1000000);
        original.add_profile_event(event);
        
        let mut buf = BytesMut::new();
        original.serialize(&mut buf).unwrap();
        let deserialized = ServerProfileInfo::deserialize(&mut buf).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_server_profile_info_deserialize_insufficient_data() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // Only 4 bytes, need 40
        
        let result = ServerProfileInfo::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }
}
