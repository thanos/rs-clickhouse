//! Server Pong message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Server Pong message for ping-pong communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerPong {
    /// Pong timestamp in nanoseconds
    pub timestamp: u64,
    /// Server uptime in seconds
    pub uptime: u64,
    /// Server version
    pub version: String,
    /// Server name
    pub server_name: String,
}

impl ServerPong {
    /// Create a new Server Pong message
    pub fn new(timestamp: u64, uptime: u64, version: impl Into<String>, server_name: impl Into<String>) -> Self {
        Self {
            timestamp,
            uptime,
            version: version.into(),
            server_name: server_name.into(),
        }
    }

    /// Create a new Server Pong message with current timestamp
    pub fn now(uptime: u64, version: impl Into<String>, server_name: impl Into<String>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        Self::new(timestamp, uptime, version, server_name)
    }

    /// Get the timestamp in nanoseconds
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get the timestamp in seconds
    pub fn timestamp_seconds(&self) -> u64 {
        self.timestamp / 1_000_000_000
    }

    /// Get the timestamp in milliseconds
    pub fn timestamp_millis(&self) -> u64 {
        self.timestamp / 1_000_000
    }

    /// Get the timestamp in microseconds
    pub fn timestamp_micros(&self) -> u64 {
        self.timestamp / 1_000
    }

    /// Get the uptime in seconds
    pub fn uptime(&self) -> u64 {
        self.uptime
    }

    /// Get the uptime in minutes
    pub fn uptime_minutes(&self) -> u64 {
        self.uptime / 60
    }

    /// Get the uptime in hours
    pub fn uptime_hours(&self) -> u64 {
        self.uptime / 3600
    }

    /// Get the uptime in days
    pub fn uptime_days(&self) -> u64 {
        self.uptime / 86400
    }

    /// Get the server version
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the server name
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Calculate round-trip time if we have the ping timestamp
    pub fn round_trip_time(&self, ping_timestamp: u64) -> u64 {
        if self.timestamp >= ping_timestamp {
            self.timestamp - ping_timestamp
        } else {
            0
        }
    }

    /// Calculate round-trip time in milliseconds
    pub fn round_trip_time_millis(&self, ping_timestamp: u64) -> u64 {
        self.round_trip_time(ping_timestamp) / 1_000_000
    }

    /// Calculate round-trip time in microseconds
    pub fn round_trip_time_micros(&self, ping_timestamp: u64) -> u64 {
        self.round_trip_time(ping_timestamp) / 1_000
    }

    /// Format uptime as a human-readable string
    pub fn uptime_formatted(&self) -> String {
        let days = self.uptime_days();
        let hours = (self.uptime % 86400) / 3600;
        let minutes = (self.uptime % 3600) / 60;
        let seconds = self.uptime % 60;

        if days > 0 {
            format!("{}d {}h {}m {}s", days, hours, minutes, seconds)
        } else if hours > 0 {
            format!("{}h {}m {}s", hours, minutes, seconds)
        } else if minutes > 0 {
            format!("{}m {}s", minutes, seconds)
        } else {
            format!("{}s", seconds)
        }
    }

    /// Format timestamp as a human-readable string
    pub fn timestamp_formatted(&self) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let duration = std::time::Duration::from_nanos(self.timestamp);
        let datetime = UNIX_EPOCH + duration;
        
        // Format as ISO 8601
        let datetime: chrono::DateTime<chrono::Utc> = chrono::DateTime::from(datetime);
        datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }
}

impl Packet for ServerPong {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerPong
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write timestamp
        buf.put_u64_le(self.timestamp);

        // Write uptime
        buf.put_u64_le(self.uptime);

        // Write version
        buf.put_u64_le(self.version.len() as u64);
        buf.extend_from_slice(self.version.as_bytes());

        // Write server name
        buf.put_u64_le(self.server_name.len() as u64);
        buf.extend_from_slice(self.server_name.as_bytes());

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read timestamp
        let timestamp = buf.get_u64_le();

        // Read uptime
        let uptime = buf.get_u64_le();

        // Read version
        let version_len = buf.get_u64_le() as usize;
        if buf.remaining() < version_len {
            return Err(Error::Protocol("Insufficient data for version".to_string()));
        }
        let version = String::from_utf8_lossy(&buf.copy_to_bytes(version_len)).to_string();

        // Read server name
        let server_name_len = buf.get_u64_le() as usize;
        if buf.remaining() < server_name_len {
            return Err(Error::Protocol("Insufficient data for server name".to_string()));
        }
        let server_name = String::from_utf8_lossy(&buf.copy_to_bytes(server_name_len)).to_string();

        Ok(Self {
            timestamp,
            uptime,
            version,
            server_name,
        })
    }
}

impl Default for ServerPong {
    fn default() -> Self {
        Self::new(
            0,
            0,
            "unknown",
            "unknown",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Packet;

    #[test]
    fn test_server_pong_new() {
        let pong = ServerPong::new(1000000000, 3600, "1.0.0", "TestServer");
        assert_eq!(pong.timestamp(), 1000000000);
        assert_eq!(pong.uptime(), 3600);
        assert_eq!(pong.version(), "1.0.0");
        assert_eq!(pong.server_name(), "TestServer");
    }

    #[test]
    fn test_server_pong_timestamp_conversions() {
        let pong = ServerPong::new(1_500_000_000, 3600, "1.0.0", "TestServer");
        assert_eq!(pong.timestamp_seconds(), 1);
        assert_eq!(pong.timestamp_millis(), 1500);
        assert_eq!(pong.timestamp_micros(), 1500000);
    }

    #[test]
    fn test_server_pong_uptime_conversions() {
        let pong = ServerPong::new(1000000000, 90061, "1.0.0", "TestServer");
        assert_eq!(pong.uptime_minutes(), 1501);
        assert_eq!(pong.uptime_hours(), 25);
        assert_eq!(pong.uptime_days(), 1);
    }

    #[test]
    fn test_server_pong_round_trip_time() {
        let pong = ServerPong::new(1000000000, 3600, "1.0.0", "TestServer");
        let ping_timestamp = 999000000;
        assert_eq!(pong.round_trip_time(ping_timestamp), 1000000);
        assert_eq!(pong.round_trip_time_millis(ping_timestamp), 1);
        assert_eq!(pong.round_trip_time_micros(ping_timestamp), 1000);
    }

    #[test]
    fn test_server_pong_uptime_formatted() {
        let pong = ServerPong::new(1000000000, 90061, "1.0.0", "TestServer");
        assert_eq!(pong.uptime_formatted(), "1d 1h 1m 1s");

        let pong = ServerPong::new(1000000000, 3661, "1.0.0", "TestServer");
        assert_eq!(pong.uptime_formatted(), "1h 1m 1s");

        let pong = ServerPong::new(1000000000, 61, "1.0.0", "TestServer");
        assert_eq!(pong.uptime_formatted(), "1m 1s");

        let pong = ServerPong::new(1000000000, 30, "1.0.0", "TestServer");
        assert_eq!(pong.uptime_formatted(), "30s");
    }

    #[test]
    fn test_server_pong_packet_type() {
        let pong = ServerPong::new(1000000000, 3600, "1.0.0", "TestServer");
        assert_eq!(pong.packet_type(), PacketType::ServerPong);
    }

    #[test]
    fn test_server_pong_serialize_deserialize() {
        let original = ServerPong::new(1000000000, 3600, "1.0.0", "TestServer");

        let mut buf = BytesMut::new();
        Packet::serialize(&original, &mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = <ServerPong as Packet>::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.timestamp, deserialized.timestamp);
        assert_eq!(original.uptime, deserialized.uptime);
        assert_eq!(original.version, deserialized.version);
        assert_eq!(original.server_name, deserialized.server_name);
    }

    #[test]
    fn test_server_pong_round_trip_time_edge_cases() {
        let pong = ServerPong::new(1000000000, 3600, "1.0.0", "TestServer");
        
        // Same timestamp
        assert_eq!(pong.round_trip_time(1000000000), 0);
        
        // Future timestamp (should return 0)
        assert_eq!(pong.round_trip_time(2000000000), 0);
    }
}
