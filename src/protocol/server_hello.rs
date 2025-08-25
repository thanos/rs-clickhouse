//! Server Hello message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Server Hello message received when establishing a connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHello {
    /// Server name (e.g., "ClickHouse")
    pub server_name: String,
    /// Server version major
    pub server_version_major: u64,
    /// Server version minor
    pub server_version_minor: u64,
    /// Server version patch
    pub server_version_patch: u64,
    /// Server revision
    pub server_revision: u64,
    /// Protocol version
    pub protocol_version: u64,
    /// Server timezone
    pub timezone: String,
    /// Server display name
    pub display_name: String,
    /// Server version patch
    pub version_patch: u64,
    /// Server revision
    pub revision: u64,
    /// Server timezone
    pub timezone_name: String,
    /// Server display name
    pub display_name_full: String,
    /// Server version patch
    pub version_patch_full: u64,
    /// Server revision
    pub revision_full: u64,
    /// Server timezone
    pub timezone_name_full: String,
    /// Server display name
    pub display_name_short: String,
    /// Server version patch
    pub version_patch_short: u64,
    /// Server revision
    pub revision_short: u64,
    /// Server timezone
    pub timezone_name_short: String,
}

impl ServerHello {
    /// Create a new Server Hello message
    pub fn new(
        server_name: impl Into<String>,
        server_version_major: u64,
        server_version_minor: u64,
        server_version_patch: u64,
        server_revision: u64,
        protocol_version: u64,
        timezone: impl Into<String>,
        display_name: impl Into<String>,
    ) -> Self {
        let server_name = server_name.into();
        let timezone = timezone.into();
        let display_name = display_name.into();
        
        Self {
            server_name: server_name.clone(),
            server_version_major,
            server_version_minor,
            server_version_patch,
            server_revision,
            protocol_version,
            timezone: timezone.clone(),
            display_name: display_name.clone(),
            version_patch: server_version_patch,
            revision: server_revision,
            timezone_name: timezone.clone(),
            display_name_full: display_name.clone(),
            version_patch_full: server_version_patch,
            revision_full: server_revision,
            timezone_name_full: timezone.clone(),
            display_name_short: display_name.clone(),
            version_patch_short: server_version_patch,
            revision_short: server_revision,
            timezone_name_short: timezone,
        }
    }

    /// Get the server version string
    pub fn server_version_string(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.server_version_major,
            self.server_version_minor,
            self.server_version_patch,
            self.server_revision
        )
    }

    /// Get the protocol version string
    pub fn protocol_version_string(&self) -> String {
        format!("{}", self.protocol_version)
    }

    /// Check if the server version is compatible with the client
    pub fn is_compatible_with(&self, client_protocol_version: u64) -> bool {
        self.protocol_version >= client_protocol_version
    }

    /// Get the server display name
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Get the server timezone
    pub fn timezone(&self) -> &str {
        &self.timezone
    }
}

impl Packet for ServerHello {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerHello
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write server name
        buf.put_u64_le(self.server_name.len() as u64);
        buf.extend_from_slice(self.server_name.as_bytes());

        // Write server version
        buf.put_u64_le(self.server_version_major);
        buf.put_u64_le(self.server_version_minor);
        buf.put_u64_le(self.server_version_patch);
        buf.put_u64_le(self.server_revision);

        // Write protocol version
        buf.put_u64_le(self.protocol_version);

        // Write timezone
        buf.put_u64_le(self.timezone.len() as u64);
        buf.extend_from_slice(self.timezone.as_bytes());

        // Write display name
        buf.put_u64_le(self.display_name.len() as u64);
        buf.extend_from_slice(self.display_name.as_bytes());

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read server name
        let name_len = buf.get_u64_le() as usize;
        if buf.remaining() < name_len {
            return Err(Error::Protocol("Insufficient data for server name".to_string()));
        }
        let server_name = String::from_utf8_lossy(&buf.copy_to_bytes(name_len)).to_string();

        // Read server version
        let server_version_major = buf.get_u64_le();
        let server_version_minor = buf.get_u64_le();
        let server_version_patch = buf.get_u64_le();
        let server_revision = buf.get_u64_le();

        // Read protocol version
        let protocol_version = buf.get_u64_le();

        // Read timezone
        let timezone_len = buf.get_u64_le() as usize;
        if buf.remaining() < timezone_len {
            return Err(Error::Protocol("Insufficient data for timezone".to_string()));
        }
        let timezone = String::from_utf8_lossy(&buf.copy_to_bytes(timezone_len)).to_string();

        // Read display name
        let display_len = buf.get_u64_le() as usize;
        if buf.remaining() < display_len {
            return Err(Error::Protocol("Insufficient data for display name".to_string()));
        }
        let display_name = String::from_utf8_lossy(&buf.copy_to_bytes(display_len)).to_string();

        Ok(Self {
            server_name,
            server_version_major,
            server_version_minor,
            server_version_patch,
            server_revision,
            protocol_version,
            timezone: timezone.clone(),
            display_name: display_name.clone(),
            version_patch: server_version_patch,
            revision: server_revision,
            timezone_name: timezone.clone(),
            display_name_full: display_name.clone(),
            version_patch_full: server_version_patch,
            revision_full: server_revision,
            timezone_name_full: timezone.clone(),
            display_name_short: display_name.clone(),
            version_patch_short: server_version_patch,
            revision_short: server_revision,
            timezone_name_short: timezone,
        })
    }
}

impl Default for ServerHello {
    fn default() -> Self {
        Self::new(
            "ClickHouse",
            22,
            8,
            0,
            0,
            super::constants::DEFAULT_PROTOCOL_VERSION,
            "UTC",
            "ClickHouse",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Packet;

    #[test]
    fn test_server_hello_new() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert_eq!(hello.server_name, "TestServer");
        assert_eq!(hello.server_version_major, 1);
        assert_eq!(hello.server_version_minor, 2);
        assert_eq!(hello.server_version_patch, 3);
        assert_eq!(hello.server_revision, 4);
        assert_eq!(hello.protocol_version, 54328);
        assert_eq!(hello.timezone, "UTC");
        assert_eq!(hello.display_name, "Test Server");
    }

    #[test]
    fn test_server_hello_server_version_string() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert_eq!(hello.server_version_string(), "1.2.3.4");
    }

    #[test]
    fn test_server_hello_protocol_version_string() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert_eq!(hello.protocol_version_string(), "54328");
    }

    #[test]
    fn test_server_hello_is_compatible_with() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert!(hello.is_compatible_with(54328));
        assert!(hello.is_compatible_with(54327));
        assert!(!hello.is_compatible_with(54329));
    }

    #[test]
    fn test_server_hello_display_name() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert_eq!(hello.display_name(), "Test Server");
    }

    #[test]
    fn test_server_hello_timezone() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert_eq!(hello.timezone(), "UTC");
    }

    #[test]
    fn test_server_hello_packet_type() {
        let hello = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");
        assert_eq!(hello.packet_type(), PacketType::ServerHello);
    }

    #[test]
    fn test_server_hello_serialize_deserialize() {
        let original = ServerHello::new("TestServer", 1, 2, 3, 4, 54328, "UTC", "Test Server");

        let mut buf = BytesMut::new();
        Packet::serialize(&original, &mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = <ServerHello as Packet>::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.server_name, deserialized.server_name);
        assert_eq!(original.server_version_major, deserialized.server_version_major);
        assert_eq!(original.server_version_minor, deserialized.server_version_minor);
        assert_eq!(original.server_version_patch, deserialized.server_version_patch);
        assert_eq!(original.server_revision, deserialized.server_revision);
        assert_eq!(original.protocol_version, deserialized.protocol_version);
        assert_eq!(original.timezone, deserialized.timezone);
        assert_eq!(original.display_name, deserialized.display_name);
    }
}
