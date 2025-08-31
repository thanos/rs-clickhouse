//! Protocol version negotiation implementation

use crate::error::{Error, Result};
use crate::protocol::{Packet, PacketType};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

/// Protocol version information
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolVersion {
    /// Major version number
    pub major: u32,
    /// Minor version number
    pub minor: u32,
    /// Patch version number
    pub patch: u32,
    /// Build number
    pub build: u32,
}

impl ProtocolVersion {
    /// Create a new protocol version
    pub fn new(major: u32, minor: u32, patch: u32, build: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            build,
        }
    }

    /// Create version from string (e.g., "21.8.1.1")
    pub fn from_string(version_str: &str) -> Result<Self> {
        let parts: Vec<&str> = version_str.split('.').collect();
        if parts.len() != 4 {
            return Err(Error::Protocol(format!(
                "Invalid version string format: {} (expected x.y.z.w)",
                version_str
            )));
        }

        let major = parts[0].parse::<u32>()
            .map_err(|e| Error::Protocol(format!("Invalid major version: {}", e)))?;
        let minor = parts[1].parse::<u32>()
            .map_err(|e| Error::Protocol(format!("Invalid minor version: {}", e)))?;
        let patch = parts[2].parse::<u32>()
            .map_err(|e| Error::Protocol(format!("Invalid patch version: {}", e)))?;
        let build = parts[3].parse::<u32>()
            .map_err(|e| Error::Protocol(format!("Invalid build version: {}", e)))?;

        Ok(ProtocolVersion::new(major, minor, patch, build))
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        format!("{}.{}.{}.{}", self.major, self.minor, self.patch, self.build)
    }

    /// Check if this version is compatible with another version
    pub fn is_compatible_with(&self, other: &ProtocolVersion) -> bool {
        // Major versions must match for compatibility
        self.major == other.major
    }

    /// Check if this version is newer than another version
    pub fn is_newer_than(&self, other: &ProtocolVersion) -> bool {
        if self.major != other.major {
            self.major > other.major
        } else if self.minor != other.minor {
            self.minor > other.minor
        } else if self.patch != other.patch {
            self.patch > other.patch
        } else {
            self.build > other.build
        }
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        // Default to ClickHouse protocol version 21.8
        Self::new(21, 8, 0, 0)
    }
}

impl PartialOrd for ProtocolVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProtocolVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.major.cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
            .then(self.build.cmp(&other.build))
    }
}

impl Eq for ProtocolVersion {}

/// Client version negotiation packet
#[derive(Debug, Clone, PartialEq)]
pub struct ClientVersionNegotiation {
    /// Client's preferred protocol version
    pub preferred_version: ProtocolVersion,
    /// List of supported protocol versions (in order of preference)
    pub supported_versions: Vec<ProtocolVersion>,
    /// Client capabilities
    pub capabilities: HashMap<String, String>,
}

impl ClientVersionNegotiation {
    /// Create a new client version negotiation packet
    pub fn new(preferred_version: ProtocolVersion) -> Self {
        let mut supported_versions = vec![preferred_version.clone()];
        
        // Add some common fallback versions
        if preferred_version.major == 21 {
            supported_versions.push(ProtocolVersion::new(21, 7, 0, 0));
            supported_versions.push(ProtocolVersion::new(21, 6, 0, 0));
        }
        
        Self {
            preferred_version,
            supported_versions,
            capabilities: HashMap::new(),
        }
    }

    /// Add a supported version
    pub fn add_supported_version(&mut self, version: ProtocolVersion) {
        if !self.supported_versions.contains(&version) {
            self.supported_versions.push(version);
        }
    }

    /// Add a capability
    pub fn add_capability(&mut self, name: String, value: String) {
        self.capabilities.insert(name, value);
    }

    /// Get a capability value
    pub fn get_capability(&self, name: &str) -> Option<&String> {
        self.capabilities.get(name)
    }
}

impl Packet for ClientVersionNegotiation {
    fn packet_type(&self) -> PacketType {
        // Note: This would need a new packet type in the enum
        // For now, we'll use ClientHello as a placeholder
        PacketType::ClientHello
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize preferred version
        buf.put_u32_le(self.preferred_version.major);
        buf.put_u32_le(self.preferred_version.minor);
        buf.put_u32_le(self.preferred_version.patch);
        buf.put_u32_le(self.preferred_version.build);
        
        // Serialize supported versions count
        buf.put_u64_le(self.supported_versions.len() as u64);
        
        // Serialize each supported version
        for version in &self.supported_versions {
            buf.put_u32_le(version.major);
            buf.put_u32_le(version.minor);
            buf.put_u32_le(version.patch);
            buf.put_u32_le(version.build);
        }
        
        // Serialize capabilities count
        buf.put_u64_le(self.capabilities.len() as u64);
        
        // Serialize each capability
        for (name, value) in &self.capabilities {
            let name_bytes = name.as_bytes();
            let value_bytes = value.as_bytes();
            
            buf.put_u64_le(name_bytes.len() as u64);
            buf.extend_from_slice(name_bytes);
            buf.put_u64_le(value_bytes.len() as u64);
            buf.extend_from_slice(value_bytes);
        }
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 16 {
            return Err(Error::Protocol("Insufficient data for ClientVersionNegotiation packet".to_string()));
        }

        // Read preferred version
        let major = buf.get_u32_le();
        let minor = buf.get_u32_le();
        let patch = buf.get_u32_le();
        let build = buf.get_u32_le();
        let preferred_version = ProtocolVersion::new(major, minor, patch, build);
        
        // Read supported versions count
        let versions_count = buf.get_u64_le() as usize;
        
        // Read supported versions
        let mut supported_versions = Vec::new();
        for _ in 0..versions_count {
            if buf.len() < 16 {
                return Err(Error::Protocol("Insufficient data for version info".to_string()));
            }
            
            let major = buf.get_u32_le();
            let minor = buf.get_u32_le();
            let patch = buf.get_u32_le();
            let build = buf.get_u32_le();
            supported_versions.push(ProtocolVersion::new(major, minor, patch, build));
        }
        
        // Read capabilities count
        let capabilities_count = buf.get_u64_le() as usize;
        
        // Read capabilities
        let mut capabilities = HashMap::new();
        for _ in 0..capabilities_count {
            if buf.len() < 16 {
                return Err(Error::Protocol("Insufficient data for capability info".to_string()));
            }
            
            // Read capability name
            let name_len = buf.get_u64_le() as usize;
            if name_len > buf.len() {
                return Err(Error::Protocol("Invalid capability name length".to_string()));
            }
            let name_bytes = buf.copy_to_bytes(name_len);
            let name = String::from_utf8(name_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in capability name: {}", e)))?;
            
            // Read capability value
            let value_len = buf.get_u64_le() as usize;
            if value_len > buf.len() {
                return Err(Error::Protocol("Invalid capability value length".to_string()));
            }
            let value_bytes = buf.copy_to_bytes(value_len);
            let value = String::from_utf8(value_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in capability value: {}", e)))?;
            
            capabilities.insert(name, value);
        }
        
        Ok(ClientVersionNegotiation {
            preferred_version,
            supported_versions,
            capabilities,
        })
    }
}

/// Server version negotiation response packet
#[derive(Debug, Clone, PartialEq)]
pub struct ServerVersionNegotiation {
    /// Selected protocol version
    pub selected_version: ProtocolVersion,
    /// Server capabilities
    pub capabilities: HashMap<String, String>,
    /// Negotiation result message
    pub message: String,
}

impl ServerVersionNegotiation {
    /// Create a new server version negotiation response
    pub fn new(selected_version: ProtocolVersion) -> Self {
        Self {
            selected_version,
            capabilities: HashMap::new(),
            message: "Version negotiation successful".to_string(),
        }
    }

    /// Add a server capability
    pub fn add_capability(&mut self, name: String, value: String) {
        self.capabilities.insert(name, value);
    }

    /// Set the negotiation message
    pub fn set_message(&mut self, message: String) {
        self.message = message;
    }
}

impl Packet for ServerVersionNegotiation {
    fn packet_type(&self) -> PacketType {
        // Note: This would need a new packet type in the enum
        // For now, we'll use ServerHello as a placeholder
        PacketType::ServerHello
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize selected version
        buf.put_u32_le(self.selected_version.major);
        buf.put_u32_le(self.selected_version.minor);
        buf.put_u32_le(self.selected_version.patch);
        buf.put_u32_le(self.selected_version.build);
        
        // Serialize capabilities count
        buf.put_u64_le(self.capabilities.len() as u64);
        
        // Serialize each capability
        for (name, value) in &self.capabilities {
            let name_bytes = name.as_bytes();
            let value_bytes = value.as_bytes();
            
            buf.put_u64_le(name_bytes.len() as u64);
            buf.extend_from_slice(name_bytes);
            buf.put_u64_le(value_bytes.len() as u64);
            buf.extend_from_slice(value_bytes);
        }
        
        // Serialize message
        let message_bytes = self.message.as_bytes();
        buf.put_u64_le(message_bytes.len() as u64);
        buf.extend_from_slice(message_bytes);
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 16 {
            return Err(Error::Protocol("Insufficient data for ServerVersionNegotiation packet".to_string()));
        }

        // Read selected version
        let major = buf.get_u32_le();
        let minor = buf.get_u32_le();
        let patch = buf.get_u32_le();
        let build = buf.get_u32_le();
        let selected_version = ProtocolVersion::new(major, minor, patch, build);
        
        // Read capabilities count
        let capabilities_count = buf.get_u64_le() as usize;
        
        // Read capabilities
        let mut capabilities = HashMap::new();
        for _ in 0..capabilities_count {
            if buf.len() < 16 {
                return Err(Error::Protocol("Insufficient data for capability info".to_string()));
            }
            
            // Read capability name
            let name_len = buf.get_u64_le() as usize;
            if name_len > buf.len() {
                return Err(Error::Protocol("Invalid capability name length".to_string()));
            }
            let name_bytes = buf.copy_to_bytes(name_len);
            let name = String::from_utf8(name_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in capability name: {}", e)))?;
            
            // Read capability value
            let value_len = buf.get_u64_le() as usize;
            if value_len > buf.len() {
                return Err(Error::Protocol("Invalid capability value length".to_string()));
            }
            let value_bytes = buf.copy_to_bytes(value_len);
            let value = String::from_utf8(value_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in capability value: {}", e)))?;
            
            capabilities.insert(name, value);
        }
        
        // Read message
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for message".to_string()));
        }
        let message_len = buf.get_u64_le() as usize;
        if message_len > buf.len() {
            return Err(Error::Protocol("Invalid message length".to_string()));
        }
        let message_bytes = buf.copy_to_bytes(message_len);
        let message = String::from_utf8(message_bytes.to_vec())
            .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in message: {}", e)))?;
        
        Ok(ServerVersionNegotiation {
            selected_version,
            capabilities,
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_protocol_version_new() {
        let version = ProtocolVersion::new(21, 8, 1, 1);
        assert_eq!(version.major, 21);
        assert_eq!(version.minor, 8);
        assert_eq!(version.patch, 1);
        assert_eq!(version.build, 1);
    }

    #[test]
    fn test_protocol_version_from_string() {
        let version = ProtocolVersion::from_string("21.8.1.1").unwrap();
        assert_eq!(version.major, 21);
        assert_eq!(version.minor, 8);
        assert_eq!(version.patch, 1);
        assert_eq!(version.build, 1);
    }

    #[test]
    fn test_protocol_version_to_string() {
        let version = ProtocolVersion::new(21, 8, 1, 1);
        assert_eq!(version.to_string(), "21.8.1.1");
    }

    #[test]
    fn test_protocol_version_compatibility() {
        let v1 = ProtocolVersion::new(21, 8, 1, 1);
        let v2 = ProtocolVersion::new(21, 7, 0, 0);
        let v3 = ProtocolVersion::new(22, 1, 0, 0);
        
        assert!(v1.is_compatible_with(&v2));
        assert!(!v1.is_compatible_with(&v3));
    }

    #[test]
    fn test_protocol_version_ordering() {
        let v1 = ProtocolVersion::new(21, 8, 1, 1);
        let v2 = ProtocolVersion::new(21, 8, 1, 2);
        let v3 = ProtocolVersion::new(21, 9, 0, 0);
        
        assert!(v2.is_newer_than(&v1));
        assert!(v3.is_newer_than(&v1));
        assert!(!v1.is_newer_than(&v2));
    }

    #[test]
    fn test_client_version_negotiation_new() {
        let version = ProtocolVersion::new(21, 8, 1, 1);
        let negotiation = ClientVersionNegotiation::new(version.clone());
        
        assert_eq!(negotiation.preferred_version, version);
        assert!(!negotiation.supported_versions.is_empty());
        assert!(negotiation.capabilities.is_empty());
    }

    #[test]
    fn test_client_version_negotiation_add_capability() {
        let mut negotiation = ClientVersionNegotiation::new(ProtocolVersion::default());
        negotiation.add_capability("compression".to_string(), "lz4".to_string());
        
        assert_eq!(negotiation.get_capability("compression"), Some(&"lz4".to_string()));
    }

    #[test]
    fn test_server_version_negotiation_new() {
        let version = ProtocolVersion::new(21, 8, 1, 1);
        let negotiation = ServerVersionNegotiation::new(version.clone());
        
        assert_eq!(negotiation.selected_version, version);
        assert!(negotiation.capabilities.is_empty());
        assert_eq!(negotiation.message, "Version negotiation successful");
    }

    #[test]
    fn test_version_negotiation_round_trip() {
        let mut client_negotiation = ClientVersionNegotiation::new(ProtocolVersion::new(21, 8, 1, 1));
        client_negotiation.add_capability("compression".to_string(), "lz4".to_string());
        
        let mut buf = BytesMut::new();
        client_negotiation.serialize(&mut buf).unwrap();
        let deserialized = ClientVersionNegotiation::deserialize(&mut buf).unwrap();
        
        assert_eq!(client_negotiation, deserialized);
    }
}
