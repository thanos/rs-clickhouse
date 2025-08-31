//! Server log packet implementation

use crate::error::{Error, Result};
use crate::protocol::{Packet, PacketType};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

/// Log level for server log messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Fatal error
    Fatal = 1,
    /// Critical error
    Critical = 2,
    /// Error
    Error = 3,
    /// Warning
    Warning = 4,
    /// Notice
    Notice = 5,
    /// Information
    Information = 6,
    /// Debug
    Debug = 7,
    /// Trace
    Trace = 8,
}

impl LogLevel {
    /// Convert from u8 to LogLevel
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(LogLevel::Fatal),
            2 => Some(LogLevel::Critical),
            3 => Some(LogLevel::Error),
            4 => Some(LogLevel::Warning),
            5 => Some(LogLevel::Notice),
            6 => Some(LogLevel::Information),
            7 => Some(LogLevel::Debug),
            8 => Some(LogLevel::Trace),
            _ => None,
        }
    }

    /// Convert to u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Get the string representation
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Fatal => "Fatal",
            LogLevel::Critical => "Critical",
            LogLevel::Error => "Error",
            LogLevel::Warning => "Warning",
            LogLevel::Notice => "Notice",
            LogLevel::Information => "Information",
            LogLevel::Debug => "Debug",
            LogLevel::Trace => "Trace",
        }
    }

    /// Check if this is an error level
    pub fn is_error(self) -> bool {
        matches!(self, LogLevel::Fatal | LogLevel::Critical | LogLevel::Error)
    }

    /// Check if this is a warning level
    pub fn is_warning(self) -> bool {
        matches!(self, LogLevel::Warning)
    }

    /// Check if this is an info level
    pub fn is_info(self) -> bool {
        matches!(self, LogLevel::Notice | LogLevel::Information)
    }

    /// Check if this is a debug level
    pub fn is_debug(self) -> bool {
        matches!(self, LogLevel::Debug | LogLevel::Trace)
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Server log packet
/// 
/// This packet contains server log messages that can be used
/// for debugging and monitoring server operations.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerLog {
    /// Log level
    pub level: LogLevel,
    /// Log message
    pub message: String,
    /// Source component (e.g., "Query", "Storage", "Network")
    pub source: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Timestamp in nanoseconds since epoch
    pub timestamp_ns: u64,
}

impl ServerLog {
    /// Create a new server log packet
    pub fn new(level: LogLevel, message: String, source: String) -> Self {
        Self {
            level,
            message,
            source,
            metadata: HashMap::new(),
            timestamp_ns: 0,
        }
    }

    /// Create a new server log packet with timestamp
    pub fn with_timestamp(level: LogLevel, message: String, source: String, timestamp_ns: u64) -> Self {
        Self {
            level,
            message,
            source,
            metadata: HashMap::new(),
            timestamp_ns,
        }
    }

    /// Add metadata key-value pair
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Check if has metadata key
    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
    }

    /// Get metadata count
    pub fn metadata_count(&self) -> usize {
        self.metadata.len()
    }

    /// Set timestamp
    pub fn set_timestamp(&mut self, timestamp_ns: u64) {
        self.timestamp_ns = timestamp_ns;
    }

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp_ns
    }
}

impl Packet for ServerLog {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerLog
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Serialize log level
        buf.put_u8(self.level.to_u8());
        
        // Serialize message length and content
        let message_bytes = self.message.as_bytes();
        buf.put_u64_le(message_bytes.len() as u64);
        buf.extend_from_slice(message_bytes);
        
        // Serialize source length and content
        let source_bytes = self.source.as_bytes();
        buf.put_u64_le(source_bytes.len() as u64);
        buf.extend_from_slice(source_bytes);
        
        // Serialize metadata count
        buf.put_u64_le(self.metadata.len() as u64);
        
        // Serialize metadata
        for (key, value) in &self.metadata {
            let key_bytes = key.as_bytes();
            buf.put_u64_le(key_bytes.len() as u64);
            buf.extend_from_slice(key_bytes);
            
            let value_bytes = value.as_bytes();
            buf.put_u64_le(value_bytes.len() as u64);
            buf.extend_from_slice(value_bytes);
        }
        
        // Serialize timestamp
        buf.put_u64_le(self.timestamp_ns);
        
        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        if buf.len() < 1 {
            return Err(Error::Protocol("Insufficient data for ServerLog packet".to_string()));
        }

        // Read log level
        let level_byte = buf.get_u8();
        let level = LogLevel::from_u8(level_byte)
            .ok_or_else(|| Error::Protocol(format!("Invalid log level: {}", level_byte)))?;
        
        // Read message
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for message length".to_string()));
        }
        let message_len = buf.get_u64_le() as usize;
        if message_len > buf.len() {
            return Err(Error::Protocol("Invalid message length".to_string()));
        }
        let message_bytes = buf.copy_to_bytes(message_len);
        let message = String::from_utf8(message_bytes.to_vec())
            .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in message: {}", e)))?;
        
        // Read source
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for source length".to_string()));
        }
        let source_len = buf.get_u64_le() as usize;
        if source_len > buf.len() {
            return Err(Error::Protocol("Invalid source length".to_string()));
        }
        let source_bytes = buf.copy_to_bytes(source_len);
        let source = String::from_utf8(source_bytes.to_vec())
            .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in source: {}", e)))?;
        
        // Read metadata count
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for metadata count".to_string()));
        }
        let metadata_count = buf.get_u64_le() as usize;
        
        // Read metadata
        let mut metadata = HashMap::new();
        for _ in 0..metadata_count {
            if buf.len() < 16 {
                return Err(Error::Protocol("Insufficient data for metadata".to_string()));
            }
            
            // Read key
            let key_len = buf.get_u64_le() as usize;
            if key_len > buf.len() {
                return Err(Error::Protocol("Invalid metadata key length".to_string()));
            }
            let key_bytes = buf.copy_to_bytes(key_len);
            let key = String::from_utf8(key_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in metadata key: {}", e)))?;
            
            // Read value
            let value_len = buf.get_u64_le() as usize;
            if value_len > buf.len() {
                return Err(Error::Protocol("Invalid metadata value length".to_string()));
            }
            let value_bytes = buf.copy_to_bytes(value_len);
            let value = String::from_utf8(value_bytes.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in metadata value: {}", e)))?;
            
            metadata.insert(key, value);
        }
        
        // Read timestamp
        if buf.len() < 8 {
            return Err(Error::Protocol("Insufficient data for timestamp".to_string()));
        }
        let timestamp_ns = buf.get_u64_le();
        
        Ok(ServerLog {
            level,
            message,
            source,
            metadata,
            timestamp_ns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_log_level_conversion() {
        assert_eq!(LogLevel::from_u8(1), Some(LogLevel::Fatal));
        assert_eq!(LogLevel::from_u8(8), Some(LogLevel::Trace));
        assert_eq!(LogLevel::from_u8(0), None);
        assert_eq!(LogLevel::from_u8(9), None);
        
        assert_eq!(LogLevel::Fatal.to_u8(), 1);
        assert_eq!(LogLevel::Trace.to_u8(), 8);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Error.to_string(), "Error");
        assert_eq!(LogLevel::Warning.to_string(), "Warning");
    }

    #[test]
    fn test_log_level_checks() {
        assert!(LogLevel::Fatal.is_error());
        assert!(LogLevel::Warning.is_warning());
        assert!(LogLevel::Information.is_info());
        assert!(LogLevel::Debug.is_debug());
    }

    #[test]
    fn test_server_log_new() {
        let log = ServerLog::new(
            LogLevel::Error,
            "Test error message".to_string(),
            "TestComponent".to_string(),
        );
        
        assert_eq!(log.level, LogLevel::Error);
        assert_eq!(log.message, "Test error message");
        assert_eq!(log.source, "TestComponent");
        assert_eq!(log.metadata_count(), 0);
        assert_eq!(log.timestamp(), 0);
        assert_eq!(log.packet_type(), PacketType::ServerLog);
    }

    #[test]
    fn test_server_log_with_timestamp() {
        let log = ServerLog::with_timestamp(
            LogLevel::Warning,
            "Test warning".to_string(),
            "TestComponent".to_string(),
            1234567890,
        );
        
        assert_eq!(log.timestamp(), 1234567890);
    }

    #[test]
    fn test_server_log_metadata() {
        let mut log = ServerLog::new(
            LogLevel::Information,
            "Test message".to_string(),
            "TestComponent".to_string(),
        );
        
        log.add_metadata("user_id".to_string(), "123".to_string());
        log.add_metadata("session_id".to_string(), "abc".to_string());
        
        assert_eq!(log.metadata_count(), 2);
        assert!(log.has_metadata("user_id"));
        assert_eq!(log.get_metadata("user_id"), Some(&"123".to_string()));
        assert!(!log.has_metadata("nonexistent"));
    }

    #[test]
    fn test_server_log_serialize_deserialize() {
        let mut log = ServerLog::new(
            LogLevel::Error,
            "Test error message".to_string(),
            "TestComponent".to_string(),
        );
        log.add_metadata("error_code".to_string(), "E001".to_string());
        log.set_timestamp(1234567890);
        
        let mut buf = BytesMut::new();
        log.serialize(&mut buf).unwrap();
        let deserialized = ServerLog::deserialize(&mut buf).unwrap();
        
        assert_eq!(log, deserialized);
    }

    #[test]
    fn test_server_log_insufficient_data() {
        let mut buf = BytesMut::new();
        let result = ServerLog::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }

    #[test]
    fn test_server_log_invalid_level() {
        let mut buf = BytesMut::new();
        buf.put_u8(99); // Invalid level
        
        let result = ServerLog::deserialize(&mut buf);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Protocol(_)));
    }
}
