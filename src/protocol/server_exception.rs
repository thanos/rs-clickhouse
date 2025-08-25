//! Server Exception message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Server Exception message for error handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerException {
    /// Exception message
    pub message: String,
    /// Exception code
    pub code: u32,
    /// Exception name
    pub name: String,
    /// Stack trace (optional)
    pub stack_trace: Option<String>,
    /// Nested exception (optional)
    pub nested: Option<Box<ServerException>>,
}

impl ServerException {
    /// Create a new Server Exception message
    pub fn new(message: impl Into<String>, code: u32, name: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code,
            name: name.into(),
            stack_trace: None,
            nested: None,
        }
    }

    /// Set stack trace
    pub fn with_stack_trace(mut self, stack_trace: impl Into<String>) -> Self {
        self.stack_trace = Some(stack_trace.into());
        self
    }

    /// Set nested exception
    pub fn with_nested(mut self, nested: ServerException) -> Self {
        self.nested = Some(Box::new(nested));
        self
    }

    /// Get the exception message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Get the exception code
    pub fn code(&self) -> u32 {
        self.code
    }

    /// Get the exception name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the stack trace
    pub fn stack_trace(&self) -> Option<&str> {
        self.stack_trace.as_deref()
    }

    /// Get the nested exception
    pub fn nested(&self) -> Option<&ServerException> {
        self.nested.as_deref()
    }

    /// Check if the exception has a stack trace
    pub fn has_stack_trace(&self) -> bool {
        self.stack_trace.is_some()
    }

    /// Check if the exception has a nested exception
    pub fn has_nested(&self) -> bool {
        self.nested.is_some()
    }

    /// Convert to a Result error
    pub fn to_error(&self) -> Error {
        Error::QueryExecution(format!("{} ({}): {}", self.name, self.code, self.message))
    }
}

impl Packet for ServerException {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerException
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write exception message
        buf.put_u64_le(self.message.len() as u64);
        buf.extend_from_slice(self.message.as_bytes());

        // Write exception code
        buf.put_u32_le(self.code);

        // Write exception name
        buf.put_u64_le(self.name.len() as u64);
        buf.extend_from_slice(self.name.as_bytes());

        // Write stack trace
        if let Some(ref stack_trace) = self.stack_trace {
            buf.put_u64_le(stack_trace.len() as u64);
            buf.extend_from_slice(stack_trace.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        // Write nested exception
        if let Some(ref nested) = self.nested {
            buf.put_u64_le(1); // Has nested exception
            // Serialize nested exception using bincode
            let nested_bytes = bincode::serialize(nested)?;
            buf.put_u64_le(nested_bytes.len() as u64);
            buf.extend_from_slice(&nested_bytes);
        } else {
            buf.put_u64_le(0); // No nested exception
        }

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read exception message
        let message_len = buf.get_u64_le() as usize;
        if buf.remaining() < message_len {
            return Err(Error::Protocol("Insufficient data for exception message".to_string()));
        }
        let message = String::from_utf8_lossy(&buf.copy_to_bytes(message_len)).to_string();

        // Read exception code
        let code = buf.get_u32_le();

        // Read exception name
        let name_len = buf.get_u64_le() as usize;
        if buf.remaining() < name_len {
            return Err(Error::Protocol("Insufficient data for exception name".to_string()));
        }
        let name = String::from_utf8_lossy(&buf.copy_to_bytes(name_len)).to_string();

        // Read stack trace
        let stack_trace_len = buf.get_u64_le() as usize;
        let stack_trace = if stack_trace_len > 0 {
            if buf.remaining() < stack_trace_len {
                return Err(Error::Protocol("Insufficient data for stack trace".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(stack_trace_len)).to_string())
        } else {
            None
        };

        // Read nested exception
        let has_nested = buf.get_u64_le() != 0;
        let nested = if has_nested {
            let nested_size = buf.get_u64_le() as usize;
            if buf.remaining() < nested_size {
                return Err(Error::Protocol("Insufficient data for nested exception".to_string()));
            }
            let nested_bytes = buf.copy_to_bytes(nested_size);
            Some(Box::new(bincode::deserialize::<ServerException>(&nested_bytes)?))
        } else {
            None
        };

        Ok(Self {
            message,
            code,
            name,
            stack_trace,
            nested,
        })
    }
}

impl std::fmt::Display for ServerException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.name, self.code, self.message)?;
        
        if let Some(ref stack_trace) = self.stack_trace {
            write!(f, "\nStack trace:\n{}", stack_trace)?;
        }
        
        if let Some(ref nested) = self.nested {
            write!(f, "\nNested exception: {}", nested)?;
        }
        
        Ok(())
    }
}

impl std::error::Error for ServerException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.nested.as_ref().map(|e| e.as_ref() as &dyn std::error::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_exception_new() {
        let exception = ServerException::new("Test error", 1001, "TestException");
        assert_eq!(exception.message(), "Test error");
        assert_eq!(exception.code(), 1001);
        assert_eq!(exception.name(), "TestException");
        assert!(!exception.has_stack_trace());
        assert!(!exception.has_nested());
    }

    #[test]
    fn test_server_exception_with_stack_trace() {
        let exception = ServerException::new("Test error", 1001, "TestException")
            .with_stack_trace("at main.rs:10");
        assert!(exception.has_stack_trace());
        assert_eq!(exception.stack_trace(), Some("at main.rs:10"));
    }

    #[test]
    fn test_server_exception_with_nested() {
        let nested = ServerException::new("Nested error", 1002, "NestedException");
        let exception = ServerException::new("Test error", 1001, "TestException")
            .with_nested(nested);
        assert!(exception.has_nested());
        assert_eq!(exception.nested().unwrap().code(), 1002);
    }

    #[test]
    fn test_server_exception_packet_type() {
        let exception = ServerException::new("Test error", 1001, "TestException");
        assert_eq!(exception.packet_type(), PacketType::ServerException);
    }

    #[test]
    fn test_server_exception_to_error() {
        let exception = ServerException::new("Test error", 1001, "TestException");
        let error = exception.to_error();
        match error {
            Error::QueryExecution(msg) => {
                assert!(msg.contains("TestException"));
                assert!(msg.contains("1001"));
                assert!(msg.contains("Test error"));
            }
            _ => panic!("Expected QueryExecution error"),
        }
    }

    #[test]
    fn test_server_exception_display() {
        let exception = ServerException::new("Test error", 1001, "TestException")
            .with_stack_trace("at main.rs:10");
        let display = format!("{}", exception);
        assert!(display.contains("TestException (1001): Test error"));
        assert!(display.contains("Stack trace:"));
        assert!(display.contains("at main.rs:10"));
    }

    #[test]
    fn test_server_exception_serialize_deserialize() {
        let nested = ServerException::new("Nested error", 1002, "NestedException");
        let original = ServerException::new("Test error", 1001, "TestException")
            .with_stack_trace("at main.rs:10")
            .with_nested(nested);

        let mut buf = BytesMut::new();
        original.serialize(&mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = ServerException::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.message, deserialized.message);
        assert_eq!(original.code, deserialized.code);
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.stack_trace, deserialized.stack_trace);
        assert!(original.nested.is_some());
        assert!(deserialized.nested.is_some());
        assert_eq!(original.nested.as_ref().unwrap().code, 
                   deserialized.nested.as_ref().unwrap().code);
    }
}
