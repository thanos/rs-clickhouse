use std::fmt;
use thiserror::Error;

/// Result type for ClickHouse operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for ClickHouse operations
#[derive(Error, Debug)]
pub enum Error {
    /// Network-related errors
    #[error("Network error: {0}")]
    Network(#[from] std::io::Error),

    /// Protocol errors
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Authentication errors
    #[error("Authentication failed: {0}")]
    Authentication(String),

    /// Query execution errors
    #[error("Query execution failed: {0}")]
    QueryExecution(String),

    /// Data type conversion errors
    #[error("Data type conversion failed: {0}")]
    TypeConversion(String),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Compression errors
    #[error("Compression error: {0}")]
    Compression(String),

    /// Connection pool errors
    #[error("Connection pool error: {0}")]
    ConnectionPool(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Timeout errors
    #[error("Operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// TLS/SSL errors
    #[error("TLS error: {0}")]
    Tls(String),

    /// HTTP errors
    #[error("HTTP error: {status} - {message}")]
    Http { status: u16, message: String },

    /// WebSocket errors
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Invalid data format
    #[error("Invalid data format: {0}")]
    InvalidData(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    /// Internal errors
    #[error("Internal error: {0}")]
    Internal(String),

    /// Custom errors
    #[error("Custom error: {0}")]
    Custom(String),
}

impl Error {
    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Network(_) | Error::Timeout(_) | Error::ConnectionPool(_)
        )
    }

    /// Check if the error is a connection error
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            Error::Network(_) | Error::Authentication(_) | Error::Tls(_)
        )
    }

    /// Get a user-friendly error message
    pub fn user_message(&self) -> String {
        match self {
            Error::Network(e) => format!("Connection failed: {}", e),
            Error::Authentication(msg) => format!("Authentication failed: {}", msg),
            Error::QueryExecution(msg) => format!("Query failed: {}", msg),
            Error::TypeConversion(msg) => format!("Data type error: {}", msg),
            Error::Timeout(duration) => format!("Operation timed out after {:?}", duration),
            Error::Http { status, message } => format!("HTTP error {}: {}", status, message),
            _ => self.to_string(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<postcard::Error> for Error {
    fn from(err: postcard::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Error::Timeout(std::time::Duration::from_secs(0))
    }
}

impl From<tungstenite::Error> for Error {
    fn from(err: tungstenite::Error) -> Self {
        Error::WebSocket(err.to_string())
    }
}

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Error::Configuration(format!("Invalid URL: {}", err))
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Error::Configuration(format!("Invalid address: {}", err))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Compression(err.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(err: uuid::Error) -> Self {
        Error::TypeConversion(format!("Invalid UUID: {}", err))
    }
}

impl From<chrono::ParseError> for Error {
    fn from(err: chrono::ParseError) -> Self {
        Error::TypeConversion(format!("Invalid date/time: {}", err))
    }
}

impl From<chrono::OutOfRangeError> for Error {
    fn from(err: chrono::OutOfRangeError) -> Self {
        Error::TypeConversion(format!("Date/time out of range: {}", err))
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::TypeConversion(format!("Invalid integer: {}", err))
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::TypeConversion(format!("Invalid float: {}", err))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::TypeConversion(format!("Invalid UTF-8: {}", err))
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::Custom(err.to_string())
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Custom(err)
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Custom(err.to_string())
    }
}

/// Error context for adding additional information
#[derive(Debug)]
pub struct ErrorContext {
    pub operation: String,
    pub details: Option<String>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ErrorContext {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            details: None,
            source: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub fn with_source(mut self, source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        self.source = Some(source);
        self
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Operation: {}", self.operation)?;
        if let Some(details) = &self.details {
            write!(f, " - Details: {}", details)?;
        }
        if let Some(source) = &self.source {
            write!(f, " - Source: {}", source)?;
        }
        Ok(())
    }
}

impl std::error::Error for ErrorContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}
