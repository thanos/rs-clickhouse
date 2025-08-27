//! String data types for ClickHouse

use super::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// FixedString type (fixed length)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FixedString(pub Vec<u8>);

impl FixedString {
    /// Create a new FixedString with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        FixedString(Vec::with_capacity(capacity))
    }

    /// Create a new FixedString from bytes
    pub fn new(bytes: Vec<u8>) -> Self {
        FixedString(bytes)
    }

    /// Get the length of the fixed string
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the fixed string is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get the capacity of the fixed string
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Get the underlying bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get mutable access to the underlying bytes
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.0
    }

    /// Resize the fixed string
    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.0.resize(new_len, value);
    }

    /// Truncate the fixed string
    pub fn truncate(&mut self, new_len: usize) {
        self.0.truncate(new_len);
    }

    /// Clear the fixed string
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Push a byte to the fixed string
    pub fn push(&mut self, byte: u8) {
        self.0.push(byte);
    }

    /// Extend the fixed string with bytes
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

// Implement Display for all string types
impl fmt::Display for FixedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "<binary data>"),
        }
    }
}

// Implement From traits for conversions
impl From<&[u8]> for FixedString {
    fn from(value: &[u8]) -> Self {
        FixedString(value.to_vec())
    }
}

impl From<Vec<u8>> for FixedString {
    fn from(value: Vec<u8>) -> Self {
        FixedString(value)
    }
}

// Implement TryFrom for Value conversions
impl TryFrom<Value> for FixedString {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::FixedString(bytes) => Ok(FixedString(bytes.to_vec())),
            _ => Err(format!("Cannot convert {} to FixedString", value.type_name())),
        }
    }
}

// Implement Default traits
impl Default for FixedString {
    fn default() -> Self {
        FixedString(Vec::new())
    }
}

// Implement Deref for convenient access
impl std::ops::Deref for FixedString {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement AsRef for convenient conversions
impl AsRef<[u8]> for FixedString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// Implement comparison traits
impl PartialEq<[u8]> for FixedString {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

impl From<FixedString> for Vec<u8> {
    fn from(value: FixedString) -> Self {
        value.0
    }
}
