//! String data types for ClickHouse

use super::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// String type (variable length)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct String(pub std::string::String);

/// FixedString type (fixed length)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FixedString(pub Vec<u8>);

/// LowCardinality type (optimized for repeated values)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LowCardinality(pub std::string::String);

impl String {
    /// Create a new String from a string slice
    pub fn new(value: impl Into<std::string::String>) -> Self {
        String(value.into())
    }

    /// Get the underlying string
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the length of the string
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the string is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert to bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Convert to owned string
    pub fn into_inner(self) -> std::string::String {
        self.0
    }
}

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

impl LowCardinality {
    /// Create a new LowCardinality string
    pub fn new(value: impl Into<std::string::String>) -> Self {
        LowCardinality(value.into())
    }

    /// Get the underlying string
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the length of the string
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the string is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert to owned string
    pub fn into_inner(self) -> std::string::String {
        self.0
    }
}

// Implement Display for all string types
impl fmt::Display for String {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for FixedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "<binary data>"),
        }
    }
}

impl fmt::Display for LowCardinality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement From traits for conversions
impl From<&str> for String {
    fn from(value: &str) -> Self {
        String(value.to_string())
    }
}

impl From<std::string::String> for String {
    fn from(value: std::string::String) -> Self {
        String(value)
    }
}

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

impl From<&str> for LowCardinality {
    fn from(value: &str) -> Self {
        LowCardinality(value.to_string())
    }
}

impl From<std::string::String> for LowCardinality {
    fn from(value: std::string::String) -> Self {
        LowCardinality(value)
    }
}

// Implement TryFrom for Value conversions
impl TryFrom<Value> for String {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(String(s)),
            Value::FixedString(bytes) => {
                std::string::String::from_utf8(bytes)
                    .map(String)
                    .map_err(|e| String(format!("Invalid UTF-8: {}", e)))
            }
            Value::UInt8(v) => Ok(String(v.to_string())),
            Value::UInt16(v) => Ok(String(v.to_string())),
            Value::UInt32(v) => Ok(String(v.to_string())),
            Value::UInt64(v) => Ok(String(v.to_string())),
            Value::Int8(v) => Ok(String(v.to_string())),
            Value::Int16(v) => Ok(String(v.to_string())),
            Value::Int32(v) => Ok(String(v.to_string())),
            Value::Int64(v) => Ok(String(v.to_string())),
            Value::Float32(v) => Ok(String(v.to_string())),
            Value::Float64(v) => Ok(String(v.to_string())),
            Value::Date(d) => Ok(String(d.format("%Y-%m-%d").to_string())),
            Value::DateTime(dt) => Ok(String(dt.format("%Y-%m-%d %H:%M:%S").to_string())),
            Value::DateTime64(dt) => Ok(String(dt.format("%Y-%m-%d %H:%M:%S").to_string())),
            Value::UUID(u) => Ok(String(u.to_string())),
            _ => Err(String(format!("Cannot convert {} to String", value.type_name()))),
        }
    }
}

impl TryFrom<Value> for FixedString {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::FixedString(bytes) => Ok(FixedString(bytes)),
            Value::String(s) => Ok(FixedString(s.into_bytes())),
            Value::UInt8(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::UInt16(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::UInt32(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::UInt64(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::Int8(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::Int16(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::Int32(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::Int64(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::Float32(v) => Ok(FixedString(v.to_string().into_bytes())),
            Value::Float64(v) => Ok(FixedString(v.to_string().into_bytes())),
            _ => Err(String(format!("Cannot convert {} to FixedString", value.type_name()))),
        }
    }
}

impl TryFrom<Value> for LowCardinality {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(LowCardinality(s)),
            Value::FixedString(bytes) => {
                std::string::String::from_utf8(bytes)
                    .map(LowCardinality)
                    .map_err(|e| String(format!("Invalid UTF-8: {}", e)))
            }
            Value::UInt8(v) => Ok(LowCardinality(v.to_string())),
            Value::UInt16(v) => Ok(LowCardinality(v.to_string())),
            Value::UInt32(v) => Ok(LowCardinality(v.to_string())),
            Value::UInt64(v) => Ok(LowCardinality(v.to_string())),
            Value::Int8(v) => Ok(LowCardinality(v.to_string())),
            Value::Int16(v) => Ok(LowCardinality(v.to_string())),
            Value::Int32(v) => Ok(LowCardinality(v.to_string())),
            Value::Int64(v) => Ok(LowCardinality(v.to_string())),
            Value::Float32(v) => Ok(LowCardinality(v.to_string())),
            Value::Float64(v) => Ok(LowCardinality(v.to_string())),
            _ => Err(String(format!("Cannot convert {} to LowCardinality", value.type_name()))),
        }
    }
}

// Implement Default traits
impl Default for String {
    fn default() -> Self {
        String(std::string::String::new())
    }
}

impl Default for FixedString {
    fn default() -> Self {
        FixedString(Vec::new())
    }
}

impl Default for LowCardinality {
    fn default() -> Self {
        LowCardinality(std::string::String::new())
    }
}

// Implement Deref for convenient access
impl std::ops::Deref for String {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::Deref for LowCardinality {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement AsRef for convenient conversions
impl AsRef<str> for String {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for FixedString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<str> for LowCardinality {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// Implement comparison traits
impl PartialEq<str> for String {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<str> for LowCardinality {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<[u8]> for FixedString {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

// Implement conversion traits
impl From<String> for std::string::String {
    fn from(value: String) -> Self {
        value.0
    }
}

impl From<LowCardinality> for std::string::String {
    fn from(value: LowCardinality) -> Self {
        value.0
    }
}

impl From<FixedString> for Vec<u8> {
    fn from(value: FixedString) -> Self {
        value.0
    }
}
