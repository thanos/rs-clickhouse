//! FixedString type for ClickHouse
//! 
//! FixedString stores strings of a fixed length, padding shorter strings with null bytes
//! and truncating longer strings to fit the specified size.

use serde::{Deserialize, Serialize};
use std::fmt;

/// FixedString type that stores strings of a fixed length
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FixedString {
    /// The fixed length of the string
    length: usize,
    /// The string data, padded to the fixed length
    data: Vec<u8>,
}

impl FixedString {
    /// Create a new FixedString with the specified length
    pub fn new(length: usize) -> Self {
        Self {
            length,
            data: vec![0; length],
        }
    }

    /// Create a FixedString from a string, padding or truncating as needed
    pub fn from_string(s: &str, length: usize) -> Self {
        let mut data = vec![0; length];
        let bytes = s.as_bytes();
        let copy_len = std::cmp::min(bytes.len(), length);
        data[..copy_len].copy_from_slice(&bytes[..copy_len]);
        
        Self { length, data }
    }

    /// Create a FixedString from bytes, padding or truncating as needed
    pub fn from_bytes(bytes: &[u8], length: usize) -> Self {
        let mut data = vec![0; length];
        let copy_len = std::cmp::min(bytes.len(), length);
        data[..copy_len].copy_from_slice(&bytes[..copy_len]);
        
        Self { length, data }
    }

    /// Get the fixed length of the string
    pub fn length(&self) -> usize {
        self.length
    }

    /// Get the string data as bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get the string data as a string slice, trimming null bytes
    pub fn as_str(&self) -> &str {
        // Find the first non-null byte
        let start = self.data
            .iter()
            .position(|&b| b != 0)
            .unwrap_or(self.length);
        
        // Find the last non-null byte
        let end = self.data
            .iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1)
            .unwrap_or(start);
        
        if start >= end {
            return "";
        }
        
        std::str::from_utf8(&self.data[start..end])
            .unwrap_or("") // Return empty string on invalid UTF-8
    }

    /// Get the string data as a string, trimming null bytes
    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }

    /// Set the string content, padding or truncating as needed
    pub fn set_string(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let copy_len = std::cmp::min(bytes.len(), self.length);
        
        // Clear existing data
        self.data.fill(0);
        
        // Copy new data
        self.data[..copy_len].copy_from_slice(&bytes[..copy_len]);
    }

    /// Set the string content from bytes, padding or truncating as needed
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        let copy_len = std::cmp::min(bytes.len(), self.length);
        
        // Clear existing data
        self.data.fill(0);
        
        // Copy new data
        self.data[..copy_len].copy_from_slice(&bytes[..copy_len]);
    }

    /// Check if the string is empty (all null bytes)
    pub fn is_empty(&self) -> bool {
        self.data.iter().all(|&b| b == 0)
    }

    /// Check if the string contains only null bytes
    pub fn is_null_padded(&self) -> bool {
        self.data.iter().any(|&b| b == 0)
    }

    /// Get the actual length of the string (excluding trailing null bytes)
    pub fn actual_length(&self) -> usize {
        self.data
            .iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1)
            .unwrap_or(0)
    }

    /// Resize the FixedString to a new length
    pub fn resize(&mut self, new_length: usize) {
        if new_length > self.length {
            // Extend with null bytes
            self.data.extend(vec![0; new_length - self.length]);
        } else if new_length < self.length {
            // Truncate
            self.data.truncate(new_length);
        }
        self.length = new_length;
    }

    /// Create a FixedString with the same content but different length
    pub fn with_length(&self, new_length: usize) -> Self {
        let mut new_fs = FixedString::new(new_length);
        let copy_len = std::cmp::min(self.actual_length(), new_length);
        new_fs.data[..copy_len].copy_from_slice(&self.data[..copy_len]);
        new_fs
    }

    /// Get a slice of the FixedString
    pub fn slice(&self, start: usize, end: usize) -> Option<Self> {
        if start > end || end > self.length {
            return None;
        }
        
        let mut new_fs = FixedString::new(end - start);
        new_fs.data.copy_from_slice(&self.data[start..end]);
        Some(new_fs)
    }

    /// Concatenate two FixedStrings
    pub fn concat(&self, other: &FixedString) -> Self {
        let total_length = self.length + other.length;
        let mut new_fs = FixedString::new(total_length);
        
        // Copy first string
        new_fs.data[..self.length].copy_from_slice(&self.data);
        // Copy second string
        new_fs.data[self.length..].copy_from_slice(&other.data);
        
        new_fs
    }

    /// Pad the string to the right with null bytes
    pub fn pad_right(&mut self, target_length: usize) {
        if target_length > self.length {
            self.resize(target_length);
        }
    }

    /// Pad the string to the left with null bytes
    pub fn pad_left(&mut self, target_length: usize) {
        if target_length > self.length {
            let padding = target_length - self.length;
            let mut new_data = vec![0; padding];
            new_data.extend_from_slice(&self.data);
            self.data = new_data;
            self.length = target_length;
        }
    }
}

impl Default for FixedString {
    fn default() -> Self {
        Self::new(0)
    }
}

impl From<String> for FixedString {
    fn from(s: String) -> Self {
        Self::from_string(&s, s.len())
    }
}

impl From<&str> for FixedString {
    fn from(s: &str) -> Self {
        Self::from_string(s, s.len())
    }
}

impl From<Vec<u8>> for FixedString {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_bytes(&bytes, bytes.len())
    }
}

impl From<&[u8]> for FixedString {
    fn from(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes, bytes.len())
    }
}

impl From<FixedString> for String {
    fn from(fs: FixedString) -> Self {
        fs.to_string()
    }
}

impl From<FixedString> for Vec<u8> {
    fn from(fs: FixedString) -> Self {
        fs.data
    }
}

impl fmt::Display for FixedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::ops::Index<usize> for FixedString {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl std::ops::IndexMut<usize> for FixedString {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

impl std::ops::Deref for FixedString {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for FixedString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixedstring_new() {
        let fs = FixedString::new(10);
        assert_eq!(fs.length(), 10);
        assert_eq!(fs.as_bytes(), vec![0; 10]);
        assert!(fs.is_empty());
    }

    #[test]
    fn test_fixedstring_from_string() {
        let fs = FixedString::from_string("hello", 10);
        assert_eq!(fs.length(), 10);
        assert_eq!(fs.as_str(), "hello");
        assert_eq!(fs.actual_length(), 5);
        assert!(fs.is_null_padded());
    }

    #[test]
    fn test_fixedstring_from_string_truncation() {
        let fs = FixedString::from_string("hello world", 5);
        assert_eq!(fs.length(), 5);
        assert_eq!(fs.as_str(), "hello");
        assert_eq!(fs.actual_length(), 5);
        assert!(!fs.is_null_padded());
    }

    #[test]
    fn test_fixedstring_from_bytes() {
        let fs = FixedString::from_bytes(b"hello", 10);
        assert_eq!(fs.length(), 10);
        assert_eq!(fs.as_bytes()[..5], b"hello"[..]);
        assert_eq!(fs.as_bytes()[5..], vec![0; 5]);
    }

    #[test]
    fn test_fixedstring_set_string() {
        let mut fs = FixedString::new(10);
        fs.set_string("world");
        assert_eq!(fs.as_str(), "world");
        assert_eq!(fs.actual_length(), 5);
    }

    #[test]
    fn test_fixedstring_resize() {
        let mut fs = FixedString::from_string("hello", 5);
        fs.resize(10);
        assert_eq!(fs.length(), 10);
        assert_eq!(fs.as_str(), "hello");
        
        fs.resize(3);
        assert_eq!(fs.length(), 3);
        assert_eq!(fs.as_str(), "hel");
    }

    #[test]
    fn test_fixedstring_with_length() {
        let fs = FixedString::from_string("hello", 5);
        let fs_long = fs.with_length(10);
        assert_eq!(fs_long.length(), 10);
        assert_eq!(fs_long.as_str(), "hello");
        
        let fs_short = fs.with_length(3);
        assert_eq!(fs_short.length(), 3);
        assert_eq!(fs_short.as_str(), "hel");
    }

    #[test]
    fn test_fixedstring_slice() {
        let fs = FixedString::from_string("hello world", 11);
        let slice = fs.slice(0, 5).unwrap();
        assert_eq!(slice.as_str(), "hello");
        
        let slice2 = fs.slice(6, 11).unwrap();
        assert_eq!(slice2.as_str(), "world");
    }

    #[test]
    fn test_fixedstring_concat() {
        let fs1 = FixedString::from_string("hello", 5);
        let fs2 = FixedString::from_string("world", 5);
        let concat = fs1.concat(&fs2);
        assert_eq!(concat.length(), 10);
        assert_eq!(concat.as_str(), "helloworld");
    }

    #[test]
    fn test_fixedstring_padding() {
        let mut fs = FixedString::from_string("hello", 5);
        
        fs.pad_right(10);
        assert_eq!(fs.length(), 10);
        assert_eq!(fs.as_str(), "hello");
        assert_eq!(fs.as_bytes().len(), 10);
        
        fs.pad_left(15);
        assert_eq!(fs.length(), 15);
        assert_eq!(fs.as_str(), "hello");
        assert_eq!(fs.as_bytes().len(), 15);
        
        // Check that the string content is preserved in the middle (bytes 5-10)
        assert_eq!(&fs.as_bytes()[5..10], b"hello");
    }

    #[test]
    fn test_fixedstring_conversions() {
        let s = "hello";
        let fs: FixedString = s.into();
        assert_eq!(fs.as_str(), s);
        
        let string: String = fs.into();
        assert_eq!(string, s);
    }

    #[test]
    fn test_fixedstring_indexing() {
        let fs = FixedString::from_string("hello", 5);
        assert_eq!(fs[0], b'h');
        assert_eq!(fs[4], b'o');
    }

    #[test]
    fn test_fixedstring_deref() {
        let fs = FixedString::from_string("hello", 5);
        let bytes: &[u8] = &fs;
        assert_eq!(bytes, b"hello");
    }
}
