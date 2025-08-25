//! Complex data types for ClickHouse

use super::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Array type (homogeneous collection of values)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Array<T>(pub Vec<T>);

/// Nullable type (value that can be null)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Nullable<T>(pub Option<T>);

/// Tuple type (heterogeneous collection of values)
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple(pub Vec<Value>);

/// Map type (key-value pairs)
#[derive(Debug, Clone, PartialEq)]
pub struct Map(pub HashMap<String, Value>);

/// UUID type (128-bit unique identifier)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UUID(pub uuid::Uuid);

impl<T> Array<T> {
    /// Create a new empty array
    pub fn new() -> Self {
        Array(Vec::new())
    }

    /// Create a new array with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Array(Vec::with_capacity(capacity))
    }

    /// Create a new array from a vector
    pub fn from_vec(vec: Vec<T>) -> Self {
        Array(vec)
    }

    /// Get the length of the array
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the array is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get the capacity of the array
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Get a reference to the underlying vector
    pub fn as_vec(&self) -> &Vec<T> {
        &self.0
    }

    /// Get a mutable reference to the underlying vector
    pub fn as_mut_vec(&mut self) -> &mut Vec<T> {
        &mut self.0
    }

    /// Push a value to the array
    pub fn push(&mut self, value: T) {
        self.0.push(value);
    }

    /// Pop a value from the array
    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&T> {
        self.0.get(index)
    }

    /// Get a mutable value by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.0.get_mut(index)
    }

    /// Iterate over the array
    pub fn iter(&self) -> std::slice::Iter<T> {
        self.0.iter()
    }

    /// Iterate over the array mutably
    pub fn iter_mut(&mut self) -> std::slice::IterMut<T> {
        self.0.iter_mut()
    }

    /// Clear the array
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Reserve additional capacity
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Shrink the array to fit
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

impl<T> Nullable<T> {
    /// Create a new nullable value that is Some
    pub fn some(value: T) -> Self {
        Nullable(Some(value))
    }

    /// Create a new nullable value that is None
    pub fn none() -> Self {
        Nullable(None)
    }

    /// Check if the value is Some
    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    /// Check if the value is None
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// Get the value if it's Some
    pub fn as_ref(&self) -> Option<&T> {
        self.0.as_ref()
    }

    /// Get a mutable reference to the value if it's Some
    pub fn as_mut(&mut self) -> Option<&mut T> {
        self.0.as_mut()
    }

    /// Unwrap the value, panicking if it's None
    pub fn unwrap(self) -> T {
        self.0.unwrap()
    }

    /// Unwrap the value, returning the default if it's None
    pub fn unwrap_or(self, default: T) -> T {
        self.0.unwrap_or(default)
    }

    /// Unwrap the value, computing the default if it's None
    pub fn unwrap_or_else<F>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        self.0.unwrap_or_else(f)
    }

    /// Map the value if it's Some
    pub fn map<U, F>(self, f: F) -> Nullable<U>
    where
        F: FnOnce(T) -> U,
    {
        Nullable(self.0.map(f))
    }

    /// Map the value if it's Some, or return the default
    pub fn map_or<U, F>(self, default: U, f: F) -> U
    where
        F: FnOnce(T) -> U,
    {
        self.0.map_or(default, f)
    }

    /// Map the value if it's Some, or compute the default
    pub fn map_or_else<U, F, D>(self, default: D, f: F) -> U
    where
        F: FnOnce(T) -> U,
        D: FnOnce() -> U,
    {
        self.0.map_or_else(default, f)
    }
}

impl Tuple {
    /// Create a new empty tuple
    pub fn new() -> Self {
        Tuple(Vec::new())
    }

    /// Create a new tuple from values
    pub fn from_values(values: Vec<Value>) -> Self {
        Tuple(values)
    }

    /// Get the length of the tuple
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the tuple is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    /// Get a mutable value by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.0.get_mut(index)
    }

    /// Push a value to the tuple
    pub fn push(&mut self, value: Value) {
        self.0.push(value);
    }

    /// Pop a value from the tuple
    pub fn pop(&mut self) -> Option<Value> {
        self.0.pop()
    }

    /// Iterate over the tuple values
    pub fn iter(&self) -> std::slice::Iter<Value> {
        self.0.iter()
    }

    /// Iterate over the tuple values mutably
    pub fn iter_mut(&mut self) -> std::slice::IterMut<Value> {
        self.0.iter_mut()
    }

    /// Clear the tuple
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl Map {
    /// Create a new empty map
    pub fn new() -> Self {
        Map(HashMap::new())
    }

    /// Create a new map with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Map(HashMap::with_capacity(capacity))
    }

    /// Get the number of key-value pairs
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the map is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    /// Get a mutable value by key
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.0.get_mut(key)
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: String, value: Value) -> Option<Value> {
        self.0.insert(key, value)
    }

    /// Remove a key-value pair
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.0.remove(key)
    }

    /// Check if the map contains a key
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// Get all keys
    pub fn keys(&self) -> std::collections::hash_map::Keys<String, Value> {
        self.0.keys()
    }

    /// Get all values
    pub fn values(&self) -> std::collections::hash_map::Values<String, Value> {
        self.0.values()
    }

    /// Get all key-value pairs
    pub fn iter(&self) -> std::collections::hash_map::Iter<String, Value> {
        self.0.iter()
    }

    /// Get all key-value pairs mutably
    pub fn iter_mut(&mut self) -> std::collections::hash_map::IterMut<String, Value> {
        self.0.iter_mut()
    }

    /// Clear the map
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl UUID {
    /// Create a new random UUID
    pub fn new() -> Self {
        UUID(uuid::Uuid::new_v4())
    }

    /// Create a UUID from a string
    pub fn parse_str(s: &str) -> Result<Self, uuid::Error> {
        uuid::Uuid::parse_str(s).map(UUID)
    }

    /// Create a UUID from bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        UUID(uuid::Uuid::from_bytes(bytes))
    }

    /// Get the UUID as bytes
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Get the UUID as a string
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    /// Get the UUID as a hyphenated string
    pub fn to_hyphenated(&self) -> uuid::fmt::Hyphenated {
        self.0.hyphenated()
    }

    /// Get the UUID as a simple string
    pub fn to_simple(&self) -> uuid::fmt::Simple {
        self.0.simple()
    }

    /// Get the UUID as a URN string
    pub fn to_urn(&self) -> uuid::fmt::Urn {
        self.0.urn()
    }
}

// Implement Display for all complex types
impl<T: fmt::Display> fmt::Display for Array<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, item) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", item)?;
        }
        write!(f, "]")
    }
}

impl<T: fmt::Display> fmt::Display for Nullable<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(value) => write!(f, "{}", value),
            None => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, value) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", value)?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for Map {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (i, (key, value)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", key, value)?;
        }
        write!(f, "}}")
    }
}

impl fmt::Display for UUID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement From traits for conversions
impl<T> From<Vec<T>> for Array<T> {
    fn from(vec: Vec<T>) -> Self {
        Array(vec)
    }
}

impl<T> From<Option<T>> for Nullable<T> {
    fn from(option: Option<T>) -> Self {
        Nullable(option)
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Tuple(values)
    }
}

impl From<HashMap<String, Value>> for Map {
    fn from(map: HashMap<String, Value>) -> Self {
        Map(map)
    }
}

impl From<uuid::Uuid> for UUID {
    fn from(uuid: uuid::Uuid) -> Self {
        UUID(uuid)
    }
}

// Implement TryFrom for Value conversions
impl<T> TryFrom<Value> for Array<T>
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Display,
{
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(values) => {
                let mut result = Vec::new();
                for val in values {
                    let converted = T::try_from(val)
                        .map_err(|e| format!("Failed to convert array element: {}", e))?;
                    result.push(converted);
                }
                Ok(Array(result))
            }
            _ => Err(format!("Cannot convert {} to Array", value.type_name())),
        }
    }
}

impl<T> TryFrom<Value> for Nullable<T>
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Display,
{
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Nullable(Some(val)) => {
                let converted = T::try_from(*val)
                    .map_err(|e| format!("Failed to convert nullable value: {}", e))?;
                Ok(Nullable::some(converted))
            }
            Value::Nullable(None) => Ok(Nullable::none()),
            Value::Null => Ok(Nullable::none()),
            _ => {
                let converted = T::try_from(value)
                    .map_err(|e| format!("Failed to convert to nullable: {}", e))?;
                Ok(Nullable::some(converted))
            }
        }
    }
}

impl TryFrom<Value> for Tuple {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Tuple(values) => Ok(Tuple(values)),
            Value::Array(values) => Ok(Tuple(values)),
            _ => Err(format!("Cannot convert {} to Tuple", value.type_name())),
        }
    }
}

impl TryFrom<Value> for Map {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Map(map) => Ok(Map(map)),
            _ => Err(format!("Cannot convert {} to Map", value.type_name())),
        }
    }
}

impl TryFrom<Value> for UUID {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UUID(uuid) => Ok(UUID(uuid)),
            Value::String(s) => {
                uuid::Uuid::parse_str(&s)
                    .map(UUID)
                    .map_err(|e| format!("Invalid UUID format: {}", e))
            }
            Value::FixedString(bytes) => {
                if bytes.len() == 16 {
                    let mut uuid_bytes = [0u8; 16];
                    uuid_bytes.copy_from_slice(&bytes);
                    Ok(UUID(uuid::Uuid::from_bytes(uuid_bytes)))
                } else {
                    Err("FixedString must be exactly 16 bytes for UUID".to_string())
                }
            }
            _ => Err(format!("Cannot convert {} to UUID", value.type_name())),
        }
    }
}

// Implement Default traits
impl<T> Default for Array<T> {
    fn default() -> Self {
        Array::new()
    }
}

impl<T> Default for Nullable<T> {
    fn default() -> Self {
        Nullable::none()
    }
}

impl Default for Tuple {
    fn default() -> Self {
        Tuple::new()
    }
}

impl Default for Map {
    fn default() -> Self {
        Map::new()
    }
}

impl Default for UUID {
    fn default() -> Self {
        UUID(uuid::Uuid::nil())
    }
}

// Implement Deref for convenient access
impl<T> std::ops::Deref for Array<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::Deref for Nullable<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::Deref for Tuple {
    type Target = Vec<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::Deref for Map {
    type Target = HashMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement IntoIterator for convenient iteration
impl<T> IntoIterator for Array<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Array<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut Array<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

// Implement conversion traits
impl<T> From<Array<T>> for Vec<T> {
    fn from(array: Array<T>) -> Self {
        array.0
    }
}

impl<T> From<Nullable<T>> for Option<T> {
    fn from(nullable: Nullable<T>) -> Self {
        nullable.0
    }
}

impl From<Tuple> for Vec<Value> {
    fn from(tuple: Tuple) -> Self {
        tuple.0
    }
}

impl From<Map> for HashMap<String, Value> {
    fn from(map: Map) -> Self {
        map.0
    }
}

impl From<UUID> for uuid::Uuid {
    fn from(uuid: UUID) -> Self {
        uuid.0
    }
}
