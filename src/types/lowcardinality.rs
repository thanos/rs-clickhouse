//! LowCardinality type implementation for ClickHouse
//! 
//! LowCardinality is an optimization that stores repeated values only once in a dictionary
//! and uses indices to reference them, significantly reducing memory usage for columns
//! with many repeated values.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// LowCardinality column data that optimizes storage of repeated values
#[derive(Debug, Clone, PartialEq)]
pub struct LowCardinality<T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    /// Dictionary of unique values
    dictionary: Vec<T>,
    /// Indices pointing to dictionary values
    indices: Vec<u32>,
    /// Reverse mapping for quick lookups
    reverse_map: HashMap<T, u32>,
}

impl<T> LowCardinality<T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    /// Create a new empty LowCardinality column
    pub fn new() -> Self {
        Self {
            dictionary: Vec::new(),
            indices: Vec::new(),
            reverse_map: HashMap::new(),
        }
    }

    /// Create a new LowCardinality column with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            dictionary: Vec::with_capacity(capacity),
            indices: Vec::with_capacity(capacity),
            reverse_map: HashMap::with_capacity(capacity),
        }
    }

    /// Add a value to the column
    pub fn push(&mut self, value: T) {
        let index = if let Some(&idx) = self.reverse_map.get(&value) {
            idx
        } else {
            let idx = self.dictionary.len() as u32;
            self.dictionary.push(value.clone());
            self.reverse_map.insert(value, idx);
            idx
        };
        self.indices.push(index);
    }

    /// Get a value at the specified index
    pub fn get(&self, index: usize) -> Option<&T> {
        self.indices
            .get(index)
            .and_then(|&idx| self.dictionary.get(idx as usize))
    }

    /// Get a value at the specified index (mutable)
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let idx = self.indices.get(index)?;
        self.dictionary.get_mut(*idx as usize)
    }

    /// Set a value at the specified index
    pub fn set(&mut self, index: usize, value: T) -> Result<(), String> {
        if index >= self.indices.len() {
            return Err("Index out of bounds".to_string());
        }

        let new_index = if let Some(&idx) = self.reverse_map.get(&value) {
            idx
        } else {
            let idx = self.dictionary.len() as u32;
            self.dictionary.push(value.clone());
            self.reverse_map.insert(value, idx);
            idx
        };

        self.indices[index] = new_index;
        Ok(())
    }

    /// Get the length of the column
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Get the number of unique values in the dictionary
    pub fn unique_count(&self) -> usize {
        self.dictionary.len()
    }

    /// Get the compression ratio (total values / unique values)
    pub fn compression_ratio(&self) -> f64 {
        if self.dictionary.is_empty() {
            0.0
        } else {
            self.indices.len() as f64 / self.dictionary.len() as f64
        }
    }

    /// Get the dictionary as a slice
    pub fn dictionary(&self) -> &[T] {
        &self.dictionary
    }

    /// Get the indices as a slice
    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.dictionary.clear();
        self.indices.clear();
        self.reverse_map.clear();
    }

    /// Reserve capacity for additional elements
    pub fn reserve(&mut self, additional: usize) {
        self.indices.reserve(additional);
        // Estimate dictionary growth based on typical compression ratios
        let estimated_dict_growth = (additional as f64 * 0.1).ceil() as usize;
        self.dictionary.reserve(estimated_dict_growth);
        self.reverse_map.reserve(estimated_dict_growth);
    }

    /// Convert to a regular vector (loses compression benefits)
    pub fn to_vec(&self) -> Vec<T> {
        self.indices
            .iter()
            .map(|&idx| self.dictionary[idx as usize].clone())
            .collect()
    }

    /// Create from a regular vector (automatically optimizes)
    pub fn from_vec(values: Vec<T>) -> Self {
        let mut lc = Self::with_capacity(values.len());
        for value in values {
            lc.push(value);
        }
        lc
    }

    /// Get an iterator over the values
    pub fn iter(&self) -> LowCardinalityIterator<T> {
        LowCardinalityIterator {
            lc: self,
            current: 0,
        }
    }
}

impl<T> Default for LowCardinality<T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<Vec<T>> for LowCardinality<T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn from(values: Vec<T>) -> Self {
        Self::from_vec(values)
    }
}

impl<T> Into<Vec<T>> for LowCardinality<T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn into(self) -> Vec<T> {
        self.to_vec()
    }
}

/// Iterator over LowCardinality values
pub struct LowCardinalityIterator<'a, T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    lc: &'a LowCardinality<T>,
    current: usize,
}

impl<'a, T> Iterator for LowCardinalityIterator<'a, T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.lc.len() {
            None
        } else {
            let value = self.lc.get(self.current);
            self.current += 1;
            value
        }
    }
}

impl<'a, T> ExactSizeIterator for LowCardinalityIterator<'a, T>
where
    T: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn len(&self) -> usize {
        self.lc.len() - self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lowcardinality_basic_operations() {
        let mut lc = LowCardinality::new();
        
        // Test empty state
        assert!(lc.is_empty());
        assert_eq!(lc.len(), 0);
        assert_eq!(lc.unique_count(), 0);
        
        // Test adding values
        lc.push("hello".to_string());
        lc.push("world".to_string());
        lc.push("hello".to_string()); // Duplicate
        
        assert_eq!(lc.len(), 3);
        assert_eq!(lc.unique_count(), 2); // Only 2 unique values
        assert_eq!(lc.get(0), Some(&"hello".to_string()));
        assert_eq!(lc.get(1), Some(&"world".to_string()));
        assert_eq!(lc.get(2), Some(&"hello".to_string()));
    }

    #[test]
    fn test_lowcardinality_compression() {
        let values = vec![
            "apple".to_string(),
            "banana".to_string(),
            "apple".to_string(),
            "cherry".to_string(),
            "banana".to_string(),
            "apple".to_string(),
        ];
        
        let lc = LowCardinality::from_vec(values);
        
        assert_eq!(lc.len(), 6);
        assert_eq!(lc.unique_count(), 3); // apple, banana, cherry
        assert!(lc.compression_ratio() > 1.0); // Should have compression
    }

    #[test]
    fn test_lowcardinality_set_value() {
        let mut lc = LowCardinality::new();
        lc.push("old".to_string());
        lc.push("value".to_string());
        
        // Set a new value
        lc.set(0, "new".to_string()).unwrap();
        assert_eq!(lc.get(0), Some(&"new".to_string()));
        assert_eq!(lc.unique_count(), 3); // old, value, new
        
        // Set to existing value
        lc.set(1, "new".to_string()).unwrap();
        assert_eq!(lc.get(1), Some(&"new".to_string()));
        assert_eq!(lc.unique_count(), 3); // Should reuse existing "new"
    }

    #[test]
    fn test_lowcardinality_iterator() {
        let values = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let lc = LowCardinality::from_vec(values);
        
        let collected: Vec<&String> = lc.iter().collect();
        assert_eq!(collected, vec![&"a".to_string(), &"b".to_string(), &"a".to_string()]);
    }

    #[test]
    fn test_lowcardinality_conversion() {
        let values = vec!["x".to_string(), "y".to_string(), "x".to_string()];
        let lc = LowCardinality::from_vec(values.clone());
        
        // Convert back to vector
        let converted: Vec<String> = lc.into();
        assert_eq!(converted, values);
    }

    #[test]
    fn test_lowcardinality_clear() {
        let mut lc = LowCardinality::from_vec(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(lc.len(), 2);
        
        lc.clear();
        assert!(lc.is_empty());
        assert_eq!(lc.unique_count(), 0);
    }

    #[test]
    fn test_lowcardinality_reserve() {
        let mut lc = LowCardinality::new();
        lc.reserve(100);
        
        // Should be able to add values without reallocation
        for i in 0..100 {
            lc.push(format!("value_{}", i));
        }
        
        assert_eq!(lc.len(), 100);
        assert_eq!(lc.unique_count(), 100);
    }
}
