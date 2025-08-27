//! Query execution and results for ClickHouse

use crate::error::{Error, Result};
use crate::types::{Block, Value};
use std::collections::HashMap;
use std::time::Duration;

/// Query settings for ClickHouse
#[derive(Debug, Clone)]
pub struct QuerySettings {
    /// Query timeout
    pub timeout: Option<Duration>,
    /// Maximum memory usage
    pub max_memory_usage: Option<u64>,
    /// Maximum block size
    pub max_block_size: Option<u64>,
    /// Whether to use async insert
    pub async_insert: Option<bool>,
    /// Whether to wait for async insert
    pub wait_for_async_insert: Option<bool>,
    /// Async insert busy timeout
    pub async_insert_busy_timeout_ms: Option<u64>,
    /// Async insert max data size
    pub async_insert_max_data_size: Option<u64>,
    /// Custom settings
    pub custom: HashMap<String, String>,
}

impl QuerySettings {
    /// Create new query settings
    pub fn new() -> Self {
        Self {
            timeout: None,
            max_memory_usage: None,
            max_block_size: None,
            async_insert: None,
            wait_for_async_insert: None,
            async_insert_busy_timeout_ms: None,
            async_insert_max_data_size: None,
            custom: HashMap::new(),
        }
    }

    /// Set query timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set maximum memory usage
    pub fn max_memory_usage(mut self, max_memory: u64) -> Self {
        self.max_memory_usage = Some(max_memory);
        self
    }

    /// Set maximum block size
    pub fn max_block_size(mut self, max_block_size: u64) -> Self {
        self.max_block_size = Some(max_block_size);
        self
    }

    /// Enable async insert
    pub fn async_insert(mut self, enabled: bool) -> Self {
        self.async_insert = Some(enabled);
        self
    }

    /// Set wait for async insert
    pub fn wait_for_async_insert(mut self, wait: bool) -> Self {
        self.wait_for_async_insert = Some(wait);
        self
    }

    /// Set async insert busy timeout
    pub fn async_insert_busy_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.async_insert_busy_timeout_ms = Some(timeout_ms);
        self
    }

    /// Set async insert max data size
    pub fn async_insert_max_data_size(mut self, max_size: u64) -> Self {
        self.async_insert_max_data_size = Some(max_size);
        self
    }

    /// Add a custom setting
    pub fn custom_setting(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }

    /// Build the settings string for ClickHouse
    pub fn build_settings_string(&self) -> String {
        let mut settings = Vec::new();

        if let Some(timeout) = self.timeout {
            settings.push(format!("timeout={}", timeout.as_secs()));
        }

        if let Some(max_memory) = self.max_memory_usage {
            settings.push(format!("max_memory_usage={}", max_memory));
        }

        if let Some(max_block_size) = self.max_block_size {
            settings.push(format!("max_block_size={}", max_block_size));
        }

        if let Some(async_insert) = self.async_insert {
            settings.push(format!("async_insert={}", if async_insert { 1 } else { 0 }));
        }

        if let Some(wait_for_async_insert) = self.wait_for_async_insert {
            settings.push(format!(
                "wait_for_async_insert={}",
                if wait_for_async_insert { 1 } else { 0 }
            ));
        }

        if let Some(timeout_ms) = self.async_insert_busy_timeout_ms {
            settings.push(format!("async_insert_busy_timeout_ms={}", timeout_ms));
        }

        if let Some(max_size) = self.async_insert_max_data_size {
            settings.push(format!("async_insert_max_data_size={}", max_size));
        }

        // Add custom settings
        for (key, value) in &self.custom {
            settings.push(format!("{}={}", key, value));
        }

        settings.join(", ")
    }
}

impl Default for QuerySettings {
    fn default() -> Self {
        Self::new()
    }
}

/// Query result from ClickHouse
#[derive(Debug)]
pub struct QueryResult {
    /// Query metadata
    pub metadata: QueryMetadata,
    /// Data blocks
    pub blocks: Vec<Block>,
    /// Statistics
    pub stats: QueryStats,
}

impl QueryResult {
    /// Create a new query result
    pub fn new(metadata: QueryMetadata, blocks: Vec<Block>, stats: QueryStats) -> Self {
        Self {
            metadata,
            blocks,
            stats,
        }
    }

    /// Get the number of rows in the result
    pub fn row_count(&self) -> usize {
        self.blocks.iter().map(|block| block.row_count).sum()
    }

    /// Get the number of columns in the result
    pub fn column_count(&self) -> usize {
        self.blocks.first().map(|block| block.column_count()).unwrap_or(0)
    }

    /// Get all rows from all blocks
    pub fn rows(&self) -> impl Iterator<Item = crate::types::Row> + '_ {
        self.blocks.iter().flat_map(|block| block.rows())
    }

    /// Get all columns from all blocks
    pub fn columns(&self) -> impl Iterator<Item = &crate::types::Column> + '_ {
        self.blocks.iter().flat_map(|block| block.columns())
    }

    /// Get the first block
    pub fn first_block(&self) -> Option<&Block> {
        self.blocks.first()
    }

    /// Get the first row
    pub fn first_row(&self) -> Option<crate::types::Row> {
        self.blocks.first()?.get_row(0)
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() || self.blocks.iter().all(|block| block.is_empty())
    }

    /// Get a specific column by name
    pub fn get_column(&self, name: &str) -> Option<&crate::types::Column> {
        for block in &self.blocks {
            if let Some(col) = block.get_column(name) {
                return Some(col);
            }
        }
        None
    }

    /// Get a specific row by index
    pub fn get_row(&self, index: usize) -> Option<crate::types::Row> {
        let mut current_index = 0;
        for block in &self.blocks {
            if current_index + block.row_count > index {
                return block.get_row(index - current_index);
            }
            current_index += block.row_count;
        }
        None
    }

    /// Convert the result to a vector of rows
    pub fn to_rows(&self) -> Vec<crate::types::Row> {
        self.rows().collect()
    }

    /// Convert the result to a vector of blocks
    pub fn to_blocks(&self) -> Vec<Block> {
        self.blocks.clone()
    }
}

/// Query metadata
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// Column names
    pub column_names: Vec<String>,
    /// Column types
    pub column_types: Vec<String>,
    /// Query ID
    pub query_id: Option<String>,
}

impl QueryMetadata {
    /// Create new query metadata
    pub fn new(column_names: Vec<String>, column_types: Vec<String>) -> Self {
        Self {
            column_names,
            column_types,
            query_id: None,
        }
    }

    /// Set the query ID
    pub fn with_query_id(mut self, query_id: String) -> Self {
        self.query_id = Some(query_id);
        self
    }

    /// Get column name by index
    pub fn get_column_name(&self, index: usize) -> Option<&str> {
        self.column_names.get(index).map(|s| s.as_str())
    }

    /// Get column type by index
    pub fn get_column_type(&self, index: usize) -> Option<&str> {
        self.column_types.get(index).map(|s| s.as_str())
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_names.iter().position(|n| n == name)
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.column_names.iter().any(|n| n == name)
    }
}

/// Query statistics
#[derive(Debug, Clone)]
pub struct QueryStats {
    /// Number of rows read
    pub rows_read: u64,
    /// Number of bytes read
    pub bytes_read: u64,
    /// Elapsed time
    pub elapsed: Duration,
    /// Rows written
    pub rows_written: Option<u64>,
    /// Bytes written
    pub bytes_written: Option<u64>,
}

impl QueryStats {
    /// Create new query stats
    pub fn new(rows_read: u64, bytes_read: u64, elapsed: Duration) -> Self {
        Self {
            rows_read,
            bytes_read,
            elapsed,
            rows_written: None,
            bytes_written: None,
        }
    }

    /// Set rows written
    pub fn with_rows_written(mut self, rows_written: u64) -> Self {
        self.rows_written = Some(rows_written);
        self
    }

    /// Set bytes written
    pub fn with_bytes_written(mut self, bytes_written: u64) -> Self {
        self.bytes_written = Some(bytes_written);
        self
    }

    /// Get the query performance in rows per second
    pub fn rows_per_second(&self) -> f64 {
        if self.elapsed.as_secs_f64() > 0.0 {
            self.rows_read as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Get the query performance in bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        if self.elapsed.as_secs_f64() > 0.0 {
            self.bytes_read as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        }
    }
}

/// Query builder for constructing complex queries
#[derive(Clone)]
pub struct Query {
    /// SQL query string
    sql: String,
    /// Query parameters
    params: HashMap<String, Value>,
    /// Query settings
    settings: QuerySettings,
}

impl Query {
    /// Create a new query
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: HashMap::new(),
            settings: QuerySettings::default(),
        }
    }

    /// Add a parameter to the query
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    /// Add multiple parameters to the query
    pub fn params(mut self, params: HashMap<String, Value>) -> Self {
        self.params.extend(params);
        self
    }

    /// Set query settings
    pub fn settings(mut self, settings: QuerySettings) -> Self {
        self.settings = settings;
        self
    }

    /// Set query timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.settings = self.settings.timeout(timeout);
        self
    }

    /// Set maximum memory usage
    pub fn max_memory_usage(mut self, max_memory: u64) -> Self {
        self.settings = self.settings.max_memory_usage(max_memory);
        self
    }

    /// Set maximum block size
    pub fn max_block_size(mut self, max_block_size: u64) -> Self {
        self.settings = self.settings.max_block_size(max_block_size);
        self
    }

    /// Enable async insert
    pub fn async_insert(mut self, enabled: bool) -> Self {
        self.settings = self.settings.async_insert(enabled);
        self
    }

    /// Set wait for async insert
    pub fn wait_for_async_insert(mut self, wait: bool) -> Self {
        self.settings = self.settings.wait_for_async_insert(wait);
        self
    }

    /// Add a custom setting
    pub fn custom_setting(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings = self.settings.custom_setting(key, value);
        self
    }

    /// Build the final query string
    pub fn build(self) -> (String, HashMap<String, Value>, QuerySettings) {
        (self.sql, self.params, self.settings)
    }

    /// Get the SQL string
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the parameters
    pub fn get_params(&self) -> &HashMap<String, Value> {
        &self.params
    }

    /// Get the settings
    pub fn get_settings(&self) -> &QuerySettings {
        &self.settings
    }
}

impl From<String> for Query {
    fn from(sql: String) -> Self {
        Query::new(sql)
    }
}

impl From<&str> for Query {
    fn from(sql: &str) -> Self {
        Query::new(sql)
    }
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)?;
        
        if !self.params.is_empty() {
            write!(f, " [params: ")?;
            for (i, (key, value)) in self.params.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}={:?}", key, value)?;
            }
            write!(f, "]")?;
        }
        
        if !self.settings.build_settings_string().is_empty() {
            write!(f, " [settings: {}]", self.settings.build_settings_string())?;
        }
        
        Ok(())
    }
}

impl std::fmt::Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("sql", &self.sql)
            .field("params", &self.params)
            .field("settings", &self.settings)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_query_settings_builder() {
        let settings = QuerySettings::new()
            .timeout(Duration::from_secs(30))
            .max_memory_usage(1024 * 1024 * 1024)
            .async_insert(true)
            .wait_for_async_insert(true)
            .custom_setting("max_threads", "4");

        let settings_str = settings.build_settings_string();
        assert!(settings_str.contains("timeout=30"));
        assert!(settings_str.contains("max_memory_usage=1073741824"));
        assert!(settings_str.contains("async_insert=1"));
        assert!(settings_str.contains("wait_for_async_insert=1"));
        assert!(settings_str.contains("max_threads=4"));
    }

    #[test]
    fn test_query_builder() {
        let query = Query::new("SELECT * FROM table WHERE id = {id}")
            .param("id", 42)
            .timeout(Duration::from_secs(10))
            .max_memory_usage(1024 * 1024);

        let (sql, params, settings) = query.build();
        assert_eq!(sql, "SELECT * FROM table WHERE id = {id}");
        assert_eq!(params.get("id"), Some(&Value::Int32(42)));
        assert_eq!(settings.timeout, Some(Duration::from_secs(10)));
        assert_eq!(settings.max_memory_usage, Some(1024 * 1024));
    }

    #[test]
    fn test_query_metadata() {
        let metadata = QueryMetadata::new(
            vec!["id".to_string(), "name".to_string()],
            vec!["UInt64".to_string(), "String".to_string()]
        );

        assert_eq!(metadata.get_column_index("id"), Some(0));
        assert_eq!(metadata.get_column_index("name"), Some(1));
        assert_eq!(metadata.get_column_index("nonexistent"), None);
        assert!(metadata.has_column("id"));
        assert!(!metadata.has_column("nonexistent"));
    }

    #[test]
    fn test_query_stats() {
        let stats = QueryStats::new(1000, 1024 * 1024, Duration::from_secs(1))
            .with_rows_written(500)
            .with_bytes_written(512 * 1024);

        assert_eq!(stats.rows_per_second(), 1000.0);
        assert_eq!(stats.bytes_per_second(), 1024.0 * 1024.0);
        assert_eq!(stats.rows_written, Some(500));
        assert_eq!(stats.bytes_written, Some(512 * 1024));
    }
}
