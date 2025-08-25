//! ClickHouse client implementation

mod connection;
mod options;
mod pool;
mod query;

pub use connection::Connection;
pub use options::ClientOptions;
pub use pool::ConnectionPool;


use crate::error::{Error, Result};
use crate::types::{Block, Value};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Main ClickHouse client
pub struct Client {
    options: ClientOptions,
    pool: Arc<ConnectionPool>,
}

impl Client {
    /// Create a new client with the specified options
    pub fn new(options: ClientOptions) -> Result<Self> {
        let pool = Arc::new(ConnectionPool::new(options.clone())?);
        Ok(Client { options, pool })
    }

    /// Create a new client with default options
    pub fn default() -> Result<Self> {
        Self::new(ClientOptions::default())
    }

    /// Execute a query and return the result
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let mut connection = self.pool.get_connection().await?;
        connection.query(sql).await
    }

    /// Execute a query with parameters
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let mut connection = self.pool.get_connection().await?;
        connection.query_with_params(sql, params).await
    }

    /// Execute a query with settings
    pub async fn query_with_settings(
        &self,
        sql: &str,
        settings: QuerySettings,
    ) -> Result<QueryResult> {
        let mut connection = self.pool.get_connection().await?;
        connection.query_with_settings(sql, settings).await
    }

    /// Execute a query and return the result
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.execute(sql).await
    }

    /// Execute a query with parameters
    pub async fn execute_with_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.execute_with_params(sql, params).await
    }

    /// Execute a query with settings
    pub async fn execute_with_settings(
        &self,
        sql: &str,
        settings: QuerySettings,
    ) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.execute_with_settings(sql, settings).await
    }

    /// Insert data into a table
    pub async fn insert(&self, table: &str, block: Block) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.insert(table, block).await
    }

    /// Insert data into a table with settings
    pub async fn insert_with_settings(
        &self,
        table: &str,
        block: Block,
        settings: QuerySettings,
    ) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.insert_with_settings(table, block, settings).await
    }

    /// Ping the server
    pub async fn ping(&self) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.ping().await
    }

    /// Get server information
    pub async fn server_info(&self) -> Result<HashMap<String, String>> {
        let mut connection = self.pool.get_connection().await?;
        connection.server_info().await
    }

    /// Get server version
    pub async fn server_version(&self) -> Result<String> {
        let mut connection = self.pool.get_connection().await?;
        connection.server_version().await
    }

    /// Reset the connection (useful for retry logic)
    pub async fn reset_connection(&self) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.reset().await
    }

    /// Get the client options
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    /// Get the connection pool
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Client {
            options: self.options.clone(),
            pool: Arc::clone(&self.pool),
        }
    }
}

/// Query settings for ClickHouse
#[derive(Debug, Clone)]
pub struct QuerySettings {
    /// Query timeout
    pub timeout: Option<std::time::Duration>,
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
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
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
}

/// Query statistics
#[derive(Debug, Clone)]
pub struct QueryStats {
    /// Number of rows read
    pub rows_read: u64,
    /// Number of bytes read
    pub bytes_read: u64,
    /// Elapsed time
    pub elapsed: std::time::Duration,
    /// Rows written
    pub rows_written: Option<u64>,
    /// Bytes written
    pub bytes_written: Option<u64>,
}

impl QueryStats {
    /// Create new query stats
    pub fn new(rows_read: u64, bytes_read: u64, elapsed: std::time::Duration) -> Self {
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
}

/// Query builder for constructing complex queries
pub struct Query {
    sql: String,
    params: HashMap<String, Value>,
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

    /// Set query settings
    pub fn settings(mut self, settings: QuerySettings) -> Self {
        self.settings = settings;
        self
    }

    /// Build the final query string
    pub fn build(self) -> (String, HashMap<String, Value>, QuerySettings) {
        (self.sql, self.params, self.settings)
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

/// Trait for executing queries
#[async_trait]
pub trait QueryExecutor {
    /// Execute a query
    async fn execute_query(&self, query: &Query) -> Result<QueryResult>;

    /// Execute a raw SQL string
    async fn execute_sql(&self, sql: &str) -> Result<QueryResult>;

    /// Execute a query with parameters
    async fn execute_with_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult>;
}

#[async_trait]
impl QueryExecutor for Client {
    async fn execute_query(&self, query: &Query) -> Result<QueryResult> {
        let (sql, _params, settings) = query.clone().build();
        self.query_with_settings(&sql, settings).await
    }

    async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        self.query(sql).await
    }

    async fn execute_with_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult> {
        self.query_with_params(sql, params).await
    }
}
