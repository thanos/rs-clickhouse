//! ClickHouse client implementation

mod connection;
mod options;
mod pool;
mod query;

pub use connection::Connection;
pub use options::ClientOptions;
pub use pool::ConnectionPool;
pub use query::{Query, QueryResult, QuerySettings, QueryMetadata, QueryStats};


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












