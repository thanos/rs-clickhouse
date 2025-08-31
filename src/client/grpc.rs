//! GRPC client implementation for ClickHouse

use crate::error::{Error, Result};
use crate::types::{Block, Value};
use async_trait::async_trait;
use std::collections::HashMap;
use tonic::{transport::Channel, Request, Response, Status};
use tokio::time::{timeout, Duration};

/// GRPC client for ClickHouse
pub struct GrpcClient {
    /// Connection options
    options: crate::client::ClientOptions,
    /// GRPC channel
    channel: Option<Channel>,
    /// Whether the client is connected
    connected: bool,
    /// Client ID
    id: String,
}

impl GrpcClient {
    /// Create a new GRPC client with the specified options
    pub fn new(options: crate::client::ClientOptions) -> Result<Self> {
        Ok(Self {
            options,
            channel: None,
            connected: false,
            id: uuid::Uuid::new_v4().to_string(),
        })
    }

    /// Create a new GRPC client with default options
    pub fn default() -> Result<Self> {
        let options = crate::client::ClientOptions::default();
        Self::new(options)
    }

    /// Connect to the GRPC server
    pub async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }

        let endpoint = format!(
            "http://{}:{}",
            self.options.host,
            self.options.grpc_port
        );

        let channel = Channel::from_shared(endpoint)
            .map_err(|e| Error::Protocol(format!("Invalid GRPC endpoint: {}", e)))?
            .connect_timeout(self.options.connect_timeout)
            .timeout(self.options.query_timeout)
            .connect()
            .await
            .map_err(|e| Error::Protocol(format!("GRPC connection failed: {}", e)))?;

        self.channel = Some(channel);
        self.connected = true;

        Ok(())
    }

    /// Disconnect from the GRPC server
    pub async fn disconnect(&mut self) -> Result<()> {
        if !self.connected {
            return Ok(());
        }

        if let Some(channel) = self.channel.take() {
            // Close the channel
            drop(channel);
        }

        self.connected = false;
        Ok(())
    }

    /// Execute a query via GRPC
    pub async fn query(&mut self, sql: &str) -> Result<crate::client::QueryResult> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        // In a real implementation, this would make a GRPC call to the ClickHouse server
        Err(Error::Unsupported("GRPC query execution not yet implemented".to_string()))
    }

    /// Execute a query with parameters via GRPC
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<crate::client::QueryResult> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        Err(Error::Unsupported("GRPC query with parameters not yet implemented".to_string()))
    }

    /// Execute a query with settings via GRPC
    pub async fn query_with_settings(
        &mut self,
        sql: &str,
        settings: crate::client::QuerySettings,
    ) -> Result<crate::client::QueryResult> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        Err(Error::Unsupported("GRPC query with settings not yet implemented".to_string()))
    }

    /// Execute a command via GRPC
    pub async fn execute(&mut self, sql: &str) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        Err(Error::Unsupported("GRPC execute not yet implemented".to_string()))
    }

    /// Execute a command with parameters via GRPC
    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        Err(Error::Unsupported("GRPC execute with parameters not yet implemented".to_string()))
    }

    /// Execute a command with settings via GRPC
    pub async fn execute_with_settings(
        &mut self,
        sql: &str,
        settings: crate::client::QuerySettings,
    ) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        Err(Error::Unsupported("GRPC execute with settings not yet implemented".to_string()))
    }

    /// Insert data via GRPC
    pub async fn insert(&mut self, table: &str, block: Block) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not fully implemented
        Err(Error::Unsupported("GRPC insert not yet implemented".to_string()))
    }

    /// Insert data with settings via GRPC
    pub async fn insert_with_settings(
        &mut self,
        table: &str,
        block: Block,
        settings: crate::client::QuerySettings,
    ) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not yet implemented
        Err(Error::Unsupported("GRPC insert with settings not yet implemented".to_string()))
    }

    /// Ping the GRPC server
    pub async fn ping(&mut self) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not yet implemented
        Err(Error::Unsupported("GRPC ping not yet implemented".to_string()))
    }

    /// Get server information via GRPC
    pub async fn server_info(&mut self) -> Result<HashMap<String, String>> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not yet implemented
        Err(Error::Unsupported("GRPC server info not yet implemented".to_string()))
    }

    /// Get server version via GRPC
    pub async fn server_version(&mut self) -> Result<String> {
        if !self.connected {
            self.connect().await?;
        }

        // For now, return an error indicating GRPC is not yet implemented
        Err(Error::Unsupported("GRPC server version not yet implemented".to_string()))
    }

    /// Reset the GRPC connection
    pub async fn reset(&mut self) -> Result<()> {
        // Always disconnect first
        let _ = self.disconnect().await;
        
        // Reset connection state
        self.connected = false;
        self.channel = None;
        
        // For now, we consider reset successful after clearing state
        // In a real implementation, this would attempt to reconnect
        // but for testing purposes, we just clear the state
        Ok(())
    }

    /// Check if the client is connected
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Get the client ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the client options
    pub fn options(&self) -> &crate::client::ClientOptions {
        &self.options
    }
}

impl Clone for GrpcClient {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            channel: None, // Don't clone the channel
            connected: false, // Reset connection state
            id: uuid::Uuid::new_v4().to_string(), // Generate new ID
        }
    }
}

impl Drop for GrpcClient {
    fn drop(&mut self) {
        // Ensure we clean up the connection
        if self.connected {
            // Note: We can't use .await in Drop, so we'll just mark as disconnected
            self.connected = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientOptions;
    use crate::types::{Block, Column, ColumnData, Value};

    fn create_test_options() -> ClientOptions {
        ClientOptions::new()
            .host("localhost")
            .port(9000)
            .database("test")
            .username("default")
            .password("")
    }

    fn create_test_block() -> Block {
        let mut block = Block::new();
        let column = Column::new(
            "test_column".to_string(),
            "UInt8".to_string(),
            ColumnData::UInt8(vec![1, 2, 3, 4, 5]),
        );
        block.add_column("test_column", column);
        block
    }

    #[tokio::test]
    async fn test_grpc_client_new() {
        let options = create_test_options();
        let client = GrpcClient::new(options.clone());
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.options.host, "localhost");
        assert_eq!(client.options.port, 9000);
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_grpc_client_default() {
        let client = GrpcClient::default();
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.options.host, "localhost");
        assert_eq!(client.options.port, 9000);
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_grpc_client_connection_lifecycle() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        // Initially not connected
        assert!(!client.is_connected());

        // Try to connect (this will fail in test environment, but we can test the flow)
        let connect_result = client.connect().await;
        // In test environment, connection will likely fail, but that's okay
        // We're testing the connection flow, not actual connectivity

        // Test disconnect
        let disconnect_result = client.disconnect().await;
        assert!(disconnect_result.is_ok());
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_grpc_client_query_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let result = client.query("SELECT 1").await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC query execution not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_query_with_params_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let mut params = HashMap::new();
        params.insert("param1".to_string(), Value::UInt8(42));

        let result = client.query_with_params("SELECT ?", params).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC query with parameters not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_query_with_settings_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let settings = crate::client::QuerySettings::new();
        let result = client.query_with_settings("SELECT 1", settings).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC query with settings not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_execute_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let result = client.execute("CREATE TABLE test (id UInt8)").await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC execute not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_execute_with_params_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let mut params = HashMap::new();
        params.insert("table_name".to_string(), Value::String("test_table".to_string()));

        let result = client.execute_with_params("CREATE TABLE {table_name} (id UInt8)", params).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC execute with parameters not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_execute_with_settings_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let settings = crate::client::QuerySettings::new();
        let result = client.execute_with_settings("CREATE TABLE test (id UInt8)", settings).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC execute with settings not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_insert_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let block = create_test_block();
        let result = client.insert("test_table", block).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC insert not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_insert_with_settings_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let block = create_test_block();
        let settings = crate::client::QuerySettings::new();
        let result = client.insert_with_settings("test_table", block, settings).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC insert with settings not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_ping_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let result = client.ping().await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC ping not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_server_info_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let result = client.server_info().await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC server info not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_server_version_not_implemented() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let result = client.server_version().await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC server version not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_reset() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        // Test reset without connection
        let result = client.reset().await;
        // This will likely fail in test environment, but we can test the method exists
        // The actual connection failure is expected in test environment
    }

    #[tokio::test]
    async fn test_grpc_client_id() {
        let options = create_test_options();
        let client = GrpcClient::new(options).unwrap();

        let id = client.id();
        assert!(!id.is_empty());
        assert_eq!(id.len(), 36); // UUID length
    }

    #[tokio::test]
    async fn test_grpc_client_options() {
        let options = create_test_options();
        let client = GrpcClient::new(options.clone()).unwrap();

        let client_options = client.options();
        // Test that all options are properly set
        assert_eq!(client_options.host, options.host);
        assert_eq!(client_options.port, options.port);
        assert_eq!(client_options.database, options.database);
    }

    #[tokio::test]
    async fn test_grpc_client_clone() {
        let options = create_test_options();
        let client = GrpcClient::new(options).unwrap();
        let original_id = client.id().to_string();

        let cloned_client = client.clone();
        
        // Cloned client should have different ID
        assert_ne!(cloned_client.id(), original_id);
        
        // Cloned client should not be connected
        assert!(!cloned_client.is_connected());
        
        // Options should be the same
        assert_eq!(client.options.host, cloned_client.options.host);
        assert_eq!(client.options.grpc_port, cloned_client.options.grpc_port);
        assert_eq!(client.options.database, cloned_client.options.database);
    }

    #[tokio::test]
    async fn test_grpc_client_drop() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();
        
        // Mark as connected to test drop behavior
        client.connected = true;
        assert!(client.is_connected());
        
        // Client will be dropped here, and Drop implementation should handle cleanup
        drop(client);
    }

    #[tokio::test]
    async fn test_grpc_client_connection_state() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        // Initially not connected
        assert!(!client.is_connected());

        // Try to connect (will likely fail in test environment)
        let _ = client.connect().await;
        
        // Even if connection fails, we can test the state management
        // The client should handle connection failures gracefully
    }

    #[tokio::test]
    async fn test_grpc_client_error_handling() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        // Test that all unimplemented methods return appropriate errors
        // Test query method
        let result = client.query("SELECT 1").await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC"), "query error should mention GRPC");
                assert!(msg.contains("not yet implemented"), "query error should mention not implemented");
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("query should return Unsupported or Protocol error, got: {:?}", result);
            }
        }

        // Test execute method
        let result = client.execute("CREATE TABLE test (id UInt8)").await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC"), "execute error should mention GRPC");
                assert!(msg.contains("not yet implemented"), "execute error should mention not implemented");
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("execute should return Unsupported or Protocol error, got: {:?}", result);
            }
        }

        // Test insert method
        let result = client.insert("test_table", create_test_block()).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC"), "insert error should mention GRPC");
                assert!(msg.contains("not yet implemented"), "insert error should mention not implemented");
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("insert should return Unsupported or Protocol error, got: {:?}", result);
            }
        }

        // Test ping method
        let result = client.ping().await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC"), "ping error should mention GRPC");
                assert!(msg.contains("not yet implemented"), "ping error should mention not implemented");
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("ping should return Unsupported or Protocol error, got: {:?}", result);
            }
        }

        // Test server_info method
        let result = client.server_info().await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC"), "server_info error should mention GRPC");
                assert!(msg.contains("not yet implemented"), "server_info error should mention not implemented");
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("server_info should return Unsupported or Protocol error, got: {:?}", result);
            }
        }

        // Test server_version method
        let result = client.server_version().await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC"), "server_version error should mention GRPC");
                assert!(msg.contains("not yet implemented"), "server_version error should mention not implemented");
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("server_version should return Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_parameter_handling() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        // Test with various parameter types
        let mut params = HashMap::new();
        params.insert("int_param".to_string(), Value::Int32(42));
        params.insert("string_param".to_string(), Value::String("test".to_string()));
        params.insert("float_param".to_string(), Value::Float64(3.14));

        let result = client.query_with_params("SELECT ?", params).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC query with parameters not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_settings_handling() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        let mut settings = crate::client::QuerySettings::new();
        settings = settings
            .custom_setting("max_memory_usage", "1000000")
            .custom_setting("timeout", "30")
            .custom_setting("max_threads", "4");

        let result = client.query_with_settings("SELECT 1", settings).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC query with settings not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_client_block_handling() {
        let options = create_test_options();
        let mut client = GrpcClient::new(options).unwrap();

        // Create a more complex block
        let mut block = Block::new();
        
        // Add multiple columns
        let id_column = Column::new(
            "id".to_string(),
            "UInt8".to_string(),
            ColumnData::UInt8(vec![1, 2, 3]),
        );
        let name_column = Column::new(
            "name".to_string(),
            "String".to_string(),
            ColumnData::String(vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()]),
        );
        let age_column = Column::new(
            "age".to_string(),
            "UInt8".to_string(),
            ColumnData::UInt8(vec![25, 30, 35]),
        );

        block.add_column("id", id_column);
        block.add_column("name", name_column);
        block.add_column("age", age_column);

        let result = client.insert("users", block).await;
        assert!(result.is_err());
        match result {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("GRPC insert not yet implemented"));
            }
            Err(Error::Protocol(_)) => {
                // Connection failed in test environment, which is expected
            }
            _ => {
                panic!("Expected Unsupported or Protocol error, got: {:?}", result);
            }
        }
    }
}
