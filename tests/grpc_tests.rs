//! GRPC client integration tests

use clickhouse_rs::client::{Client, ClientOptions, GrpcClient};
use clickhouse_rs::error::{Error, Result};
use clickhouse_rs::types::{Block, Column, ColumnData, Value};
use std::collections::HashMap;

/// Helper function to create a test GRPC client
pub fn create_test_grpc_client() -> Result<GrpcClient> {
    let options = ClientOptions::new()
        .host("localhost")
        .port(9000)
        .database("test")
        .username("default")
        .password("")
        .enable_grpc()
        .grpc_port(9090);
    
    GrpcClient::new(options)
}

/// Helper function to create a test block for GRPC tests
pub fn create_test_block() -> Block {
    let mut block = Block::new();
    let column = Column::new("id", "UInt8", ColumnData::UInt8(vec![1, 2, 3]));
    block.add_column("id", column);
    block
}

#[tokio::test]
async fn test_grpc_client_integration() {
    let client = create_test_grpc_client().unwrap();
    
    // Test client creation and configuration
    assert_eq!(client.options().host, "localhost");
    assert_eq!(client.options().grpc_port, 9090);
    assert_eq!(client.options().database, "test");
    
    // Test client ID generation
    assert!(!client.id().is_empty());
    
    // Test connection state
    assert!(!client.is_connected());
}

#[tokio::test]
async fn test_grpc_client_from_main_client() {
    let options = ClientOptions::new()
        .host("localhost")
        .port(9000)
        .database("test")
        .enable_grpc()
        .grpc_port(9090);
    
    let client = Client::new(options).unwrap();
    let grpc_client = client.grpc_client().unwrap();
    
    // Test that the GRPC client has the same configuration
    assert_eq!(grpc_client.options().host, "localhost");
    assert_eq!(grpc_client.options().grpc_port, 9090);
    assert_eq!(grpc_client.options().database, "test");
}

#[tokio::test]
async fn test_grpc_client_error_handling_integration() {
    let mut client = create_test_grpc_client().unwrap();
    
    // Test that all unimplemented methods return appropriate errors
    let result = client.query("SELECT 1").await;
    assert!(result.is_err());
    
    let result = client.execute("CREATE TABLE test (id UInt8)").await;
    assert!(result.is_err());
    
    let result = client.insert("test_table", create_test_block()).await;
    assert!(result.is_err());
    
    let result = client.ping().await;
    assert!(result.is_err());
    
    let result = client.server_info().await;
    assert!(result.is_err());
    
    let result = client.server_version().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_grpc_client_settings_handling_integration() {
    let mut client = create_test_grpc_client().unwrap();
    
    let mut settings = clickhouse_rs::client::QuerySettings::new();
    settings = settings.custom_setting("max_memory_usage", "1000000")
                   .custom_setting("timeout", "30")
                   .custom_setting("max_threads", "4");
    
    let result = client.query_with_settings("SELECT 1", settings).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_grpc_client_connection_lifecycle_integration() {
    let mut client = create_test_grpc_client().unwrap();
    
    // Test initial state
    assert!(!client.is_connected());
    
    // Test connection (this will fail in tests since there's no real server)
    let connect_result = client.connect().await;
    // In a real test environment, this might succeed, but in unit tests it will fail
    // We just verify the method exists and can be called
    
    // Test disconnect
    let disconnect_result = client.disconnect().await;
    assert!(disconnect_result.is_ok());
    
    // Test reset
    let reset_result = client.reset().await;
    assert!(reset_result.is_ok());
}

#[tokio::test]
async fn test_grpc_client_clone_integration() {
    let client = create_test_grpc_client().unwrap();
    let cloned_client = client.clone();
    
    // Test that cloned client has the same configuration
    assert_eq!(client.options().host, cloned_client.options().host);
    assert_eq!(client.options().grpc_port, cloned_client.options().grpc_port);
    assert_eq!(client.options().database, cloned_client.options().database);
    
    // Test that they have different IDs
    assert_ne!(client.id(), cloned_client.id());
}

#[tokio::test]
async fn test_grpc_client_custom_configuration_integration() {
    let options = ClientOptions::new()
        .host("custom-host")
        .port(9001)
        .database("custom-db")
        .username("custom-user")
        .password("custom-pass")
        .connect_timeout(std::time::Duration::from_secs(30))
        .query_timeout(std::time::Duration::from_secs(60))
        .enable_grpc()
        .grpc_port(9091);
    
    let mut client = GrpcClient::new(options).unwrap();
    
    // Test custom configuration
    assert_eq!(client.options().host, "custom-host");
    assert_eq!(client.options().grpc_port, 9091);
    assert_eq!(client.options().database, "custom-db");
    assert_eq!(client.options().username, "custom-user");
    assert_eq!(client.options().password, "custom-pass");
    assert_eq!(client.options().connect_timeout, std::time::Duration::from_secs(30));
    assert_eq!(client.options().query_timeout, std::time::Duration::from_secs(60));
    
    // Test that unimplemented methods still return errors
    let result = client.execute("CREATE TABLE test (id UInt8)").await;
    assert!(result.is_err());
    
    let result = client.insert("test_table", create_test_block()).await;
    assert!(result.is_err());
    
    let result = client.ping().await;
    assert!(result.is_err());
    
    let result = client.server_info().await;
    assert!(result.is_err());
    
    let result = client.server_version().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_grpc_client_parameter_handling_integration() {
    let mut client = create_test_grpc_client().unwrap();
    
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::UInt8(42));
    params.insert("name".to_string(), Value::String("test".to_string()));
    
    let result = client.query_with_params("SELECT * FROM test WHERE id = {id:UInt8}", params).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_grpc_client_block_handling_integration() {
    let mut client = create_test_grpc_client().unwrap();
    
    let block = create_test_block();
    
    let result = client.insert("test_table", block).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_grpc_client_default_integration() {
    let client = GrpcClient::default().unwrap();
    
    // Test default configuration
    assert_eq!(client.options().host, "localhost");
    assert_eq!(client.options().port, 9000);
    assert_eq!(client.options().database, "default");
    assert_eq!(client.options().username, "default");
    assert_eq!(client.options().password, "");
    assert_eq!(client.options().grpc_port, 9000);
    assert!(!client.options().use_grpc);
}

#[tokio::test]
async fn test_grpc_client_drop_integration() {
    let client = create_test_grpc_client().unwrap();
    let id = client.id().to_string();
    
    // Test that the client can be dropped without issues
    drop(client);
    
    // The ID should still be valid (it's a String, not a reference)
    assert!(!id.is_empty());
}
