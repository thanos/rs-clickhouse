//! Coverage tests for ClickHouse Rust client library
//! These tests are designed to exercise more code paths and edge cases
//! to improve code coverage metrics.

use clickhouse_rs::client::ClientOptions;
use clickhouse_rs::error::Error;
use clickhouse_rs::types::{Block, Column, Value, ColumnData};
use clickhouse_rs::client::GrpcClient;
use std::time::Duration;

mod common;

/// Test coverage for error handling edge cases
#[test]
fn test_error_coverage() {
    // Test all error variants for coverage
    let errors = vec![
        Error::Network(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "Connection failed")),
        Error::Protocol("Protocol error".to_string()),
        Error::Authentication("Auth failed".to_string()),
        Error::QueryExecution("Query failed".to_string()),
        Error::TypeConversion("Type conversion failed".to_string()),
        Error::Serialization("Serialization failed".to_string()),
        Error::Compression("Compression failed".to_string()),
        Error::ConnectionPool("Pool error".to_string()),
        Error::Configuration("Config error".to_string()),
        Error::Timeout(Duration::from_secs(5)),
        Error::Tls("TLS error".to_string()),
        Error::Http { status: 500, message: "HTTP error".to_string() },
        Error::WebSocket("WebSocket error".to_string()),
        Error::InvalidData("Invalid data".to_string()),
        Error::Unsupported("Unsupported feature".to_string()),
        Error::Internal("Internal error".to_string()),
        Error::Custom("Custom error".to_string()),
    ];
    
    for error in errors {
        // Test error formatting
        let _display = format!("{}", error);
        let _debug = format!("{:?}", error);
        
        // Test error conversion
        let _string = error.to_string();
    }
}

/// Test coverage for all Value variants
#[test]
fn test_value_coverage() {
    // Test all numeric types
    let numeric_values = vec![
        Value::UInt8(0), Value::UInt8(255),
        Value::UInt16(0), Value::UInt16(65535),
        Value::UInt32(0), Value::UInt32(4294967295),
        Value::UInt64(0), Value::UInt64(18446744073709551615),
        Value::Int8(-128), Value::Int8(127),
        Value::Int16(-32768), Value::Int16(32767),
        Value::Int32(-2147483648), Value::Int32(2147483647),
        Value::Int64(-9223372036854775808), Value::Int64(9223372036854775807),
        Value::Float32(0.0), Value::Float32(3.14), Value::Float32(-3.14),
        Value::Float64(0.0), Value::Float64(2.718), Value::Float64(-2.718),
    ];
    
    // Test string types
    let string_values = vec![
        Value::String("".to_string()),
        Value::String("hello world".to_string()),
        Value::FixedString("fixed".to_string().into()),
    ];
    
    // Test all values for coverage
    let all_values = [numeric_values, string_values].concat();
    
    for value in all_values {
        // Test type name
        let _type_name = value.type_name();
        
        // Test display formatting
        let _display = format!("{}", value);
        
        // Test debug formatting
        let _debug = format!("{:?}", value);
    }
}

/// Test coverage for complex Value types
#[test]
fn test_complex_value_coverage() {
    // Test Array with various element types
    let array_values = vec![
        Value::Array(vec![Value::UInt32(1), Value::UInt32(2), Value::UInt32(3)]),
        Value::Array(vec![Value::String("a".to_string()), Value::String("b".to_string())]),
        Value::Array(vec![]), // Empty array
    ];
    
    // Test Nullable with various types
    let nullable_values = vec![
        Value::Nullable(None),
        Value::Nullable(Some(Box::new(Value::UInt32(42)))),
        Value::Nullable(Some(Box::new(Value::String("nullable".to_string())))),
    ];
    
    // Test all complex values for coverage
    let all_complex = [array_values, nullable_values].concat();
    
    for value in all_complex {
        let _type_name = value.type_name();
        let _display = format!("{}", value);
        let _debug = format!("{:?}", value);
    }
}

/// Test coverage for Block operations
#[test]
fn test_block_coverage() {
    let mut block = Block::new();
    
    // Test adding various column types
    block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3])));
    block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
        "Alice".to_string(), "Bob".to_string(), "Charlie".to_string()
    ])));
    
    // Test block properties
    assert_eq!(block.row_count(), 3);
    assert_eq!(block.column_count(), 2);
    
    // Test column access
    let id_col = block.get_column("id").expect("Should have id column");
    let name_col = block.get_column("name").expect("Should have name column");
    
    // Test column properties
    assert_eq!(id_col.name, "id");
    assert_eq!(id_col.type_name, "UInt32");
    assert_eq!(name_col.name, "name");
    assert_eq!(name_col.type_name, "String");
    
    // Test row access
    for row_idx in 0..block.row_count() {
        let row = block.get_row(row_idx).expect("Should have row");
        
        // Test row length
        assert_eq!(row.len(), 2);
        
        // Test value access
        let id_val = row.get(0).and_then(|v| v.as_ref()).expect("Should have id value");
        let name_val = row.get(1).and_then(|v| v.as_ref()).expect("Should have name value");
        
        // Test value types
        assert!(matches!(id_val, Value::UInt32(_)));
        assert!(matches!(name_val, Value::String(_)));
    }
    
    // Test iterator methods
    let rows: Vec<_> = block.rows().collect();
    assert_eq!(rows.len(), 3);
    
    let columns: Vec<_> = block.columns().collect();
    assert_eq!(columns.len(), 2);
}

/// Test coverage for ClientOptions
#[test]
fn test_client_options_coverage() {
    // Test all configuration options
    let options = ClientOptions::new()
        .host("test.example.com")
        .port(9001)
        .database("test_db")
        .username("test_user")
        .password("test_pass")
        .enable_tls()
        .enable_websocket()
        .enable_http()
        .connect_timeout(Duration::from_secs(15))
        .query_timeout(Duration::from_secs(45))
        .max_connections(20)
        .min_connections(5);
    
    // Verify all options were set
    assert_eq!(options.host, "test.example.com");
    assert_eq!(options.port, 9001);
    assert_eq!(options.database, "test_db");
    assert_eq!(options.username, "test_user");
    assert_eq!(options.password, "test_pass");
    assert!(options.use_tls);
    assert!(options.use_websocket);
    assert!(options.use_http);
    assert_eq!(options.connect_timeout, Duration::from_secs(15));
    assert_eq!(options.query_timeout, Duration::from_secs(45));
    assert_eq!(options.max_connections, 20);
    assert_eq!(options.min_connections, 5);
    
    // Test default values
    let default_options = ClientOptions::new();
    assert_eq!(default_options.host, "localhost");
    assert_eq!(default_options.port, 9000);
    assert_eq!(default_options.database, "default");
    assert_eq!(default_options.username, "default");
    assert_eq!(default_options.password, "");
    assert!(!default_options.use_tls);
    assert!(!default_options.use_websocket);
    assert!(!default_options.use_http);
    assert_eq!(default_options.max_connections, 10);
    assert_eq!(default_options.min_connections, 2);

    // Test connection string building
    let options = ClientOptions::new()
        .host("example.com")
        .port(9000);

    let conn_string = options.build_connection_string();
    assert_eq!(conn_string, "example.com:9000");

    // Test GRPC connection string
    let options = ClientOptions::new()
        .host("grpc.example.com")
        .enable_grpc()
        .grpc_port(9090);

    let conn_string = options.build_connection_string();
    assert_eq!(conn_string, "grpc://grpc.example.com:9090");

    // Test that GRPC takes priority over other protocols
    let options = options
        .enable_http()
        .enable_websocket();

    let conn_string = options.build_connection_string();
    assert_eq!(conn_string, "grpc://grpc.example.com:9090");

    // Test GRPC client creation
    let grpc_client = GrpcClient::new(options.clone()).unwrap();
    assert_eq!(grpc_client.options().host, "grpc.example.com");
    assert_eq!(grpc_client.options().grpc_port, 9090);
    assert!(!grpc_client.is_connected());
    assert!(!grpc_client.id().is_empty());
}

/// Test coverage for edge cases and error conditions
#[test]
fn test_edge_cases_coverage() {
    // Test empty block
    let empty_block = Block::new();
    assert_eq!(empty_block.row_count(), 0);
    assert_eq!(empty_block.column_count(), 0);
    
    // Test block with single row
    let mut single_row_block = Block::new();
    single_row_block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![42])));
    assert_eq!(single_row_block.row_count(), 1);
    assert_eq!(single_row_block.column_count(), 1);
    
    // Test block with single column
    let mut single_col_block = Block::new();
    single_col_block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
        "Alice".to_string(), "Bob".to_string(), "Charlie".to_string()
    ])));
    assert_eq!(single_col_block.row_count(), 3);
    assert_eq!(single_col_block.column_count(), 1);
    
    // Test column access edge cases
    let single_col = single_col_block.get_column("name").expect("Should have name column");
    assert_eq!(single_col.name, "name");
    assert_eq!(single_col.type_name, "String");
    
    // Test non-existent column
    let non_existent = single_col_block.get_column("nonexistent");
    assert!(non_existent.is_none());
    
    // Test row access edge cases
    let first_row = single_col_block.get_row(0).expect("Should have first row");
    assert_eq!(first_row.len(), 1);
    
    let last_row = single_col_block.get_row(2).expect("Should have last row");
    assert_eq!(last_row.len(), 1);
    
    // Test out-of-bounds row access
    let out_of_bounds = single_col_block.get_row(10);
    assert!(out_of_bounds.is_none());
}

/// Test coverage for data type conversions
#[test]
fn test_type_conversion_coverage() {
    // Test various data type combinations
    let test_cases = vec![
        (Value::UInt8(42), "UInt8"),
        (Value::UInt16(1000), "UInt16"),
        (Value::UInt32(1000000), "UInt32"),
        (Value::UInt64(1000000000), "UInt64"),
        (Value::Int8(-42), "Int8"),
        (Value::Int16(-1000), "Int16"),
        (Value::Int32(-1000000), "Int32"),
        (Value::Int64(-1000000000), "Int64"),
        (Value::Float32(3.14), "Float32"),
        (Value::Float64(2.718), "Float64"),
        (Value::String("hello".to_string()), "String"),
        (Value::FixedString("fixed".to_string().into()), "FixedString"),
    ];
    
    for (value, expected_type) in test_cases {
        let actual_type = value.type_name();
        assert_eq!(actual_type, expected_type, "Type mismatch for {:?}", value);
        
        // Test display formatting
        let display_str = format!("{}", value);
        assert!(!display_str.is_empty(), "Display string should not be empty for {:?}", value);
        
        // Test debug formatting
        let debug_str = format!("{:?}", value);
        assert!(!debug_str.is_empty(), "Debug string should not be empty for {:?}", value);
    }
}
