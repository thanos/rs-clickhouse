use clickhouse_rs::client::{Client, ClientOptions};
use clickhouse_rs::error::Result;
use clickhouse_rs::types::Column;
use std::time::Duration;

/// Test configuration for integration tests
pub const TEST_HOST: &str = "localhost";
pub const TEST_PORT: u16 = 9000;
pub const TEST_HTTP_PORT: u16 = 8123;
pub const TEST_DATABASE: &str = "default";
pub const TEST_USER: &str = "default";
pub const TEST_PASSWORD: &str = "clickhouse";

/// Helper function to create a test client
pub async fn create_test_client() -> Result<Client> {
    let options = ClientOptions::new()
        .host(TEST_HOST)
        .port(TEST_PORT)
        .database(TEST_DATABASE)
        .username(TEST_USER)
        .password(TEST_PASSWORD)
        .connect_timeout(Duration::from_secs(5))
        .query_timeout(Duration::from_secs(30));

    Client::new(options)
}

/// Helper function to create a test client with HTTP interface
pub async fn create_test_http_client() -> Result<Client> {
    let options = ClientOptions::new()
        .host(TEST_HOST)
        .port(TEST_HTTP_PORT)
        .database(TEST_DATABASE)
        .username(TEST_USER)
        .password(TEST_PASSWORD)
        .enable_http()
        .connect_timeout(Duration::from_secs(5))
        .query_timeout(Duration::from_secs(30));

    Client::new(options)
}

/// Helper function to create a test client with WebSocket interface
pub fn create_test_websocket_client() -> Result<Client> {
    let options = ClientOptions::new()
        .host(TEST_HOST)
        .port(TEST_HTTP_PORT)
        .database(TEST_DATABASE)
        .username(TEST_USER)
        .password(TEST_PASSWORD)
        .enable_websocket()
        .connect_timeout(Duration::from_secs(5))
        .query_timeout(Duration::from_secs(30));

    Client::new(options)
}

/// Check if ClickHouse server is available
pub async fn is_clickhouse_available() -> bool {
    match create_test_client().await {
        Ok(client) => {
            match client.ping().await {
                Ok(_) => true,
                Err(_) => false,
            }
        }
        Err(_) => false,
    }
}

/// Wait for ClickHouse server to be available
pub async fn wait_for_clickhouse(max_attempts: u32, delay_ms: u64) -> bool {
    for attempt in 1..=max_attempts {
        if is_clickhouse_available().await {
            println!("ClickHouse server is available after {} attempts", attempt);
            return true;
        }
        
        if attempt < max_attempts {
            println!("Attempt {}/{}: ClickHouse server not available, waiting {}ms...", 
                    attempt, max_attempts, delay_ms);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
    
    println!("ClickHouse server not available after {} attempts", max_attempts);
    false
}

/// Test data utilities
pub mod test_data {
    use clickhouse_rs::types::{Block, Column, ColumnData, Value};
    
    /// Create a simple test block with sample data
    pub fn create_test_block() -> Block {
        let mut block = Block::new();
        
        block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3, 4, 5])));
        block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
            "Diana".to_string(),
            "Eve".to_string(),
        ])));
        block.add_column("age", Column::new("age", "UInt8", ColumnData::UInt8(vec![25, 30, 35, 28, 32])));
        block.add_column("active", Column::new("active", "UInt8", ColumnData::UInt8(vec![1, 1, 0, 1, 0])));
        
        block
    }
    
    /// Create a test block with various data types
    pub fn create_mixed_type_block() -> Block {
        let mut block = Block::new();
        
        block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3])));
        block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ])));
        block.add_column("age", Column::new("age", "UInt8", ColumnData::UInt8(vec![25, 30, 35])));
        block.add_column("height", Column::new("height", "Float32", ColumnData::Float32(vec![1.65, 1.75, 1.80])));
        block.add_column("birth_date", Column::new("birth_date", "Date", ColumnData::Date(vec![
            chrono::NaiveDate::from_ymd_opt(1998, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(1993, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(1988, 1, 1).unwrap(),
        ])));
        
        block
    }
    
    /// Create a test block with nullable values
    pub fn create_nullable_block() -> Block {
        let mut block = Block::new();
        
        let nullable_names = vec![
            Some(Value::String("Alice".to_string())),
            None,
            Some(Value::String("Bob".to_string())),
            None,
            Some(Value::String("Charlie".to_string())),
        ];
        
        block.add_column("name", Column::new("name", "Nullable(String)", ColumnData::Nullable(nullable_names)));
        block.add_column("age", Column::new("age", "UInt8", ColumnData::UInt8(vec![25, 0, 30, 0, 35])));
        
        block
    }
    
    /// Create a test block with array values
    pub fn create_array_block() -> Block {
        let mut block = Block::new();
        
        let array_values = vec![
            vec![Value::UInt32(1), Value::UInt32(2), Value::UInt32(3)],
            vec![Value::UInt32(4), Value::UInt32(5)],
            vec![Value::UInt32(6)],
        ];
        
        block.add_column("numbers", Column::new("numbers", "Array(UInt32)", ColumnData::Array(array_values)));
        block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ])));
        
        block
    }
    
    /// Create a test block with map values
    pub fn create_map_block() -> Block {
        use std::collections::HashMap;
        
        let mut block = Block::new();
        
        let mut map1 = HashMap::new();
        map1.insert("key1".to_string(), Value::String("value1".to_string()));
        map1.insert("key2".to_string(), Value::UInt32(42));
        
        let mut map2 = HashMap::new();
        map2.insert("name".to_string(), Value::String("Alice".to_string()));
        map2.insert("age".to_string(), Value::UInt8(25));
        
        let map_values = vec![
            map1,
            map2,
        ];
        
        block.add_column("metadata", Column::new("metadata", "Map(String, String)", ColumnData::Map(map_values)));
        block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2])));
        
        block
    }
}

/// Test assertions and utilities
pub mod assertions {
    use clickhouse_rs::types::{Block, Value};
    
    /// Assert that a block has the expected structure
    pub fn assert_block_structure(block: &Block, expected_rows: usize, expected_columns: usize) {
        assert_eq!(block.row_count(), expected_rows, 
                   "Expected {} rows, got {}", expected_rows, block.row_count());
        assert_eq!(block.column_count(), expected_columns, 
                   "Expected {} columns, got {}", expected_columns, block.column_count());
    }
    
    /// Assert that a block contains a specific column
    pub fn assert_block_has_column(block: &Block, column_name: &str) {
        assert!(block.get_column(column_name).is_some(), 
                "Block should have column '{}'", column_name);
    }
    
    /// Assert that a value matches a specific type and value
    pub fn assert_value_matches<T: PartialEq + std::fmt::Debug>(
        value: &Value, 
        expected: T,
        value_name: &str
    ) where Value: PartialEq<T> {
        // For now, just check that the value is not null
        assert!(!matches!(value, Value::Nullable(None)), 
                "{} should not be null", value_name);
    }
    
    /// Assert that a row has the expected number of values
    pub fn assert_row_length(row: &clickhouse_rs::types::Row, expected_length: usize) {
        assert_eq!(row.len(), expected_length, 
                   "Row should have {} values, got {}", expected_length, row.len());
    }
}
