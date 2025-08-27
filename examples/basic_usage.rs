//! Basic usage example for ClickHouse Rust client

use clickhouse_rs::{
    Client, ClientOptions, Connection, ConnectionPool, Query, QuerySettings,
    types::{Block, Column, ColumnData, Value, UInt8, String as ClickHouseString},
    error::Result,
};
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("ClickHouse Rust Client - Basic Usage Example");
    println!("=============================================");

    // Create client options
    let options = ClientOptions::new()
        .host("localhost")
        .port(9000)
        .database("default")
        .username("default")
        .password("")
        .timeout_secs(30)
        .compression_method(clickhouse_rs::compression::CompressionMethod::LZ4)
        .compression_level(clickhouse_rs::compression::CompressionLevel::default())
        .pool_size(5);

    println!("Connecting to ClickHouse server...");

    // Create a client
    let client = Client::new(options.clone())?;

    // Test connection
    match client.ping().await {
        Ok(_) => println!("✓ Successfully connected to ClickHouse server"),
        Err(e) => {
            println!("✗ Failed to connect to ClickHouse server: {}", e);
            println!("Note: Make sure ClickHouse is running on localhost:9000");
            return Ok(());
        }
    }

    // Get server info
    match client.server_info().await {
        Ok(info) => {
            println!("✓ Server info:");
            for (key, value) in info {
                println!("  {}: {}", key, value);
            }
        }
        Err(e) => println!("✗ Failed to get server info: {}", e),
    }

    // Create a simple table
    let create_table_sql = r#"
        CREATE TABLE IF NOT EXISTS example_table (
            id UInt8,
            name String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id
    "#;

    println!("\nCreating example table...");
    match client.execute(create_table_sql).await {
        Ok(_) => println!("✓ Table created successfully"),
        Err(e) => println!("✗ Failed to create table: {}", e),
    }

    // Insert some data
    println!("\nInserting sample data...");
    
    // Create a block with sample data
    let mut block = Block::new();
    
    // Add ID column
    let id_column = Column::new(
        "id".to_string(),
        ColumnData::UInt8(vec![1, 2, 3, 4, 5]),
    );
    block.add_column(id_column);
    
    // Add name column
    let name_column = Column::new(
        "name".to_string(),
        ColumnData::String(vec![
            ClickHouseString::new("Alice"),
            ClickHouseString::new("Bob"),
            ClickHouseString::new("Charlie"),
            ClickHouseString::new("Diana"),
            ClickHouseString::new("Eve"),
        ]),
    );
    block.add_column(name_column);
    
    // Add value column
    let value_column = Column::new(
        "value".to_string(),
        ColumnData::Float64(vec![10.5, 20.3, 15.7, 8.9, 12.1]),
    );
    block.add_column(value_column);

    // Insert the data
    match client.insert("example_table", block).await {
        Ok(_) => println!("✓ Data inserted successfully"),
        Err(e) => println!("✗ Failed to insert data: {}", e),
    }

    // Query the data
    println!("\nQuerying data...");
    let query = Query::new("SELECT * FROM example_table ORDER BY id");
    
    match client.query(query).await {
        Ok(result) => {
            println!("✓ Query executed successfully");
            println!("  Rows returned: {}", result.row_count());
            println!("  Columns: {}", result.columns().len());
            
            // Print column names
            println!("  Column names: {:?}", result.columns());
            
            // Print first few rows
            if let Some(first_block) = result.first_block() {
                println!("  First block row count: {}", first_block.row_count());
                println!("  First block column count: {}", first_block.column_count());
            }
        }
        Err(e) => println!("✗ Query failed: {}", e),
    }

    // Test with settings
    println!("\nTesting query with custom settings...");
    let settings = QuerySettings::new()
        .timeout_secs(10)
        .max_memory_usage(1024 * 1024 * 100); // 100MB
    
    let query_with_settings = Query::new("SELECT COUNT(*) FROM example_table")
        .settings(settings);
    
    match client.query(query_with_settings).await {
        Ok(result) => {
            println!("✓ Query with settings executed successfully");
            println!("  Rows returned: {}", result.row_count());
        }
        Err(e) => println!("✗ Query with settings failed: {}", e),
    }

    // Test connection pool
    println!("\nTesting connection pool...");
    let pool = ConnectionPool::new(options)?;
    
    match pool.stats().await {
        Ok(stats) => {
            println!("✓ Connection pool stats:");
            println!("  Total connections: {}", stats.total_connections);
            println!("  Active connections: {}", stats.active_connections);
            println!("  Idle connections: {}", stats.idle_connections);
        }
        Err(e) => println!("✗ Failed to get pool stats: {}", e),
    }

    // Test individual connection
    println!("\nTesting individual connection...");
    let mut connection = Connection::new(options);
    
    match connection.connect().await {
        Ok(_) => {
            println!("✓ Individual connection established");
            
            // Test ping
            match connection.ping().await {
                Ok(_) => println!("✓ Ping successful"),
                Err(e) => println!("✗ Ping failed: {}", e),
            }
            
            // Disconnect
            match connection.disconnect().await {
                Ok(_) => println!("✓ Connection disconnected"),
                Err(e) => println!("✗ Disconnect failed: {}", e),
            }
        }
        Err(e) => println!("✗ Failed to establish individual connection: {}", e),
    }

    println!("\nExample completed successfully!");
    println!("This demonstrates the basic functionality of the ClickHouse Rust client.");
    println!("You can now explore more advanced features like:");
    println!("- Batch inserts with compression");
    println!("- Parameterized queries");
    println!("- Async streaming of results");
    println!("- Custom data type handling");
    println!("- Connection pooling and load balancing");

    Ok(())
}
