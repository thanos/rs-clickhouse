# ClickHouse Rust Client

A high-performance, async Rust client library for ClickHouse database.

[![Crates.io](https://img.shields.io/crates/v/clickhouse-rust)](https://crates.io/crates/clickhouse-rust)
[![Documentation](https://docs.rs/clickhouse-rust/badge.svg)](https://docs.rs/clickhouse-rust)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70+-blue.svg)](https://www.rust-lang.org/)


**DO NOT USE - WORK IN PROGESS - PLAN TO HAVE THIS READY BY 1st September 2025**

**Note**: This is a work in progress. The API may change as we approach the 1.0 release.

## Features

- **Async/Await Support**: Built with Tokio for high-performance async operations
- **Multiple Protocols**: Support for ClickHouse native protocol, HTTP, and WebSocket
- **Connection Pooling**: Efficient connection management with configurable pool sizes
- **Compression**: Built-in support for LZ4 and ZSTD compression
- **Type Safety**: Strongly typed data structures for all ClickHouse data types
- **Error Handling**: Comprehensive error types with context information
- **Query Building**: Fluent query builder with parameter support
- **Batch Operations**: Efficient batch inserts and bulk operations
- **Load Balancing**: Support for multiple server endpoints with failover
- **TLS Support**: Secure connections with configurable TLS options
- **Metrics & Tracing**: Built-in observability and debugging support

## Supported Data Types

- **Numeric**: `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`
- **Signed**: `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`
- **Float**: `Float32`, `Float64`
- **String**: `String`, `FixedString`, `LowCardinality`
- **Date/Time**: `Date`, `DateTime`, `DateTime64`
- **Complex**: `Array`, `Nullable`, `Tuple`, `Map`, `UUID`
- **Geometric**: `Point`, `Ring`, `Polygon`, `MultiPolygon`

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
clickhouse-rust = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
```

### Basic Usage

```rust
use clickhouse_rust::{Client, ClientOptions, Query, types::*};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client options
    let options = ClientOptions::new()
        .host("localhost")
        .port(9000)
        .database("default")
        .username("default")
        .password("");

    // Create client
    let client = Client::new(options)?;

    // Execute a query
    let query = Query::new("SELECT * FROM my_table LIMIT 10");
    let result = client.query(query).await?;

    println!("Rows returned: {}", result.row_count());
    Ok(())
}
```

### Connection Pooling

```rust
use clickhouse_rust::{ConnectionPool, ClientOptions};

let pool = ConnectionPool::new(options)?;

// Get a connection from the pool
let connection = pool.get_connection().await?;

// Use the connection
let result = connection.query("SELECT 1").await?;

// Connection is automatically returned to the pool when dropped
```

### Batch Insert

```rust
use clickhouse_rust::types::{Block, Column, ColumnData};

let mut block = Block::new();

// Add columns with data
let id_column = Column::new("id".to_string(), ColumnData::UInt32(vec![1, 2, 3]));
let name_column = Column::new("name".to_string(), ColumnData::String(vec![
    String::new("Alice"),
    String::new("Bob"),
    String::new("Charlie"),
]));

block.add_column(id_column);
block.add_column(name_column);

// Insert the block
client.insert("users", block).await?;
```

### With Compression

```rust
use clickhouse_rust::{ClientOptions, compression::*};

let options = ClientOptions::new()
    .host("localhost")
    .port(9000)
    .compression_method(CompressionMethod::LZ4)
    .compression_level(CompressionLevel::best());

let client = Client::new(options)?;
```

## Advanced Features

### Custom Query Settings

```rust
use clickhouse_rust::{Query, QuerySettings};

let settings = QuerySettings::new()
    .timeout_secs(30)
    .max_memory_usage(1024 * 1024 * 100) // 100MB
    .async_insert(true);

let query = Query::new("SELECT * FROM large_table")
    .settings(settings);

let result = client.query(query).await?;
```

### Parameterized Queries

```rust
use clickhouse_rust::{Query, types::Value};

let query = Query::new("SELECT * FROM users WHERE age > ? AND city = ?")
    .param("age", Value::UInt32(18))
    .param("city", Value::String("New York".to_string()));

let result = client.query(query).await?;
```

### Load Balancing

```rust
use clickhouse_rust::{ClientOptions, LoadBalancingStrategy};

let options = ClientOptions::new()
    .add_server("server1:9000")
    .add_server("server2:9000")
    .add_server("server3:9000")
    .load_balancing_strategy(LoadBalancingStrategy::RoundRobin)
    .failover_enabled(true);

let client = Client::new(options)?;
```

### TLS Connection

```rust
use clickhouse_rust::{ClientOptions, TlsConfig};

let tls_config = TlsConfig::new()
    .ca_cert_path("/path/to/ca.crt")
    .client_cert_path("/path/to/client.crt")
    .client_key_path("/path/to/client.key");

let options = ClientOptions::new()
    .host("secure-server.com")
    .port(9440)
    .tls_config(tls_config);

let client = Client::new(options)?;
```

## Examples

Check out the [examples](./examples/) directory for more detailed usage examples:

- [Basic Usage](./examples/basic_usage.rs) - Simple connection and query examples
- [Batch Operations](./examples/batch_operations.rs) - Efficient bulk data operations
- [Connection Pooling](./examples/connection_pooling.rs) - Connection management examples
- [Compression](./examples/compression.rs) - Data compression examples
- [Load Balancing](./examples/load_balancing.rs) - Multi-server setup examples

## Configuration

### Client Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `"localhost"` | Server hostname |
| `port` | `9000` | Native protocol port |
| `http_port` | `8123` | HTTP port |
| `database` | `"default"` | Database name |
| `username` | `"default"` | Username |
| `password` | `""` | Password |
| `timeout_secs` | `30` | Connection timeout |
| `pool_size` | `10` | Connection pool size |
| `compression` | `LZ4` | Compression method |
| `compression_level` | `3` | Compression level (0-9) |

### Environment Variables

You can also configure the client using environment variables:

```bash
export CLICKHOUSE_HOST=my-server.com
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_DATABASE=my_db
export CLICKHOUSE_USERNAME=my_user
export CLICKHOUSE_PASSWORD=my_password
```

## Error Handling

The library provides comprehensive error handling with detailed context:

```rust
use clickhouse_rust::error::{Error, Result};

match client.query(query).await {
    Ok(result) => {
        // Handle successful result
        println!("Query successful: {} rows", result.row_count());
    }
    Err(Error::Connection(e)) => {
        // Handle connection errors
        eprintln!("Connection failed: {}", e);
    }
    Err(Error::QueryExecution(e)) => {
        // Handle query execution errors
        eprintln!("Query failed: {}", e);
    }
    Err(Error::Protocol(e)) => {
        // Handle protocol errors
        eprintln!("Protocol error: {}", e);
    }
    Err(e) => {
        // Handle other errors
        eprintln!("Unexpected error: {}", e);
    }
}
```

## Performance Considerations

- **Connection Pooling**: Use connection pools for high-throughput applications
- **Batch Operations**: Prefer batch inserts over individual inserts
- **Compression**: Enable compression for large data transfers
- **Async Operations**: Leverage async/await for concurrent operations
- **Memory Management**: Use appropriate batch sizes to balance memory usage

## Testing

Run the test suite:

```bash
cargo test
```

Run with specific features:

```bash
cargo test --features "tls,compression"
```

## Benchmarks

Run performance benchmarks:

```bash
cargo bench
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

1. Clone the repository
2. Install dependencies: `cargo build`
3. Run tests: `cargo test`
4. Run examples: `cargo run --example basic_usage`

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [ClickHouse](https://clickhouse.com/) - The amazing database this client connects to
- [Tokio](https://tokio.rs/) - Async runtime for Rust
- [Serde](https://serde.rs/) - Serialization framework
- [Bytes](https://github.com/tokio-rs/bytes) - Efficient byte handling

## Support

- **Documentation**: [docs.rs/clickhouse-rust](https://docs.rs/clickhouse-rust)
- **Issues**: [GitHub Issues](https://github.com/your-org/clickhouse-rust/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/clickhouse-rust/discussions)

## Roadmap

- [ ] HTTP/2 support
- [ ] Query result streaming
- [ ] More compression algorithms (GZIP, BZIP2, XZ)
- [ ] Advanced load balancing strategies
- [ ] Query result caching
- [ ] Metrics and monitoring integration
- [ ] More data type support
- [ ] Performance optimizations

---

**Note**: This is a work in progress. The API may change as we approach the 1.0 release.
