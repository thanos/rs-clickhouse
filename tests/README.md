# ClickHouse Rust Client Test Suite

This directory contains comprehensive tests for the ClickHouse Rust client library, including unit tests, integration tests, and test utilities.

## 🚀 Quick Start

### Run All Tests
```bash
# Using the test runner script
./tests/run_tests.sh

# Or using cargo directly
cargo test --tests
```

### Run Specific Test Suites
```bash
# Unit tests only (no ClickHouse server required)
./tests/run_tests.sh unit

# Integration tests only
./tests/run_tests.sh integration

# Coverage tests only
./tests/run_tests.sh coverage

# Check ClickHouse availability
./tests/run_tests.sh check
```

## 📁 Test Structure

```
tests/
├── README.md                 # This file
├── run_tests.sh             # Test runner script
├── common/                  # Shared test utilities
│   └── mod.rs              # Common test functions and data
├── unit_tests.rs            # Unit tests (no external dependencies)
├── integration_tests.rs     # Integration tests (requires ClickHouse)
└── coverage_tests.rs        # Coverage tests (exercises edge cases)
```

## 🧪 Test Categories

### 1. Unit Tests (`unit_tests.rs`)
**No ClickHouse server required** - Tests core functionality, data structures, and types.

- **Block Operations**: Creating, manipulating, and querying data blocks
- **Value Types**: All ClickHouse data types and their conversions
- **Column Data**: Column operations and data handling
- **Row Operations**: Row access and manipulation
- **Complex Types**: Arrays, maps, tuples, and nullable values
- **Error Handling**: All error types and their properties

### 2. Integration Tests (`integration_tests.rs`)
**Requires ClickHouse server** - Tests actual client-server communication.

- **Client Creation**: Client instantiation with various options
- **Connection Management**: Native protocol and HTTP connections
- **Server Information**: Server info and version queries
- **Query Execution**: Basic SQL queries and data type handling
- **Connection Pooling**: Connection pool management
- **Error Scenarios**: Invalid queries and error handling

### 3. Common Test Utilities (`common/mod.rs`)
Shared functions and test data for consistent testing across all test suites.

- **Test Configuration**: Connection settings and constants
- **Client Factories**: Functions to create test clients
- **Test Data**: Pre-built test blocks with various data types
- **Assertions**: Common assertion functions for test validation
- **Server Detection**: Functions to check ClickHouse availability

### 4. Coverage Tests (`coverage_tests.rs`)
**Comprehensive coverage testing** - Tests edge cases and exercises more code paths.

- **Error Coverage**: All error variants and edge cases
- **Value Coverage**: All data type variants and conversions
- **Complex Type Coverage**: Arrays, maps, tuples, nullable values
- **Block Coverage**: Block operations and edge cases
- **Column Coverage**: Column operations and data handling
- **Row Coverage**: Row access and bounds checking
- **Client Options Coverage**: All configuration options
- **Async Coverage**: Asynchronous operations and error handling

## 🔧 Test Configuration

### Default Test Settings
```rust
const TEST_HOST: &str = "localhost";
const TEST_PORT: u16 = 9000;        // Native protocol
const TEST_HTTP_PORT: u16 = 8123;   // HTTP interface
const TEST_DATABASE: &str = "default";
const TEST_USER: &str = "default";
const TEST_PASSWORD: &str = "";
```

### Environment Variables
You can override test settings using environment variables:
```bash
export CLICKHOUSE_TEST_HOST=your-server.com
export CLICKHOUSE_TEST_PORT=9000
export CLICKHOUSE_TEST_DATABASE=test_db
export CLICKHOUSE_TEST_USER=test_user
export CLICKHOUSE_TEST_PASSWORD=test_pass
```

## 📊 Coverage Testing

### Code Coverage Tools
The project supports multiple coverage testing approaches:

#### 1. Coverage Tests (`coverage_tests.rs`)
Run comprehensive coverage tests that exercise edge cases:
```bash
# Run coverage tests only
./tests/run_tests.sh coverage

# Or directly with cargo
cargo test --test coverage_tests
```

#### 2. Coverage Reports with cargo-tarpaulin
Generate detailed coverage reports:
```bash
# Generate HTML coverage report
./tests/run_tests.sh tarpaulin

# Or directly with cargo-tarpaulin
cargo tarpaulin --out Html --output-dir coverage
```

#### 3. Coverage Reports with grcov
Alternative coverage tool:
```bash
# Generate coverage report
./tests/run_tests.sh coverage-report

# Install grcov first if needed
cargo install grcov
```

### Coverage Configuration
The project includes a `tarpaulin.toml` configuration file that customizes:
- Output formats (HTML, XML, Markdown)
- Coverage thresholds
- Timeout settings
- Thread configuration
- Output directory structure

### Coverage Goals
- **Unit Tests**: 100% coverage of core data structures
- **Integration Tests**: Coverage of client-server interactions
- **Coverage Tests**: Edge cases and error conditions
- **Overall Target**: 90%+ code coverage

## 🐳 ClickHouse Server Setup

### Option 1: Docker (Recommended for Testing)
```bash
# Start ClickHouse server
docker run -d \
  --name clickhouse-server \
  -p 9000:9000 \
  -p 8123:8123 \
  clickhouse/clickhouse-server:latest

# Check if it's running
docker ps | grep clickhouse

# View logs
docker logs clickhouse-server

# Stop server
docker stop clickhouse-server
docker rm clickhouse-server
```

### Option 2: System Installation
```bash
# Ubuntu/Debian
sudo apt-get install clickhouse-server clickhouse-client

# CentOS/RHEL
sudo yum install clickhouse-server clickhouse-client

# macOS
brew install clickhouse

# Start service
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server
```

### Option 3: Manual Installation
Download from [ClickHouse official website](https://clickhouse.com/docs/en/install) and follow installation instructions.

## 🧪 Running Tests

### Basic Test Execution
```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test unit_tests
cargo test --test integration_tests

# Run specific test function
cargo test test_block_creation
cargo test test_value_conversions
```

### Using the Test Runner Script
```bash
# Make script executable (first time only)
chmod +x tests/run_tests.sh

# Run all tests
./tests/run_tests.sh

# Run unit tests only
./tests/run_tests.sh unit

# Run integration tests only
./tests/run_tests.sh integration

# Run with verbose output
./tests/run_tests.sh verbose

# Check ClickHouse availability
./tests/run_tests.sh check

# Show help
./tests/run_tests.sh help
```

### Test Output Options
```bash
# Run tests with output capture
cargo test -- --nocapture

# Run tests with verbose output
cargo test -- --nocapture --test-threads=1

# Run tests in parallel (default)
cargo test -- --test-threads=4
```

## 📊 Test Coverage

### Generate Coverage Report
```bash
# Install grcov
cargo install grcov

# Run tests with coverage
./tests/run_tests.sh coverage

# View coverage report
open target/debug/coverage/index.html
```

### Alternative Coverage Tool
```bash
# Install cargo-tarpaulin
cargo install cargo-tarpaulin

# Run coverage
cargo tarpaulin --out Html
```

## 🔍 Test Patterns

### Running Tests by Pattern
```bash
# Run tests containing "block" in the name
cargo test block

# Run tests containing "connection" in the name
cargo test connection

# Run tests containing "value" in the name
cargo test value
```

### Running Tests by Feature
```bash
# Run tests with specific features
cargo test --features tls
cargo test --features compression
cargo test --features websocket
```

## 🐛 Debugging Tests

### Enable Debug Output
```bash
# Set log level
export RUST_LOG=debug

# Run tests with debug output
cargo test -- --nocapture
```

### Single Test Execution
```bash
# Run a single test with output
cargo test test_name -- --nocapture --exact

# Example
cargo test test_block_creation -- --nocapture --exact
```

### Test Timeout
```bash
# Increase test timeout (useful for slow connections)
cargo test -- --timeout 300
```

## 📝 Writing New Tests

### Unit Test Template
```rust
#[test]
fn test_new_feature() {
    // Arrange
    let input = "test data";
    
    // Act
    let result = process_data(input);
    
    // Assert
    assert_eq!(result, "expected output");
    assert!(result.len() > 0);
}
```

### Integration Test Template
```rust
#[tokio::test]
async fn test_server_feature() {
    // Skip if server not available
    if !is_clickhouse_available().await {
        println!("Skipping test - ClickHouse not available");
        return;
    }
    
    // Test implementation
    let client = create_test_client().expect("Failed to create client");
    let result = client.some_method().await;
    
    match result {
        Ok(data) => {
            // Assertions
            assert!(!data.is_empty());
        }
        Err(Error::Unsupported(msg)) => {
            println!("Feature not yet implemented: {}", msg);
        }
        Err(e) => {
            println!("Unexpected error: {:?}", e);
        }
    }
}
```

### Test Data Helper
```rust
use common::test_data::create_test_block;

#[test]
fn test_with_sample_data() {
    let block = create_test_block();
    
    // Use the pre-built test data
    assert_eq!(block.row_count(), 5);
    assert_eq!(block.column_count(), 4);
}
```

## 🚨 Common Issues

### ClickHouse Connection Failed
```bash
# Check if ClickHouse is running
./tests/run_tests.sh check

# Check ports
netstat -an | grep :9000
netstat -an | grep :8123

# Check ClickHouse logs
docker logs clickhouse-server
# or
sudo journalctl -u clickhouse-server
```

### Test Compilation Errors
```bash
# Clean and rebuild
cargo clean
cargo build

# Check dependencies
cargo check
```

### Permission Denied
```bash
# Make test runner executable
chmod +x tests/run_tests.sh
```

## 📚 Additional Resources

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Rust Testing Guide](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [Tokio Testing](https://tokio.rs/tokio/tutorial/testing)
- [Cargo Test Documentation](https://doc.rust-lang.org/cargo/commands/cargo-test.html)

## 🤝 Contributing

When adding new tests:

1. **Follow the existing patterns** in the test files
2. **Use the common utilities** from `common/mod.rs`
3. **Add appropriate assertions** to validate behavior
4. **Handle unsupported features gracefully** with `Error::Unsupported`
5. **Update this README** if adding new test categories or utilities
6. **Ensure tests pass** both with and without ClickHouse server

## 📈 Test Metrics

Track test performance and coverage:
```bash
# Run tests with timing
cargo test -- --nocapture --test-threads=1

# Generate coverage report
./tests/run_tests.sh coverage

# Count test functions
grep -r "fn test_" tests/ | wc -l
```
