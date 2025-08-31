//! # ClickHouse Rust Client
//!
//! A high-performance, async Rust client library for ClickHouse database.
//!
//! ## Features
//!
//! - **Async/Await Support**: Built on top of Tokio for high-performance async operations
//! - **Type Safety**: Strongly typed data structures for all ClickHouse data types
//! - **Compression**: Built-in support for LZ4 and ZSTD compression
//! - **Connection Pooling**: Efficient connection management
//! - **Batch Operations**: Optimized for bulk data operations
//! - **Error Handling**: Comprehensive error types with context
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use clickhouse_rs::{Client, ClientOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create client
//!     let client = Client::new(ClientOptions::default()
//!         .host("localhost")
//!         .port(9000)
//!         .database("default")
//!         .username("default")
//!         .password(""))?;
//!
//!     // Execute query
//!     let result = client.query("SELECT 1 as number").await?;
//!     
//!     // Process results
//!     for row in result.rows() {
//!         if let Some(value) = row.get(0) {
//!             if let Some(number) = value.as_ref() {
//!                 match number {
//!                     clickhouse_rs::types::Value::Int32(num) => {
//!                         println!("Number: {}", num);
//!                     }
//!                     clickhouse_rs::types::Value::UInt32(num) => {
//!                         println!("Number: {}", num);
//!                     }
//!                     _ => {
//!                         println!("Unexpected value type: {:?}", number);
//!                     }
//!                 }
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Supported Data Types
//!
//! - **Numeric**: `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`
//! - **Signed**: `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`
//! - **Float**: `Float32`, `Float64`
//! - **Decimal**: `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`
//! - **String**: `String`, `FixedString(N)`, `LowCardinality(String)`
//! - **Date/Time**: `Date`, `DateTime`, `DateTime64`
//! - **Complex**: `Array(T)`, `Nullable(T)`, `Tuple`, `Map`, `UUID`
//! - **Geometric**: `Point`, `Ring`, `Polygon`, `MultiPolygon`
//!
//! ## License
//!
//! Licensed under the Apache License, Version 2.0.

pub mod client;
pub mod types;
pub mod protocol;
pub mod compression;
pub mod error;

// Re-export main types for convenience
pub use client::{Client, ClientOptions, Connection, ConnectionPool};
pub use types::{
    Block, Column, Row, Value,
    // Numeric types
    UInt8, UInt16, UInt32, UInt64, UInt128, UInt256,
    Int8, Int16, Int32, Int64, Int128, Int256,
    Float32, Float64,
    // String types
    String, FixedString, LowCardinality,
    // Date/Time types
    Date, DateTime, DateTime64,
    // Complex types
    Array, Nullable, Tuple, Map, UUID,
    // Geometric types
    Point, Ring, Polygon, MultiPolygon,
};
pub use error::{Error, Result};

// Re-export async traits
pub use async_trait::async_trait;

// Re-export commonly used types
pub use chrono::{DateTime as ChronoDateTime, Utc, NaiveDateTime, NaiveDate};
pub use uuid::Uuid;
pub use serde::{Serialize, Deserialize};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // Basic test to ensure the library compiles
        assert_eq!(2 + 2, 4);
    }
}
