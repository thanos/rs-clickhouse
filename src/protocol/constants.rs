//! Protocol constants for ClickHouse

/// Default protocol version
pub const DEFAULT_PROTOCOL_VERSION: u64 = 54328;

/// Default database name
pub const DEFAULT_DATABASE: &str = "default";

/// Default username
pub const DEFAULT_USERNAME: &str = "default";

/// Default password
pub const DEFAULT_PASSWORD: &str = "";

/// Default port
pub const DEFAULT_PORT: u16 = 9000;

/// Default HTTP port
pub const DEFAULT_HTTP_PORT: u16 = 8123;

/// Default WebSocket port
pub const DEFAULT_WEBSOCKET_PORT: u16 = 8123;

/// Default timeout in seconds
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: usize = 10;

/// Default compression threshold in bytes
pub const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024;

/// Default compression level
pub const DEFAULT_COMPRESSION_LEVEL: u8 = 3;

/// Maximum packet size in bytes
pub const MAX_PACKET_SIZE: usize = 1024 * 1024 * 1024; // 1GB

/// Maximum string length
pub const MAX_STRING_LENGTH: usize = 1024 * 1024; // 1MB

/// Maximum array size
pub const MAX_ARRAY_SIZE: usize = 1024 * 1024; // 1M elements

/// Maximum nested depth
pub const MAX_NESTED_DEPTH: usize = 100;

/// Default buffer size
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// Default batch size
pub const DEFAULT_BATCH_SIZE: usize = 1000;
