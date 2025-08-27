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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_constants() {
        // Test protocol version
        assert_eq!(DEFAULT_PROTOCOL_VERSION, 54328);
        
        // Test database constants
        assert_eq!(DEFAULT_DATABASE, "default");
        assert_eq!(DEFAULT_USERNAME, "default");
        assert_eq!(DEFAULT_PASSWORD, "");
        
        // Test port constants
        assert_eq!(DEFAULT_PORT, 9000);
        assert_eq!(DEFAULT_HTTP_PORT, 8123);
        assert_eq!(DEFAULT_WEBSOCKET_PORT, 8123);
        
        // Test timeout and pool constants
        assert_eq!(DEFAULT_TIMEOUT_SECS, 30);
        assert_eq!(DEFAULT_POOL_SIZE, 10);
        
        // Test compression constants
        assert_eq!(DEFAULT_COMPRESSION_THRESHOLD, 1024);
        assert_eq!(DEFAULT_COMPRESSION_LEVEL, 3);
        
        // Test size constants
        assert_eq!(MAX_PACKET_SIZE, 1024 * 1024 * 1024);
        assert_eq!(MAX_STRING_LENGTH, 1024 * 1024);
        assert_eq!(MAX_ARRAY_SIZE, 1024 * 1024);
        assert_eq!(MAX_NESTED_DEPTH, 100);
        assert_eq!(DEFAULT_BUFFER_SIZE, 8192);
        assert_eq!(DEFAULT_BATCH_SIZE, 1000);
    }

    #[test]
    fn test_constant_values_are_reasonable() {
        // Test that constants have reasonable values
        assert!(DEFAULT_PROTOCOL_VERSION > 0);
        assert!(DEFAULT_PORT > 0);
        assert!(DEFAULT_HTTP_PORT > 0);
        assert!(DEFAULT_WEBSOCKET_PORT > 0);
        assert!(DEFAULT_TIMEOUT_SECS > 0);
        assert!(DEFAULT_POOL_SIZE > 0);
        assert!(DEFAULT_COMPRESSION_LEVEL > 0);
        assert!(MAX_PACKET_SIZE > 0);
        assert!(MAX_STRING_LENGTH > 0);
        assert!(MAX_ARRAY_SIZE > 0);
        assert!(MAX_NESTED_DEPTH > 0);
        assert!(DEFAULT_BUFFER_SIZE > 0);
        assert!(DEFAULT_BATCH_SIZE > 0);
    }

    #[test]
    fn test_constant_relationships() {
        // Test logical relationships between constants
        assert!(MAX_PACKET_SIZE >= MAX_STRING_LENGTH);
        assert!(MAX_PACKET_SIZE >= MAX_ARRAY_SIZE);
        assert!(DEFAULT_BUFFER_SIZE <= MAX_PACKET_SIZE);
        assert!(DEFAULT_COMPRESSION_THRESHOLD <= DEFAULT_BUFFER_SIZE);
        assert!(DEFAULT_BATCH_SIZE <= MAX_ARRAY_SIZE);
    }

    #[test]
    fn test_port_constants_are_different() {
        // Test that different port constants have different values
        assert_ne!(DEFAULT_PORT, DEFAULT_HTTP_PORT);
        assert_ne!(DEFAULT_PORT, DEFAULT_WEBSOCKET_PORT);
        // Note: HTTP and WebSocket ports can be the same in some configurations
    }

    #[test]
    fn test_size_constants_are_powers_of_2() {
        // Test that size-related constants are powers of 2 (common in computing)
        assert!(DEFAULT_BUFFER_SIZE.is_power_of_two());
        assert!(MAX_PACKET_SIZE.is_power_of_two());
        assert!(MAX_STRING_LENGTH.is_power_of_two());
        assert!(MAX_ARRAY_SIZE.is_power_of_two());
    }
}
