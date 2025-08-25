//! Client options for ClickHouse

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// ClickHouse client options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientOptions {
    /// Server hostname or IP address
    pub host: String,
    /// Server port
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Query timeout
    pub query_timeout: Duration,
    /// Read timeout
    pub read_timeout: Duration,
    /// Write timeout
    pub write_timeout: Duration,
    /// Keep alive timeout
    pub keep_alive_timeout: Duration,
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    /// Minimum number of connections in the pool
    pub min_connections: usize,
    /// Connection idle timeout
    pub idle_timeout: Duration,
    /// Whether to use TLS/SSL
    pub use_tls: bool,
    /// TLS certificate path
    pub tls_cert_path: Option<String>,
    /// TLS key path
    pub tls_key_path: Option<String>,
    /// TLS CA path
    pub tls_ca_path: Option<String>,
    /// Whether to verify TLS certificates
    pub tls_verify: bool,
    /// Compression method
    pub compression: CompressionMethod,
    /// Whether to use HTTP interface
    pub use_http: bool,
    /// HTTP path
    pub http_path: String,
    /// HTTP headers
    pub http_headers: Vec<(String, String)>,
    /// Whether to use HTTP/2
    pub use_http2: bool,
    /// Whether to use WebSocket interface
    pub use_websocket: bool,
    /// WebSocket path
    pub websocket_path: String,
    /// Whether to use native protocol
    pub use_native_protocol: bool,
    /// Native protocol version
    pub native_protocol_version: u32,
    /// Whether to use compression
    pub use_compression: bool,
    /// Compression level
    pub compression_level: u8,
    /// Whether to use connection pooling
    pub use_connection_pool: bool,
    /// Pool acquire timeout
    pub pool_acquire_timeout: Duration,
    /// Whether to use retry logic
    pub use_retry: bool,
    /// Maximum retry attempts
    pub max_retries: usize,
    /// Retry delay
    pub retry_delay: Duration,
    /// Whether to use load balancing
    pub use_load_balancing: bool,
    /// Load balancing strategy
    pub load_balancing_strategy: LoadBalancingStrategy,
    /// Server list for load balancing
    pub servers: Vec<ServerInfo>,
    /// Whether to use failover
    pub use_failover: bool,
    /// Failover timeout
    pub failover_timeout: Duration,
    /// Whether to use health checks
    pub use_health_checks: bool,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Whether to use metrics
    pub use_metrics: bool,
    /// Metrics prefix
    pub metrics_prefix: String,
    /// Whether to use tracing
    pub use_tracing: bool,
    /// Tracing level
    pub tracing_level: TracingLevel,
}

impl ClientOptions {
    /// Create new client options with default values
    pub fn new() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9000,
            database: "default".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            connect_timeout: Duration::from_secs(10),
            query_timeout: Duration::from_secs(300),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(300),
            max_connections: 10,
            min_connections: 2,
            idle_timeout: Duration::from_secs(600),
            use_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_verify: true,
            compression: CompressionMethod::LZ4,
            use_http: false,
            http_path: "/".to_string(),
            http_headers: Vec::new(),
            use_http2: false,
            use_websocket: false,
            websocket_path: "/".to_string(),
            use_native_protocol: true,
            native_protocol_version: 54428,
            use_compression: true,
            compression_level: 3,
            use_connection_pool: true,
            pool_acquire_timeout: Duration::from_secs(30),
            use_retry: true,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            use_load_balancing: false,
            load_balancing_strategy: LoadBalancingStrategy::RoundRobin,
            servers: Vec::new(),
            use_failover: false,
            failover_timeout: Duration::from_secs(5),
            use_health_checks: false,
            health_check_interval: Duration::from_secs(30),
            use_metrics: false,
            metrics_prefix: "clickhouse".to_string(),
            use_tracing: false,
            tracing_level: TracingLevel::Info,
        }
    }

    /// Set the server host
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the server port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the database name
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Set the username
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = username.into();
        self
    }

    /// Set the password
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    /// Set the connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the query timeout
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Set the read timeout
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Set the write timeout
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Set the keep alive timeout
    pub fn keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.keep_alive_timeout = timeout;
        self
    }

    /// Set the maximum number of connections
    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set the minimum number of connections
    pub fn min_connections(mut self, min_connections: usize) -> Self {
        self.min_connections = min_connections;
        self
    }

    /// Set the idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Enable TLS
    pub fn enable_tls(mut self) -> Self {
        self.use_tls = true;
        self
    }

    /// Disable TLS
    pub fn disable_tls(mut self) -> Self {
        self.use_tls = false;
        self
    }

    /// Set TLS certificate path
    pub fn tls_cert_path(mut self, path: impl Into<String>) -> Self {
        self.tls_cert_path = Some(path.into());
        self
    }

    /// Set TLS key path
    pub fn tls_key_path(mut self, path: impl Into<String>) -> Self {
        self.tls_key_path = Some(path.into());
        self
    }

    /// Set TLS CA path
    pub fn tls_ca_path(mut self, path: impl Into<String>) -> Self {
        self.tls_ca_path = Some(path.into());
        self
    }

    /// Set TLS verification
    pub fn tls_verify(mut self, verify: bool) -> Self {
        self.tls_verify = verify;
        self
    }

    /// Set compression method
    pub fn compression(mut self, method: CompressionMethod) -> Self {
        self.compression = method;
        self
    }

    /// Enable HTTP interface
    pub fn enable_http(mut self) -> Self {
        self.use_http = true;
        self
    }

    /// Disable HTTP interface
    pub fn disable_http(mut self) -> Self {
        self.use_http = false;
        self
    }

    /// Set HTTP path
    pub fn http_path(mut self, path: impl Into<String>) -> Self {
        self.http_path = path.into();
        self
    }

    /// Add HTTP header
    pub fn http_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.http_headers.push((key.into(), value.into()));
        self
    }

    /// Enable HTTP/2
    pub fn enable_http2(mut self) -> Self {
        self.use_http2 = true;
        self
    }

    /// Disable HTTP/2
    pub fn disable_http2(mut self) -> Self {
        self.use_http2 = false;
        self
    }

    /// Enable WebSocket interface
    pub fn enable_websocket(mut self) -> Self {
        self.use_websocket = true;
        self
    }

    /// Disable WebSocket interface
    pub fn disable_websocket(mut self) -> Self {
        self.use_websocket = false;
        self
    }

    /// Set WebSocket path
    pub fn websocket_path(mut self, path: impl Into<String>) -> Self {
        self.websocket_path = path.into();
        self
    }

    /// Enable native protocol
    pub fn enable_native_protocol(mut self) -> Self {
        self.use_native_protocol = true;
        self
    }

    /// Disable native protocol
    pub fn disable_native_protocol(mut self) -> Self {
        self.use_native_protocol = false;
        self
    }

    /// Set native protocol version
    pub fn native_protocol_version(mut self, version: u32) -> Self {
        self.native_protocol_version = version;
        self
    }

    /// Enable compression
    pub fn enable_compression(mut self) -> Self {
        self.use_compression = true;
        self
    }

    /// Disable compression
    pub fn disable_compression(mut self) -> Self {
        self.use_compression = false;
        self
    }

    /// Set compression level
    pub fn compression_level(mut self, level: u8) -> Self {
        self.compression_level = level;
        self
    }

    /// Enable connection pooling
    pub fn enable_connection_pool(mut self) -> Self {
        self.use_connection_pool = true;
        self
    }

    /// Disable connection pooling
    pub fn disable_connection_pool(mut self) -> Self {
        self.use_connection_pool = false;
        self
    }

    /// Set pool acquire timeout
    pub fn pool_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.pool_acquire_timeout = timeout;
        self
    }

    /// Enable retry logic
    pub fn enable_retry(mut self) -> Self {
        self.use_retry = true;
        self
    }

    /// Disable retry logic
    pub fn disable_retry(mut self) -> Self {
        self.use_retry = false;
        self
    }

    /// Set maximum retry attempts
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set retry delay
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Enable load balancing
    pub fn enable_load_balancing(mut self) -> Self {
        self.use_load_balancing = true;
        self
    }

    /// Disable load balancing
    pub fn disable_load_balancing(mut self) -> Self {
        self.use_load_balancing = false;
        self
    }

    /// Set load balancing strategy
    pub fn load_balancing_strategy(mut self, strategy: LoadBalancingStrategy) -> Self {
        self.load_balancing_strategy = strategy;
        self
    }

    /// Add server for load balancing
    pub fn add_server(mut self, server: ServerInfo) -> Self {
        self.servers.push(server);
        self
    }

    /// Enable failover
    pub fn enable_failover(mut self) -> Self {
        self.use_failover = true;
        self
    }

    /// Disable failover
    pub fn disable_failover(mut self) -> Self {
        self.use_failover = false;
        self
    }

    /// Set failover timeout
    pub fn failover_timeout(mut self, timeout: Duration) -> Self {
        self.failover_timeout = timeout;
        self
    }

    /// Enable health checks
    pub fn enable_health_checks(mut self) -> Self {
        self.use_health_checks = true;
        self
    }

    /// Disable health checks
    pub fn disable_health_checks(mut self) -> Self {
        self.use_health_checks = false;
        self
    }

    /// Set health check interval
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Enable metrics
    pub fn enable_metrics(mut self) -> Self {
        self.use_metrics = true;
        self
    }

    /// Disable metrics
    pub fn disable_metrics(mut self) -> Self {
        self.use_metrics = false;
        self
    }

    /// Set metrics prefix
    pub fn metrics_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.metrics_prefix = prefix.into();
        self
    }

    /// Enable tracing
    pub fn enable_tracing(mut self) -> Self {
        self.use_tracing = true;
        self
    }

    /// Disable tracing
    pub fn disable_tracing(mut self) -> Self {
        self.use_tracing = false;
        self
    }

    /// Set tracing level
    pub fn tracing_level(mut self, level: TracingLevel) -> Self {
        self.tracing_level = level;
        self
    }

    /// Build connection string
    pub fn build_connection_string(&self) -> String {
        if self.use_http {
            let protocol = if self.use_http2 { "https" } else { "http" };
            format!("{}://{}:{}{}", protocol, self.host, self.port, self.http_path)
        } else if self.use_websocket {
            let protocol = if self.use_tls { "wss" } else { "ws" };
            format!("{}://{}:{}{}", protocol, self.host, self.port, self.websocket_path)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// Validate the options
    pub fn validate(&self) -> Result<()> {
        if self.host.is_empty() {
            return Err(Error::Configuration("Host cannot be empty".to_string()));
        }

        if self.port == 0 {
            return Err(Error::Configuration("Port cannot be 0".to_string()));
        }

        if self.database.is_empty() {
            return Err(Error::Configuration("Database cannot be empty".to_string()));
        }

        if self.username.is_empty() {
            return Err(Error::Configuration("Username cannot be empty".to_string()));
        }

        if self.max_connections < self.min_connections {
            return Err(Error::Configuration(
                "Max connections cannot be less than min connections".to_string(),
            ));
        }

        if self.use_tls {
            if let (Some(cert), Some(key)) = (&self.tls_cert_path, &self.tls_key_path) {
                if cert.is_empty() || key.is_empty() {
                    return Err(Error::Configuration(
                        "TLS certificate and key paths cannot be empty".to_string(),
                    ));
                }
            }
        }

        if self.use_compression {
            if self.compression_level > 9 {
                return Err(Error::Configuration(
                    "Compression level must be between 0 and 9".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Compression methods supported by ClickHouse
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionMethod {
    /// No compression
    None,
    /// LZ4 compression
    LZ4,
    /// ZSTD compression
    ZSTD,
    /// GZIP compression
    GZIP,
    /// BZIP2 compression
    BZIP2,
    /// XZ compression
    XZ,
}

impl CompressionMethod {
    /// Get the compression method name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionMethod::None => "none",
            CompressionMethod::LZ4 => "lz4",
            CompressionMethod::ZSTD => "zstd",
            CompressionMethod::GZIP => "gzip",
            CompressionMethod::BZIP2 => "bzip2",
            CompressionMethod::XZ => "xz",
        }
    }
}

/// Load balancing strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoadBalancingStrategy {
    /// Round-robin load balancing
    RoundRobin,
    /// Random load balancing
    Random,
    /// Least connections load balancing
    LeastConnections,
    /// Weighted round-robin load balancing
    WeightedRoundRobin,
}

impl LoadBalancingStrategy {
    /// Get the strategy name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            LoadBalancingStrategy::RoundRobin => "round_robin",
            LoadBalancingStrategy::Random => "random",
            LoadBalancingStrategy::LeastConnections => "least_connections",
            LoadBalancingStrategy::WeightedRoundRobin => "weighted_round_robin",
        }
    }
}

/// Server information for load balancing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Server hostname or IP address
    pub host: String,
    /// Server port
    pub port: u16,
    /// Server weight (for weighted load balancing)
    pub weight: u32,
    /// Whether the server is healthy
    pub healthy: bool,
    /// Server priority
    pub priority: u32,
}

impl ServerInfo {
    /// Create new server info
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            weight: 1,
            healthy: true,
            priority: 0,
        }
    }

    /// Set server weight
    pub fn weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    /// Set server priority
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Mark server as healthy
    pub fn mark_healthy(&mut self) {
        self.healthy = true;
    }

    /// Mark server as unhealthy
    pub fn mark_unhealthy(&mut self) {
        self.healthy = false;
    }
}

/// Tracing levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TracingLevel {
    /// Error level
    Error,
    /// Warning level
    Warning,
    /// Info level
    Info,
    /// Debug level
    Debug,
    /// Trace level
    Trace,
}

impl TracingLevel {
    /// Get the level name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            TracingLevel::Error => "error",
            TracingLevel::Warning => "warning",
            TracingLevel::Info => "info",
            TracingLevel::Debug => "debug",
            TracingLevel::Trace => "trace",
        }
    }
}
