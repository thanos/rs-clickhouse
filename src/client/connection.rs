//! Connection management for ClickHouse

use crate::error::{Error, Result};
use crate::types::{Block, Value};
use crate::client::{QueryResult, QuerySettings, QueryMetadata, QueryStats};
use std::collections::HashMap;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};

use tungstenite::Message;

/// Connection to a ClickHouse server
pub struct Connection {
    /// Connection options
    options: crate::client::ClientOptions,
    /// TCP stream for native protocol
    tcp_stream: Option<TcpStream>,
    /// WebSocket stream for HTTP/WebSocket interface
    websocket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    /// Whether the connection is connected
    connected: bool,
    /// Connection ID
    id: String,
    /// Last activity timestamp
    last_activity: Instant,
}

impl Connection {
    /// Create a new connection
    pub fn new(options: crate::client::ClientOptions) -> Self {
        Self {
            options,
            tcp_stream: None,
            websocket: None,
            connected: false,
            id: uuid::Uuid::new_v4().to_string(),
            last_activity: Instant::now(),
        }
    }

    /// Connect to the server
    pub async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }

        let start_time = Instant::now();
        
        if self.options.use_websocket {
            self.connect_websocket().await?;
        } else if self.options.use_http {
            self.connect_http().await?;
        } else {
            self.connect_native().await?;
        }

        self.connected = true;
        self.last_activity = Instant::now();

        tracing::debug!(
            "Connected to {}:{} in {:?}",
            self.options.host,
            self.options.port,
            start_time.elapsed()
        );

        Ok(())
    }

    /// Connect using native protocol
    async fn connect_native(&mut self) -> Result<()> {
        let addr = format!("{}:{}", self.options.host, self.options.port);
        let stream = timeout(
            self.options.connect_timeout,
            TcpStream::connect(&addr)
        ).await
            .map_err(|_| Error::Timeout(self.options.connect_timeout))??;

        // Set TCP options
        stream.set_nodelay(true)?;
        // Note: TCP keepalive configuration is platform-specific and may need adjustment
        // For now, we'll use default keepalive settings

        self.tcp_stream = Some(stream);
        Ok(())
    }

    /// Connect using WebSocket
    async fn connect_websocket(&mut self) -> Result<()> {
        let url = if self.options.use_tls {
            format!("wss://{}:{}{}", self.options.host, self.options.port, self.options.websocket_path)
        } else {
            format!("ws://{}:{}{}", self.options.host, self.options.port, self.options.websocket_path)
        };

        let (ws_stream, _) = timeout(
            self.options.connect_timeout,
            connect_async(url)
        ).await
            .map_err(|_| Error::Timeout(self.options.connect_timeout))??;

        self.websocket = Some(ws_stream);
        Ok(())
    }

    /// Connect using HTTP (placeholder for future implementation)
    async fn connect_http(&mut self) -> Result<()> {
        // HTTP connection will be implemented separately
        Err(Error::Unsupported("HTTP interface not yet implemented".to_string()))
    }

    /// Disconnect from the server
    pub async fn disconnect(&mut self) -> Result<()> {
        if !self.connected {
            return Ok(());
        }

        if let Some(mut ws) = self.websocket.take() {
            let _ = ws.close(None).await;
        }

        if let Some(mut stream) = self.tcp_stream.take() {
            let _ = stream.shutdown().await;
        }

        self.connected = false;
        tracing::debug!("Disconnected from {}:{}", self.options.host, self.options.port);
        Ok(())
    }

    /// Execute a query
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        if !self.connected {
            self.connect().await?;
        }

        let start_time = Instant::now();
        self.last_activity = Instant::now();

        let result = if self.options.use_websocket {
            self.query_websocket(sql).await?
        } else if self.options.use_http {
            self.query_http(sql).await?
        } else {
            self.query_native(sql).await?
        };

        let elapsed = start_time.elapsed();
        tracing::debug!("Query executed in {:?}", elapsed);

        Ok(result)
    }

    /// Execute a query with parameters
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult> {
        // Replace parameters in SQL
        let mut final_sql = sql.to_string();
        for (key, value) in params {
            let placeholder = format!("{{{}}}", key);
            let value_str = match value {
                Value::String(s) => format!("'{}'", s),
                Value::UInt8(v) => v.to_string(),
                Value::UInt16(v) => v.to_string(),
                Value::UInt32(v) => v.to_string(),
                Value::UInt64(v) => v.to_string(),
                Value::Int8(v) => v.to_string(),
                Value::Int16(v) => v.to_string(),
                Value::Int32(v) => v.to_string(),
                Value::Int64(v) => v.to_string(),
                Value::Float32(v) => v.to_string(),
                Value::Float64(v) => v.to_string(),
                Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
                Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S")),
                Value::UUID(u) => format!("'{}'", u),
                _ => format!("{:?}", value),
            };
            final_sql = final_sql.replace(&placeholder, &value_str);
        }

        self.query(&final_sql).await
    }

    /// Execute a query with settings
    pub async fn query_with_settings(
        &mut self,
        sql: &str,
        settings: QuerySettings,
    ) -> Result<QueryResult> {
        let settings_str = settings.build_settings_string();
        let final_sql = if settings_str.is_empty() {
            sql.to_string()
        } else {
            format!("{} SETTINGS {}", sql, settings_str)
        };

        self.query(&final_sql).await
    }

    /// Execute a query (no result)
    pub async fn execute(&mut self, sql: &str) -> Result<()> {
        let _ = self.query(sql).await?;
        Ok(())
    }

    /// Execute a query with parameters (no result)
    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<()> {
        let _ = self.query_with_params(sql, params).await?;
        Ok(())
    }

    /// Execute a query with settings (no result)
    pub async fn execute_with_settings(
        &mut self,
        sql: &str,
        settings: QuerySettings,
    ) -> Result<()> {
        let _ = self.query_with_settings(sql, settings).await?;
        Ok(())
    }

    /// Insert data into a table
    pub async fn insert(&mut self, table: &str, block: Block) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        self.last_activity = Instant::now();

        if self.options.use_websocket {
            self.insert_websocket(table, block).await?;
        } else if self.options.use_http {
            self.insert_http(table, block).await?;
        } else {
            self.insert_native(table, block).await?;
        }

        Ok(())
    }

    /// Insert data with settings
    pub async fn insert_with_settings(
        &mut self,
        table: &str,
        block: Block,
        settings: QuerySettings,
    ) -> Result<()> {
        // For now, just insert without settings
        // TODO: Implement settings support for inserts
        self.insert(table, block).await
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<()> {
        if !self.connected {
            self.connect().await?;
        }

        self.last_activity = Instant::now();

        if self.options.use_websocket {
            self.ping_websocket().await?;
        } else if self.options.use_http {
            self.ping_http().await?;
        } else {
            self.ping_native().await?;
        }

        Ok(())
    }

    /// Get server information
    pub async fn server_info(&mut self) -> Result<HashMap<String, String>> {
        let result = self.query("SELECT name, value FROM system.settings WHERE name IN ('version', 'revision', 'build')").await?;
        
        let mut info = HashMap::new();
        for row in result.rows() {
            if let (Some(name), Some(value)) = (row.get(0), row.get(1)) {
                if let (Some(name_str), Some(value_str)) = (name.as_ref(), value.as_ref()) {
                    if let (Ok(name), Ok(value)) = (crate::types::String::try_from(name_str.clone()), crate::types::String::try_from(value_str.clone())) {
                        info.insert(name.into_inner(), value.into_inner());
                    }
                }
            }
        }
        
        Ok(info)
    }

    /// Get server version
    pub async fn server_version(&mut self) -> Result<String> {
        let info = self.server_info().await?;
        Ok(info.get("version").cloned().unwrap_or_else(|| "unknown".to_string()))
    }

    /// Reset the connection
    pub async fn reset(&mut self) -> Result<()> {
        self.disconnect().await?;
        self.connect().await?;
        Ok(())
    }

    /// Check if the connection is connected
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Get the connection ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the last activity timestamp
    pub fn last_activity(&self) -> Instant {
        self.last_activity
    }

    /// Check if the connection is idle
    pub fn is_idle(&self, timeout: std::time::Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    // Native protocol implementations (placeholders)
    async fn query_native(&mut self, _sql: &str) -> Result<QueryResult> {
        // TODO: Implement native protocol query execution
        Err(Error::Unsupported("Native protocol not yet implemented".to_string()))
    }

    async fn insert_native(&mut self, _table: &str, _block: Block) -> Result<()> {
        // TODO: Implement native protocol insert
        Err(Error::Unsupported("Native protocol not yet implemented".to_string()))
    }

    async fn ping_native(&mut self) -> Result<()> {
        // TODO: Implement native protocol ping
        Err(Error::Unsupported("Native protocol not yet implemented".to_string()))
    }

    // WebSocket implementations (placeholders)
    async fn query_websocket(&mut self, _sql: &str) -> Result<QueryResult> {
        // TODO: Implement WebSocket query execution
        Err(Error::Unsupported("WebSocket interface not yet implemented".to_string()))
    }

    async fn insert_websocket(&mut self, _table: &str, _block: Block) -> Result<()> {
        // TODO: Implement WebSocket insert
        Err(Error::Unsupported("WebSocket interface not yet implemented".to_string()))
    }

    async fn ping_websocket(&mut self) -> Result<()> {
        // TODO: Implement WebSocket ping
        Err(Error::Unsupported("WebSocket interface not yet implemented".to_string()))
    }

    // HTTP implementations (placeholders)
    async fn query_http(&mut self, _sql: &str) -> Result<QueryResult> {
        // TODO: Implement HTTP query execution
        Err(Error::Unsupported("HTTP interface not yet implemented".to_string()))
    }

    async fn insert_http(&mut self, _table: &str, _block: Block) -> Result<()> {
        // TODO: Implement HTTP insert
        Err(Error::Unsupported("HTTP interface not yet implemented".to_string()))
    }

    async fn ping_http(&mut self) -> Result<()> {
        // TODO: Implement HTTP ping
        Err(Error::Unsupported("HTTP interface not yet implemented".to_string()))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.connected {
            // Try to disconnect, but don't block
            let _ = tokio::task::spawn(async move {
                // This is a bit of a hack, but it's the best we can do in Drop
                // In practice, users should call disconnect() explicitly
            });
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("options", &self.options)
            .field("connected", &self.connected)
            .field("id", &self.id)
            .field("last_activity", &self.last_activity)
            .finish()
    }
}
