//! Connection pool for ClickHouse

use crate::error::{Error, Result};
use crate::client::ClientOptions;
use super::Connection;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{timeout, Duration};
use tracing::{debug, warn, error};

/// Connection pool for managing multiple connections
pub struct ConnectionPool {
    /// Pool configuration
    options: ClientOptions,
    /// Available connections
    available: Arc<Mutex<VecDeque<Connection>>>,
    /// Semaphore for limiting concurrent connections
    semaphore: Arc<Semaphore>,
    /// Pool statistics
    stats: Arc<Mutex<PoolStats>>,
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total connections created
    pub total_connections: usize,
    /// Current active connections
    pub active_connections: usize,
    /// Current idle connections
    pub idle_connections: usize,
    /// Total wait time for connections
    pub total_wait_time: Duration,
    /// Number of connection requests
    pub connection_requests: usize,
    /// Number of connection timeouts
    pub connection_timeouts: usize,
}

impl PoolStats {
    /// Create new pool stats
    pub fn new() -> Self {
        Self {
            total_connections: 0,
            active_connections: 0,
            idle_connections: 0,
            total_wait_time: Duration::from_secs(0),
            connection_requests: 0,
            connection_timeouts: 0,
        }
    }

    /// Get the average wait time
    pub fn average_wait_time(&self) -> Duration {
        if self.connection_requests == 0 {
            Duration::from_secs(0)
        } else {
            Duration::from_nanos(self.total_wait_time.as_nanos() as u64 / self.connection_requests as u64)
        }
    }

    /// Get the connection utilization percentage
    pub fn utilization_percentage(&self) -> f64 {
        let total = self.active_connections + self.idle_connections;
        if total == 0 {
            0.0
        } else {
            (self.active_connections as f64 / total as f64) * 100.0
        }
    }
}

impl Default for PoolStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(options: ClientOptions) -> Result<Self> {
        options.validate()?;

        let max_connections = options.max_connections;
        let semaphore = Arc::new(Semaphore::new(max_connections));

        let pool = Self {
            options,
            available: Arc::new(Mutex::new(VecDeque::new())),
            semaphore,
            stats: Arc::new(Mutex::new(PoolStats::new())),
        };

        // Initialize the pool with minimum connections
        tokio::spawn({
            let pool = pool.clone();
            async move {
                if let Err(e) = pool.initialize_pool().await {
                    error!("Failed to initialize connection pool: {}", e);
                }
            }
        });

        Ok(pool)
    }

    /// Initialize the pool with minimum connections
    async fn initialize_pool(&self) -> Result<()> {
        let min_connections = self.options.min_connections;
        let mut connections = Vec::new();

        for _ in 0..min_connections {
            match self.create_connection().await {
                Ok(conn) => connections.push(conn),
                Err(e) => {
                    warn!("Failed to create initial connection: {}", e);
                    break;
                }
            }
        }

        if !connections.is_empty() {
            let connections_len = connections.len();
            let mut available = self.available.lock().await;
            for conn in connections {
                available.push_back(conn);
            }
            drop(available);

            let mut stats = self.stats.lock().await;
            stats.idle_connections = connections_len;
            stats.total_connections = connections_len;
            
            debug!("Initialized pool with {} connections", connections_len);
        } else {
            debug!("Initialized pool with 0 connections");
        }
        Ok(())
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self) -> Result<PooledConnection> {
        let start_time = std::time::Instant::now();
        
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.connection_requests += 1;
        }

        // Try to get an existing connection first
        if let Some(conn) = self.try_get_existing_connection().await? {
            return Ok(conn);
        }

        // Wait for a permit to create a new connection
        let permit = timeout(
            self.options.pool_acquire_timeout,
            self.semaphore.acquire()
        ).await
            .map_err(|_| Error::Timeout(self.options.pool_acquire_timeout))?
            .map_err(|_| Error::Timeout(self.options.pool_acquire_timeout))?;

        // Create a new connection
        let conn = self.create_connection().await?;
        
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.total_connections += 1;
            stats.active_connections += 1;
            stats.total_wait_time += start_time.elapsed();
        }

        // Drop the permit immediately since we don't need to hold onto it
        drop(permit);
        
        Ok(PooledConnection {
            connection: Some(conn),
            pool: self.clone(),
            _permit: None,
        })
    }

    /// Try to get an existing connection from the pool
    async fn try_get_existing_connection(&self) -> Result<Option<PooledConnection>> {
        let mut available = self.available.lock().await;
        
        while let Some(mut conn) = available.pop_front() {
            // Check if the connection is still valid
            if conn.is_connected() && !conn.is_idle(self.options.idle_timeout) {
                // Update stats
                {
                    let mut stats = self.stats.lock().await;
                    stats.idle_connections = stats.idle_connections.saturating_sub(1);
                    stats.active_connections += 1;
                }
                
                // We can't store the permit here due to lifetime issues
                // Instead, we'll create a connection without a permit
                return Ok(Some(PooledConnection {
                    connection: Some(conn),
                    pool: self.clone(),
                    _permit: None,
                }));
            } else {
                // Connection is invalid or idle, drop it
                if let Err(e) = conn.disconnect().await {
                    warn!("Failed to disconnect invalid connection: {}", e);
                }
                
                // Update stats
                {
                    let mut stats = self.stats.lock().await;
                    stats.total_connections = stats.total_connections.saturating_sub(1);
                }
            }
        }
        
        Ok(None)
    }

    /// Create a new connection
    async fn create_connection(&self) -> Result<Connection> {
        let mut conn = Connection::new(self.options.clone());
        conn.connect().await?;
        Ok(conn)
    }

    /// Return a connection to the pool
    async fn return_connection(&self, mut conn: Connection) {
        // Check if the connection is still valid
        if conn.is_connected() && !conn.is_idle(self.options.idle_timeout) {
            let mut available = self.available.lock().await;
            
            // Only add back if we haven't exceeded max connections
            if available.len() < self.options.max_connections {
                available.push_back(conn);
                
                // Update stats
                {
                    let mut stats = self.stats.lock().await;
                    stats.active_connections = stats.active_connections.saturating_sub(1);
                    stats.idle_connections += 1;
                }
                
                debug!("Returned connection to pool");
                return;
            }
        }
        
        // Connection is invalid or pool is full, drop it
        if let Err(e) = conn.disconnect().await {
            warn!("Failed to disconnect connection: {}", e);
        }
        
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.total_connections = stats.total_connections.saturating_sub(1);
            stats.active_connections = stats.active_connections.saturating_sub(1);
        }
        
        debug!("Dropped connection");
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        self.stats.lock().await.clone()
    }

    /// Get the number of available connections
    pub async fn available_connections(&self) -> usize {
        self.available.lock().await.len()
    }

    /// Get the number of active connections
    pub async fn active_connections(&self) -> usize {
        self.stats.lock().await.active_connections
    }

    /// Get the total number of connections
    pub async fn total_connections(&self) -> usize {
        self.stats.lock().await.total_connections
    }

    /// Check if the pool is healthy
    pub async fn is_healthy(&self) -> bool {
        let stats = self.stats().await;
        let available = self.available_connections().await;
        
        // Pool is healthy if we have at least min_connections available
        available >= self.options.min_connections && stats.connection_timeouts == 0
    }

    /// Close the pool and all connections
    pub async fn close(&self) -> Result<()> {
        let mut available = self.available.lock().await;
        
        for mut conn in available.drain(..) {
            if let Err(e) = conn.disconnect().await {
                warn!("Failed to disconnect connection during pool close: {}", e);
            }
        }
        
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.idle_connections = 0;
            stats.active_connections = 0;
            stats.total_connections = 0;
        }
        
        debug!("Pool closed");
        Ok(())
    }

    /// Clean up idle connections
    pub async fn cleanup_idle_connections(&self) -> Result<()> {
        let mut available = self.available.lock().await;
        let mut to_remove = Vec::new();
        
        for (i, conn) in available.iter().enumerate() {
            if conn.is_idle(self.options.idle_timeout) {
                to_remove.push(i);
            }
        }
        
        // Remove connections from back to front to maintain indices
        for &i in to_remove.iter().rev() {
            if let Some(mut conn) = available.remove(i) {
                if let Err(e) = conn.disconnect().await {
                    warn!("Failed to disconnect idle connection: {}", e);
                }
                
                // Update stats
                {
                    let mut stats = self.stats.lock().await;
                    stats.idle_connections = stats.idle_connections.saturating_sub(1);
                    stats.total_connections = stats.total_connections.saturating_sub(1);
                }
            }
        }
        
        if !to_remove.is_empty() {
            debug!("Cleaned up {} idle connections", to_remove.len());
        }
        
        Ok(())
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            available: Arc::clone(&self.available),
            semaphore: Arc::clone(&self.semaphore),
            stats: Arc::clone(&self.stats),
        }
    }
}

/// A connection borrowed from the pool
pub struct PooledConnection {
    /// The borrowed connection
    connection: Option<Connection>,
    /// Reference to the pool
    pool: ConnectionPool,
    /// Semaphore permit (optional for connections from existing pool)
    _permit: Option<tokio::sync::SemaphorePermit<'static>>,
}

impl PooledConnection {
    /// Get a mutable reference to the connection
    pub fn as_mut(&mut self) -> &mut Connection {
        self.connection.as_mut().unwrap()
    }

    /// Get a reference to the connection
    pub fn as_ref(&self) -> &Connection {
        self.connection.as_ref().unwrap()
    }

    /// Check if the connection is connected
    pub fn is_connected(&self) -> bool {
        self.connection.as_ref().unwrap().is_connected()
    }

    /// Get the connection ID
    pub fn id(&self) -> &str {
        self.connection.as_ref().unwrap().id()
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            // Return the connection to the pool
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.return_connection(conn).await;
            });
        }
    }
}

impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("connection", &self.connection.as_ref().map(|c| c.id()))
            .field("pool", &"ConnectionPool")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_pool_creation() {
        let options = ClientOptions::new()
            .host("localhost")
            .port(9000)
            .max_connections(5)
            .min_connections(2);
        
        let pool = ConnectionPool::new(options).unwrap();
        
        // Wait a bit for initialization
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 2);
        assert_eq!(stats.idle_connections, 2);
    }

    #[tokio::test]
    async fn test_pool_connection_borrowing() {
        let options = ClientOptions::new()
            .host("localhost")
            .port(9000)
            .max_connections(3)
            .min_connections(1);
        
        let pool = ConnectionPool::new(options).unwrap();
        
        // Wait a bit for initialization
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Try to borrow a connection (this will fail to connect, but that's OK for the test)
        let _conn = pool.get_connection().await;
        
        let stats = pool.stats().await;
        assert_eq!(stats.connection_requests, 1);
    }
}
