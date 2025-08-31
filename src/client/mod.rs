//! ClickHouse client implementation

mod connection;
mod options;
mod pool;
mod query;
mod grpc;
mod retry;
mod load_balancer;
mod metrics;
mod circuit_breaker;

pub use connection::Connection;
pub use options::ClientOptions;
pub use pool::ConnectionPool;
pub use query::{Query, QueryResult, QuerySettings, QueryMetadata, QueryStats};
pub use grpc::GrpcClient;
pub use retry::{RetryConfig, RetryStrategy, with_retry, with_retry_config};
pub use load_balancer::{LoadBalancer, LoadBalancingStrategy, ServerInfo};
pub use metrics::{MetricsRegistry, MetricsCollector, Metric, MetricType, MetricValue};
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerBuilder, CircuitBreakerState};

use crate::error::Result;
use crate::types::{Block, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Main ClickHouse client
pub struct Client {
    options: ClientOptions,
    pool: Arc<ConnectionPool>,
    load_balancer: Option<Arc<LoadBalancer>>,
    metrics: Arc<MetricsRegistry>,
    circuit_breaker: Arc<CircuitBreaker>,
    retry_config: RetryConfig,
}

impl Client {
    /// Create a new client with the specified options
    pub fn new(options: ClientOptions) -> Result<Self> {
        let pool = Arc::new(ConnectionPool::new(options.clone())?);
        
        let load_balancer = if options.use_load_balancing && !options.servers.is_empty() {
            Some(Arc::new(LoadBalancer::from_options(&options)?))
        } else {
            None
        };

        let metrics = Arc::new(MetricsRegistry::new(options.metrics_prefix.clone()));
        
        let circuit_breaker = Arc::new(CircuitBreakerBuilder::new()
            .failure_threshold(options.max_retries)
            .open_timeout(Duration::from_secs(30))
            .success_threshold(3)
            .enabled(options.use_retry)
            .build());

        let retry_config = RetryConfig::new()
            .max_attempts(options.max_retries)
            .strategy(RetryStrategy::ExponentialBackoff {
                initial_delay: options.retry_delay,
                max_delay: Duration::from_secs(30),
                multiplier: 2.0,
                jitter: true,
            })
            .retry_on(|e| e.is_retryable())
            .operation_timeout(options.query_timeout);

        Ok(Client {
            options,
            pool,
            load_balancer,
            metrics,
            circuit_breaker,
            retry_config,
        })
    }

    /// Create a new client with default options
    pub fn default() -> Result<Self> {
        Self::new(ClientOptions::default())
    }

    /// Execute a query and return the result with retry logic
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let collector = MetricsCollector::new(self.metrics.clone(), "query".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.query(sql).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Execute a query with parameters and retry logic
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let collector = MetricsCollector::new(self.metrics.clone(), "query_with_params".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.query_with_params(sql, params.clone()).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Execute a query with settings and retry logic
    pub async fn query_with_settings(
        &self,
        sql: &str,
        settings: QuerySettings,
    ) -> Result<QueryResult> {
        let collector = MetricsCollector::new(self.metrics.clone(), "query_with_settings".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.query_with_settings(sql, settings.clone()).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Execute a query and return the result with retry logic
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let collector = MetricsCollector::new(self.metrics.clone(), "execute".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.execute(sql).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Execute a query with parameters and retry logic
    pub async fn execute_with_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<()> {
        let collector = MetricsCollector::new(self.metrics.clone(), "execute_with_params".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.execute_with_params(sql, params.clone()).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Execute a query with settings and retry logic
    pub async fn execute_with_settings(
        &self,
        sql: &str,
        settings: QuerySettings,
    ) -> Result<()> {
        let collector = MetricsCollector::new(self.metrics.clone(), "execute_with_settings".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.execute_with_settings(sql, settings.clone()).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Insert data into a table with retry logic
    pub async fn insert(&self, table: &str, block: Block) -> Result<()> {
        let collector = MetricsCollector::new(self.metrics.clone(), "insert".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.insert(table, block.clone()).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Insert data into a table with settings and retry logic
    pub async fn insert_with_settings(
        &self,
        table: &str,
        block: Block,
        settings: QuerySettings,
    ) -> Result<()> {
        let collector = MetricsCollector::new(self.metrics.clone(), "insert_with_settings".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.insert_with_settings(table, block.clone(), settings.clone()).await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Ping the server with retry logic
    pub async fn ping(&self) -> Result<()> {
        let collector = MetricsCollector::new(self.metrics.clone(), "ping".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.ping().await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Get server information with retry logic
    pub async fn server_info(&self) -> Result<HashMap<String, String>> {
        let collector = MetricsCollector::new(self.metrics.clone(), "server_info".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.server_info().await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Get server version with retry logic
    pub async fn server_version(&self) -> Result<String> {
        let collector = MetricsCollector::new(self.metrics.clone(), "server_version".to_string());
        
        let result = self.circuit_breaker.execute(|| async {
            let mut connection = self.pool.get_connection().await?;
            connection.server_version().await
        }).await;

        collector.record_result(&result, None).await?;
        result
    }

    /// Reset the connection (useful for retry logic)
    pub async fn reset_connection(&self) -> Result<()> {
        let mut connection = self.pool.get_connection().await?;
        connection.reset().await
    }

    /// Execute a query with custom retry configuration
    pub async fn query_with_retry(
        &self,
        sql: &str,
        retry_config: RetryConfig,
    ) -> Result<QueryResult> {
        with_retry_config(retry_config, || async {
            let mut connection = self.pool.get_connection().await?;
            connection.query(sql).await
        }).await
    }

    /// Execute a query with custom retry configuration and parameters
    pub async fn query_with_retry_and_params(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
        retry_config: RetryConfig,
    ) -> Result<QueryResult> {
        with_retry_config(retry_config, || async {
            let mut connection = self.pool.get_connection().await?;
            connection.query_with_params(sql, params.clone()).await
        }).await
    }

    /// Get the client options
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    /// Get the connection pool
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }

    /// Get the load balancer
    pub fn load_balancer(&self) -> Option<&Arc<LoadBalancer>> {
        self.load_balancer.as_ref()
    }

    /// Get the metrics registry
    pub fn metrics(&self) -> &Arc<MetricsRegistry> {
        &self.metrics
    }

    /// Get the circuit breaker
    pub fn circuit_breaker(&self) -> &Arc<CircuitBreaker> {
        &self.circuit_breaker
    }

    /// Get the retry configuration
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Create a GRPC client with the same options
    pub fn grpc_client(&self) -> Result<GrpcClient> {
        GrpcClient::new(self.options.clone())
    }

    /// Get client health status
    pub async fn health_check(&self) -> ClientHealth {
        let pool_stats = self.pool.stats().await;
        let circuit_breaker_health = self.circuit_breaker.get_health_status().await;
        let load_balancer_stats = if let Some(lb) = &self.load_balancer {
            Some(lb.get_stats().await)
        } else {
            None
        };

        ClientHealth {
            pool_stats,
            circuit_breaker_health,
            load_balancer_stats,
            metrics_enabled: self.metrics.is_enabled(),
        }
    }

    /// Export metrics in Prometheus format
    pub async fn export_metrics(&self) -> String {
        self.metrics.export_prometheus().await
    }

    /// Update connection pool metrics
    async fn update_pool_metrics(&self) {
        let pool_stats = self.pool.stats().await;
        
        self.metrics.set_gauge("connection_pool_size", pool_stats.total_connections as f64, None).await.ok();
        self.metrics.set_gauge("connection_pool_active", pool_stats.active_connections as f64, None).await.ok();
        self.metrics.set_gauge("connection_pool_idle", pool_stats.idle_connections as f64, None).await.ok();
        self.metrics.observe_histogram("connection_pool_wait_time", pool_stats.average_wait_time().as_secs_f64(), None).await.ok();
    }

    /// Update load balancer metrics
    async fn update_load_balancer_metrics(&self) {
        if let Some(lb) = &self.load_balancer {
            let lb_stats = lb.get_stats().await;
            
            self.metrics.set_gauge("load_balancer_servers_total", lb_stats.total_servers as f64, None).await.ok();
            self.metrics.set_gauge("load_balancer_servers_healthy", lb_stats.healthy_servers as f64, None).await.ok();
            
            if let Some(avg_response_time) = lb_stats.avg_response_time {
                self.metrics.observe_histogram("load_balancer_server_response_time", avg_response_time.as_secs_f64(), None).await.ok();
            }
        }
    }

    /// Start background metric updates
    pub fn start_metric_updates(&self) {
        let metrics = self.metrics.clone();
        let pool = self.pool.clone();
        let load_balancer = self.load_balancer.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // Update pool metrics
                let pool_stats = pool.stats().await;
                metrics.set_gauge("connection_pool_size", pool_stats.total_connections as f64, None).await.ok();
                metrics.set_gauge("connection_pool_active", pool_stats.active_connections as f64, None).await.ok();
                metrics.set_gauge("connection_pool_idle", pool_stats.idle_connections as f64, None).await.ok();
                metrics.observe_histogram("connection_pool_wait_time", pool_stats.average_wait_time().as_secs_f64(), None).await.ok();
                
                // Update load balancer metrics
                if let Some(lb) = &load_balancer {
                    let lb_stats = lb.get_stats().await;
                    metrics.set_gauge("load_balancer_servers_total", lb_stats.total_servers as f64, None).await.ok();
                    metrics.set_gauge("load_balancer_servers_healthy", lb_stats.healthy_servers as f64, None).await.ok();
                    
                    if let Some(avg_response_time) = lb_stats.avg_response_time {
                        metrics.observe_histogram("load_balancer_server_response_time", avg_response_time.as_secs_f64(), None).await.ok();
                    }
                }
            }
        });
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Client {
            options: self.options.clone(),
            pool: Arc::clone(&self.pool),
            load_balancer: self.load_balancer.clone(),
            metrics: Arc::clone(&self.metrics),
            circuit_breaker: Arc::clone(&self.circuit_breaker),
            retry_config: self.retry_config.clone(),
        }
    }
}

/// Client health status
#[derive(Clone)]
pub struct ClientHealth {
    /// Connection pool statistics
    pub pool_stats: crate::client::pool::PoolStats,
    /// Circuit breaker health
    pub circuit_breaker_health: crate::client::circuit_breaker::CircuitBreakerHealth,
    /// Load balancer statistics (if enabled)
    pub load_balancer_stats: Option<crate::client::load_balancer::LoadBalancerStats>,
    /// Whether metrics are enabled
    pub metrics_enabled: bool,
}

impl ClientHealth {
    /// Check if the client is healthy overall
    pub fn is_healthy(&self) -> bool {
        self.pool_stats.idle_connections > 0 && 
        self.circuit_breaker_health.is_healthy
    }

    /// Get a summary of the client health
    pub fn summary(&self) -> String {
        let pool_status = if self.pool_stats.idle_connections > 0 { "OK" } else { "WARNING" };
        let circuit_status = if self.circuit_breaker_health.is_healthy { "OK" } else { "OPEN" };
        
        format!("Pool: {}, Circuit Breaker: {}, Metrics: {}", 
                pool_status, circuit_status, 
                if self.metrics_enabled { "Enabled" } else { "Disabled" })
    }
}












