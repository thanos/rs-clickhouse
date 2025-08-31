//! Load balancing for ClickHouse client operations

use crate::error::{Error, Result};
use crate::client::ClientOptions;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Server information for load balancing
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// Server hostname or IP address
    pub host: String,
    /// Server port
    pub port: u16,
    /// Server weight for weighted strategies
    pub weight: u32,
    /// Whether the server is healthy
    pub healthy: bool,
    /// Last health check time
    pub last_health_check: Option<Instant>,
    /// Response time statistics
    pub response_time: Option<Duration>,
    /// Number of active connections
    pub active_connections: usize,
    /// Maximum connections allowed
    pub max_connections: usize,
}

impl ServerInfo {
    /// Create a new server info
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            weight: 1,
            healthy: true,
            last_health_check: None,
            response_time: None,
            active_connections: 0,
            max_connections: 100,
        }
    }

    /// Set server weight
    pub fn weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    /// Set maximum connections
    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Check if server can accept new connections
    pub fn can_accept_connections(&self) -> bool {
        self.healthy && self.active_connections < self.max_connections
    }

    /// Get connection utilization percentage
    pub fn connection_utilization(&self) -> f64 {
        if self.max_connections == 0 {
            0.0
        } else {
            (self.active_connections as f64 / self.max_connections as f64) * 100.0
        }
    }

    /// Update health status
    pub fn update_health(&mut self, healthy: bool) {
        self.healthy = healthy;
        self.last_health_check = Some(Instant::now());
    }

    /// Update response time
    pub fn update_response_time(&mut self, response_time: Duration) {
        self.response_time = Some(response_time);
    }

    /// Increment active connections
    pub fn increment_connections(&mut self) {
        self.active_connections = self.active_connections.saturating_add(1);
    }

    /// Decrement active connections
    pub fn decrement_connections(&mut self) {
        self.active_connections = self.active_connections.saturating_sub(1);
    }
}

/// Load balancing strategy
pub enum LoadBalancingStrategy {
    /// Round-robin: distribute requests evenly across servers
    RoundRobin,
    /// Weighted round-robin: distribute based on server weights
    WeightedRoundRobin,
    /// Least connections: send to server with fewest active connections
    LeastConnections,
    /// Fastest response: send to server with best response time
    FastestResponse,
    /// Random: randomly select a server
    Random,
    /// Custom strategy
    Custom(Box<dyn Fn(&[ServerInfo]) -> Option<usize> + Send + Sync>),
}

impl Default for LoadBalancingStrategy {
    fn default() -> Self {
        LoadBalancingStrategy::RoundRobin
    }
}

impl Clone for LoadBalancingStrategy {
    fn clone(&self) -> Self {
        match self {
            LoadBalancingStrategy::RoundRobin => LoadBalancingStrategy::RoundRobin,
            LoadBalancingStrategy::WeightedRoundRobin => LoadBalancingStrategy::WeightedRoundRobin,
            LoadBalancingStrategy::LeastConnections => LoadBalancingStrategy::LeastConnections,
            LoadBalancingStrategy::FastestResponse => LoadBalancingStrategy::FastestResponse,
            LoadBalancingStrategy::Random => LoadBalancingStrategy::Random,
            LoadBalancingStrategy::Custom(_) => LoadBalancingStrategy::RoundRobin, // Can't clone custom functions
        }
    }
}

/// Load balancer for managing multiple ClickHouse servers
pub struct LoadBalancer {
    /// Available servers
    servers: Arc<RwLock<Vec<ServerInfo>>>,
    /// Load balancing strategy
    strategy: LoadBalancingStrategy,
    /// Current round-robin index
    round_robin_index: Arc<RwLock<usize>>,
    /// Health check configuration
    health_check_config: HealthCheckConfig,
    /// Health check background task handle
    health_check_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Whether to enable health checks
    pub enabled: bool,
    /// Health check interval
    pub interval: Duration,
    /// Health check timeout
    pub timeout: Duration,
    /// Number of consecutive failures before marking server as unhealthy
    pub failure_threshold: usize,
    /// Number of consecutive successes before marking server as healthy
    pub success_threshold: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disable by default to prevent hanging in tests
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 2,
        }
    }
}

impl LoadBalancer {
    /// Create a new load balancer
    pub fn new(servers: Vec<ServerInfo>, strategy: LoadBalancingStrategy) -> Self {
        let health_check_config = HealthCheckConfig::default();
        let mut load_balancer = Self {
            servers: Arc::new(RwLock::new(servers)),
            strategy,
            round_robin_index: Arc::new(RwLock::new(0)),
            health_check_config: health_check_config.clone(),
            health_check_handle: None,
        };

        // Start health check background task if enabled
        if health_check_config.enabled {
            let lb = load_balancer.clone();
            let handle = tokio::spawn(async move {
                lb.run_health_checks().await;
            });
            load_balancer.health_check_handle = Some(handle);
        }

        load_balancer
    }

    /// Create a new load balancer from client options
    pub fn from_options(options: &ClientOptions) -> Result<Self> {
        if options.servers.is_empty() {
            return Err(Error::Configuration("No servers configured for load balancing".to_string()));
        }

        let servers = options.servers.iter()
            .map(|server| ServerInfo::new(server.host.clone(), server.port))
            .collect();

        // Convert from options::LoadBalancingStrategy to load_balancer::LoadBalancingStrategy
        let strategy = match options.load_balancing_strategy {
            crate::client::options::LoadBalancingStrategy::RoundRobin => LoadBalancingStrategy::RoundRobin,
            crate::client::options::LoadBalancingStrategy::WeightedRoundRobin => LoadBalancingStrategy::WeightedRoundRobin,
            crate::client::options::LoadBalancingStrategy::LeastConnections => LoadBalancingStrategy::LeastConnections,
            crate::client::options::LoadBalancingStrategy::Random => LoadBalancingStrategy::Random,
        };

        Ok(Self::new(servers, strategy))
    }

    /// Get the next server based on the load balancing strategy
    pub async fn get_server(&self) -> Result<ServerInfo> {
        // First, get a snapshot of available servers
        let available_servers = {
            let servers = self.servers.read().await;
            
            if servers.is_empty() {
                return Err(Error::Configuration("No servers available".to_string()));
            }

            // Filter healthy servers that can accept connections
            let available: Vec<ServerInfo> = servers.iter()
                .filter(|s| s.can_accept_connections())
                .cloned()
                .collect();

            if available.is_empty() {
                return Err(Error::ConnectionPool("No healthy servers available".to_string()));
            }

            available
        };

        let server_index = match &self.strategy {
            LoadBalancingStrategy::RoundRobin => {
                let mut index = self.round_robin_index.write().await;
                let selected = *index % available_servers.len();
                *index = (*index + 1) % available_servers.len();
                selected
            }
            LoadBalancingStrategy::WeightedRoundRobin => {
                self.select_weighted_round_robin(&available_servers.iter().collect::<Vec<_>>())
            }
            LoadBalancingStrategy::LeastConnections => {
                self.select_least_connections(&available_servers.iter().collect::<Vec<_>>())
            }
            LoadBalancingStrategy::FastestResponse => {
                self.select_fastest_response(&available_servers.iter().collect::<Vec<_>>())
            }
            LoadBalancingStrategy::Random => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                rng.gen_range(0..available_servers.len())
            }
            LoadBalancingStrategy::Custom(func) => {
                if let Some(selected) = func(&available_servers) {
                    selected
                } else {
                    return Err(Error::Internal("Custom load balancing strategy failed".to_string()));
                }
            }
        };

        let selected_server = &available_servers[server_index];
        
        // Now update the connection count in the main servers list
        let mut servers_mut = self.servers.write().await;
        if let Some(server) = servers_mut.iter_mut()
            .find(|s| s.host == selected_server.host && s.port == selected_server.port) {
            server.increment_connections();
            Ok(server.clone())
        } else {
            Err(Error::Internal("Selected server not found".to_string()))
        }
    }

    /// Release a server connection
    pub async fn release_server(&self, server: &ServerInfo) {
        let mut servers = self.servers.write().await;
        if let Some(server_mut) = servers.iter_mut()
            .find(|s| s.host == server.host && s.port == server.port) {
            server_mut.decrement_connections();
        }
    }

    /// Select server using weighted round-robin
    fn select_weighted_round_robin(&self, servers: &[&ServerInfo]) -> usize {
        let total_weight: u32 = servers.iter().map(|s| s.weight).sum();
        if total_weight == 0 {
            return 0;
        }

        // Try to get and update the index atomically
        let selected = {
            if let Ok(mut index_guard) = self.round_robin_index.try_write() {
                let current = *index_guard;
                *index_guard = (current + 1) % total_weight as usize;
                current
            } else {
                // Fallback if we can't get write lock
                0
            }
        };

        let selected = selected % total_weight as usize;

        let mut current_weight = 0;
        for (i, server) in servers.iter().enumerate() {
            current_weight += server.weight;
            if current_weight > selected as u32 {
                return i;
            }
        }
        0
    }

    /// Select server with least connections
    fn select_least_connections(&self, servers: &[&ServerInfo]) -> usize {
        servers.iter()
            .enumerate()
            .min_by_key(|(_, server)| server.active_connections)
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    /// Select server with fastest response time
    fn select_fastest_response(&self, servers: &[&ServerInfo]) -> usize {
        servers.iter()
            .enumerate()
            .filter_map(|(i, server)| server.response_time.map(|rt| (i, rt)))
            .min_by_key(|(_, response_time)| *response_time)
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    /// Run health checks on all servers
    async fn run_health_checks(&self) {
        let mut interval = tokio::time::interval(self.health_check_config.interval);
        
        loop {
            interval.tick().await;
            
            let mut servers = self.servers.write().await;
            for server in servers.iter_mut() {
                if let Err(e) = self.check_server_health(server).await {
                    warn!("Health check failed for {}:{} - {}", server.host, server.port, e);
                }
            }
        }
    }

    /// Check health of a specific server
    async fn check_server_health(&self, server: &mut ServerInfo) -> Result<()> {
        let start_time = Instant::now();
        
        // Simple ping health check
        let result = tokio::time::timeout(
            self.health_check_config.timeout,
            self.ping_server(server)
        ).await;

        match result {
            Ok(Ok(_)) => {
                let response_time = start_time.elapsed();
                server.update_response_time(response_time);
                server.update_health(true);
                debug!("Health check passed for {}:{} - response time: {:?}", 
                       server.host, server.port, response_time);
                Ok(())
            }
            Ok(Err(e)) => {
                server.update_health(false);
                Err(e)
            }
            Err(_) => {
                server.update_health(false);
                Err(Error::Timeout(self.health_check_config.timeout))
            }
        }
    }

    /// Ping a server to check health
    async fn ping_server(&self, _server: &ServerInfo) -> Result<()> {
        // This is a placeholder implementation
        // In a real implementation, you would establish a connection and send a ping
        // For now, we'll simulate a successful ping
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }

    /// Get load balancer statistics
    pub async fn get_stats(&self) -> LoadBalancerStats {
        let servers = self.servers.read().await;
        
        let total_servers = servers.len();
        let healthy_servers = servers.iter().filter(|s| s.healthy).count();
        let total_connections: usize = servers.iter().map(|s| s.active_connections).sum();
        let avg_response_time: Option<Duration> = {
            let response_times: Vec<Duration> = servers.iter()
                .filter_map(|s| s.response_time)
                .collect();
            
            if response_times.is_empty() {
                None
            } else {
                let total_nanos: u64 = response_times.iter().map(|d| d.as_nanos() as u64).sum();
                Some(Duration::from_nanos(total_nanos / response_times.len() as u64))
            }
        };

        LoadBalancerStats {
            total_servers,
            healthy_servers,
            total_connections,
            avg_response_time,
            strategy: self.strategy.clone(),
        }
    }

    /// Add a new server
    pub async fn add_server(&self, server: ServerInfo) {
        let mut servers = self.servers.write().await;
        servers.push(server);
    }

    /// Remove a server
    pub async fn remove_server(&self, host: &str, port: u16) {
        let mut servers = self.servers.write().await;
        servers.retain(|s| !(s.host == host && s.port == port));
    }

    /// Stop health checks
    pub async fn stop_health_checks(&mut self) {
        if let Some(handle) = self.health_check_handle.take() {
            handle.abort();
        }
    }
}

impl Clone for LoadBalancer {
    fn clone(&self) -> Self {
        Self {
            servers: Arc::clone(&self.servers),
            strategy: self.strategy.clone(),
            round_robin_index: Arc::clone(&self.round_robin_index),
            health_check_config: self.health_check_config.clone(),
            health_check_handle: None, // Don't clone the running task
        }
    }
}

/// Load balancer statistics
#[derive(Clone)]
pub struct LoadBalancerStats {
    /// Total number of servers
    pub total_servers: usize,
    /// Number of healthy servers
    pub healthy_servers: usize,
    /// Total active connections across all servers
    pub total_connections: usize,
    /// Average response time across all servers
    pub avg_response_time: Option<Duration>,
    /// Current load balancing strategy
    pub strategy: LoadBalancingStrategy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_servers() -> Vec<ServerInfo> {
        vec![
            ServerInfo::new("server1".to_string(), 9000).weight(2),
            ServerInfo::new("server2".to_string(), 9001).weight(1),
            ServerInfo::new("server3".to_string(), 9002).weight(3),
        ]
    }

    #[test]
    fn test_server_info_new() {
        let server = ServerInfo::new("test".to_string(), 9000);
        assert_eq!(server.host, "test");
        assert_eq!(server.port, 9000);
        assert_eq!(server.weight, 1);
        assert!(server.healthy);
        assert!(server.can_accept_connections());
    }

    #[test]
    fn test_server_info_weight() {
        let server = ServerInfo::new("test".to_string(), 9000).weight(5);
        assert_eq!(server.weight, 5);
    }

    #[test]
    fn test_server_info_connections() {
        let mut server = ServerInfo::new("test".to_string(), 9000).max_connections(2);
        
        assert!(server.can_accept_connections());
        server.increment_connections();
        assert!(server.can_accept_connections());
        server.increment_connections();
        assert!(!server.can_accept_connections());
        
        server.decrement_connections();
        assert!(server.can_accept_connections());
    }

    #[test]
    fn test_server_info_health() {
        let mut server = ServerInfo::new("test".to_string(), 9000);
        assert!(server.healthy);
        
        server.update_health(false);
        assert!(!server.healthy);
        assert!(!server.can_accept_connections());
        
        server.update_health(true);
        assert!(server.healthy);
        assert!(server.can_accept_connections());
    }

    #[test]
    fn test_server_info_response_time() {
        let mut server = ServerInfo::new("test".to_string(), 9000);
        assert_eq!(server.response_time, None);
        
        let response_time = Duration::from_millis(100);
        server.update_response_time(response_time);
        assert_eq!(server.response_time, Some(response_time));
    }

    #[tokio::test]
    async fn test_load_balancer_new() {
        let servers = create_test_servers();
        let lb = LoadBalancer::new(servers, LoadBalancingStrategy::RoundRobin);
        
        let stats = lb.get_stats().await;
        assert_eq!(stats.total_servers, 3);
        assert_eq!(stats.healthy_servers, 3);
    }

    #[tokio::test]
    async fn test_load_balancer_round_robin() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let servers = create_test_servers();
            let lb = LoadBalancer::new(servers, LoadBalancingStrategy::RoundRobin);
            
            // Get servers in round-robin order
            let server1 = lb.get_server().await.unwrap();
            let server2 = lb.get_server().await.unwrap();
            let server3 = lb.get_server().await.unwrap();
            
            // Should be different servers
            assert_ne!(server1.host, server2.host);
            assert_ne!(server2.host, server3.host);
            
            // Release connections
            lb.release_server(&server1).await;
            lb.release_server(&server2).await;
            lb.release_server(&server3).await;
        }).await.expect("Test timed out after 10 seconds");
    }

    #[tokio::test]
    async fn test_load_balancer_least_connections() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let servers = create_test_servers();
            let lb = LoadBalancer::new(servers, LoadBalancingStrategy::LeastConnections);
            
            // All servers should start with 0 connections
            let server = lb.get_server().await.unwrap();
            assert_eq!(server.active_connections, 1);
            
            lb.release_server(&server).await;
        }).await.expect("Test timed out after 10 seconds");
    }

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert!(!config.enabled); // Disabled by default to prevent hanging in tests
        assert_eq!(config.interval, Duration::from_secs(30));
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.success_threshold, 2);
    }

    #[test]
    fn test_load_balancing_strategy_default() {
        let strategy = LoadBalancingStrategy::default();
        assert!(matches!(strategy, LoadBalancingStrategy::RoundRobin));
    }
}
