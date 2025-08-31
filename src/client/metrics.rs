//! Metrics and monitoring for ClickHouse client operations

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

/// Metric type
#[derive(Debug, Clone, PartialEq)]
pub enum MetricType {
    /// Counter metric (monotonically increasing)
    Counter,
    /// Gauge metric (can go up or down)
    Gauge,
    /// Histogram metric (distribution of values)
    Histogram,
    /// Summary metric (quantiles)
    Summary,
}

/// Metric value
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
    /// Histogram buckets
    Histogram(Vec<HistogramBucket>),
    /// Summary quantiles
    Summary(Vec<SummaryQuantile>),
}

/// Histogram bucket
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Upper bound of the bucket
    pub upper_bound: f64,
    /// Count of values in this bucket
    pub count: u64,
}

/// Summary quantile
#[derive(Debug, Clone)]
pub struct SummaryQuantile {
    /// Quantile value (0.0 to 1.0)
    pub quantile: f64,
    /// Value at this quantile
    pub value: f64,
}

/// Metric with labels
#[derive(Debug, Clone)]
pub struct Metric {
    /// Metric name
    pub name: String,
    /// Metric type
    pub metric_type: MetricType,
    /// Metric value
    pub value: MetricValue,
    /// Metric labels
    pub labels: HashMap<String, String>,
    /// Metric description
    pub description: String,
    /// Metric unit
    pub unit: Option<String>,
    /// Timestamp when metric was last updated
    pub timestamp: Instant,
}

impl Metric {
    /// Create a new metric
    pub fn new(name: String, metric_type: MetricType, description: String) -> Self {
        Self {
            name,
            metric_type: metric_type.clone(),
            value: match metric_type {
                MetricType::Counter => MetricValue::Counter(0),
                MetricType::Gauge => MetricValue::Gauge(0.0),
                MetricType::Histogram => MetricValue::Histogram(Vec::new()),
                MetricType::Summary => MetricValue::Summary(Vec::new()),
            },
            labels: HashMap::new(),
            description,
            unit: None,
            timestamp: Instant::now(),
        }
    }

    /// Add a label to the metric
    pub fn label(mut self, key: String, value: String) -> Self {
        self.labels.insert(key, value);
        self
    }

    /// Set the metric unit
    pub fn unit(mut self, unit: String) -> Self {
        self.unit = Some(unit);
        self
    }

    /// Update counter value
    pub fn increment(&mut self, value: u64) -> Result<()> {
        match &mut self.value {
            MetricValue::Counter(counter) => {
                *counter += value;
                self.timestamp = Instant::now();
                Ok(())
            }
            _ => Err(Error::Internal("Cannot increment non-counter metric".to_string())),
        }
    }

    /// Update gauge value
    pub fn set_gauge(&mut self, value: f64) -> Result<()> {
        match &mut self.value {
            MetricValue::Gauge(gauge) => {
                *gauge = value;
                self.timestamp = Instant::now();
                Ok(())
            }
            _ => Err(Error::Internal("Cannot set gauge on non-gauge metric".to_string())),
        }
    }

    /// Add histogram observation
    pub fn observe_histogram(&mut self, value: f64) -> Result<()> {
        match &mut self.value {
            MetricValue::Histogram(buckets) => {
                // Simple histogram implementation - in production you might want more sophisticated bucketing
                let bucket = HistogramBucket {
                    upper_bound: value,
                    count: 1,
                };
                buckets.push(bucket);
                self.timestamp = Instant::now();
                Ok(())
            }
            _ => Err(Error::Internal("Cannot observe histogram on non-histogram metric".to_string())),
        }
    }

    /// Add summary observation
    pub fn observe_summary(&mut self, value: f64) -> Result<()> {
        match &mut self.value {
            MetricValue::Summary(quantiles) => {
                // Simple summary implementation - in production you might want more sophisticated quantile calculation
                let quantile = SummaryQuantile {
                    quantile: 0.5, // Median
                    value,
                };
                quantiles.push(quantile);
                self.timestamp = Instant::now();
                Ok(())
            }
            _ => Err(Error::Internal("Cannot observe summary on non-summary metric".to_string())),
        }
    }

    /// Get metric value as string for export
    pub fn export_value(&self) -> String {
        match &self.value {
            MetricValue::Counter(counter) => counter.to_string(),
            MetricValue::Gauge(gauge) => gauge.to_string(),
            MetricValue::Histogram(buckets) => format!("{} buckets", buckets.len()),
            MetricValue::Summary(quantiles) => format!("{} quantiles", quantiles.len()),
        }
    }
}

/// Metrics registry for collecting and managing metrics
pub struct MetricsRegistry {
    /// Registered metrics
    metrics: Arc<RwLock<HashMap<String, Metric>>>,
    /// Metrics prefix
    prefix: String,
    /// Whether metrics collection is enabled
    enabled: bool,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new(prefix: String) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            prefix,
            enabled: true,
        }
    }

    /// Create a new metrics registry with default prefix
    pub fn default() -> Self {
        Self::new("clickhouse_client".to_string())
    }

    /// Register a new metric
    pub async fn register_metric(&self, metric: Metric) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let name = format!("{}_{}", self.prefix, metric.name);
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.clone(), metric);
        debug!("Registered metric: {}", name);
        Ok(())
    }

    /// Get a metric by name
    pub async fn get_metric(&self, name: &str) -> Option<Metric> {
        let full_name = format!("{}_{}", self.prefix, name);
        let metrics = self.metrics.read().await;
        metrics.get(&full_name).cloned()
    }

    /// Update a counter metric
    pub async fn increment_counter(&self, name: &str, value: u64, labels: Option<HashMap<String, String>>) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let full_name = format!("{}_{}", self.prefix, name);
        let mut metrics = self.metrics.write().await;
        
        if let Some(metric) = metrics.get_mut(&full_name) {
            metric.increment(value)?;
        } else {
            // Auto-create metric if it doesn't exist
            let mut metric = Metric::new(
                name.to_string(),
                MetricType::Counter,
                format!("Counter metric for {}", name),
            );
            
            if let Some(labels) = labels {
                for (key, value) in labels {
                    metric = metric.label(key, value);
                }
            }
            
            metric.increment(value)?;
            metrics.insert(full_name, metric);
        }
        
        Ok(())
    }

    /// Update a gauge metric
    pub async fn set_gauge(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let full_name = format!("{}_{}", self.prefix, name);
        let mut metrics = self.metrics.write().await;
        
        if let Some(metric) = metrics.get_mut(&full_name) {
            metric.set_gauge(value)?;
        } else {
            // Auto-create metric if it doesn't exist
            let mut metric = Metric::new(
                name.to_string(),
                MetricType::Gauge,
                format!("Gauge metric for {}", name),
            );
            
            if let Some(labels) = labels {
                for (key, value) in labels {
                    metric = metric.label(key, value);
                }
            }
            
            metric.set_gauge(value)?;
            metrics.insert(full_name, metric);
        }
        
        Ok(())
    }

    /// Observe a histogram value
    pub async fn observe_histogram(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let full_name = format!("{}_{}", self.prefix, name);
        let mut metrics = self.metrics.write().await;
        
        if let Some(metric) = metrics.get_mut(&full_name) {
            metric.observe_histogram(value)?;
        } else {
            // Auto-create metric if it doesn't exist
            let mut metric = Metric::new(
                name.to_string(),
                MetricType::Histogram,
                format!("Histogram metric for {}", name),
            );
            
            if let Some(labels) = labels {
                for (key, value) in labels {
                    metric = metric.label(key, value);
                }
            }
            
            metric.observe_histogram(value)?;
            metrics.insert(full_name, metric);
        }
        
        Ok(())
    }

    /// Get all metrics
    pub async fn get_all_metrics(&self) -> Vec<Metric> {
        let metrics = self.metrics.read().await;
        metrics.values().cloned().collect()
    }

    /// Export metrics in Prometheus format
    pub async fn export_prometheus(&self) -> String {
        let metrics = self.get_all_metrics().await;
        let mut output = String::new();
        
        for metric in metrics {
            // Add metric help
            output.push_str(&format!("# HELP {} {}\n", metric.name, metric.description));
            
            // Add metric type
            let metric_type = match metric.metric_type {
                MetricType::Counter => "counter",
                MetricType::Gauge => "gauge",
                MetricType::Histogram => "histogram",
                MetricType::Summary => "summary",
            };
            output.push_str(&format!("# TYPE {} {}\n", metric.name, metric_type));
            
            // Add metric value with labels
            let labels_str = if metric.labels.is_empty() {
                String::new()
            } else {
                let labels: Vec<String> = metric.labels.iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v))
                    .collect();
                format!("{{{}}}", labels.join(","))
            };
            
            output.push_str(&format!("{}{} {}\n", metric.name, labels_str, metric.export_value()));
        }
        
        output
    }

    /// Enable or disable metrics collection
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if metrics collection is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Metrics collector for specific operations
pub struct MetricsCollector {
    /// Metrics registry
    registry: Arc<MetricsRegistry>,
    /// Operation start time
    start_time: Instant,
    /// Operation name
    operation_name: String,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(registry: Arc<MetricsRegistry>, operation_name: String) -> Self {
        Self {
            registry,
            start_time: Instant::now(),
            operation_name,
        }
    }

    /// Record operation success
    pub async fn record_success(&self, labels: Option<HashMap<String, String>>) -> Result<()> {
        let duration = self.start_time.elapsed();
        
        // Record operation duration
        self.registry.observe_histogram(
            &format!("{}_duration_seconds", self.operation_name),
            duration.as_secs_f64(),
            labels.clone(),
        ).await?;
        
        // Increment success counter
        self.registry.increment_counter(
            &format!("{}_success_total", self.operation_name),
            1,
            labels,
        ).await?;
        
        Ok(())
    }

    /// Record operation failure
    pub async fn record_failure(&self, error: &Error, labels: Option<HashMap<String, String>>) -> Result<()> {
        let duration = self.start_time.elapsed();
        
        // Record operation duration
        self.registry.observe_histogram(
            &format!("{}_duration_seconds", self.operation_name),
            duration.as_secs_f64(),
            labels.clone(),
        ).await?;
        
        // Increment failure counter
        let mut failure_labels = labels.unwrap_or_default();
        failure_labels.insert("error_type".to_string(), error.to_string());
        
        self.registry.increment_counter(
            &format!("{}_failure_total", self.operation_name),
            1,
            Some(failure_labels),
        ).await?;
        
        Ok(())
    }

    /// Record operation result
    pub async fn record_result<T>(&self, result: &Result<T>, labels: Option<HashMap<String, String>>) -> Result<()> {
        match result {
            Ok(_) => self.record_success(labels).await,
            Err(e) => self.record_failure(e, labels).await,
        }
    }
}

/// Predefined metric names
pub mod metric_names {
    /// Connection pool metrics
    pub const CONNECTION_POOL_SIZE: &str = "connection_pool_size";
    pub const CONNECTION_POOL_ACTIVE: &str = "connection_pool_active";
    pub const CONNECTION_POOL_IDLE: &str = "connection_pool_idle";
    pub const CONNECTION_POOL_WAIT_TIME: &str = "connection_pool_wait_time";
    
    /// Query execution metrics
    pub const QUERY_DURATION: &str = "query_duration_seconds";
    pub const QUERY_SUCCESS_TOTAL: &str = "query_success_total";
    pub const QUERY_FAILURE_TOTAL: &str = "query_failure_total";
    pub const QUERY_ROWS_PROCESSED: &str = "query_rows_processed";
    
    /// Network metrics
    pub const NETWORK_CONNECTIONS_TOTAL: &str = "network_connections_total";
    pub const NETWORK_BYTES_SENT: &str = "network_bytes_sent";
    pub const NETWORK_BYTES_RECEIVED: &str = "network_bytes_received";
    
    /// Load balancer metrics
    pub const LOAD_BALANCER_SERVERS_TOTAL: &str = "load_balancer_servers_total";
    pub const LOAD_BALANCER_SERVERS_HEALTHY: &str = "load_balancer_servers_healthy";
    pub const LOAD_BALANCER_SERVER_RESPONSE_TIME: &str = "load_balancer_server_response_time";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_new() {
        let metric = Metric::new(
            "test_counter".to_string(),
            MetricType::Counter,
            "Test counter metric".to_string(),
        );
        
        assert_eq!(metric.name, "test_counter");
        assert_eq!(metric.metric_type, MetricType::Counter);
        assert_eq!(metric.description, "Test counter metric");
        assert!(metric.labels.is_empty());
    }

    #[test]
    fn test_metric_label() {
        let metric = Metric::new(
            "test_metric".to_string(),
            MetricType::Counter,
            "Test metric".to_string(),
        ).label("test_key".to_string(), "test_value".to_string());
        
        assert_eq!(metric.labels.get("test_key"), Some(&"test_value".to_string()));
    }

    #[test]
    fn test_metric_increment() {
        let mut metric = Metric::new(
            "test_counter".to_string(),
            MetricType::Counter,
            "Test counter".to_string(),
        );
        
        assert!(metric.increment(5).is_ok());
        assert!(metric.increment(3).is_ok());
        
        if let MetricValue::Counter(value) = metric.value {
            assert_eq!(value, 8);
        } else {
            panic!("Expected counter value");
        }
    }

    #[test]
    fn test_metric_set_gauge() {
        let mut metric = Metric::new(
            "test_gauge".to_string(),
            MetricType::Gauge,
            "Test gauge".to_string(),
        );
        
        assert!(metric.set_gauge(42.5).is_ok());
        
        if let MetricValue::Gauge(value) = metric.value {
            assert_eq!(value, 42.5);
        } else {
            panic!("Expected gauge value");
        }
    }

    #[tokio::test]
    async fn test_metrics_registry_new() {
        let registry = MetricsRegistry::new("test_prefix".to_string());
        assert_eq!(registry.prefix, "test_prefix");
        assert!(registry.is_enabled());
    }

    #[tokio::test]
    async fn test_metrics_registry_default() {
        let registry = MetricsRegistry::default();
        assert_eq!(registry.prefix, "clickhouse_client");
    }

    #[tokio::test]
    async fn test_metrics_registry_register_metric() {
        let registry = MetricsRegistry::new("test".to_string());
        let metric = Metric::new(
            "test_metric".to_string(),
            MetricType::Counter,
            "Test metric".to_string(),
        );
        
        assert!(registry.register_metric(metric).await.is_ok());
        
        let registered_metric = registry.get_metric("test_metric").await;
        assert!(registered_metric.is_some());
    }

    #[tokio::test]
    async fn test_metrics_registry_increment_counter() {
        let registry = MetricsRegistry::new("test".to_string());
        
        assert!(registry.increment_counter("test_counter", 5, None).await.is_ok());
        assert!(registry.increment_counter("test_counter", 3, None).await.is_ok());
        
        let metric = registry.get_metric("test_counter").await.unwrap();
        if let MetricValue::Counter(value) = metric.value {
            assert_eq!(value, 8);
        } else {
            panic!("Expected counter value");
        }
    }

    #[tokio::test]
    async fn test_metrics_registry_set_gauge() {
        let registry = MetricsRegistry::new("test".to_string());
        
        assert!(registry.set_gauge("test_gauge", 42.5, None).await.is_ok());
        
        let metric = registry.get_metric("test_gauge").await.unwrap();
        if let MetricValue::Gauge(value) = metric.value {
            assert_eq!(value, 42.5);
        } else {
            panic!("Expected gauge value");
        }
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let registry = Arc::new(MetricsRegistry::new("test".to_string()));
        let collector = MetricsCollector::new(registry.clone(), "test_operation".to_string());
        
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Record success
        assert!(collector.record_success(None).await.is_ok());
        
        // Check that metrics were recorded
        let duration_metric = registry.get_metric("test_operation_duration_seconds").await;
        assert!(duration_metric.is_some());
        
        let success_metric = registry.get_metric("test_operation_success_total").await;
        assert!(success_metric.is_some());
    }

    #[tokio::test]
    async fn test_metrics_registry_export_prometheus() {
        let registry = MetricsRegistry::new("test".to_string());
        
        // Add some metrics
        registry.increment_counter("test_counter", 42, None).await.unwrap();
        registry.set_gauge("test_gauge", 3.14, None).await.unwrap();
        
        let prometheus_output = registry.export_prometheus().await;
        
        // Check that the output contains our metrics
        assert!(prometheus_output.contains("test_counter"));
        assert!(prometheus_output.contains("test_gauge"));
        assert!(prometheus_output.contains("42"));
        assert!(prometheus_output.contains("3.14"));
    }
}
