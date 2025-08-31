//! Retry logic for ClickHouse client operations

use crate::error::{Error, Result};
use std::time::Duration;
use tokio::time::{sleep, timeout as tokio_timeout};
use tracing::{debug, warn, info};

/// Retry strategy for handling failed operations
pub enum RetryStrategy {
    /// No retry
    NoRetry,
    /// Fixed delay between retries
    FixedDelay(Duration),
    /// Exponential backoff with optional jitter
    ExponentialBackoff {
        initial_delay: Duration,
        max_delay: Duration,
        multiplier: f64,
        jitter: bool,
    },
    /// Custom retry function
    Custom(Box<dyn Fn(usize, &Error) -> Duration + Send + Sync>),
}

impl Clone for RetryStrategy {
    fn clone(&self) -> Self {
        match self {
            RetryStrategy::NoRetry => RetryStrategy::NoRetry,
            RetryStrategy::FixedDelay(delay) => RetryStrategy::FixedDelay(*delay),
            RetryStrategy::ExponentialBackoff { initial_delay, max_delay, multiplier, jitter } => {
                RetryStrategy::ExponentialBackoff {
                    initial_delay: *initial_delay,
                    max_delay: *max_delay,
                    multiplier: *multiplier,
                    jitter: *jitter,
                }
            }
            RetryStrategy::Custom(_) => RetryStrategy::NoRetry, // Can't clone custom functions
        }
    }
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

/// Retry configuration
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: usize,
    /// Retry strategy
    pub strategy: RetryStrategy,
    /// Whether to retry on specific error types
    pub retry_on: Box<dyn Fn(&Error) -> bool + Send + Sync>,
    /// Timeout for the entire retry operation
    pub operation_timeout: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            strategy: RetryStrategy::default(),
            retry_on: Box::new(|e| e.is_retryable()),
            operation_timeout: None,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum retry attempts
    pub fn max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set retry strategy
    pub fn strategy(mut self, strategy: RetryStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set custom retry condition
    pub fn retry_on<F>(mut self, retry_on: F) -> Self
    where
        F: Fn(&Error) -> bool + Send + Sync + 'static,
    {
        self.retry_on = Box::new(retry_on);
        self
    }

    /// Set operation timeout
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = Some(timeout);
        self
    }

    /// Clone the retry configuration
    /// Note: The retry_on function will be reset to default behavior
    pub fn clone(&self) -> Self {
        Self {
            max_attempts: self.max_attempts,
            strategy: self.strategy.clone(),
            retry_on: Box::new(|e| e.is_retryable()), // Default retry behavior
            operation_timeout: self.operation_timeout,
        }
    }

    /// Execute an operation with retry logic
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        let start_time = std::time::Instant::now();
        let mut last_error = None;
        let mut attempt = 0;

        loop {
            attempt += 1;
            debug!("Executing operation, attempt {}/{}", attempt, self.max_attempts);

            // Check operation timeout
            if let Some(op_timeout) = self.operation_timeout {
                if start_time.elapsed() > op_timeout {
                    return Err(Error::Timeout(op_timeout));
                }
            }

            // Execute the operation
            let result = match self.operation_timeout {
                Some(timeout) => tokio_timeout(timeout, operation()).await.map_err(|_| Error::Timeout(timeout))?,
                None => operation().await,
            };

            match result {
                Ok(value) => {
                    if attempt > 1 {
                        info!("Operation succeeded after {} attempts", attempt);
                    }
                    return Ok(value);
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    
                    // Check if we should retry
                    if attempt >= self.max_attempts || !(self.retry_on)(&e) {
                        debug!("Operation failed after {} attempts, not retrying", attempt);
                        break;
                    }

                    // Calculate delay for next retry
                    let delay = self.calculate_delay(attempt, &e);
                    warn!("Operation failed (attempt {}/{}), retrying in {:?}: {}", 
                          attempt, self.max_attempts, delay, e);
                    
                    sleep(delay).await;
                }
            }
        }

        Err(Error::Internal(last_error.unwrap_or_else(|| "Unknown error during retry".to_string())))
    }

    /// Calculate delay for the next retry attempt
    fn calculate_delay(&self, attempt: usize, _error: &Error) -> Duration {
        match &self.strategy {
            RetryStrategy::NoRetry => Duration::from_secs(0),
            RetryStrategy::FixedDelay(delay) => *delay,
            RetryStrategy::ExponentialBackoff {
                initial_delay,
                max_delay,
                multiplier,
                jitter,
            } => {
                let delay = initial_delay.mul_f64(multiplier.powi((attempt - 1) as i32));
                let delay = delay.min(*max_delay);
                
                if *jitter {
                    self.add_jitter(delay)
                } else {
                    delay
                }
            }
            RetryStrategy::Custom(func) => func(attempt, _error),
        }
    }

    /// Add jitter to delay to prevent thundering herd
    fn add_jitter(&self, delay: Duration) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let jitter_factor = rng.gen_range(0.8..1.2);
        Duration::from_nanos((delay.as_nanos() as f64 * jitter_factor) as u64)
    }
}

/// Retry context for tracking retry state
#[derive(Debug, Clone)]
pub struct RetryContext {
    /// Current attempt number
    pub attempt: usize,
    /// Maximum attempts allowed
    pub max_attempts: usize,
    /// Total time spent retrying
    pub total_retry_time: Duration,
    /// Last error encountered
    pub last_error: Option<String>,
}

impl RetryContext {
    /// Create a new retry context
    pub fn new(max_attempts: usize) -> Self {
        Self {
            attempt: 0,
            max_attempts,
            total_retry_time: Duration::from_secs(0),
            last_error: None,
        }
    }

    /// Check if retry is allowed
    pub fn can_retry(&self) -> bool {
        self.attempt < self.max_attempts
    }

    /// Increment attempt counter
    pub fn increment_attempt(&mut self) {
        self.attempt += 1;
    }

    /// Record an error
    pub fn record_error(&mut self, error: Error) {
        self.last_error = Some(error.to_string());
    }

    /// Get retry statistics
    pub fn stats(&self) -> RetryStats {
        RetryStats {
            attempts: self.attempt,
            max_attempts: self.max_attempts,
            total_retry_time: self.total_retry_time,
            success_rate: if self.attempt == 0 {
                0.0
            } else {
                (self.attempt as f64 / self.max_attempts as f64) * 100.0
            },
        }
    }
}

/// Retry statistics
#[derive(Debug, Clone)]
pub struct RetryStats {
    /// Number of attempts made
    pub attempts: usize,
    /// Maximum attempts allowed
    pub max_attempts: usize,
    /// Total time spent retrying
    pub total_retry_time: Duration,
    /// Success rate percentage
    pub success_rate: f64,
}

/// Execute an operation with retry logic using default configuration
pub async fn with_retry<F, Fut, T>(operation: F) -> Result<T>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T>> + Send,
{
    RetryConfig::default().execute(operation).await
}

/// Execute an operation with retry logic using custom configuration
pub async fn with_retry_config<F, Fut, T>(
    config: RetryConfig,
    operation: F,
) -> Result<T>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T>> + Send,
{
    config.execute(operation).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert!(matches!(config.strategy, RetryStrategy::ExponentialBackoff { .. }));
    }

    #[tokio::test]
    async fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .max_attempts(5)
            .strategy(RetryStrategy::FixedDelay(Duration::from_millis(100)))
            .operation_timeout(Duration::from_secs(10));

        assert_eq!(config.max_attempts, 5);
        assert!(matches!(config.strategy, RetryStrategy::FixedDelay(_)));
        assert_eq!(config.operation_timeout, Some(Duration::from_secs(10)));
    }

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let config = RetryConfig::new().max_attempts(3);
        let counter = Arc::new(AtomicUsize::new(0));

        let result = config
            .execute(|| async {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(42)
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let config = RetryConfig::new()
            .max_attempts(3)
            .strategy(RetryStrategy::FixedDelay(Duration::from_millis(10)));
        let counter = Arc::new(AtomicUsize::new(0));

        let result = config
            .execute(|| async {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(Error::Network(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Connection refused",
                    )))
                } else {
                    Ok::<usize, Error>(42)
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_max_attempts_exceeded() {
        let config = RetryConfig::new()
            .max_attempts(2)
            .strategy(RetryStrategy::FixedDelay(Duration::from_millis(10)));
        let counter = Arc::new(AtomicUsize::new(0));

        let result: Result<()> = config
            .execute(|| async {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::Network(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Connection refused",
                )))
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_context() {
        let mut context = RetryContext::new(3);
        assert!(context.can_retry());

        context.increment_attempt();
        assert!(context.can_retry());

        context.increment_attempt();
        assert!(context.can_retry());

        context.increment_attempt();
        assert!(!context.can_retry());

        let stats = context.stats();
        assert_eq!(stats.attempts, 3);
        assert_eq!(stats.max_attempts, 3);
        assert_eq!(stats.success_rate, 100.0);
    }

    #[tokio::test]
    async fn test_with_retry_utility() {
        let result = with_retry(|| async { Ok::<usize, Error>(42) }).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
