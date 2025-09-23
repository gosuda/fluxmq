//! Configuration types for FluxMQ client

use std::time::Duration;

/// Client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// List of broker addresses
    pub brokers: Vec<String>,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Retry configuration
    pub retry_config: RetryConfig,
    /// Security configuration
    pub security_config: SecurityConfig,
    /// Client identifier
    pub client_id: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            connection_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            retry_config: RetryConfig::default(),
            security_config: SecurityConfig::default(),
            client_id: None,
        }
    }
}

/// Producer-specific configuration
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Base client configuration
    pub client_config: ClientConfig,
    /// Acknowledgment level (0, 1, -1/all)
    pub acks: i16,
    /// Message batching configuration
    pub batch_config: BatchConfig,
    /// Compression configuration
    pub compression: CompressionType,
    /// Maximum message size
    pub max_message_size: usize,
    /// Delivery timeout
    pub delivery_timeout: Duration,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            client_config: ClientConfig::default(),
            acks: 1, // Wait for leader acknowledgment
            batch_config: BatchConfig::default(),
            compression: CompressionType::None,
            max_message_size: 1024 * 1024, // 1MB
            delivery_timeout: Duration::from_secs(120),
        }
    }
}

/// Consumer-specific configuration
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Base client configuration
    pub client_config: ClientConfig,
    /// Consumer group ID
    pub group_id: Option<String>,
    /// Topics to subscribe to
    pub topics: Vec<String>,
    /// Auto-commit configuration
    pub auto_commit: AutoCommitConfig,
    /// Session timeout for consumer groups
    pub session_timeout: Duration,
    /// Heartbeat interval for consumer groups
    pub heartbeat_interval: Duration,
    /// Maximum poll records
    pub max_poll_records: usize,
    /// Fetch configuration
    pub fetch_config: FetchConfig,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            client_config: ClientConfig::default(),
            group_id: None,
            topics: Vec::new(),
            auto_commit: AutoCommitConfig::default(),
            session_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
            max_poll_records: 500,
            fetch_config: FetchConfig::default(),
        }
    }
}

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries
    pub max_retries: usize,
    /// Initial retry delay
    pub initial_delay: Duration,
    /// Maximum retry delay
    pub max_delay: Duration,
    /// Retry delay multiplier
    pub multiplier: f64,
    /// Jitter for retry delays
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

/// Security configuration
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// Enable TLS
    pub enable_tls: bool,
    /// TLS certificate path
    pub cert_path: Option<String>,
    /// TLS key path
    pub key_path: Option<String>,
    /// CA certificate path
    pub ca_path: Option<String>,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_tls: false,
            cert_path: None,
            key_path: None,
            ca_path: None,
        }
    }
}

/// Message batching configuration
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size in messages
    pub batch_size: usize,
    /// Maximum batch size in bytes
    pub batch_size_bytes: usize,
    /// Maximum time to wait for batch to fill
    pub linger_ms: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 16384,                   // 16K messages
            batch_size_bytes: 16 * 1024 * 1024,  // 16MB
            linger_ms: Duration::from_millis(5), // 5ms
        }
    }
}

/// Compression types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Auto-commit configuration
#[derive(Debug, Clone)]
pub struct AutoCommitConfig {
    /// Enable automatic offset commits
    pub enable: bool,
    /// Auto-commit interval
    pub interval: Duration,
}

impl Default for AutoCommitConfig {
    fn default() -> Self {
        Self {
            enable: true,
            interval: Duration::from_secs(5),
        }
    }
}

/// Fetch configuration
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Minimum fetch size in bytes
    pub min_bytes: u32,
    /// Maximum fetch size in bytes
    pub max_bytes: u32,
    /// Maximum time to wait for fetch
    pub max_wait: Duration,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            min_bytes: 1,                         // 1 byte minimum
            max_bytes: 50 * 1024 * 1024,          // 50MB maximum
            max_wait: Duration::from_millis(500), // 500ms maximum wait
        }
    }
}

/// Builder for ClientConfig
#[derive(Debug, Default)]
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl ClientConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn brokers<I, S>(mut self, brokers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.brokers = brokers.into_iter().map(|s| s.into()).collect();
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    pub fn client_id<S: Into<String>>(mut self, client_id: S) -> Self {
        self.config.client_id = Some(client_id.into());
        self
    }

    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.config.retry_config = retry_config;
        self
    }

    pub fn security_config(mut self, security_config: SecurityConfig) -> Self {
        self.config.security_config = security_config;
        self
    }

    pub fn build(self) -> ClientConfig {
        self.config
    }
}

/// Builder for ProducerConfig
#[derive(Debug)]
pub struct ProducerConfigBuilder {
    config: ProducerConfig,
}

impl Default for ProducerConfigBuilder {
    fn default() -> Self {
        Self {
            config: ProducerConfig::default(),
        }
    }
}

impl ProducerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn client_config(mut self, client_config: ClientConfig) -> Self {
        self.config.client_config = client_config;
        self
    }

    pub fn brokers<I, S>(mut self, brokers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.client_config.brokers = brokers.into_iter().map(|s| s.into()).collect();
        self
    }

    pub fn acks(mut self, acks: i16) -> Self {
        self.config.acks = acks;
        self
    }

    pub fn batch_config(mut self, batch_config: BatchConfig) -> Self {
        self.config.batch_config = batch_config;
        self
    }

    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.config.compression = compression;
        self
    }

    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }

    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    pub fn build(self) -> ProducerConfig {
        self.config
    }
}

/// Builder for ConsumerConfig
#[derive(Debug)]
pub struct ConsumerConfigBuilder {
    config: ConsumerConfig,
}

impl Default for ConsumerConfigBuilder {
    fn default() -> Self {
        Self {
            config: ConsumerConfig::default(),
        }
    }
}

impl ConsumerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn client_config(mut self, client_config: ClientConfig) -> Self {
        self.config.client_config = client_config;
        self
    }

    pub fn brokers<I, S>(mut self, brokers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.client_config.brokers = brokers.into_iter().map(|s| s.into()).collect();
        self
    }

    pub fn group_id<S: Into<String>>(mut self, group_id: S) -> Self {
        self.config.group_id = Some(group_id.into());
        self
    }

    pub fn topics<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.topics = topics.into_iter().map(|s| s.into()).collect();
        self
    }

    pub fn auto_commit(mut self, auto_commit: AutoCommitConfig) -> Self {
        self.config.auto_commit = auto_commit;
        self
    }

    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    pub fn max_poll_records(mut self, max_records: usize) -> Self {
        self.config.max_poll_records = max_records;
        self
    }

    pub fn fetch_config(mut self, fetch_config: FetchConfig) -> Self {
        self.config.fetch_config = fetch_config;
        self
    }

    pub fn build(self) -> ConsumerConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_builder() {
        let config = ClientConfigBuilder::new()
            .brokers(vec!["broker1:9092", "broker2:9092"])
            .connection_timeout(Duration::from_secs(10))
            .client_id("test-client")
            .build();

        assert_eq!(config.brokers, vec!["broker1:9092", "broker2:9092"]);
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_producer_config_builder() {
        let config = ProducerConfigBuilder::new()
            .brokers(vec!["localhost:9092"])
            .acks(-1)
            .max_message_size(2 * 1024 * 1024)
            .build();

        assert_eq!(config.client_config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.acks, -1);
        assert_eq!(config.max_message_size, 2 * 1024 * 1024);
    }

    #[test]
    fn test_consumer_config_builder() {
        let config = ConsumerConfigBuilder::new()
            .brokers(vec!["localhost:9092"])
            .group_id("test-group")
            .topics(vec!["topic1", "topic2"])
            .max_poll_records(100)
            .build();

        assert_eq!(config.client_config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.group_id, Some("test-group".to_string()));
        assert_eq!(config.topics, vec!["topic1", "topic2"]);
        assert_eq!(config.max_poll_records, 100);
    }
}
