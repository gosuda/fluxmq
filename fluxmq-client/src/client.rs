//! High-level client interface

use crate::admin::AdminClient;
use crate::config::ClientConfig;
use crate::consumer::ConsumerBuilder;
use crate::error::FluxmqClientError;
use crate::producer::ProducerBuilder;

/// Main client for FluxMQ operations
pub struct FluxmqClient {
    config: ClientConfig,
}

impl FluxmqClient {
    /// Create a new FluxMQ client
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Create a producer with default configuration
    pub fn producer(&self) -> ProducerBuilder {
        ProducerBuilder::new().brokers(self.config.brokers.clone())
    }

    /// Create a consumer with default configuration
    pub fn consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder::new().brokers(self.config.brokers.clone())
    }

    /// Create an admin client
    pub async fn admin(&self) -> Result<AdminClient, FluxmqClientError> {
        AdminClient::new(self.config.clone()).await
    }
}

/// Convenience functions for quick client creation
impl FluxmqClient {
    /// Create a client with default configuration for localhost
    pub fn localhost() -> Self {
        Self::new(ClientConfig::default())
    }

    /// Create a client with custom brokers
    pub fn with_brokers<I, S>(brokers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut config = ClientConfig::default();
        config.brokers = brokers.into_iter().map(|s| s.into()).collect();
        Self::new(config)
    }
}
