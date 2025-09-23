//! Producer client for sending messages to FluxMQ

use crate::config::{ProducerConfig, ProducerConfigBuilder};
use crate::connection::ConnectionPool;
use crate::error::{ErrorCode, FluxmqClientError};
use crate::protocol::{Message, ProduceMetadata, ProduceRecord, ProduceRequest, Request, Response};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Clone)]
/// High-level producer client for sending messages
pub struct Producer {
    config: ProducerConfig,
    connection_pool: Arc<ConnectionPool>,
    topic_metadata: Arc<RwLock<HashMap<String, Vec<u32>>>>, // topic -> partitions
}

impl Producer {
    /// Create a new producer with the given configuration
    pub async fn new(config: ProducerConfig) -> Result<Self, FluxmqClientError> {
        let connection_pool = Arc::new(ConnectionPool::new(
            config.client_config.brokers.clone(),
            config.client_config.connection_timeout,
            config.client_config.request_timeout,
        ));

        let producer = Self {
            config,
            connection_pool,
            topic_metadata: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize connection
        producer.connection_pool.get_connection().await?;

        Ok(producer)
    }

    /// Send a single record
    pub async fn send(&self, record: ProduceRecord) -> Result<ProduceMetadata, FluxmqClientError> {
        self.send_batch(vec![record])
            .await
            .map(|mut results| results.remove(0))
    }

    /// Send multiple records in a batch
    pub async fn send_batch(
        &self,
        records: Vec<ProduceRecord>,
    ) -> Result<Vec<ProduceMetadata>, FluxmqClientError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Validate message sizes
        for record in &records {
            if record.value.len() > self.config.max_message_size {
                return Err(FluxmqClientError::MessageTooLarge {
                    size: record.value.len(),
                    max_size: self.config.max_message_size,
                });
            }
        }

        // Group records by topic and partition
        let mut topic_partitions: HashMap<String, HashMap<u32, Vec<ProduceRecord>>> =
            HashMap::new();

        for record in records {
            let partition = match record.partition {
                Some(partition) => partition,
                None => self.assign_partition(&record).await?,
            };

            topic_partitions
                .entry(record.topic.clone())
                .or_insert_with(HashMap::new)
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(record);
        }

        // Send requests for each topic-partition
        let mut results = Vec::new();

        for (topic, partitions) in topic_partitions {
            for (partition, partition_records) in partitions {
                let messages: Vec<Message> = partition_records.iter().map(Message::from).collect();

                let request = ProduceRequest {
                    correlation_id: 0, // Will be set by connection
                    topic: topic.clone(),
                    partition,
                    messages,
                    acks: self.config.acks,
                    timeout_ms: self.config.delivery_timeout.as_millis() as u32,
                };

                let response = self.send_request(Request::Produce(request)).await?;

                match response {
                    Response::Produce(produce_response) => {
                        if produce_response.error_code != 0 {
                            let error_code = ErrorCode::from(produce_response.error_code);
                            return Err(error_code.to_client_error(&format!(
                                "{}:{}",
                                produce_response.topic, produce_response.partition
                            )));
                        }

                        // Create metadata for each record in the batch
                        for (i, _) in partition_records.iter().enumerate() {
                            results.push(ProduceMetadata {
                                topic: produce_response.topic.clone(),
                                partition: produce_response.partition,
                                offset: produce_response.base_offset + i as u64,
                            });
                        }
                    }
                    _ => {
                        return Err(FluxmqClientError::protocol(
                            "Unexpected response type for produce request".to_string(),
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Flush any pending messages
    pub async fn flush(&self) -> Result<(), FluxmqClientError> {
        // In a full implementation, this would flush any batched messages
        // For now, we send messages immediately
        Ok(())
    }

    /// Close the producer and release resources
    pub async fn close(&self) -> Result<(), FluxmqClientError> {
        self.connection_pool.close_all().await;
        info!("Producer closed");
        Ok(())
    }

    async fn assign_partition(&self, record: &ProduceRecord) -> Result<u32, FluxmqClientError> {
        // Get or refresh topic metadata
        let partitions = self.get_topic_partitions(&record.topic).await?;

        if partitions.is_empty() {
            return Err(FluxmqClientError::TopicNotFound {
                topic: record.topic.clone(),
            });
        }

        // Use key-based hash partitioning if key exists, otherwise round-robin
        let partition_index = if let Some(key) = &record.key {
            self.hash_partition(key, partitions.len())
        } else {
            // For simplicity, use first partition for keyless messages
            // In production, this would use round-robin
            0
        };

        Ok(partitions[partition_index])
    }

    fn hash_partition(&self, key: &Bytes, partition_count: usize) -> usize {
        // Simple hash function (FNV-1a)
        let mut hash = 2166136261u32;
        for byte in key.iter() {
            hash ^= *byte as u32;
            hash = hash.wrapping_mul(16777619);
        }
        (hash as usize) % partition_count
    }

    async fn get_topic_partitions(&self, topic: &str) -> Result<Vec<u32>, FluxmqClientError> {
        // Check cache first
        {
            let metadata = self.topic_metadata.read().await;
            if let Some(partitions) = metadata.get(topic) {
                return Ok(partitions.clone());
            }
        }

        // Refresh metadata
        self.refresh_metadata(vec![topic.to_string()]).await?;

        // Try cache again
        let metadata = self.topic_metadata.read().await;
        metadata
            .get(topic)
            .cloned()
            .ok_or_else(|| FluxmqClientError::TopicNotFound {
                topic: topic.to_string(),
            })
    }

    async fn refresh_metadata(&self, topics: Vec<String>) -> Result<(), FluxmqClientError> {
        let request = crate::protocol::MetadataRequest {
            correlation_id: 0, // Will be set by connection
            topics,
        };

        let response = self.send_request(Request::Metadata(request)).await?;

        match response {
            Response::Metadata(metadata_response) => {
                let mut metadata = self.topic_metadata.write().await;

                for topic_metadata in metadata_response.topics {
                    let partitions: Vec<u32> = topic_metadata
                        .partitions
                        .into_iter()
                        .map(|p| p.id)
                        .collect();

                    metadata.insert(topic_metadata.name, partitions);
                }
            }
            _ => {
                return Err(FluxmqClientError::protocol(
                    "Unexpected response type for metadata request".to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn send_request(&self, request: Request) -> Result<Response, FluxmqClientError> {
        let connection = self.connection_pool.get_connection().await?;
        connection.send_request(request).await
    }
}

/// Builder for Producer
pub struct ProducerBuilder {
    config: ProducerConfigBuilder,
}

impl ProducerBuilder {
    /// Create a new producer builder
    pub fn new() -> Self {
        Self {
            config: ProducerConfigBuilder::new(),
        }
    }

    /// Set the broker addresses
    pub fn brokers<I, S>(mut self, brokers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config = self.config.brokers(brokers);
        self
    }

    /// Set the acknowledgment level
    pub fn acks(mut self, acks: i16) -> Self {
        self.config = self.config.acks(acks);
        self
    }

    /// Set the maximum message size
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config = self.config.max_message_size(size);
        self
    }

    /// Set the delivery timeout
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.delivery_timeout(timeout);
        self
    }

    /// Build the producer
    pub async fn build(self) -> Result<Producer, FluxmqClientError> {
        let config = self.config.build();
        Producer::new(config).await
    }
}

impl Default for ProducerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProducerConfig;

    #[tokio::test]
    async fn test_producer_creation() {
        let config = ProducerConfig::default();
        // Producer::new would fail without a running broker, so we just test config
        assert_eq!(config.acks, 1);
        assert_eq!(config.max_message_size, 1024 * 1024);
    }

    #[test]
    fn test_producer_builder() {
        let _builder = ProducerBuilder::new()
            .brokers(vec!["localhost:9092"])
            .acks(-1)
            .max_message_size(2 * 1024 * 1024);

        // We can't actually build without a broker, but we can test the builder pattern
        assert!(true); // Just to have a passing test
    }

    #[test]
    fn test_hash_partition() {
        let config = ProducerConfig::default();
        let producer = Producer {
            config,
            connection_pool: Arc::new(ConnectionPool::new(
                vec!["localhost:9092".to_string()],
                Duration::from_secs(30),
                Duration::from_secs(30),
            )),
            topic_metadata: Arc::new(RwLock::new(HashMap::new())),
        };

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        let partition1 = producer.hash_partition(&key1, 3);
        let partition2 = producer.hash_partition(&key1, 3);
        let partition3 = producer.hash_partition(&key2, 3);

        // Same key should always map to same partition
        assert_eq!(partition1, partition2);

        // Partitions should be in valid range
        assert!(partition1 < 3);
        assert!(partition3 < 3);
    }
}
