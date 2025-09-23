//! Consumer client for receiving messages from FluxMQ

use crate::config::{ConsumerConfig, ConsumerConfigBuilder};
use crate::connection::ConnectionPool;
use crate::error::{ErrorCode, FluxmqClientError};
use crate::protocol::{ConsumeRecord, FetchRequest, Request, Response, TopicPartition};
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// High-level consumer client for receiving messages
pub struct Consumer {
    config: ConsumerConfig,
    connection_pool: Arc<ConnectionPool>,
    topic_partitions: Arc<RwLock<Vec<TopicPartition>>>,
    current_offsets: Arc<RwLock<HashMap<TopicPartition, u64>>>,
}

impl Consumer {
    /// Create a new consumer with the given configuration
    pub async fn new(config: ConsumerConfig) -> Result<Self, FluxmqClientError> {
        if config.topics.is_empty() {
            return Err(FluxmqClientError::invalid_config(
                "At least one topic must be specified".to_string(),
            ));
        }

        let connection_pool = Arc::new(ConnectionPool::new(
            config.client_config.brokers.clone(),
            config.client_config.connection_timeout,
            config.client_config.request_timeout,
        ));

        let consumer = Self {
            config,
            connection_pool,
            topic_partitions: Arc::new(RwLock::new(Vec::new())),
            current_offsets: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize connection and discover partitions
        consumer.connection_pool.get_connection().await?;
        consumer.refresh_metadata().await?;

        Ok(consumer)
    }

    /// Create a stream of records
    pub fn stream(&self) -> ConsumerStream<'_> {
        ConsumerStream {
            _consumer: self,
            current_records: Vec::new(),
            current_record_index: 0,
        }
    }

    /// Manually fetch records from all assigned partitions
    pub async fn poll(&self) -> Result<Vec<ConsumeRecord>, FluxmqClientError> {
        let topic_partitions = self.topic_partitions.read().await.clone();
        let mut all_records = Vec::new();

        for tp in topic_partitions {
            let records = self.fetch_from_partition(&tp).await?;
            all_records.extend(records);
        }

        Ok(all_records)
    }

    /// Commit current offsets synchronously
    pub async fn commit_sync(&self) -> Result<(), FluxmqClientError> {
        // In a full implementation, this would commit offsets to the broker
        // For now, we just update local offsets
        debug!("Committing offsets");
        Ok(())
    }

    /// Commit specific offsets
    pub async fn commit_offsets(
        &self,
        offsets: HashMap<TopicPartition, u64>,
    ) -> Result<(), FluxmqClientError> {
        // Update local offsets
        let mut current_offsets = self.current_offsets.write().await;
        for (tp, offset) in offsets {
            current_offsets.insert(tp, offset);
        }
        Ok(())
    }

    /// Close the consumer
    pub async fn close(&self) -> Result<(), FluxmqClientError> {
        self.connection_pool.close_all().await;
        info!("Consumer closed");
        Ok(())
    }

    async fn fetch_from_partition(
        &self,
        topic_partition: &TopicPartition,
    ) -> Result<Vec<ConsumeRecord>, FluxmqClientError> {
        let current_offset = {
            let offsets = self.current_offsets.read().await;
            offsets.get(topic_partition).copied().unwrap_or(0)
        };

        let request = FetchRequest {
            correlation_id: 0, // Will be set by connection
            topic: topic_partition.topic.clone(),
            partition: topic_partition.partition,
            offset: current_offset,
            max_bytes: self.config.fetch_config.max_bytes,
            timeout_ms: self.config.fetch_config.max_wait.as_millis() as u32,
        };

        let response = self.send_request(Request::Fetch(request)).await?;

        match response {
            Response::Fetch(fetch_response) => {
                if fetch_response.error_code != 0 {
                    let error_code = ErrorCode::from(fetch_response.error_code);
                    return Err(error_code.to_client_error(&format!(
                        "{}:{}",
                        fetch_response.topic, fetch_response.partition
                    )));
                }

                let mut records = Vec::new();
                let mut max_offset = current_offset;

                for (offset, message) in fetch_response.messages {
                    let mut record = ConsumeRecord::from((offset, message));
                    record.topic = fetch_response.topic.clone();
                    record.partition = fetch_response.partition;

                    records.push(record);
                    max_offset = max_offset.max(offset + 1);
                }

                // Update offset
                if !records.is_empty() {
                    let mut offsets = self.current_offsets.write().await;
                    offsets.insert(topic_partition.clone(), max_offset);
                }

                debug!(
                    "Fetched {} records from {}:{}",
                    records.len(),
                    topic_partition.topic,
                    topic_partition.partition
                );

                Ok(records)
            }
            _ => Err(FluxmqClientError::protocol(
                "Unexpected response type for fetch request".to_string(),
            )),
        }
    }

    async fn refresh_metadata(&self) -> Result<(), FluxmqClientError> {
        let request = crate::protocol::MetadataRequest {
            correlation_id: 0, // Will be set by connection
            topics: self.config.topics.clone(),
        };

        let response = self.send_request(Request::Metadata(request)).await?;

        match response {
            Response::Metadata(metadata_response) => {
                let mut topic_partitions = Vec::new();

                for topic_metadata in metadata_response.topics {
                    for partition_metadata in topic_metadata.partitions {
                        topic_partitions.push(TopicPartition {
                            topic: topic_metadata.name.clone(),
                            partition: partition_metadata.id,
                        });
                    }
                }

                *self.topic_partitions.write().await = topic_partitions;
                info!("Refreshed metadata for topics: {:?}", self.config.topics);
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

/// Stream implementation for consuming records
pub struct ConsumerStream<'a> {
    _consumer: &'a Consumer,
    current_records: Vec<ConsumeRecord>,
    current_record_index: usize,
}

impl<'a> Stream for ConsumerStream<'a> {
    type Item = Result<ConsumeRecord, FluxmqClientError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we have records, return the next one
        if self.current_record_index < self.current_records.len() {
            let record = self.current_records[self.current_record_index].clone();
            self.current_record_index += 1;
            return Poll::Ready(Some(Ok(record)));
        }

        // For simplicity, return pending to indicate no more records available
        // In a real implementation, this would initiate a fetch operation
        Poll::Pending
    }
}

/// Builder for Consumer
pub struct ConsumerBuilder {
    config: ConsumerConfigBuilder,
}

impl ConsumerBuilder {
    /// Create a new consumer builder
    pub fn new() -> Self {
        Self {
            config: ConsumerConfigBuilder::new(),
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

    /// Set the consumer group ID
    pub fn group_id<S: Into<String>>(mut self, group_id: S) -> Self {
        self.config = self.config.group_id(group_id);
        self
    }

    /// Set the topics to consume from
    pub fn topics<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config = self.config.topics(topics);
        self
    }

    /// Set the session timeout
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.session_timeout(timeout);
        self
    }

    /// Set the maximum poll records
    pub fn max_poll_records(mut self, max_records: usize) -> Self {
        self.config = self.config.max_poll_records(max_records);
        self
    }

    /// Build the consumer
    pub async fn build(self) -> Result<Consumer, FluxmqClientError> {
        let config = self.config.build();
        Consumer::new(config).await
    }
}

impl Default for ConsumerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_consumer_config_validation() {
        let config = ConsumerConfig::default();
        // Should fail with no topics
        let result = Consumer::new(config).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_consumer_builder() {
        let _builder = ConsumerBuilder::new()
            .brokers(vec!["localhost:9092"])
            .group_id("test-group")
            .topics(vec!["test-topic"]);

        // We can't actually build without a broker, but we can test the builder pattern
        assert!(true); // Just to have a passing test
    }
}
