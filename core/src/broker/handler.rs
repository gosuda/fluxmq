use crate::{
    acl::{AclManager, AuthorizationResult, Operation, Principal, ResourceType},
    consumer::{ConsumerGroupConfig, ConsumerGroupCoordinator, ConsumerGroupMessage},
    metrics::MetricsRegistry,
    performance::{object_pool::MessagePools, ultra_performance::UltraPerformanceBroker},
    protocol::{
        high_performance_codec::HighPerformanceKafkaCodec, AlterConfigsRequest,
        AlterConfigsResponse, BrokerMetadata, CreateTopicsRequest, CreateTopicsResponse,
        DeleteTopicsRequest, DeleteTopicsResponse, DescribeConfigsRequest, DescribeConfigsResponse,
        FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, Message,
        MetadataRequest, MetadataResponse, MultiFetchRequest, MultiFetchResponse, Offset,
        PartitionFetchResponse, PartitionId, PartitionMetadata, ProduceRequest, ProduceResponse,
        Request, Response, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
        SaslHandshakeResponse, TopicFetchResponse, TopicMetadata,
    },
    replication::{BrokerId, ReplicationConfig, ReplicationCoordinator},
    storage::HybridStorage,
    topic_manager::{PartitionAssigner, PartitionStrategy, TopicManager},
    Result,
};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, error, info};

/// Enhanced topic description structure for detailed metadata
#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub name: String,
    pub partitions: Vec<PartitionDescription>,
    pub is_internal: bool,
    pub error_code: i16,
}

#[derive(Debug, Clone)]
pub struct PartitionDescription {
    pub id: u32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
    pub high_watermark: i64,
    pub low_watermark: i64,
}

pub struct MessageHandler {
    broker_id: BrokerId,
    broker_port: u16,
    storage: Arc<HybridStorage>,
    topic_manager: Arc<TopicManager>,
    partition_assigner: Arc<PartitionAssigner>,
    replication_coordinator: Option<Arc<ReplicationCoordinator>>,
    consumer_group_coordinator: Option<Arc<ConsumerGroupCoordinator>>,
    metrics: Arc<MetricsRegistry>,
    acl_manager: Option<Arc<parking_lot::RwLock<AclManager>>>,
    ultra_performance_broker: Arc<UltraPerformanceBroker>,
    #[allow(dead_code)]
    high_performance_codec: HighPerformanceKafkaCodec,
    #[allow(dead_code)]
    message_pools: MessagePools,
}

impl MessageHandler {
    pub fn new() -> Result<Self> {
        Self::new_with_broker_id(0, 9092, false)
    }

    pub async fn new_with_recovery() -> Result<Self> {
        Self::new_with_broker_id_and_recovery(0, 9092, false).await
    }

    pub fn new_with_broker_id(
        broker_id: BrokerId,
        broker_port: u16,
        enable_replication: bool,
    ) -> Result<Self> {
        Self::new_with_features(broker_id, broker_port, enable_replication, false)
    }

    pub async fn new_with_broker_id_and_recovery(
        broker_id: BrokerId,
        broker_port: u16,
        enable_replication: bool,
    ) -> Result<Self> {
        Self::new_with_features_and_recovery(broker_id, broker_port, enable_replication, false)
            .await
    }

    pub fn new_with_features(
        broker_id: BrokerId,
        broker_port: u16,
        enable_replication: bool,
        enable_consumer_groups: bool,
    ) -> Result<Self> {
        let storage = Arc::new(HybridStorage::new("./data")?);
        let topic_manager = Arc::new(TopicManager::new());
        let partition_assigner = Arc::new(PartitionAssigner::new(Arc::clone(&topic_manager)));

        let replication_coordinator = if enable_replication {
            Some(Arc::new(ReplicationCoordinator::new(
                broker_id,
                ReplicationConfig::default(),
            )))
        } else {
            None
        };

        let consumer_group_coordinator = if enable_consumer_groups {
            Some(Arc::new(ConsumerGroupCoordinator::new(
                ConsumerGroupConfig::default(),
                Arc::clone(&topic_manager),
                Some(Arc::clone(&storage)),
                None, // No metadata persistence directory configured by default
            )))
        } else {
            None
        };

        let metrics = Arc::new(MetricsRegistry::new());

        // Start metrics background tasks
        let metrics_clone = Arc::clone(&metrics);
        tokio::spawn(async move {
            metrics_clone.start_background_tasks().await;
        });

        let ultra_performance_broker = Arc::new(UltraPerformanceBroker::new());

        let handler = Self {
            broker_id,
            broker_port,
            storage,
            topic_manager,
            partition_assigner,
            replication_coordinator,
            consumer_group_coordinator,
            metrics,
            acl_manager: None, // ACL disabled by default
            ultra_performance_broker,
            high_performance_codec: HighPerformanceKafkaCodec::new(),
            message_pools: MessagePools::new(),
        };

        // Update topic metrics
        handler.update_topic_metrics();

        Ok(handler)
    }

    pub async fn new_with_features_and_recovery(
        broker_id: BrokerId,
        broker_port: u16,
        enable_replication: bool,
        enable_consumer_groups: bool,
    ) -> Result<Self> {
        let storage = Arc::new(HybridStorage::new("./data")?);
        storage.load_from_disk().await?;
        let topic_manager = Arc::new(TopicManager::new());

        // Register topics from storage recovery into topic manager
        Self::register_recovered_topics(Arc::clone(&storage), Arc::clone(&topic_manager))?;

        let partition_assigner = Arc::new(PartitionAssigner::new(Arc::clone(&topic_manager)));

        let replication_coordinator = if enable_replication {
            Some(Arc::new(ReplicationCoordinator::new(
                broker_id,
                ReplicationConfig::default(),
            )))
        } else {
            None
        };

        let consumer_group_coordinator = if enable_consumer_groups {
            let coordinator = Arc::new(ConsumerGroupCoordinator::new(
                ConsumerGroupConfig::default(),
                Arc::clone(&topic_manager),
                Some(Arc::clone(&storage)),
                None, // No metadata persistence directory configured by default
            ));

            // Start the coordinator
            coordinator.clone().start().await?;
            Some(coordinator)
        } else {
            None
        };

        let metrics = Arc::new(MetricsRegistry::new());

        // Start metrics background tasks
        let metrics_clone = Arc::clone(&metrics);
        tokio::spawn(async move {
            metrics_clone.start_background_tasks().await;
        });

        let ultra_performance_broker = Arc::new(UltraPerformanceBroker::new());

        let handler = Self {
            broker_id,
            broker_port,
            storage,
            topic_manager,
            partition_assigner,
            replication_coordinator,
            consumer_group_coordinator,
            metrics,
            acl_manager: None, // ACL disabled by default
            ultra_performance_broker,
            high_performance_codec: HighPerformanceKafkaCodec::new(),
            message_pools: MessagePools::new(),
        };

        // Update topic metrics after recovery
        handler.update_topic_metrics();

        Ok(handler)
    }

    pub async fn handle_request(&self, request: Request) -> Result<Response> {
        // Record request received
        self.metrics.broker.request_received();
        match request {
            Request::Produce(req) => {
                debug!(
                    "Handling produce request for topic: {}, partition: {}",
                    req.topic, req.partition
                );
                self.handle_produce(req).await
            }
            Request::Fetch(req) => {
                debug!(
                    "Handling fetch request for topic: {}, partition: {}, offset: {}",
                    req.topic, req.partition, req.offset
                );
                self.handle_fetch(req).await
            }
            Request::MultiFetch(req) => {
                debug!(
                    "Handling multi-fetch request for {} topics with {} total partitions",
                    req.topics.len(),
                    req.topics.iter().map(|t| t.partitions.len()).sum::<usize>()
                );
                self.handle_multi_fetch(req).await
            }
            Request::ListOffsets(req) => {
                debug!(
                    "Handling list offsets request for topic: {}, partition: {}, timestamp: {}",
                    req.topic, req.partition, req.timestamp
                );
                self.handle_list_offsets(req).await
            }
            Request::Metadata(req) => {
                debug!("Handling metadata request for topics: {:?}", req.topics);
                self.handle_metadata(req).await
            }
            Request::CreateTopics(req) => {
                debug!(
                    "Handling create topics request for {} topics",
                    req.topics.len()
                );
                self.handle_create_topics(req).await
            }
            Request::DeleteTopics(req) => {
                debug!(
                    "Handling delete topics request for {} topics",
                    req.topic_names.len()
                );
                self.handle_delete_topics(req).await
            }
            Request::DescribeConfigs(req) => {
                debug!(
                    "Handling describe configs request for {} resources",
                    req.resources.len()
                );
                self.handle_describe_configs(req).await
            }
            Request::AlterConfigs(req) => {
                debug!(
                    "Handling alter configs request for {} resources",
                    req.resources.len()
                );
                self.handle_alter_configs(req).await
            }
            Request::SaslHandshake(req) => {
                debug!(
                    "Handling SASL handshake request for mechanism: {}",
                    req.mechanism
                );
                self.handle_sasl_handshake(req).await
            }
            Request::SaslAuthenticate(req) => {
                debug!(
                    "Handling SASL authenticate request with {} bytes",
                    req.auth_bytes.len()
                );
                self.handle_sasl_authenticate(req).await
            }
        }
    }

    async fn handle_produce(&self, request: ProduceRequest) -> Result<Response> {
        // 🚀 JAVA CLIENT COMPATIBILITY: Start performance timing for Java client optimization
        let start_time = std::time::Instant::now();

        // If partition is u32::MAX, use dynamic assignment
        let partition = if request.partition == u32::MAX {
            // Auto-assign partition based on message key
            let key = request
                .messages
                .first()
                .and_then(|msg| msg.key.as_ref())
                .map(|bytes| bytes.as_ref());

            let strategy = if key.is_some() {
                PartitionStrategy::KeyHash
            } else {
                PartitionStrategy::RoundRobin
            };

            let assigned_partition =
                self.partition_assigner
                    .assign_partition(&request.topic, key, strategy)?;
            // Update topic metrics when a topic is potentially created
            self.update_topic_metrics();
            assigned_partition
        } else {
            // Use specified partition, ensure topic exists with proper partition count
            self.topic_manager.ensure_topic_exists(&request.topic)?;
            self.update_topic_metrics();
            if !self
                .topic_manager
                .partition_exists(&request.topic, request.partition)
            {
                let topic_clone = request.topic.clone();
                return Ok(Response::Produce(ProduceResponse {
                    correlation_id: request.correlation_id,
                    topic: request.topic,
                    partition: request.partition,
                    base_offset: 0,
                    error_code: 3, // Unknown topic or partition
                    error_message: Some(format!(
                        "Partition {} does not exist for topic {}",
                        request.partition, topic_clone
                    )),
                }));
            }
            request.partition
        };

        // Calculate metrics before moving messages
        let message_count = request.messages.len() as u64;

        // Calculate total bytes for metrics (performance optimized)
        let total_bytes: u64 = request
            .messages
            .iter()
            .map(|msg| {
                let key_size = msg.key.as_ref().map(|k| k.len()).unwrap_or(0);
                let value_size = msg.value.len();
                let headers_size = msg
                    .headers
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
                // Include timestamp (8 bytes)
                key_size + value_size + headers_size + 8
            })
            .sum::<usize>() as u64;

        // Create shared message reference to avoid cloning
        let messages_arc = Arc::new(request.messages);

        // 🚀 JAVA CLIENT COMPATIBILITY: Use ultra-performance broker with fast acknowledgment
        let base_offset = match self.ultra_performance_broker.append_messages_ultra_shared(
            &request.topic,
            partition,
            Arc::clone(&messages_arc),
        ) {
            Ok(offset) => {
                let processing_duration = start_time.elapsed();
                if processing_duration > std::time::Duration::from_millis(100) {
                    info!("⚠️ JAVA CLIENT WARNING: Slow message processing detected: {:?} (target: <100ms)", processing_duration);
                }
                info!("🚀 ULTRA-PERFORMANCE: Successfully used ultra-performance broker, offset: {}, duration: {:?}", offset, processing_duration);
                offset
            }
            Err(e) => {
                let processing_duration = start_time.elapsed();
                info!("⚠️ ULTRA-PERFORMANCE: Ultra-performance broker failed ({}) after {:?}, falling back to traditional storage", e, processing_duration);
                // Fallback to basic storage if ultra-performance fails
                // Convert Arc back to Vec for compatibility
                let messages = Arc::try_unwrap(messages_arc).unwrap_or_else(|arc| (*arc).clone());
                self.storage
                    .append_messages(&request.topic, partition, messages)?
            }
        };

        // Record metrics with actual message data
        info!(
            "📊 METRICS DEBUG: Recording {} messages, {} bytes",
            message_count, total_bytes
        );
        self.metrics
            .throughput
            .record_produced(message_count, total_bytes);
        self.metrics.storage.record_message_stored(total_bytes);

        // If replication is enabled and this broker is the leader, replicate to followers
        if let Some(ref coordinator) = self.replication_coordinator {
            if coordinator.is_leader(&request.topic, partition).await {
                // In a full implementation, we'd wait for replication acknowledgments
                // based on the required acknowledgments (acks) setting
                debug!(
                    "Replicating messages for {}:{} as leader (base_offset: {})",
                    request.topic, partition, base_offset
                );
            }
        }

        // 🚀 JAVA CLIENT COMPATIBILITY: Measure total processing time and log performance
        let total_duration = start_time.elapsed();
        if total_duration > std::time::Duration::from_millis(500) {
            info!("🚨 JAVA CLIENT CRITICAL: Total processing time exceeded 500ms: {:?} (Java client timeout risk!)", total_duration);
        } else if total_duration > std::time::Duration::from_millis(100) {
            info!(
                "⚠️ JAVA CLIENT WARNING: Processing time: {:?} (target: <100ms for Java clients)",
                total_duration
            );
        } else {
            info!(
                "✅ JAVA CLIENT OPTIMIZED: Fast processing achieved: {:?}",
                total_duration
            );
        }

        info!(
            "Produced messages to topic: {}, partition: {}, base_offset: {}, acks: {}, processing_time: {:?}",
            request.topic, partition, base_offset, request.acks, total_duration
        );

        // Handle acks=0 (fire-and-forget) - no response should be sent
        if request.acks == 0 {
            info!("🔥 FIRE-AND-FORGET: acks=0, not sending response to client");
            return Ok(Response::NoResponse);
        }

        // 🚀 JAVA CLIENT COMPATIBILITY: Force immediate buffer flush for Java clients
        // This ensures data is persisted immediately and reduces timeout risk
        self.force_immediate_flush(&request.topic, partition).await;

        // 🚀 JAVA CLIENT COMPATIBILITY: Prioritized response construction for fast acknowledgment
        let response = ProduceResponse {
            correlation_id: request.correlation_id,
            topic: request.topic,
            partition,
            base_offset,
            error_code: 0,
            error_message: None,
        };

        info!("📤 JAVA CLIENT ACK: Sending immediate acknowledgment for correlation_id: {}, total_time: {:?}", request.correlation_id, total_duration);
        Ok(Response::Produce(response))
    }

    /// Force immediate buffer flush for Java clients to reduce timeout risk
    async fn force_immediate_flush(&self, topic: &str, partition: u32) {
        // 🚀 JAVA CLIENT BUFFER OPTIMIZATION: Force all buffered data to disk immediately
        let flush_start = std::time::Instant::now();

        // Force ultra-performance broker to flush its buffers
        if let Err(e) = self
            .ultra_performance_broker
            .force_flush_partition(topic, partition)
            .await
        {
            info!(
                "⚠️ FLUSH WARNING: Ultra-performance broker flush failed: {}",
                e
            );
        }

        // Force traditional storage to flush as well (fallback)
        if let Err(e) = self.storage.flush_partition(topic, partition) {
            info!("⚠️ FLUSH WARNING: Traditional storage flush failed: {}", e);
        }

        let flush_duration = flush_start.elapsed();
        if flush_duration > std::time::Duration::from_millis(10) {
            info!(
                "⚠️ FLUSH SLOW: Buffer flush took {:?} (target: <10ms)",
                flush_duration
            );
        } else {
            info!(
                "✅ FLUSH FAST: Buffer flush completed in {:?}",
                flush_duration
            );
        }
    }

    async fn handle_fetch(&self, request: FetchRequest) -> Result<Response> {
        // Enhanced fetch with timeout and improved error handling
        let start_time = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_millis(request.timeout_ms as u64);

        debug!(
            "Handling fetch request for {}:{} from offset {}, max_bytes={}, timeout_ms={}",
            request.topic, request.partition, request.offset, request.max_bytes, request.timeout_ms
        );

        // Validate topic and partition existence
        let validation_result = self
            .validate_topic_partition(&request.topic, request.partition)
            .await;
        if let Some(error_response) = validation_result {
            return Ok(Response::Fetch(FetchResponse {
                correlation_id: request.correlation_id,
                topic: request.topic,
                partition: request.partition,
                messages: vec![],
                error_code: error_response.0,
                error_message: Some(error_response.1),
            }));
        }

        // Check if we have messages immediately available
        let mut messages = self
            .fetch_messages_with_limit(
                &request.topic,
                request.partition,
                request.offset,
                request.max_bytes,
            )
            .await?;

        // If no messages and timeout > 0, wait for new messages
        if messages.is_empty() && request.timeout_ms > 0 {
            messages = self
                .wait_for_messages(
                    &request.topic,
                    request.partition,
                    request.offset,
                    request.max_bytes,
                    timeout_duration,
                    start_time,
                )
                .await?;
        }

        let bytes_returned: usize = messages
            .iter()
            .map(|(_, msg)| msg.value.len() + msg.key.as_ref().map(|k| k.len()).unwrap_or(0))
            .sum();

        // Record metrics
        let message_count = messages.len() as u64;
        self.metrics
            .throughput
            .record_consumed(message_count, bytes_returned as u64);

        debug!(
            "Fetch completed: {} messages, {} bytes for {}:{} from offset {}",
            messages.len(),
            bytes_returned,
            request.topic,
            request.partition,
            request.offset
        );

        let response = FetchResponse {
            correlation_id: request.correlation_id,
            topic: request.topic,
            partition: request.partition,
            messages,
            error_code: 0,
            error_message: None,
        };

        Ok(Response::Fetch(response))
    }

    async fn handle_metadata(&self, request: MetadataRequest) -> Result<Response> {
        debug!(
            "Metadata request: topics={:?}, allow_auto_topic_creation={}",
            request.topics, request.allow_auto_topic_creation
        );

        let requested_topics = if request.topics.is_empty() {
            self.topic_manager.list_topics()
        } else {
            request.topics
        };

        let mut topic_metadata = Vec::new();
        for topic in requested_topics {
            if let Some(topic_info) = self.topic_manager.get_topic(&topic) {
                let partition_metadata: Vec<PartitionMetadata> = topic_info
                    .partitions
                    .into_iter()
                    .map(|partition_info| PartitionMetadata {
                        id: partition_info.id,
                        leader: partition_info.leader.map(|id| id as i32),
                        replicas: partition_info
                            .replicas
                            .into_iter()
                            .map(|id| id as i32)
                            .collect(),
                        isr: partition_info
                            .in_sync_replicas
                            .into_iter()
                            .map(|id| id as i32)
                            .collect(),
                        leader_epoch: 0, // Default epoch for Java client compatibility
                    })
                    .collect();

                topic_metadata.push(TopicMetadata {
                    name: topic,
                    error_code: 0, // NO_ERROR
                    partitions: partition_metadata,
                });
            } else if request.allow_auto_topic_creation {
                // Auto-create topic if allowed and return its metadata
                match self.topic_manager.ensure_topic_exists(&topic) {
                    Ok(topic_info) => {
                        let partition_metadata: Vec<PartitionMetadata> = topic_info
                            .partitions
                            .into_iter()
                            .map(|partition_info| PartitionMetadata {
                                id: partition_info.id,
                                leader: partition_info.leader.map(|id| id as i32),
                                replicas: partition_info
                                    .replicas
                                    .into_iter()
                                    .map(|id| id as i32)
                                    .collect(),
                                isr: partition_info
                                    .in_sync_replicas
                                    .into_iter()
                                    .map(|id| id as i32)
                                    .collect(),
                                leader_epoch: 0, // Default epoch for Java client compatibility
                            })
                            .collect();

                        topic_metadata.push(TopicMetadata {
                            name: topic,
                            error_code: 0, // NO_ERROR
                            partitions: partition_metadata,
                        });
                        // Update topic metrics when a topic is auto-created
                        self.update_topic_metrics();
                    }
                    Err(_) => {
                        // Failed to auto-create, return error
                        topic_metadata.push(TopicMetadata {
                            name: topic,
                            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                            partitions: Vec::new(),
                        });
                    }
                }
            } else {
                // Return unknown topic error for non-existent topics
                topic_metadata.push(TopicMetadata {
                    name: topic,
                    error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                    partitions: Vec::new(),
                });
            }
        }

        let response = MetadataResponse {
            correlation_id: request.correlation_id,
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: "localhost".to_string(),
                port: self.broker_port as i32,
            }],
            topics: topic_metadata,
            api_version: request.api_version, // Forward the requested API version
        };

        Ok(Response::Metadata(response))
    }

    async fn handle_list_offsets(&self, request: ListOffsetsRequest) -> Result<Response> {
        // Ensure topic exists
        if self.topic_manager.get_topic(&request.topic).is_none() {
            return Ok(Response::ListOffsets(ListOffsetsResponse {
                correlation_id: request.correlation_id,
                topic: request.topic,
                partition: request.partition,
                timestamp: request.timestamp,
                offset: -1,
                error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                error_message: Some("Unknown topic".to_string()),
            }));
        }

        // Check if partition exists
        if !self
            .topic_manager
            .partition_exists(&request.topic, request.partition)
        {
            return Ok(Response::ListOffsets(ListOffsetsResponse {
                correlation_id: request.correlation_id,
                topic: request.topic,
                partition: request.partition,
                timestamp: request.timestamp,
                offset: -1,
                error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                error_message: Some("Unknown partition".to_string()),
            }));
        }

        // Get offset based on timestamp
        let offset = match request.timestamp {
            -2 => {
                // Earliest offset
                match self
                    .storage
                    .get_earliest_offset(&request.topic, request.partition)
                {
                    Some(earliest_offset) => earliest_offset as i64,
                    None => 0, // If no messages, earliest is 0
                }
            }
            -1 => {
                // Latest offset - get from storage
                match self
                    .storage
                    .get_latest_offset(&request.topic, request.partition)
                {
                    Some(latest_offset) => latest_offset as i64,
                    None => 0, // If no messages, latest is 0
                }
            }
            timestamp if timestamp >= 0 => {
                // Find offset by timestamp
                match self.storage.get_offset_by_timestamp(
                    &request.topic,
                    request.partition,
                    timestamp as u64,
                ) {
                    Some(found_offset) => found_offset as i64,
                    None => {
                        // If no message found with that timestamp, return latest offset
                        match self
                            .storage
                            .get_latest_offset(&request.topic, request.partition)
                        {
                            Some(latest_offset) => latest_offset as i64,
                            None => 0,
                        }
                    }
                }
            }
            _invalid_timestamp => {
                // Invalid timestamp, return error
                return Ok(Response::ListOffsets(ListOffsetsResponse {
                    correlation_id: request.correlation_id,
                    topic: request.topic,
                    partition: request.partition,
                    timestamp: request.timestamp,
                    offset: -1,
                    error_code: 43, // INVALID_TIMESTAMP
                    error_message: Some("Invalid timestamp value".to_string()),
                }));
            }
        };

        debug!(
            "ListOffsets for {}:{} timestamp={} -> offset={}",
            request.topic, request.partition, request.timestamp, offset
        );

        Ok(Response::ListOffsets(ListOffsetsResponse {
            correlation_id: request.correlation_id,
            topic: request.topic,
            partition: request.partition,
            timestamp: request.timestamp,
            offset,
            error_code: 0, // NO_ERROR
            error_message: None,
        }))
    }

    /// Enable replication for a partition as leader
    pub async fn become_leader(
        &self,
        topic: &str,
        partition: PartitionId,
        replicas: Vec<BrokerId>,
    ) -> Result<()> {
        if let Some(ref coordinator) = self.replication_coordinator {
            coordinator
                .become_leader(topic, partition, replicas)
                .await?;
            info!(
                "Broker {} became leader for {}:{}",
                self.broker_id, topic, partition
            );
        }
        Ok(())
    }

    /// Enable replication for a partition as follower
    pub async fn become_follower(
        &self,
        topic: &str,
        partition: PartitionId,
        leader_id: BrokerId,
    ) -> Result<()> {
        if let Some(ref coordinator) = self.replication_coordinator {
            coordinator
                .become_follower(topic, partition, leader_id)
                .await?;
            info!(
                "Broker {} became follower for {}:{} with leader {}",
                self.broker_id, topic, partition, leader_id
            );
        }
        Ok(())
    }

    /// Check if this broker is the leader for a partition
    pub async fn is_leader(&self, topic: &str, partition: PartitionId) -> bool {
        if let Some(ref coordinator) = self.replication_coordinator {
            coordinator.is_leader(topic, partition).await
        } else {
            true // If replication is disabled, treat as leader
        }
    }

    /// Get the leader for a partition
    pub async fn get_leader(&self, topic: &str, partition: PartitionId) -> Option<BrokerId> {
        if let Some(ref coordinator) = self.replication_coordinator {
            coordinator.get_leader(topic, partition).await
        } else {
            Some(self.broker_id) // If replication is disabled, this broker is the leader
        }
    }

    /// Handle consumer group message
    pub async fn handle_consumer_group_message(
        &self,
        message: ConsumerGroupMessage,
    ) -> Result<ConsumerGroupMessage> {
        if let Some(ref coordinator) = self.consumer_group_coordinator {
            let response = match &message {
                ConsumerGroupMessage::JoinGroup { .. } => {
                    coordinator.handle_join_group(message).await
                }
                ConsumerGroupMessage::SyncGroup { .. } => {
                    coordinator.handle_sync_group(message).await
                }
                ConsumerGroupMessage::Heartbeat { .. } => {
                    coordinator.handle_heartbeat(message).await
                }
                ConsumerGroupMessage::LeaveGroup { .. } => {
                    coordinator.handle_leave_group(message).await
                }
                ConsumerGroupMessage::ListGroups => coordinator.handle_list_groups().await,
                ConsumerGroupMessage::DescribeGroups { .. } => {
                    coordinator.handle_describe_groups(message).await
                }
                _ => {
                    // Return error for unsupported message types
                    ConsumerGroupMessage::HeartbeatResponse {
                        error_code: crate::consumer::error_codes::INVALID_GROUP_ID,
                    }
                }
            };
            Ok(response)
        } else {
            // Consumer groups are not enabled
            Ok(ConsumerGroupMessage::HeartbeatResponse {
                error_code: crate::consumer::error_codes::CONSUMER_COORDINATOR_NOT_AVAILABLE,
            })
        }
    }

    /// Get consumer group coordinator (if enabled)
    pub fn get_consumer_group_coordinator(&self) -> Option<Arc<ConsumerGroupCoordinator>> {
        self.consumer_group_coordinator.clone()
    }

    /// Check if consumer groups are enabled
    pub fn consumer_groups_enabled(&self) -> bool {
        self.consumer_group_coordinator.is_some()
    }

    /// Get metrics registry
    pub fn get_metrics(&self) -> Arc<MetricsRegistry> {
        Arc::clone(&self.metrics)
    }

    /// Get broker port
    pub fn get_broker_port(&self) -> u16 {
        self.broker_port
    }

    /// Update topic and partition metrics
    pub fn update_topic_metrics(&self) {
        let topic_names = self.topic_manager.list_topics();
        let topic_count = topic_names.len();
        let mut partition_count = 0usize;

        for topic_name in topic_names {
            if let Some(topic_metadata) = self.topic_manager.get_topic(&topic_name) {
                partition_count += topic_metadata.num_partitions as usize;
            }
        }

        self.metrics.broker.update_topic_count(topic_count);
        self.metrics.broker.update_partition_count(partition_count);
    }

    /// Ensure a topic exists, creating it with default config if it doesn't
    pub fn ensure_topic_exists(
        &self,
        topic_name: &str,
    ) -> Result<crate::topic_manager::TopicMetadata> {
        self.topic_manager.ensure_topic_exists(topic_name)
    }

    /// Get topic metadata for Kafka metadata response
    pub fn get_topic_metadata_for_kafka(
        &self,
        requested_topics: Option<Vec<String>>,
    ) -> Vec<(String, u32)> {
        if let Some(topics) = requested_topics {
            // Return metadata for specific requested topics
            topics
                .into_iter()
                .filter_map(|topic| {
                    if let Some(topic_info) = self.topic_manager.get_topic(&topic) {
                        Some((topic, topic_info.num_partitions))
                    } else {
                        // Return the topic with error (0 partitions indicates error)
                        Some((topic, 0))
                    }
                })
                .collect()
        } else {
            // Return all topics
            self.topic_manager
                .list_topics()
                .into_iter()
                .filter_map(|topic| {
                    if let Some(topic_info) = self.topic_manager.get_topic(&topic) {
                        Some((topic, topic_info.num_partitions))
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    /// Get enhanced topic descriptions for DescribeTopics functionality
    pub fn get_enhanced_topic_descriptions(
        &self,
        requested_topics: Option<Vec<String>>,
    ) -> Vec<TopicDescription> {
        let topics_to_describe = if let Some(topics) = requested_topics {
            // Kafka protocol: empty topics array means "return all topics"
            if topics.is_empty() {
                info!("Empty topics array - should return all topics");
                let all_topics = self.topic_manager.list_topics();
                info!(
                    "Available topics from topic_manager.list_topics(): {:?}",
                    all_topics
                );
                all_topics
            } else {
                info!("Specific topics requested: {:?}", topics);
                topics
            }
        } else {
            info!("No topics specified - returning all topics");
            let all_topics = self.topic_manager.list_topics();
            info!(
                "Available topics from topic_manager.list_topics(): {:?}",
                all_topics
            );
            all_topics
        };

        topics_to_describe
            .into_iter()
            .map(|topic_name| {
                if let Some(topic_info) = self.topic_manager.get_topic(&topic_name) {
                    // Topic exists - get detailed partition information
                    let mut partitions = Vec::new();

                    for partition_id in 0..topic_info.num_partitions {
                        // Get storage info for high/low watermarks
                        let high_watermark = self
                            .storage
                            .get_latest_offset(&topic_name, partition_id)
                            .map(|offset| offset + 1) // High watermark is next offset to be written
                            .unwrap_or(0);
                        let low_watermark = self
                            .storage
                            .get_earliest_offset(&topic_name, partition_id)
                            .unwrap_or(0);

                        partitions.push(PartitionDescription {
                            id: partition_id,
                            leader: 0, // This broker is always the leader for now
                            leader_epoch: 0,
                            replicas: vec![0],        // Only this broker
                            isr: vec![0],             // In-sync replicas - only this broker
                            offline_replicas: vec![], // No offline replicas
                            high_watermark: high_watermark as i64,
                            low_watermark: low_watermark as i64,
                        });
                    }

                    TopicDescription {
                        name: topic_name,
                        partitions,
                        is_internal: false,
                        error_code: 0,
                    }
                } else {
                    // Topic doesn't exist
                    TopicDescription {
                        name: topic_name,
                        partitions: vec![],
                        is_internal: false,
                        error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                    }
                }
            })
            .collect()
    }

    /// Validate topic and partition existence
    async fn validate_topic_partition(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<(i16, String)> {
        if let Some(topic_metadata) = self.topic_manager.get_topic(topic) {
            if partition >= topic_metadata.num_partitions {
                Some((
                    3,
                    format!(
                        "Partition {} does not exist for topic {} (has {} partitions)",
                        partition, topic, topic_metadata.num_partitions
                    ),
                ))
            } else {
                None // Valid topic and partition
            }
        } else {
            Some((3, format!("Topic {} does not exist", topic)))
        }
    }

    /// Fetch messages with proper byte and message limits
    async fn fetch_messages_with_limit(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        max_bytes: u32,
    ) -> Result<Vec<(Offset, Message)>> {
        // Try ultra-performance fetch first
        let all_messages = match self
            .ultra_performance_broker
            .fetch_messages_ultra(topic, partition, offset, max_bytes)
            .await
        {
            Ok(messages) if !messages.is_empty() => messages,
            _ => {
                // Fallback to basic storage
                self.storage
                    .fetch_messages(topic, partition, offset, max_bytes)?
            }
        };

        // Apply stricter byte limiting
        let mut result = Vec::new();
        let mut bytes_accumulated = 0u32;

        for (msg_offset, message) in all_messages {
            let msg_size = message.value.len() as u32
                + message.key.as_ref().map(|k| k.len() as u32).unwrap_or(0);

            // Always include at least one message, even if it exceeds max_bytes
            if result.is_empty() || bytes_accumulated + msg_size <= max_bytes {
                bytes_accumulated += msg_size;
                result.push((msg_offset, message));
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Wait for new messages with timeout
    async fn wait_for_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        max_bytes: u32,
        timeout_duration: std::time::Duration,
        start_time: std::time::Instant,
    ) -> Result<Vec<(Offset, Message)>> {
        // Create a notification subscription
        let mut notification_rx = self.storage.subscribe_to_messages();

        loop {
            let elapsed = start_time.elapsed();
            if elapsed >= timeout_duration {
                debug!(
                    "Fetch timeout after {:?} for {}:{}",
                    elapsed, topic, partition
                );
                break;
            }

            // Calculate remaining timeout
            let remaining_timeout = timeout_duration.saturating_sub(elapsed);

            // Wait for notification or timeout
            match tokio::time::timeout(remaining_timeout, notification_rx.recv()).await {
                Ok(Ok(notification)) => {
                    // Check if the notification is for our topic/partition and has messages at or after our offset
                    if notification.topic == topic && notification.partition == partition {
                        let (start_offset, end_offset) = notification.offset_range;

                        // Check if there are new messages at or after the offset we're waiting for
                        if end_offset > offset {
                            debug!(
                                "Received notification for {}:{} - new messages from {} to {} (waiting for {}+)",
                                topic, partition, start_offset, end_offset, offset
                            );

                            // Check for new messages
                            let messages = self
                                .fetch_messages_with_limit(topic, partition, offset, max_bytes)
                                .await?;
                            if !messages.is_empty() {
                                debug!(
                                    "Found {} messages after notification wait {:?}",
                                    messages.len(),
                                    elapsed
                                );
                                return Ok(messages);
                            }
                        }
                    }
                }
                Ok(Err(RecvError::Lagged(_))) => {
                    // Channel lagged, try to fetch immediately
                    debug!("Notification channel lagged, checking for messages immediately");
                    let messages = self
                        .fetch_messages_with_limit(topic, partition, offset, max_bytes)
                        .await?;
                    if !messages.is_empty() {
                        return Ok(messages);
                    }
                }
                Ok(Err(RecvError::Closed)) => {
                    // Channel closed, fall back to polling
                    debug!("Notification channel closed, falling back to immediate check");
                    break;
                }
                Err(_) => {
                    // Timeout - check once more before giving up
                    debug!(
                        "No notifications received within timeout for {}:{}",
                        topic, partition
                    );
                    break;
                }
            }
        }

        // Final check for messages before returning empty result
        let messages = self
            .fetch_messages_with_limit(topic, partition, offset, max_bytes)
            .await?;
        if !messages.is_empty() {
            debug!("Found {} messages on final check", messages.len());
        }

        Ok(messages)
    }

    /// Handle multi-topic fetch request
    async fn handle_multi_fetch(&self, request: MultiFetchRequest) -> Result<Response> {
        let start_time = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_millis(request.max_wait_ms as u64);

        debug!(
            "Handling multi-fetch request for {} topics, max_wait_ms={}, min_bytes={}, max_bytes={}",
            request.topics.len(), request.max_wait_ms, request.min_bytes, request.max_bytes
        );

        let mut topic_responses = Vec::new();
        let mut total_bytes = 0u32;
        let mut has_messages = false;

        // Process each topic
        for topic_request in request.topics {
            let mut partition_responses = Vec::new();

            // Process each partition in the topic
            for partition_request in topic_request.partitions {
                // Validate topic and partition
                let validation_result = self
                    .validate_topic_partition(&topic_request.topic, partition_request.partition)
                    .await;

                if let Some(error_response) = validation_result {
                    partition_responses.push(PartitionFetchResponse {
                        partition: partition_request.partition,
                        messages: vec![],
                        error_code: error_response.0,
                        error_message: Some(error_response.1),
                    });
                    continue;
                }

                // Fetch messages for this partition
                let messages = self
                    .fetch_messages_with_limit(
                        &topic_request.topic,
                        partition_request.partition,
                        partition_request.offset,
                        partition_request.max_bytes,
                    )
                    .await?;

                if !messages.is_empty() {
                    has_messages = true;
                    // Calculate bytes for this partition
                    let partition_bytes: u32 = messages
                        .iter()
                        .map(|(_, msg)| {
                            msg.value.len() as u32
                                + msg.key.as_ref().map(|k| k.len() as u32).unwrap_or(0)
                        })
                        .sum();
                    total_bytes += partition_bytes;
                }

                partition_responses.push(PartitionFetchResponse {
                    partition: partition_request.partition,
                    messages,
                    error_code: 0,
                    error_message: None,
                });

                // Check if we've hit the max_bytes limit across all partitions
                if total_bytes >= request.max_bytes {
                    debug!(
                        "Multi-fetch reached max_bytes limit: {} >= {}",
                        total_bytes, request.max_bytes
                    );
                    break;
                }
            }

            topic_responses.push(TopicFetchResponse {
                topic: topic_request.topic,
                partitions: partition_responses,
            });

            // Check byte limit across topics too
            if total_bytes >= request.max_bytes {
                break;
            }
        }

        // If no messages and min_bytes not satisfied, wait for timeout
        if !has_messages && total_bytes < request.min_bytes && request.max_wait_ms > 0 {
            debug!(
                "Multi-fetch waiting for more data: {} bytes < {} min_bytes",
                total_bytes, request.min_bytes
            );
            // In a real implementation, we'd wait for new messages or timeout
            // For now, just add a small delay
            tokio::time::sleep(std::cmp::min(
                timeout_duration,
                std::time::Duration::from_millis(50),
            ))
            .await;
        }

        let elapsed = start_time.elapsed();
        debug!(
            "Multi-fetch completed: {} topics, {} total bytes in {:?}",
            topic_responses.len(),
            total_bytes,
            elapsed
        );

        Ok(Response::MultiFetch(MultiFetchResponse {
            correlation_id: request.correlation_id,
            topics: topic_responses,
            error_code: 0,
            error_message: None,
        }))
    }

    /// Handle create topics admin request
    async fn handle_create_topics(&self, request: CreateTopicsRequest) -> Result<Response> {
        use crate::protocol::{CreatableTopicConfigs, CreatableTopicResult};

        let mut topic_results = Vec::new();

        for topic in request.topics {
            // Validate topic configuration
            if topic.name.is_empty() {
                topic_results.push(CreatableTopicResult {
                    name: topic.name.clone(),
                    topic_id: None,
                    error_code: 60, // INVALID_TOPIC_EXCEPTION
                    error_message: Some("Topic name cannot be empty".to_string()),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: Vec::new(),
                });
                continue;
            }

            // Check if topic already exists
            if self.topic_manager.get_topic(&topic.name).is_some() {
                topic_results.push(CreatableTopicResult {
                    name: topic.name.clone(),
                    topic_id: None,
                    error_code: 36, // TOPIC_ALREADY_EXISTS
                    error_message: Some(format!("Topic '{}' already exists", topic.name)),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: Vec::new(),
                });
                continue;
            }

            // Validate only mode
            if request.validate_only {
                topic_results.push(CreatableTopicResult {
                    name: topic.name.clone(),
                    topic_id: None,
                    error_code: 0, // Success for validation
                    error_message: None,
                    num_partitions: topic.num_partitions,
                    replication_factor: topic.replication_factor,
                    configs: topic
                        .configs
                        .into_iter()
                        .map(|c| CreatableTopicConfigs {
                            name: c.name,
                            value: c.value,
                            read_only: false,
                            config_source: 1, // DYNAMIC_TOPIC_CONFIG
                            is_sensitive: false,
                        })
                        .collect(),
                });
                continue;
            }

            // Create topic
            let config = crate::topic_manager::TopicConfig {
                num_partitions: topic.num_partitions as u32,
                replication_factor: topic.replication_factor as u32,
                segment_size: 1024 * 1024 * 1024, // 1GB default
                retention_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days default
            };
            match self.topic_manager.create_topic(&topic.name, config) {
                Ok(_) => {
                    info!(
                        "Created topic '{}' with {} partitions",
                        topic.name, topic.num_partitions
                    );
                    topic_results.push(CreatableTopicResult {
                        name: topic.name.clone(),
                        topic_id: Some(format!("topic-{}", topic.name)),
                        error_code: 0,
                        error_message: None,
                        num_partitions: topic.num_partitions,
                        replication_factor: topic.replication_factor,
                        configs: topic
                            .configs
                            .into_iter()
                            .map(|c| CreatableTopicConfigs {
                                name: c.name,
                                value: c.value,
                                read_only: false,
                                config_source: 1,
                                is_sensitive: false,
                            })
                            .collect(),
                    });
                }
                Err(e) => {
                    error!("Failed to create topic '{}': {}", topic.name, e);
                    topic_results.push(CreatableTopicResult {
                        name: topic.name.clone(),
                        topic_id: None,
                        error_code: 1, // UNKNOWN_SERVER_ERROR
                        error_message: Some(format!("Failed to create topic: {}", e)),
                        num_partitions: -1,
                        replication_factor: -1,
                        configs: Vec::new(),
                    });
                }
            }
        }

        Ok(Response::CreateTopics(CreateTopicsResponse {
            correlation_id: request.correlation_id,
            throttle_time_ms: 0,
            topics: topic_results,
        }))
    }

    /// Handle delete topics admin request
    async fn handle_delete_topics(&self, request: DeleteTopicsRequest) -> Result<Response> {
        use crate::protocol::DeletableTopicResult;

        let mut responses = Vec::new();

        for topic_name in request.topic_names {
            // Check if topic exists
            if self.topic_manager.get_topic(&topic_name).is_none() {
                responses.push(DeletableTopicResult {
                    name: topic_name.clone(),
                    topic_id: None,
                    error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                    error_message: Some(format!("Topic '{}' does not exist", topic_name)),
                });
                continue;
            }

            // Delete topic (for now, just remove from topic manager)
            // In a full implementation, this would also clean up storage
            match self.topic_manager.delete_topic(&topic_name) {
                Ok(_) => {
                    info!("Deleted topic '{}'", topic_name);
                    responses.push(DeletableTopicResult {
                        name: topic_name.clone(),
                        topic_id: Some(format!("topic-{}", topic_name)),
                        error_code: 0,
                        error_message: None,
                    });
                }
                Err(e) => {
                    error!("Failed to delete topic '{}': {}", topic_name, e);
                    responses.push(DeletableTopicResult {
                        name: topic_name.clone(),
                        topic_id: None,
                        error_code: 1, // UNKNOWN_SERVER_ERROR
                        error_message: Some(format!("Failed to delete topic: {}", e)),
                    });
                }
            }
        }

        Ok(Response::DeleteTopics(DeleteTopicsResponse {
            correlation_id: request.correlation_id,
            throttle_time_ms: 0,
            responses,
        }))
    }

    /// Handle describe configs admin request
    async fn handle_describe_configs(&self, request: DescribeConfigsRequest) -> Result<Response> {
        use crate::protocol::{DescribeConfigsResourceResult, DescribeConfigsResult};

        let mut results = Vec::new();

        for resource in request.resources {
            match resource.resource_type {
                2 => {
                    // Topic resource
                    if let Some(_topic) = self.topic_manager.get_topic(&resource.resource_name) {
                        // Return basic topic configurations
                        let mut configs = Vec::new();

                        // Add common topic configs
                        configs.push(DescribeConfigsResourceResult {
                            name: "cleanup.policy".to_string(),
                            value: "delete".to_string(),
                            read_only: false,
                            is_default: true,
                            config_source: 1, // DYNAMIC_TOPIC_CONFIG
                            is_sensitive: false,
                            synonyms: Vec::new(),
                            config_type: 4, // STRING
                            documentation: if request.include_documentation {
                                Some("The cleanup policy for segments".to_string())
                            } else {
                                None
                            },
                        });

                        configs.push(DescribeConfigsResourceResult {
                            name: "retention.ms".to_string(),
                            value: "604800000".to_string(), // 7 days
                            read_only: false,
                            is_default: true,
                            config_source: 1,
                            is_sensitive: false,
                            synonyms: Vec::new(),
                            config_type: 3, // LONG
                            documentation: if request.include_documentation {
                                Some("The retention time for log segments".to_string())
                            } else {
                                None
                            },
                        });

                        results.push(DescribeConfigsResult {
                            error_code: 0,
                            error_message: None,
                            resource_type: resource.resource_type,
                            resource_name: resource.resource_name.clone(),
                            configs,
                        });
                    } else {
                        results.push(DescribeConfigsResult {
                            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                            error_message: Some(format!(
                                "Topic '{}' does not exist",
                                resource.resource_name
                            )),
                            resource_type: resource.resource_type,
                            resource_name: resource.resource_name.clone(),
                            configs: Vec::new(),
                        });
                    }
                }
                4 => {
                    // Broker resource
                    // Return basic broker configurations
                    let mut configs = Vec::new();

                    configs.push(DescribeConfigsResourceResult {
                        name: "log.retention.hours".to_string(),
                        value: "168".to_string(), // 7 days
                        read_only: false,
                        is_default: true,
                        config_source: 2, // DYNAMIC_BROKER_CONFIG
                        is_sensitive: false,
                        synonyms: Vec::new(),
                        config_type: 2, // INT
                        documentation: if request.include_documentation {
                            Some("The number of hours to keep log files".to_string())
                        } else {
                            None
                        },
                    });

                    results.push(DescribeConfigsResult {
                        error_code: 0,
                        error_message: None,
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                        configs,
                    });
                }
                _ => {
                    results.push(DescribeConfigsResult {
                        error_code: 40, // INVALID_REQUEST
                        error_message: Some(format!(
                            "Unknown resource type: {}",
                            resource.resource_type
                        )),
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                        configs: Vec::new(),
                    });
                }
            }
        }

        Ok(Response::DescribeConfigs(DescribeConfigsResponse {
            correlation_id: request.correlation_id,
            throttle_time_ms: 0,
            results,
        }))
    }

    /// Handle alter configs admin request
    async fn handle_alter_configs(&self, request: AlterConfigsRequest) -> Result<Response> {
        use crate::protocol::AlterConfigsResourceResponse;

        let mut responses = Vec::new();

        for resource in request.resources {
            match resource.resource_type {
                2 => {
                    // Topic resource
                    if let Some(_topic) = self.topic_manager.get_topic(&resource.resource_name) {
                        if request.validate_only {
                            info!(
                                "Validating config changes for topic '{}'",
                                resource.resource_name
                            );
                            responses.push(AlterConfigsResourceResponse {
                                error_code: 0,
                                error_message: None,
                                resource_type: resource.resource_type,
                                resource_name: resource.resource_name.clone(),
                            });
                        } else {
                            // For now, just log the config changes
                            // In a full implementation, this would update topic configurations
                            for config in &resource.configs {
                                info!(
                                    "Setting config '{}' = {:?} for topic '{}'",
                                    config.name, config.value, resource.resource_name
                                );
                            }

                            responses.push(AlterConfigsResourceResponse {
                                error_code: 0,
                                error_message: None,
                                resource_type: resource.resource_type,
                                resource_name: resource.resource_name.clone(),
                            });
                        }
                    } else {
                        responses.push(AlterConfigsResourceResponse {
                            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                            error_message: Some(format!(
                                "Topic '{}' does not exist",
                                resource.resource_name
                            )),
                            resource_type: resource.resource_type,
                            resource_name: resource.resource_name.clone(),
                        });
                    }
                }
                4 => {
                    // Broker resource
                    if request.validate_only {
                        info!(
                            "Validating config changes for broker '{}'",
                            resource.resource_name
                        );
                    } else {
                        // For now, just log the config changes
                        for config in &resource.configs {
                            info!(
                                "Setting broker config '{}' = {:?}",
                                config.name, config.value
                            );
                        }
                    }

                    responses.push(AlterConfigsResourceResponse {
                        error_code: 0,
                        error_message: None,
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                    });
                }
                _ => {
                    responses.push(AlterConfigsResourceResponse {
                        error_code: 40, // INVALID_REQUEST
                        error_message: Some(format!(
                            "Unknown resource type: {}",
                            resource.resource_type
                        )),
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                    });
                }
            }
        }

        Ok(Response::AlterConfigs(AlterConfigsResponse {
            correlation_id: request.correlation_id,
            throttle_time_ms: 0,
            responses,
        }))
    }

    async fn handle_sasl_handshake(&self, request: SaslHandshakeRequest) -> Result<Response> {
        info!(
            "SASL handshake requested for mechanism: {}",
            request.mechanism
        );

        // For now, support PLAIN mechanism
        let supported_mechanisms = vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()];

        let error_code = if supported_mechanisms.contains(&request.mechanism) {
            0 // No error
        } else {
            33 // UNSUPPORTED_SASL_MECHANISM
        };

        Ok(Response::SaslHandshake(SaslHandshakeResponse {
            correlation_id: request.correlation_id,
            error_code,
            mechanisms: supported_mechanisms,
        }))
    }

    async fn handle_sasl_authenticate(&self, request: SaslAuthenticateRequest) -> Result<Response> {
        info!(
            "SASL authenticate requested with {} bytes of auth data",
            request.auth_bytes.len()
        );

        // For now, return a simple successful authentication response
        // In a full implementation, this would:
        // 1. Parse the auth_bytes based on the SASL mechanism
        // 2. Validate credentials against a user database
        // 3. Create a session token if authentication succeeds

        Ok(Response::SaslAuthenticate(SaslAuthenticateResponse {
            correlation_id: request.correlation_id,
            error_code: 0, // Success
            error_message: None,
            auth_bytes: Vec::new(),       // Empty response for successful auth
            session_lifetime_ms: 3600000, // 1 hour session
        }))
    }

    /// Register topics recovered from storage into the topic manager
    fn register_recovered_topics(
        storage: Arc<HybridStorage>,
        topic_manager: Arc<TopicManager>,
    ) -> Result<()> {
        let recovered_topics = storage.get_topics();

        for topic_name in recovered_topics {
            let partitions = storage.get_partitions(&topic_name);
            let partition_count = partitions.len() as u32;

            if partition_count > 0 {
                use crate::topic_manager::TopicConfig;
                let config = TopicConfig {
                    num_partitions: partition_count,
                    ..Default::default()
                };

                topic_manager.create_topic(&topic_name, config)?;
                info!(
                    "Registered recovered topic '{}' with {} partitions",
                    topic_name, partition_count
                );
            }
        }

        Ok(())
    }

    /// Initialize ACL manager with configuration
    pub fn with_acl_manager(mut self, acl_manager: AclManager) -> Self {
        self.acl_manager = Some(Arc::new(parking_lot::RwLock::new(acl_manager)));
        self
    }

    /// Check if ACLs are enabled
    pub fn acl_enabled(&self) -> bool {
        self.acl_manager.is_some()
    }

    /// Authorize a request based on principal and operation
    pub fn authorize_request(
        &self,
        principal: &Principal,
        resource_type: &ResourceType,
        resource_name: &str,
        operation: &Operation,
        host: Option<&str>,
    ) -> AuthorizationResult {
        match &self.acl_manager {
            Some(acl_manager) => {
                let acl_guard = acl_manager.read();
                acl_guard.authorize(principal, resource_type, resource_name, operation, host)
            }
            None => AuthorizationResult::Allowed, // No ACL = allow all
        }
    }

    /// Add ACL entry (requires write access)
    pub fn add_acl(&self, acl: crate::acl::AclEntry) -> Result<()> {
        match &self.acl_manager {
            Some(acl_manager) => {
                let mut acl_guard = acl_manager.write();
                acl_guard.add_acl(acl);
                Ok(())
            }
            None => Err(crate::FluxmqError::Config("ACL not enabled".to_string())),
        }
    }

    /// Get ACL manager for external operations (read-only)
    pub fn get_acl_manager(&self) -> Option<Arc<parking_lot::RwLock<AclManager>>> {
        self.acl_manager.clone()
    }
}
