use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TopicName = String;
pub type PartitionId = u32;
pub type Offset = u64;
pub type CorrelationId = i32;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Message {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: u64,
    pub headers: HashMap<String, Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceRequest {
    pub correlation_id: CorrelationId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub messages: Vec<Message>,
    pub acks: i16, // 0, 1, or -1 (all)
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceResponse {
    pub correlation_id: CorrelationId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub base_offset: Offset,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub correlation_id: CorrelationId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: Offset,
    pub max_bytes: u32,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub correlation_id: CorrelationId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub messages: Vec<(Offset, Message)>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiFetchRequest {
    pub correlation_id: CorrelationId,
    pub topics: Vec<TopicFetchRequest>,
    pub max_wait_ms: u32,
    pub min_bytes: u32,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFetchRequest {
    pub topic: TopicName,
    pub partitions: Vec<PartitionFetchRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFetchRequest {
    pub partition: PartitionId,
    pub offset: Offset,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiFetchResponse {
    pub correlation_id: CorrelationId,
    pub topics: Vec<TopicFetchResponse>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFetchResponse {
    pub topic: TopicName,
    pub partitions: Vec<PartitionFetchResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFetchResponse {
    pub partition: PartitionId,
    pub messages: Vec<(Offset, Message)>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataRequest {
    pub correlation_id: CorrelationId,
    pub topics: Vec<TopicName>,
    pub api_version: i16, // Preserve API version for proper response encoding
    pub allow_auto_topic_creation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: TopicName,
    pub error_code: i16,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub id: PartitionId,
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub leader_epoch: i32, // Required for Kafka v7+ metadata responses
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub correlation_id: CorrelationId,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
    pub api_version: i16, // Preserve API version for proper response encoding
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListOffsetsRequest {
    pub correlation_id: CorrelationId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub timestamp: i64, // -1 = latest, -2 = earliest
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListOffsetsResponse {
    pub correlation_id: CorrelationId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub timestamp: i64,
    pub offset: i64,
    pub error_code: i16,
    pub error_message: Option<String>,
}

// Admin API structures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicsRequest {
    pub correlation_id: CorrelationId,
    pub topics: Vec<CreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatableTopic {
    pub name: TopicName,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<CreatableReplicaAssignment>,
    pub configs: Vec<CreateableTopicConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatableReplicaAssignment {
    pub partition_index: PartitionId,
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateableTopicConfig {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicsResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub topics: Vec<CreatableTopicResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatableTopicResult {
    pub name: TopicName,
    pub topic_id: Option<String>,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<CreatableTopicConfigs>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatableTopicConfigs {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTopicsRequest {
    pub correlation_id: CorrelationId,
    pub topic_names: Vec<TopicName>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTopicsResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub responses: Vec<DeletableTopicResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletableTopicResult {
    pub name: TopicName,
    pub topic_id: Option<String>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

// SASL Authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslHandshakeRequest {
    pub correlation_id: CorrelationId,
    pub mechanism: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslHandshakeResponse {
    pub correlation_id: CorrelationId,
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslAuthenticateRequest {
    pub correlation_id: CorrelationId,
    pub auth_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslAuthenticateResponse {
    pub correlation_id: CorrelationId,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
    pub session_lifetime_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeConfigsRequest {
    pub correlation_id: CorrelationId,
    pub resources: Vec<ConfigResource>,
    pub include_synonyms: bool,
    pub include_documentation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResource {
    pub resource_type: i8, // 2 = Topic, 4 = Broker
    pub resource_name: String,
    pub configuration_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeConfigsResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub results: Vec<DescribeConfigsResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeConfigsResult {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsResourceResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeConfigsResourceResult {
    pub name: String,
    pub value: String,
    pub read_only: bool,
    pub is_default: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<ConfigSynonym>,
    pub config_type: i8,
    pub documentation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterConfigsRequest {
    pub correlation_id: CorrelationId,
    pub resources: Vec<AlterConfigsResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<AlterableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterableConfig {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterConfigsResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub responses: Vec<AlterConfigsResourceResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterConfigsResourceResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    MultiFetch(MultiFetchRequest),
    ListOffsets(ListOffsetsRequest),
    Metadata(MetadataRequest),
    // Admin APIs
    CreateTopics(CreateTopicsRequest),
    DeleteTopics(DeleteTopicsRequest),
    DescribeConfigs(DescribeConfigsRequest),
    AlterConfigs(AlterConfigsRequest),
    // SASL APIs
    SaslHandshake(SaslHandshakeRequest),
    SaslAuthenticate(SaslAuthenticateRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    MultiFetch(MultiFetchResponse),
    ListOffsets(ListOffsetsResponse),
    Metadata(MetadataResponse),
    // Admin APIs
    CreateTopics(CreateTopicsResponse),
    DeleteTopics(DeleteTopicsResponse),
    DescribeConfigs(DescribeConfigsResponse),
    AlterConfigs(AlterConfigsResponse),
    // SASL APIs
    SaslHandshake(SaslHandshakeResponse),
    SaslAuthenticate(SaslAuthenticateResponse),
    // Special response for acks=0 fire-and-forget requests
    NoResponse,
}

impl Message {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: value.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: HashMap::new(),
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }
}
