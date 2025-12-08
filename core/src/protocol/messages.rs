use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Transaction API types
pub use crate::transaction::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, EndTxnRequest, EndTxnResponse, InitProducerIdRequest,
    InitProducerIdResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
    WriteTxnMarkersRequest, WriteTxnMarkersResponse,
};

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

// ==================== Consumer Group APIs ====================

// API 10: FindCoordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCoordinatorRequest {
    pub correlation_id: CorrelationId,
    pub key: String,  // Group ID or Transaction ID
    pub key_type: i8, // 0 = Group, 1 = Transaction
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCoordinatorResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

// API 11: JoinGroup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupRequest {
    pub correlation_id: CorrelationId,
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<GroupProtocol>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProtocol {
    pub name: String,    // "range", "roundrobin", "sticky"
    pub metadata: Bytes, // Serialized protocol metadata
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: String,
    pub leader: String, // Leader member ID
    pub member_id: String,
    pub members: Vec<JoinGroupResponseMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupResponseMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

// API 14: SyncGroup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncGroupRequest {
    pub correlation_id: CorrelationId,
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncGroupRequestAssignment {
    pub member_id: String,
    pub assignment: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncGroupResponse {
    pub correlation_id: CorrelationId,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Bytes,
}

// API 12: Heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions response encoding
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions encoding
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

// API 13: LeaveGroup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveGroupRequest {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions response encoding
    pub group_id: String,
    pub member_id: String,
    pub members: Vec<MemberIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberIdentity {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveGroupResponse {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions encoding
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub members: Vec<MemberResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberResponse {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub error_code: i16,
}

// API 8: OffsetCommit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitRequest {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions response encoding
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub retention_time_ms: i64,
    pub topics: Vec<OffsetCommitRequestTopic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitRequestTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitRequestPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub committed_metadata: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitResponse {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions encoding
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetCommitResponseTopic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

// API 9: OffsetFetch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetFetchRequest {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions response encoding
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    pub require_stable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetFetchRequestTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetFetchResponse {
    pub correlation_id: CorrelationId,
    pub api_version: i16, // Needed for flexible versions encoding
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetFetchResponseTopic>,
    pub error_code: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetFetchResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetFetchResponsePartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: Option<String>,
    pub error_code: i16,
}

// API 15: DescribeGroups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeGroupsRequest {
    pub correlation_id: CorrelationId,
    pub api_version: i16,
    pub groups: Vec<String>,
    pub include_authorized_operations: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeGroupsResponse {
    pub correlation_id: CorrelationId,
    pub api_version: i16,
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribedGroup {
    pub error_code: i16,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribedGroupMember>,
    pub authorized_operations: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Bytes,
    pub member_assignment: Bytes,
}

// API 16: ListGroups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListGroupsRequest {
    pub correlation_id: CorrelationId,
    pub api_version: i16,
    pub states_filter: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListGroupsResponse {
    pub correlation_id: CorrelationId,
    pub api_version: i16,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<ListedGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String,
}

// ==================== End of Consumer Group APIs ====================

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
    // Consumer Group APIs
    FindCoordinator(FindCoordinatorRequest),
    JoinGroup(JoinGroupRequest),
    SyncGroup(SyncGroupRequest),
    Heartbeat(HeartbeatRequest),
    LeaveGroup(LeaveGroupRequest),
    OffsetCommit(OffsetCommitRequest),
    OffsetFetch(OffsetFetchRequest),
    DescribeGroups(DescribeGroupsRequest),
    ListGroups(ListGroupsRequest),
    // Transaction APIs
    InitProducerId(InitProducerIdRequest),
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    EndTxn(EndTxnRequest),
    WriteTxnMarkers(WriteTxnMarkersRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
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
    // Consumer Group APIs
    FindCoordinator(FindCoordinatorResponse),
    JoinGroup(JoinGroupResponse),
    SyncGroup(SyncGroupResponse),
    Heartbeat(HeartbeatResponse),
    LeaveGroup(LeaveGroupResponse),
    OffsetCommit(OffsetCommitResponse),
    OffsetFetch(OffsetFetchResponse),
    DescribeGroups(DescribeGroupsResponse),
    ListGroups(ListGroupsResponse),
    // Transaction APIs
    InitProducerId(InitProducerIdResponse),
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    AddOffsetsToTxn(AddOffsetsToTxnResponse),
    EndTxn(EndTxnResponse),
    WriteTxnMarkers(WriteTxnMarkersResponse),
    TxnOffsetCommit(TxnOffsetCommitResponse),
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
