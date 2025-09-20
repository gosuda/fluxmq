//! Kafka Protocol Message Structures
//!
//! This module defines the message structures for the Kafka wire protocol.
//! These structures closely follow the official Kafka protocol specification.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Kafka request header present in all requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaRequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

/// Kafka response header present in all responses  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaResponseHeader {
    pub correlation_id: i32,
}

/// Kafka Record (version 2) - used in produce/fetch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaRecord {
    pub length: i32,
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key_length: i32,
    pub key: Option<Bytes>,
    pub value_length: i32,
    pub value: Bytes,
    pub headers: Vec<KafkaHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

// ============================================================================
// PRODUCE API (ApiKey = 0)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaProduceRequest {
    pub header: KafkaRequestHeader,
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<KafkaTopicProduceData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTopicProduceData {
    pub topic: String,
    pub partition_data: Vec<KafkaPartitionProduceData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPartitionProduceData {
    pub partition: i32,
    pub records: Option<Bytes>, // Compressed record batch
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaProduceResponse {
    pub header: KafkaResponseHeader,
    pub responses: Vec<KafkaTopicProduceResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTopicProduceResponse {
    pub topic: String,
    pub partition_responses: Vec<KafkaPartitionProduceResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPartitionProduceResponse {
    pub partition: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

// ============================================================================
// FETCH API (ApiKey = 1)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaFetchRequest {
    pub header: KafkaRequestHeader,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<KafkaTopicFetchData>,
    pub forgotten_topics_data: Vec<KafkaForgottenTopic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTopicFetchData {
    pub topic: String,
    pub partitions: Vec<KafkaPartitionFetchData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPartitionFetchData {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaForgottenTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaFetchResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub session_id: i32,
    pub responses: Vec<KafkaTopicFetchResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTopicFetchResponse {
    pub topic: String,
    pub partitions: Vec<KafkaPartitionFetchResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPartitionFetchResponse {
    pub partition: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<KafkaAbortedTransaction>,
    pub preferred_read_replica: i32,
    pub records: Option<Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

// ============================================================================
// LIST OFFSETS API (ApiKey = 2)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListOffsetsRequest {
    pub header: KafkaRequestHeader,
    pub replica_id: i32,
    pub isolation_level: i8,
    pub topics: Vec<KafkaListOffsetsTopic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListOffsetsTopic {
    pub topic: String,
    pub partitions: Vec<KafkaListOffsetsPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListOffsetsPartition {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub timestamp: i64, // -1 = latest, -2 = earliest
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListOffsetsResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub topics: Vec<KafkaListOffsetsTopicResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListOffsetsTopicResponse {
    pub topic: String,
    pub partitions: Vec<KafkaListOffsetsPartitionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListOffsetsPartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
}

// ============================================================================
// METADATA API (ApiKey = 3)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMetadataRequest {
    pub header: KafkaRequestHeader,
    pub topics: Option<Vec<String>>, // None means all topics
    pub allow_auto_topic_creation: bool,
    pub include_cluster_authorized_operations: bool,
    pub include_topic_authorized_operations: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMetadataResponse {
    pub header: KafkaResponseHeader,
    pub api_version: i16, // Store the request API version for flexible encoding
    pub throttle_time_ms: i32,
    pub brokers: Vec<KafkaBrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<KafkaTopicMetadata>,
    pub cluster_authorized_operations: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaBrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTopicMetadata {
    pub error_code: i16,
    pub topic: String,
    pub is_internal: bool,
    pub partitions: Vec<KafkaPartitionMetadata>,
    pub topic_authorized_operations: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPartitionMetadata {
    pub error_code: i16,
    pub partition: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

// ============================================================================
// CONSUMER GROUP APIs
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaJoinGroupRequest {
    pub header: KafkaRequestHeader,
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<KafkaJoinGroupProtocol>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaJoinGroupProtocol {
    pub name: String,
    pub metadata: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaJoinGroupResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<KafkaJoinGroupMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaJoinGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

// ============================================================================
// OFFSET COMMIT API (ApiKey = 8)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetCommitRequest {
    pub header: KafkaRequestHeader,
    pub group_id: String,
    pub generation_id: i32,
    pub consumer_id: String,
    pub retention_time_ms: i64,
    pub topics: Vec<KafkaOffsetCommitTopic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetCommitTopic {
    pub topic: String,
    pub partitions: Vec<KafkaOffsetCommitPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetCommitPartition {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetCommitResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub topics: Vec<KafkaOffsetCommitTopicResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetCommitTopicResponse {
    pub topic: String,
    pub partitions: Vec<KafkaOffsetCommitPartitionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetCommitPartitionResponse {
    pub partition: i32,
    pub error_code: i16,
}

// ============================================================================
// OFFSET FETCH API (ApiKey = 9)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetFetchRequest {
    pub header: KafkaRequestHeader,
    pub group_id: String,
    pub topics: Option<Vec<KafkaOffsetFetchTopic>>, // None means all topics
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetFetchTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetFetchResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub topics: Vec<KafkaOffsetFetchTopicResponse>,
    pub error_code: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetFetchTopicResponse {
    pub topic: String,
    pub partitions: Vec<KafkaOffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOffsetFetchPartitionResponse {
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub error_code: i16,
}

// ============================================================================
// FIND COORDINATOR API (ApiKey = 10)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaFindCoordinatorRequest {
    pub header: KafkaRequestHeader,
    pub coordinator_key: String, // Usually the consumer group ID
    pub coordinator_type: i8,    // 0 = GROUP, 1 = TRANSACTION
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaFindCoordinatorResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

// ============================================================================
// LIST GROUPS API (ApiKey = 16)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListGroupsRequest {
    pub header: KafkaRequestHeader,
    pub states_filter: Vec<String>, // Filter by group states
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListGroupsResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<KafkaListedGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String,
}

// ============================================================================
// HEARTBEAT API (ApiKey = 12)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaHeartbeatRequest {
    pub header: KafkaRequestHeader,
    pub group_id: String,
    pub generation_id: i32,
    pub consumer_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaHeartbeatResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

// ============================================================================
// LEAVE GROUP API (ApiKey = 13)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaveGroupRequest {
    pub header: KafkaRequestHeader,
    pub group_id: String,
    pub consumer_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaveGroupResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

// ============================================================================
// SYNC GROUP API (ApiKey = 14)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSyncGroupRequest {
    pub header: KafkaRequestHeader,
    pub group_id: String,
    pub generation_id: i32,
    pub consumer_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocol_name: String,
    pub assignments: Vec<KafkaSyncGroupAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSyncGroupAssignment {
    pub consumer_id: String,
    pub assignment: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSyncGroupResponse {
    pub header: KafkaResponseHeader,
    pub api_version: i16, // Track API version for protocol compliance
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub protocol_type: String,
    pub protocol_name: String,
    pub assignment: Bytes,
}

// ============================================================================
// DESCRIBE GROUPS API (ApiKey = 15)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDescribeGroupsRequest {
    pub header: KafkaRequestHeader,
    pub groups: Vec<String>, // Group IDs to describe
    pub include_authorized_operations: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDescribeGroupsResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub groups: Vec<KafkaDescribedGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDescribedGroup {
    pub error_code: i16,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<KafkaDescribedGroupMember>,
    pub authorized_operations: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDescribedGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Bytes,
    pub member_assignment: Bytes,
}

// ============================================================================
// API VERSIONS API (ApiKey = 18)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaApiVersionsRequest {
    pub header: KafkaRequestHeader,
    pub client_software_name: Option<String>, // v3+: nullable for backward compatibility
    pub client_software_version: Option<String>, // v3+: nullable for backward compatibility
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaApiVersionsResponse {
    pub header: KafkaResponseHeader,
    pub api_version: i16,
    pub error_code: i16,
    pub api_keys: Vec<KafkaApiVersion>,
    pub throttle_time_ms: i32,
    // New fields for v3+ Java client compatibility
    pub cluster_id: Option<String>,      // v3+: Cluster identifier
    pub controller_id: Option<i32>,      // v3+: Controller broker ID
    pub supported_features: Vec<String>, // v3+: Feature compatibility flags
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// Unified Kafka request enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaRequest {
    Produce(KafkaProduceRequest),
    Fetch(KafkaFetchRequest),
    ListOffsets(KafkaListOffsetsRequest),
    Metadata(KafkaMetadataRequest),
    LeaderAndIsr(KafkaLeaderAndIsrRequest),
    StopReplica(KafkaStopReplicaRequest),
    UpdateMetadata(KafkaUpdateMetadataRequest),
    ControlledShutdown(KafkaControlledShutdownRequest),
    OffsetCommit(KafkaOffsetCommitRequest),
    OffsetFetch(KafkaOffsetFetchRequest),
    FindCoordinator(KafkaFindCoordinatorRequest),
    ListGroups(KafkaListGroupsRequest),
    JoinGroup(KafkaJoinGroupRequest),
    Heartbeat(KafkaHeartbeatRequest),
    LeaveGroup(KafkaLeaveGroupRequest),
    SyncGroup(KafkaSyncGroupRequest),
    DescribeGroups(KafkaDescribeGroupsRequest),
    ApiVersions(KafkaApiVersionsRequest),
    CreateTopics(KafkaCreateTopicsRequest),
    DeleteTopics(KafkaDeleteTopicsRequest),
    DescribeConfigs(KafkaDescribeConfigsRequest),
    AlterConfigs(KafkaAlterConfigsRequest),
    SaslHandshake(KafkaSaslHandshakeRequest),
    SaslAuthenticate(KafkaSaslAuthenticateRequest),
    GetTelemetrySubscriptions(KafkaGetTelemetrySubscriptionsRequest),
}

/// Unified Kafka response enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaResponse {
    Produce(KafkaProduceResponse),
    Fetch(KafkaFetchResponse),
    ListOffsets(KafkaListOffsetsResponse),
    Metadata(KafkaMetadataResponse),
    LeaderAndIsr(KafkaLeaderAndIsrResponse),
    StopReplica(KafkaStopReplicaResponse),
    UpdateMetadata(KafkaUpdateMetadataResponse),
    ControlledShutdown(KafkaControlledShutdownResponse),
    OffsetCommit(KafkaOffsetCommitResponse),
    OffsetFetch(KafkaOffsetFetchResponse),
    FindCoordinator(KafkaFindCoordinatorResponse),
    ListGroups(KafkaListGroupsResponse),
    JoinGroup(KafkaJoinGroupResponse),
    Heartbeat(KafkaHeartbeatResponse),
    LeaveGroup(KafkaLeaveGroupResponse),
    SyncGroup(KafkaSyncGroupResponse),
    DescribeGroups(KafkaDescribeGroupsResponse),
    ApiVersions(KafkaApiVersionsResponse),
    CreateTopics(KafkaCreateTopicsResponse),
    DeleteTopics(KafkaDeleteTopicsResponse),
    DescribeConfigs(KafkaDescribeConfigsResponse),
    AlterConfigs(KafkaAlterConfigsResponse),
    SaslHandshake(KafkaSaslHandshakeResponse),
    SaslAuthenticate(KafkaSaslAuthenticateResponse),
    GetTelemetrySubscriptions(KafkaGetTelemetrySubscriptionsResponse),
}

impl KafkaRequest {
    /// Get the API key for this request
    pub fn api_key(&self) -> i16 {
        match self {
            KafkaRequest::Produce(_) => 0,
            KafkaRequest::Fetch(_) => 1,
            KafkaRequest::ListOffsets(_) => 2,
            KafkaRequest::Metadata(_) => 3,
            KafkaRequest::LeaderAndIsr(_) => 4,
            KafkaRequest::StopReplica(_) => 5,
            KafkaRequest::UpdateMetadata(_) => 6,
            KafkaRequest::ControlledShutdown(_) => 7,
            KafkaRequest::OffsetCommit(_) => 8,
            KafkaRequest::OffsetFetch(_) => 9,
            KafkaRequest::FindCoordinator(_) => 10,
            KafkaRequest::ListGroups(_) => 16,
            KafkaRequest::JoinGroup(_) => 11,
            KafkaRequest::Heartbeat(_) => 12,
            KafkaRequest::LeaveGroup(_) => 13,
            KafkaRequest::SyncGroup(_) => 14,
            KafkaRequest::DescribeGroups(_) => 15,
            KafkaRequest::ApiVersions(_) => 18,
            KafkaRequest::CreateTopics(_) => 19,
            KafkaRequest::DeleteTopics(_) => 20,
            KafkaRequest::DescribeConfigs(_) => 32,
            KafkaRequest::AlterConfigs(_) => 33,
            KafkaRequest::SaslHandshake(_) => 17,
            KafkaRequest::SaslAuthenticate(_) => 36,
            KafkaRequest::GetTelemetrySubscriptions(_) => 71,
        }
    }

    /// Get the correlation ID for this request
    pub fn correlation_id(&self) -> i32 {
        match self {
            KafkaRequest::Produce(req) => req.header.correlation_id,
            KafkaRequest::Fetch(req) => req.header.correlation_id,
            KafkaRequest::ListOffsets(req) => req.header.correlation_id,
            KafkaRequest::Metadata(req) => req.header.correlation_id,
            KafkaRequest::LeaderAndIsr(req) => req.header.correlation_id,
            KafkaRequest::StopReplica(req) => req.header.correlation_id,
            KafkaRequest::UpdateMetadata(req) => req.header.correlation_id,
            KafkaRequest::ControlledShutdown(req) => req.header.correlation_id,
            KafkaRequest::OffsetCommit(req) => req.header.correlation_id,
            KafkaRequest::OffsetFetch(req) => req.header.correlation_id,
            KafkaRequest::FindCoordinator(req) => req.header.correlation_id,
            KafkaRequest::ListGroups(req) => req.header.correlation_id,
            KafkaRequest::JoinGroup(req) => req.header.correlation_id,
            KafkaRequest::Heartbeat(req) => req.header.correlation_id,
            KafkaRequest::LeaveGroup(req) => req.header.correlation_id,
            KafkaRequest::SyncGroup(req) => req.header.correlation_id,
            KafkaRequest::DescribeGroups(req) => req.header.correlation_id,
            KafkaRequest::ApiVersions(req) => req.header.correlation_id,
            KafkaRequest::CreateTopics(req) => req.header.correlation_id,
            KafkaRequest::DeleteTopics(req) => req.correlation_id,
            KafkaRequest::DescribeConfigs(req) => req.correlation_id,
            KafkaRequest::AlterConfigs(req) => req.correlation_id,
            KafkaRequest::SaslHandshake(req) => req.correlation_id,
            KafkaRequest::SaslAuthenticate(req) => req.correlation_id,
            KafkaRequest::GetTelemetrySubscriptions(req) => req.header.correlation_id,
        }
    }

    /// Get the API version for this request
    pub fn api_version(&self) -> i16 {
        match self {
            KafkaRequest::Produce(req) => req.header.api_version,
            KafkaRequest::Fetch(req) => req.header.api_version,
            KafkaRequest::ListOffsets(req) => req.header.api_version,
            KafkaRequest::Metadata(req) => req.header.api_version,
            KafkaRequest::LeaderAndIsr(req) => req.header.api_version,
            KafkaRequest::StopReplica(req) => req.header.api_version,
            KafkaRequest::UpdateMetadata(req) => req.header.api_version,
            KafkaRequest::ControlledShutdown(req) => req.header.api_version,
            KafkaRequest::OffsetCommit(req) => req.header.api_version,
            KafkaRequest::OffsetFetch(req) => req.header.api_version,
            KafkaRequest::FindCoordinator(req) => req.header.api_version,
            KafkaRequest::ListGroups(req) => req.header.api_version,
            KafkaRequest::JoinGroup(req) => req.header.api_version,
            KafkaRequest::Heartbeat(req) => req.header.api_version,
            KafkaRequest::LeaveGroup(req) => req.header.api_version,
            KafkaRequest::SyncGroup(req) => req.header.api_version,
            KafkaRequest::DescribeGroups(req) => req.header.api_version,
            KafkaRequest::ApiVersions(req) => req.header.api_version,
            KafkaRequest::CreateTopics(req) => req.header.api_version,
            KafkaRequest::DeleteTopics(_) => 0, // No header in DeleteTopics, use default
            KafkaRequest::DescribeConfigs(req) => req.api_version,
            KafkaRequest::AlterConfigs(req) => req.api_version,
            KafkaRequest::SaslHandshake(_) => 0, // No header in SASL, use default
            KafkaRequest::SaslAuthenticate(_) => 0, // No header in SASL, use default
            KafkaRequest::GetTelemetrySubscriptions(req) => req.header.api_version,
        }
    }
}

impl KafkaResponse {
    /// Get the correlation ID for this response
    pub fn correlation_id(&self) -> i32 {
        match self {
            KafkaResponse::Produce(resp) => resp.header.correlation_id,
            KafkaResponse::Fetch(resp) => resp.header.correlation_id,
            KafkaResponse::ListOffsets(resp) => resp.header.correlation_id,
            KafkaResponse::Metadata(resp) => resp.header.correlation_id,
            KafkaResponse::OffsetCommit(resp) => resp.header.correlation_id,
            KafkaResponse::OffsetFetch(resp) => resp.header.correlation_id,
            KafkaResponse::FindCoordinator(resp) => resp.header.correlation_id,
            KafkaResponse::ListGroups(resp) => resp.header.correlation_id,
            KafkaResponse::JoinGroup(resp) => resp.header.correlation_id,
            KafkaResponse::Heartbeat(resp) => resp.header.correlation_id,
            KafkaResponse::LeaveGroup(resp) => resp.header.correlation_id,
            KafkaResponse::SyncGroup(resp) => resp.header.correlation_id,
            KafkaResponse::DescribeGroups(resp) => resp.header.correlation_id,
            KafkaResponse::ApiVersions(resp) => resp.header.correlation_id,
            KafkaResponse::CreateTopics(resp) => resp.header.correlation_id,
            KafkaResponse::DeleteTopics(resp) => resp.correlation_id,
            KafkaResponse::DescribeConfigs(resp) => resp.correlation_id,
            KafkaResponse::AlterConfigs(resp) => resp.correlation_id,
            KafkaResponse::SaslHandshake(resp) => resp.correlation_id,
            KafkaResponse::SaslAuthenticate(resp) => resp.correlation_id,
            KafkaResponse::LeaderAndIsr(resp) => resp.header.correlation_id,
            KafkaResponse::StopReplica(resp) => resp.header.correlation_id,
            KafkaResponse::UpdateMetadata(resp) => resp.header.correlation_id,
            KafkaResponse::ControlledShutdown(resp) => resp.header.correlation_id,
            KafkaResponse::GetTelemetrySubscriptions(resp) => resp.header.correlation_id,
        }
    }
}

// ============================================================================
// CREATE TOPICS API (ApiKey = 19)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCreateTopicsRequest {
    pub header: KafkaRequestHeader,
    pub topics: Vec<KafkaCreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<KafkaCreatableReplicaAssignment>,
    pub configs: Option<Vec<KafkaCreatableTopicConfigs>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCreatableReplicaAssignment {
    pub partition_index: i32,
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCreatableTopicConfigs {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCreateTopicsResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub topics: Vec<KafkaCreatableTopicResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCreatableTopicResult {
    pub name: String,
    pub topic_id: Option<String>, // UUID for newer versions
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topic_config_error_code: Option<i16>,
    pub num_partitions: Option<i32>,
    pub replication_factor: Option<i16>,
    pub configs: Option<Vec<KafkaCreatableTopicConfigs>>,
}

// ============================================================================
// DELETE TOPICS API (ApiKey = 20)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDeleteTopicsRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDeleteTopicsResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub responses: Vec<DeletableTopicResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletableTopicResult {
    pub name: String,
    pub topic_id: Option<String>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

// SASL Authentication API (17) - SASL Handshake
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSaslHandshakeRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub mechanism: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSaslHandshakeResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}

// SASL Authentication API (36) - SASL Authenticate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSaslAuthenticateRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub auth_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSaslAuthenticateResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
    pub session_lifetime_ms: i64,
}

// ============================================================================
// DESCRIBE CONFIGS API (ApiKey = 32)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDescribeConfigsRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub api_version: i16,
    pub resources: Vec<KafkaConfigResource>,
    pub include_synonyms: bool,
    pub include_documentation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfigResource {
    pub resource_type: i8, // 2 = Topic, 4 = Broker
    pub resource_name: String,
    pub configuration_keys: Option<Vec<String>>, // None = get all configs
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDescribeConfigsResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub results: Vec<KafkaConfigResourceResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfigResourceResult {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<KafkaConfigEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub is_default: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<KafkaConfigSynonym>,
    pub config_type: i8,
    pub documentation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfigSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

// ============================================================================
// ALTER CONFIGS API (ApiKey = 33)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAlterConfigsRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub api_version: i16,
    pub resources: Vec<KafkaAlterConfigsResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAlterConfigsResource {
    pub resource_type: i8, // 2 = Topic, 4 = Broker
    pub resource_name: String,
    pub configs: Vec<KafkaAlterableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAlterableConfig {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAlterConfigsResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub responses: Vec<KafkaAlterConfigsResourceResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAlterConfigsResourceResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

// ============================================================================
// LEADER_AND_ISR API (ApiKey = 4) - Replication Leadership Coordination
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaderAndIsrRequest {
    pub header: KafkaRequestHeader,
    pub controller_id: i32,
    pub controller_epoch: i32,
    pub partition_states: Vec<KafkaLeaderAndIsrPartitionState>,
    pub live_leaders: Vec<KafkaLeaderAndIsrLiveLeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaderAndIsrPartitionState {
    pub topic: String,
    pub partition: i32,
    pub controller_epoch: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub isr: Vec<i32>,
    pub zk_version: i32,
    pub replicas: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaderAndIsrLiveLeader {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaderAndIsrResponse {
    pub header: KafkaResponseHeader,
    pub error_code: i16,
    pub partitions: Vec<KafkaLeaderAndIsrPartitionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLeaderAndIsrPartitionResponse {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
}

// ============================================================================
// STOP_REPLICA API (ApiKey = 5) - Replica Stopping and Removal
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaStopReplicaRequest {
    pub header: KafkaRequestHeader,
    pub controller_id: i32,
    pub controller_epoch: i32,
    pub broker_epoch: i64,
    pub delete_partitions: bool,
    pub topics: Vec<KafkaStopReplicaTopicState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaStopReplicaTopicState {
    pub topic: String,
    pub partition_states: Vec<KafkaStopReplicaPartitionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaStopReplicaPartitionState {
    pub partition: i32,
    pub leader_epoch: i32,
    pub delete_partition: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaStopReplicaResponse {
    pub header: KafkaResponseHeader,
    pub error_code: i16,
    pub partition_errors: Vec<KafkaStopReplicaPartitionError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaStopReplicaPartitionError {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
}

// ============================================================================
// UPDATE_METADATA API (ApiKey = 6) - Cluster Metadata Updates
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaUpdateMetadataRequest {
    pub header: KafkaRequestHeader,
    pub controller_id: i32,
    pub controller_epoch: i32,
    pub broker_epoch: i64,
    pub partition_states: Vec<KafkaUpdateMetadataPartitionState>,
    pub live_brokers: Vec<KafkaUpdateMetadataBroker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaUpdateMetadataPartitionState {
    pub topic: String,
    pub partition: i32,
    pub controller_epoch: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub isr: Vec<i32>,
    pub zk_version: i32,
    pub replicas: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaUpdateMetadataBroker {
    pub broker_id: i32,
    pub endpoints: Vec<KafkaUpdateMetadataEndpoint>,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaUpdateMetadataEndpoint {
    pub port: i32,
    pub host: String,
    pub listener_name: String,
    pub security_protocol: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaUpdateMetadataResponse {
    pub header: KafkaResponseHeader,
    pub error_code: i16,
}

// ============================================================================
// CONTROLLED_SHUTDOWN API (ApiKey = 7) - Graceful Broker Shutdown
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaControlledShutdownRequest {
    pub header: KafkaRequestHeader,
    pub broker_id: i32,
    pub broker_epoch: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaControlledShutdownResponse {
    pub header: KafkaResponseHeader,
    pub error_code: i16,
    pub partitions_remaining: Vec<KafkaRemainingPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaRemainingPartition {
    pub topic: String,
    pub partition: i32,
}

// ============================================================================
// GET_TELEMETRY_SUBSCRIPTIONS API (ApiKey = 71) - KIP-714
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaGetTelemetrySubscriptionsRequest {
    pub header: KafkaRequestHeader,
    pub client_instance_id: [u8; 16], // UUID bytes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaGetTelemetrySubscriptionsResponse {
    pub header: KafkaResponseHeader,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub client_instance_id: [u8; 16], // UUID bytes
    pub subscription_id: i32,
    pub accepted_compression_types: Vec<i8>,
    pub push_interval_ms: i32,
    pub telemetry_max_bytes: i32,
    pub delta_temporality: bool,
    pub requested_metrics: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_record_creation() {
        let record = KafkaRecord {
            length: 100,
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key_length: 4,
            key: Some(Bytes::from("test")),
            value_length: 11,
            value: Bytes::from("hello world"),
            headers: vec![],
        };

        assert_eq!(record.key_length, 4);
        assert_eq!(record.value, Bytes::from("hello world"));
    }

    #[test]
    fn test_request_api_keys() {
        let produce_req = KafkaRequest::Produce(KafkaProduceRequest {
            header: KafkaRequestHeader {
                api_key: 0,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: None,
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![],
        });

        assert_eq!(produce_req.api_key(), 0);
        assert_eq!(produce_req.correlation_id(), 123);
    }
}
