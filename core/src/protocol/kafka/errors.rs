//! Kafka Error Codes
//!
//! This module defines error codes that match the official Kafka protocol.
//! These error codes are returned in responses to indicate various error conditions.

/// Kafka protocol error codes (matching official Kafka specification)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum KafkaErrorCode {
    // Success
    NoError = 0,

    // Server errors
    Unknown = -1,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    KafkaStorageError = 56,
    LogDirNotFound = 57,
    SaslAuthenticationFailed = 58,
    UnknownProducerId = 59,
    ReassignmentInProgress = 60,
    DelegationTokenAuthDisabled = 61,
    DelegationTokenNotFound = 62,
    DelegationTokenOwnerMismatch = 63,
    DelegationTokenRequestNotAllowed = 64,
    DelegationTokenAuthorizationFailed = 65,
    DelegationTokenExpired = 66,
    InvalidPrincipalType = 67,
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,
    FetchSessionIdNotFound = 70,
    InvalidFetchSessionEpoch = 71,
    ListenerNotFound = 72,
    TopicDeletionDisabled = 73,
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnsupportedCompressionType = 76,
    StaleBrokerEpoch = 77,
    OffsetNotAvailable = 78,
    MemberIdRequired = 79,
    PreferredLeaderNotAvailable = 80,
    GroupMaxSizeReached = 81,
    FencedInstanceId = 82,
}

impl KafkaErrorCode {
    /// Convert error code to i16 for wire protocol
    pub fn as_i16(self) -> i16 {
        self as i16
    }

    /// Create error code from i16 value
    pub fn from_i16(code: i16) -> Self {
        match code {
            0 => KafkaErrorCode::NoError,
            -1 => KafkaErrorCode::Unknown,
            1 => KafkaErrorCode::OffsetOutOfRange,
            2 => KafkaErrorCode::CorruptMessage,
            3 => KafkaErrorCode::UnknownTopicOrPartition,
            4 => KafkaErrorCode::InvalidFetchSize,
            5 => KafkaErrorCode::LeaderNotAvailable,
            6 => KafkaErrorCode::NotLeaderForPartition,
            7 => KafkaErrorCode::RequestTimedOut,
            8 => KafkaErrorCode::BrokerNotAvailable,
            9 => KafkaErrorCode::ReplicaNotAvailable,
            10 => KafkaErrorCode::MessageTooLarge,
            11 => KafkaErrorCode::StaleControllerEpoch,
            12 => KafkaErrorCode::OffsetMetadataTooLarge,
            13 => KafkaErrorCode::NetworkException,
            14 => KafkaErrorCode::CoordinatorLoadInProgress,
            15 => KafkaErrorCode::CoordinatorNotAvailable,
            16 => KafkaErrorCode::NotCoordinator,
            17 => KafkaErrorCode::InvalidTopicException,
            18 => KafkaErrorCode::RecordListTooLarge,
            19 => KafkaErrorCode::NotEnoughReplicas,
            20 => KafkaErrorCode::NotEnoughReplicasAfterAppend,
            21 => KafkaErrorCode::InvalidRequiredAcks,
            22 => KafkaErrorCode::IllegalGeneration,
            23 => KafkaErrorCode::InconsistentGroupProtocol,
            24 => KafkaErrorCode::InvalidGroupId,
            25 => KafkaErrorCode::UnknownMemberId,
            26 => KafkaErrorCode::InvalidSessionTimeout,
            27 => KafkaErrorCode::RebalanceInProgress,
            28 => KafkaErrorCode::InvalidCommitOffsetSize,
            29 => KafkaErrorCode::TopicAuthorizationFailed,
            30 => KafkaErrorCode::GroupAuthorizationFailed,
            31 => KafkaErrorCode::ClusterAuthorizationFailed,
            32 => KafkaErrorCode::InvalidTimestamp,
            33 => KafkaErrorCode::UnsupportedSaslMechanism,
            34 => KafkaErrorCode::IllegalSaslState,
            35 => KafkaErrorCode::UnsupportedVersion,
            36 => KafkaErrorCode::TopicAlreadyExists,
            37 => KafkaErrorCode::InvalidPartitions,
            38 => KafkaErrorCode::InvalidReplicationFactor,
            39 => KafkaErrorCode::InvalidReplicaAssignment,
            40 => KafkaErrorCode::InvalidConfig,
            41 => KafkaErrorCode::NotController,
            42 => KafkaErrorCode::InvalidRequest,
            43 => KafkaErrorCode::UnsupportedForMessageFormat,
            44 => KafkaErrorCode::PolicyViolation,
            45 => KafkaErrorCode::OutOfOrderSequenceNumber,
            46 => KafkaErrorCode::DuplicateSequenceNumber,
            47 => KafkaErrorCode::InvalidProducerEpoch,
            48 => KafkaErrorCode::InvalidTxnState,
            49 => KafkaErrorCode::InvalidProducerIdMapping,
            50 => KafkaErrorCode::InvalidTransactionTimeout,
            51 => KafkaErrorCode::ConcurrentTransactions,
            52 => KafkaErrorCode::TransactionCoordinatorFenced,
            53 => KafkaErrorCode::TransactionalIdAuthorizationFailed,
            54 => KafkaErrorCode::SecurityDisabled,
            55 => KafkaErrorCode::OperationNotAttempted,
            56 => KafkaErrorCode::KafkaStorageError,
            57 => KafkaErrorCode::LogDirNotFound,
            58 => KafkaErrorCode::SaslAuthenticationFailed,
            59 => KafkaErrorCode::UnknownProducerId,
            60 => KafkaErrorCode::ReassignmentInProgress,
            61 => KafkaErrorCode::DelegationTokenAuthDisabled,
            62 => KafkaErrorCode::DelegationTokenNotFound,
            63 => KafkaErrorCode::DelegationTokenOwnerMismatch,
            64 => KafkaErrorCode::DelegationTokenRequestNotAllowed,
            65 => KafkaErrorCode::DelegationTokenAuthorizationFailed,
            66 => KafkaErrorCode::DelegationTokenExpired,
            67 => KafkaErrorCode::InvalidPrincipalType,
            68 => KafkaErrorCode::NonEmptyGroup,
            69 => KafkaErrorCode::GroupIdNotFound,
            70 => KafkaErrorCode::FetchSessionIdNotFound,
            71 => KafkaErrorCode::InvalidFetchSessionEpoch,
            72 => KafkaErrorCode::ListenerNotFound,
            73 => KafkaErrorCode::TopicDeletionDisabled,
            74 => KafkaErrorCode::FencedLeaderEpoch,
            75 => KafkaErrorCode::UnknownLeaderEpoch,
            76 => KafkaErrorCode::UnsupportedCompressionType,
            77 => KafkaErrorCode::StaleBrokerEpoch,
            78 => KafkaErrorCode::OffsetNotAvailable,
            79 => KafkaErrorCode::MemberIdRequired,
            80 => KafkaErrorCode::PreferredLeaderNotAvailable,
            81 => KafkaErrorCode::GroupMaxSizeReached,
            82 => KafkaErrorCode::FencedInstanceId,
            _ => KafkaErrorCode::Unknown,
        }
    }

    /// Check if this error code indicates a retriable error
    pub fn is_retriable(self) -> bool {
        match self {
            KafkaErrorCode::NoError => false,
            KafkaErrorCode::UnknownTopicOrPartition => true,
            KafkaErrorCode::LeaderNotAvailable => true,
            KafkaErrorCode::NotLeaderForPartition => true,
            KafkaErrorCode::RequestTimedOut => true,
            KafkaErrorCode::BrokerNotAvailable => true,
            KafkaErrorCode::ReplicaNotAvailable => true,
            KafkaErrorCode::NetworkException => true,
            KafkaErrorCode::CoordinatorLoadInProgress => true,
            KafkaErrorCode::CoordinatorNotAvailable => true,
            KafkaErrorCode::NotCoordinator => true,
            KafkaErrorCode::NotEnoughReplicas => true,
            KafkaErrorCode::NotEnoughReplicasAfterAppend => true,
            KafkaErrorCode::RebalanceInProgress => true,
            KafkaErrorCode::NotController => true,
            KafkaErrorCode::PreferredLeaderNotAvailable => true,
            _ => false,
        }
    }

    /// Get human-readable error message
    pub fn message(self) -> &'static str {
        match self {
            KafkaErrorCode::NoError => "The server experienced an unexpected error when processing the request",
            KafkaErrorCode::Unknown => "The server experienced an unexpected error when processing the request",
            KafkaErrorCode::OffsetOutOfRange => "The requested offset is not within the range of offsets maintained by the server",
            KafkaErrorCode::CorruptMessage => "The message contents does not match the message CRC or the message is otherwise corrupt",
            KafkaErrorCode::UnknownTopicOrPartition => "The topic or partition does not exist",
            KafkaErrorCode::InvalidFetchSize => "The fetch size is invalid",
            KafkaErrorCode::LeaderNotAvailable => "There is no leader for this topic-partition as we are in the middle of a leadership election",
            KafkaErrorCode::NotLeaderForPartition => "This server is not the leader for that topic-partition",
            KafkaErrorCode::RequestTimedOut => "The request timed out",
            KafkaErrorCode::BrokerNotAvailable => "The broker is not available",
            KafkaErrorCode::ReplicaNotAvailable => "The replica is not available for the requested topic-partition",
            KafkaErrorCode::MessageTooLarge => "The request included a message larger than the max message size the server will accept",
            KafkaErrorCode::StaleControllerEpoch => "The controller moved to another broker",
            KafkaErrorCode::OffsetMetadataTooLarge => "The metadata field of the offset request was too large",
            KafkaErrorCode::NetworkException => "The server disconnected before a response was received",
            KafkaErrorCode::CoordinatorLoadInProgress => "The coordinator is loading and hence can't process requests",
            KafkaErrorCode::CoordinatorNotAvailable => "The coordinator is not available",
            KafkaErrorCode::NotCoordinator => "This is not the correct coordinator",
            KafkaErrorCode::InvalidTopicException => "The request attempted to perform an operation on an invalid topic",
            KafkaErrorCode::RecordListTooLarge => "The request included message batch larger than the configured segment size on the server",
            KafkaErrorCode::NotEnoughReplicas => "Messages are rejected since there are fewer in-sync replicas than required",
            KafkaErrorCode::NotEnoughReplicasAfterAppend => "Messages are written to the log, but to fewer in-sync replicas than required",
            KafkaErrorCode::InvalidRequiredAcks => "Produce request specified an invalid value for required acks",
            KafkaErrorCode::IllegalGeneration => "Specified group generation id is not valid",
            KafkaErrorCode::InconsistentGroupProtocol => "The group member's supported protocols are incompatible with those of existing members",
            KafkaErrorCode::InvalidGroupId => "The configured groupId is invalid",
            KafkaErrorCode::UnknownMemberId => "The coordinator is not aware of this member",
            KafkaErrorCode::InvalidSessionTimeout => "The session timeout is not within the range allowed by the broker",
            KafkaErrorCode::RebalanceInProgress => "The group is rebalancing, so a rejoin is needed",
            KafkaErrorCode::InvalidCommitOffsetSize => "The committing offset data size is not valid",
            KafkaErrorCode::TopicAuthorizationFailed => "Topic authorization failed",
            KafkaErrorCode::GroupAuthorizationFailed => "Group authorization failed",
            KafkaErrorCode::ClusterAuthorizationFailed => "Cluster authorization failed",
            KafkaErrorCode::InvalidTimestamp => "The timestamp of the message is out of acceptable range",
            KafkaErrorCode::UnsupportedSaslMechanism => "The broker does not support the requested SASL mechanism",
            KafkaErrorCode::IllegalSaslState => "Request is not valid given the current SASL state",
            KafkaErrorCode::UnsupportedVersion => "The version of API is not supported",
            KafkaErrorCode::TopicAlreadyExists => "Topic with this name already exists",
            KafkaErrorCode::InvalidPartitions => "Number of partitions is invalid",
            KafkaErrorCode::InvalidReplicationFactor => "Replication-factor is invalid",
            KafkaErrorCode::InvalidReplicaAssignment => "Replica assignment is invalid",
            KafkaErrorCode::InvalidConfig => "Configuration is invalid",
            KafkaErrorCode::NotController => "This is not the correct controller for this cluster",
            KafkaErrorCode::InvalidRequest => "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker",
            KafkaErrorCode::UnsupportedForMessageFormat => "The message format version does not support the request",
            KafkaErrorCode::PolicyViolation => "Request parameters do not satisfy the configured policy",
            KafkaErrorCode::OutOfOrderSequenceNumber => "The broker received an out of order sequence number",
            KafkaErrorCode::DuplicateSequenceNumber => "The broker received a duplicate sequence number",
            KafkaErrorCode::InvalidProducerEpoch => "Producer attempted an operation with an old epoch",
            KafkaErrorCode::InvalidTxnState => "The producer attempted a transactional operation in an invalid state",
            KafkaErrorCode::InvalidProducerIdMapping => "The producer attempted to use a producer id which is not currently assigned to its transactional id",
            KafkaErrorCode::InvalidTransactionTimeout => "The transaction timeout is larger than the maximum value allowed by the broker",
            KafkaErrorCode::ConcurrentTransactions => "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing",
            KafkaErrorCode::TransactionCoordinatorFenced => "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producerId",
            KafkaErrorCode::TransactionalIdAuthorizationFailed => "Transactional Id authorization failed",
            KafkaErrorCode::SecurityDisabled => "Security features are disabled",
            KafkaErrorCode::OperationNotAttempted => "The broker did not attempt to execute this operation",
            KafkaErrorCode::KafkaStorageError => "Disk error when trying to access log file on the disk",
            KafkaErrorCode::LogDirNotFound => "The user-specified log directory is not found in the broker config",
            KafkaErrorCode::SaslAuthenticationFailed => "SASL Authentication failed",
            KafkaErrorCode::UnknownProducerId => "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question",
            KafkaErrorCode::ReassignmentInProgress => "A partition reassignment is in progress",
            KafkaErrorCode::DelegationTokenAuthDisabled => "Delegation Token feature is not enabled",
            KafkaErrorCode::DelegationTokenNotFound => "Delegation Token is not found on server",
            KafkaErrorCode::DelegationTokenOwnerMismatch => "Specified Principal is not valid Owner/Renewer",
            KafkaErrorCode::DelegationTokenRequestNotAllowed => "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels",
            KafkaErrorCode::DelegationTokenAuthorizationFailed => "Delegation Token authorization failed",
            KafkaErrorCode::DelegationTokenExpired => "Delegation Token is expired",
            KafkaErrorCode::InvalidPrincipalType => "Supplied principalType is not supported",
            KafkaErrorCode::NonEmptyGroup => "The group is not empty",
            KafkaErrorCode::GroupIdNotFound => "The group id does not exist",
            KafkaErrorCode::FetchSessionIdNotFound => "The fetch session ID was not found",
            KafkaErrorCode::InvalidFetchSessionEpoch => "The fetch session epoch is invalid",
            KafkaErrorCode::ListenerNotFound => "There is no listener on the leader broker that matches the listener on which metadata request was processed",
            KafkaErrorCode::TopicDeletionDisabled => "Topic deletion is disabled",
            KafkaErrorCode::FencedLeaderEpoch => "The leader epoch in the request is older than the leader epoch on the broker",
            KafkaErrorCode::UnknownLeaderEpoch => "The leader epoch in the request is newer than the leader epoch on the broker",
            KafkaErrorCode::UnsupportedCompressionType => "The requesting client does not support the compression type of given partition",
            KafkaErrorCode::StaleBrokerEpoch => "Broker epoch has changed",
            KafkaErrorCode::OffsetNotAvailable => "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing",
            KafkaErrorCode::MemberIdRequired => "The group member needs to have a valid member id before actually entering a consumer group",
            KafkaErrorCode::PreferredLeaderNotAvailable => "The preferred leader was not available",
            KafkaErrorCode::GroupMaxSizeReached => "The consumer group has reached its max size",
            KafkaErrorCode::FencedInstanceId => "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id",
        }
    }
}

impl Default for KafkaErrorCode {
    fn default() -> Self {
        KafkaErrorCode::NoError
    }
}

impl std::fmt::Display for KafkaErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            *self as i16,
            format!("{:?}", self),
            self.message()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_conversion() {
        assert_eq!(KafkaErrorCode::NoError.as_i16(), 0);
        assert_eq!(KafkaErrorCode::Unknown.as_i16(), -1);
        assert_eq!(KafkaErrorCode::UnknownTopicOrPartition.as_i16(), 3);

        assert_eq!(KafkaErrorCode::from_i16(0), KafkaErrorCode::NoError);
        assert_eq!(KafkaErrorCode::from_i16(-1), KafkaErrorCode::Unknown);
        assert_eq!(
            KafkaErrorCode::from_i16(3),
            KafkaErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(KafkaErrorCode::from_i16(999), KafkaErrorCode::Unknown); // Unknown code
    }

    #[test]
    fn test_retriable_errors() {
        assert!(!KafkaErrorCode::NoError.is_retriable());
        assert!(!KafkaErrorCode::Unknown.is_retriable());
        assert!(KafkaErrorCode::UnknownTopicOrPartition.is_retriable());
        assert!(KafkaErrorCode::LeaderNotAvailable.is_retriable());
        assert!(KafkaErrorCode::RequestTimedOut.is_retriable());
        assert!(!KafkaErrorCode::MessageTooLarge.is_retriable());
    }

    #[test]
    fn test_error_messages() {
        assert!(!KafkaErrorCode::NoError.message().is_empty());
        assert!(!KafkaErrorCode::UnknownTopicOrPartition.message().is_empty());
        assert!(KafkaErrorCode::UnknownTopicOrPartition
            .message()
            .contains("topic or partition"));
    }

    #[test]
    fn test_error_display() {
        let error = KafkaErrorCode::UnknownTopicOrPartition;
        let display = format!("{}", error);
        assert!(display.contains("3"));
        assert!(display.contains("UnknownTopicOrPartition"));
        assert!(display.contains("topic or partition"));
    }
}
