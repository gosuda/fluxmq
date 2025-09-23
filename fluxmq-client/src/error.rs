//! Error types for the FluxMQ client library

/// Main error type for FluxMQ client operations
#[derive(Debug, thiserror::Error)]
pub enum FluxmqClientError {
    /// Connection-related errors
    #[error("Connection error: {message}")]
    Connection { message: String },

    /// Protocol-related errors
    #[error("Protocol error: {message}")]
    Protocol { message: String },

    /// Serialization/deserialization errors
    #[error("Serialization error: {message}")]
    Serialization { message: String },

    /// Deserialization errors  
    #[error("Deserialization error: {message}")]
    Deserialization { message: String },

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Timeout errors
    #[error("Operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// Producer-specific errors
    #[error("Producer error: {message}")]
    Producer { message: String },

    /// Consumer-specific errors
    #[error("Consumer error: {message}")]
    Consumer { message: String },

    /// Admin client errors
    #[error("Admin error: {message}")]
    Admin { message: String },

    /// Topic does not exist
    #[error("Topic '{topic}' does not exist")]
    TopicNotFound { topic: String },

    /// Partition does not exist
    #[error("Partition {partition} does not exist for topic '{topic}'")]
    PartitionNotFound { topic: String, partition: u32 },

    /// Invalid configuration
    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },

    /// Broker not available
    #[error("No available brokers")]
    NoBrokersAvailable,

    /// Message too large
    #[error("Message size {size} exceeds maximum {max_size}")]
    MessageTooLarge { size: usize, max_size: usize },

    /// Consumer group errors
    #[error("Consumer group error: {message}")]
    ConsumerGroup { message: String },

    /// Generic client error
    #[error("{message}")]
    Generic { message: String },
}

impl FluxmqClientError {
    /// Create a new connection error
    pub fn connection<S: Into<String>>(message: S) -> Self {
        Self::Connection {
            message: message.into(),
        }
    }

    /// Create a new protocol error
    pub fn protocol<S: Into<String>>(message: S) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    /// Create a new producer error
    pub fn producer<S: Into<String>>(message: S) -> Self {
        Self::Producer {
            message: message.into(),
        }
    }

    /// Create a new consumer error
    pub fn consumer<S: Into<String>>(message: S) -> Self {
        Self::Consumer {
            message: message.into(),
        }
    }

    /// Create a new admin error
    pub fn admin<S: Into<String>>(message: S) -> Self {
        Self::Admin {
            message: message.into(),
        }
    }

    /// Create a new invalid config error
    pub fn invalid_config<S: Into<String>>(message: S) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    /// Create a new generic error
    pub fn generic<S: Into<String>>(message: S) -> Self {
        Self::Generic {
            message: message.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout(timeout_ms: u64) -> Self {
        Self::Timeout { timeout_ms }
    }

    /// Create a serialization error
    pub fn serialization<S: Into<String>>(message: S) -> Self {
        Self::Serialization {
            message: message.into(),
        }
    }

    /// Create a deserialization error
    pub fn deserialization<S: Into<String>>(message: S) -> Self {
        Self::Deserialization {
            message: message.into(),
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection { .. } => true,
            Self::Timeout { .. } => true,
            Self::NoBrokersAvailable => true,
            Self::Io(_) => true,
            _ => false,
        }
    }

    /// Check if this error is a connection error
    pub fn is_connection_error(&self) -> bool {
        matches!(self, Self::Connection { .. } | Self::Io(_))
    }

    /// Check if this error is a timeout
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout { .. })
    }
}

/// Error code mapping for FluxMQ protocol errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// No error
    None = 0,
    /// Unknown server error
    Unknown = -1,
    /// Offset out of range
    OffsetOutOfRange = 1,
    /// Invalid message
    InvalidMessage = 2,
    /// Unknown topic or partition
    UnknownTopicOrPartition = 3,
    /// Invalid message size
    InvalidMessageSize = 4,
    /// Leader not available
    LeaderNotAvailable = 5,
    /// Not leader for partition
    NotLeaderForPartition = 6,
    /// Request timed out
    RequestTimedOut = 7,
    /// Broker not available
    BrokerNotAvailable = 8,
    /// Unknown consumer
    UnknownConsumer = 25,
    /// Consumer coordinator not available
    CoordinatorNotAvailable = 15,
}

impl ErrorCode {
    /// Convert error code to FluxMQ client error
    pub fn to_client_error(self, context: &str) -> FluxmqClientError {
        match self {
            ErrorCode::None => FluxmqClientError::generic("No error"),
            ErrorCode::Unknown => FluxmqClientError::generic(format!("Unknown error: {}", context)),
            ErrorCode::OffsetOutOfRange => {
                FluxmqClientError::consumer(format!("Offset out of range: {}", context))
            }
            ErrorCode::InvalidMessage => {
                FluxmqClientError::protocol(format!("Invalid message: {}", context))
            }
            ErrorCode::UnknownTopicOrPartition => FluxmqClientError::TopicNotFound {
                topic: context.to_string(),
            },
            ErrorCode::InvalidMessageSize => {
                FluxmqClientError::protocol(format!("Invalid message size: {}", context))
            }
            ErrorCode::LeaderNotAvailable => {
                FluxmqClientError::connection(format!("Leader not available: {}", context))
            }
            ErrorCode::NotLeaderForPartition => {
                FluxmqClientError::connection(format!("Not leader for partition: {}", context))
            }
            ErrorCode::RequestTimedOut => FluxmqClientError::timeout(5000), // Default timeout
            ErrorCode::BrokerNotAvailable => FluxmqClientError::NoBrokersAvailable,
            ErrorCode::UnknownConsumer => {
                FluxmqClientError::consumer(format!("Unknown consumer: {}", context))
            }
            ErrorCode::CoordinatorNotAvailable => FluxmqClientError::ConsumerGroup {
                message: format!("Coordinator not available: {}", context),
            },
        }
    }
}

impl From<i16> for ErrorCode {
    fn from(code: i16) -> Self {
        match code {
            0 => ErrorCode::None,
            -1 => ErrorCode::Unknown,
            1 => ErrorCode::OffsetOutOfRange,
            2 => ErrorCode::InvalidMessage,
            3 => ErrorCode::UnknownTopicOrPartition,
            4 => ErrorCode::InvalidMessageSize,
            5 => ErrorCode::LeaderNotAvailable,
            6 => ErrorCode::NotLeaderForPartition,
            7 => ErrorCode::RequestTimedOut,
            8 => ErrorCode::BrokerNotAvailable,
            15 => ErrorCode::CoordinatorNotAvailable,
            25 => ErrorCode::UnknownConsumer,
            _ => ErrorCode::Unknown,
        }
    }
}
