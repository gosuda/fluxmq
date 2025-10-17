//! Kafka Transaction Support Module
//!
//! This module implements exactly-once semantics through Kafka's transactional messaging system.
//! It provides full compatibility with Kafka's transaction APIs to enable atomic, idempotent
//! message production and consumption.
//!
//! ## Features
//!
//! - **Producer ID Management**: Unique producer identification with epoch tracking
//! - **Transaction Coordination**: Centralized transaction state management
//! - **Exactly-Once Semantics**: Atomic read-process-write operations
//! - **Control Messages**: Transaction markers for commit/abort operations
//! - **Crash Recovery**: Persistent transaction log with recovery capabilities
//!
//! ## Transaction APIs Supported
//!
//! - `InitProducerId` (API 22): Initialize transactional producer
//! - `AddPartitionsToTxn` (API 24): Add partitions to ongoing transaction
//! - `AddOffsetsToTxn` (API 25): Add consumer offsets to transaction
//! - `EndTxn` (API 26): Commit or abort transaction
//! - `WriteTxnMarkers` (API 27): Write control messages to logs
//! - `TxnOffsetCommit` (API 28): Atomic offset commit within transaction
//!
//! ## Usage Example
//!
//! ```rust
//! use fluxmq::transaction::{TransactionCoordinator, TransactionState};
//!
//! // Initialize transaction coordinator
//! let coordinator = TransactionCoordinator::new().await?;
//!
//! // Start transactional producer
//! let producer_id = coordinator.init_producer_id("my-transactional-app", 60000).await?;
//!
//! // Begin transaction
//! coordinator.begin_transaction(producer_id, 0).await?;
//!
//! // Add partitions to transaction
//! coordinator.add_partitions_to_txn(producer_id, 0, vec![("topic1", 0), ("topic2", 1)]).await?;
//!
//! // Commit transaction
//! coordinator.end_transaction(producer_id, 0, true).await?;
//! ```

pub mod coordinator;
pub mod messages;
pub mod producer_id_manager;
pub mod state_machine;
pub mod transaction_log;

pub use coordinator::{TransactionCoordinator, TransactionCoordinatorConfig};
pub use messages::*;
pub use producer_id_manager::{ProducerId, ProducerIdManager, ProducerMetadata};
pub use state_machine::TransactionStateMachine;
pub use transaction_log::{TransactionLog, TransactionLogEntry};

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;

/// Transaction-related errors
#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("Invalid transaction state: {0}")]
    InvalidState(String),

    #[error("Producer ID not found: {0}")]
    ProducerIdNotFound(i64),

    #[error("Invalid producer epoch: expected {expected}, got {actual}")]
    InvalidProducerEpoch { expected: i16, actual: i16 },

    #[error("Transaction timeout exceeded: {0}ms")]
    TransactionTimeout(i32),

    #[error("Concurrent transaction detected")]
    ConcurrentTransaction,

    #[error("Transaction coordinator fenced")]
    CoordinatorFenced,

    #[error("Invalid transactional ID: {0}")]
    InvalidTransactionalId(String),

    #[error("Storage error: {0}")]
    Storage(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Result type for transaction operations
pub type TransactionResult<T> = std::result::Result<T, TransactionError>;

/// Transaction state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxnState {
    /// Transaction is ongoing and can accept new partitions
    Ongoing,
    /// Transaction is preparing to commit
    PrepareCommit,
    /// Transaction is preparing to abort
    PrepareAbort,
    /// Transaction completed successfully
    CompleteCommit,
    /// Transaction was aborted
    CompleteAbort,
    /// Producer is in empty state (no ongoing transaction)
    Empty,
    /// Producer epoch has been fenced
    Dead,
    /// Transaction coordinator is preparing to fence the producer
    PrepareEpochFence,
}

impl TxnState {
    /// Check if transaction can accept new operations
    pub fn can_add_partitions(&self) -> bool {
        matches!(self, TxnState::Ongoing)
    }

    /// Check if transaction can be committed or aborted
    pub fn can_end(&self) -> bool {
        matches!(self, TxnState::Ongoing)
    }

    /// Check if transaction is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TxnState::CompleteCommit | TxnState::CompleteAbort | TxnState::Dead
        )
    }
}

/// Transaction metadata for a producer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    /// Unique transactional ID
    pub transactional_id: String,
    /// Producer ID assigned by coordinator
    pub producer_id: i64,
    /// Current producer epoch
    pub producer_epoch: i16,
    /// Current transaction state
    pub state: TxnState,
    /// Partitions currently in the transaction
    pub partitions: HashSet<TopicPartition>,
    /// Transaction timeout in milliseconds
    pub transaction_timeout_ms: i32,
    /// Timestamp when transaction started
    pub txn_start_timestamp: i64,
    /// Last update timestamp
    pub txn_last_update_timestamp: i64,
}

/// Topic-partition identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }
}

/// Transaction coordinator metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorMetadata {
    /// Current coordinator epoch
    pub coordinator_epoch: i32,
    /// Transaction timeout configuration
    pub transaction_max_timeout_ms: i32,
    /// Number of transaction log partitions
    pub transaction_log_num_partitions: i32,
    /// Active transactions count
    pub active_txn_count: u64,
}

impl Default for CoordinatorMetadata {
    fn default() -> Self {
        Self {
            coordinator_epoch: 0,
            transaction_max_timeout_ms: 900_000, // 15 minutes
            transaction_log_num_partitions: 50,
            active_txn_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_state_transitions() {
        assert!(TxnState::Ongoing.can_add_partitions());
        assert!(TxnState::Ongoing.can_end());
        assert!(!TxnState::PrepareCommit.can_add_partitions());
        assert!(TxnState::CompleteCommit.is_terminal());
        assert!(TxnState::Dead.is_terminal());
    }

    #[test]
    fn test_topic_partition() {
        let tp1 = TopicPartition::new("test-topic".to_string(), 0);
        let tp2 = TopicPartition::new("test-topic".to_string(), 0);
        let tp3 = TopicPartition::new("test-topic".to_string(), 1);

        assert_eq!(tp1, tp2);
        assert_ne!(tp1, tp3);
    }

    #[test]
    fn test_transaction_metadata_serialization() {
        let metadata = TransactionMetadata {
            transactional_id: "test-txn".to_string(),
            producer_id: 12345,
            producer_epoch: 0,
            state: TxnState::Ongoing,
            partitions: HashSet::new(),
            transaction_timeout_ms: 60000,
            txn_start_timestamp: 1234567890,
            txn_last_update_timestamp: 1234567890,
        };

        let serialized = serde_json::to_string(&metadata).unwrap();
        let deserialized: TransactionMetadata = serde_json::from_str(&serialized).unwrap();

        assert_eq!(metadata.transactional_id, deserialized.transactional_id);
        assert_eq!(metadata.producer_id, deserialized.producer_id);
        assert_eq!(metadata.state, deserialized.state);
    }
}
