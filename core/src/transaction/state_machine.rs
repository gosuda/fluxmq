//! Transaction State Machine
//!
//! This module implements the core transaction state machine that manages the lifecycle
//! of transactions in FluxMQ. It ensures proper state transitions and validates
//! transaction operations according to Kafka's exactly-once semantics.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use super::{
    ProducerId, TopicPartition, TransactionError, TransactionMetadata, TransactionResult, TxnState,
};

/// Transaction state machine for managing transaction lifecycle
#[derive(Debug, Clone)]
pub struct TransactionStateMachine {
    /// Current transaction metadata
    metadata: TransactionMetadata,
}

impl TransactionStateMachine {
    /// Create new transaction state machine
    pub fn new(
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        transaction_timeout_ms: i32,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let metadata = TransactionMetadata {
            transactional_id,
            producer_id,
            producer_epoch,
            state: TxnState::Empty,
            partitions: HashSet::new(),
            transaction_timeout_ms,
            txn_start_timestamp: now,
            txn_last_update_timestamp: now,
        };

        Self { metadata }
    }

    /// Create from existing metadata
    pub fn from_metadata(metadata: TransactionMetadata) -> Self {
        Self { metadata }
    }

    /// Get current transaction metadata
    pub fn metadata(&self) -> &TransactionMetadata {
        &self.metadata
    }

    /// Get current transaction state
    pub fn current_state(&self) -> TxnState {
        self.metadata.state
    }

    /// Get producer ID
    pub fn producer_id(&self) -> ProducerId {
        self.metadata.producer_id
    }

    /// Get producer epoch
    pub fn producer_epoch(&self) -> i16 {
        self.metadata.producer_epoch
    }

    /// Get transactional ID
    pub fn transactional_id(&self) -> &str {
        &self.metadata.transactional_id
    }

    /// Get partitions in current transaction
    pub fn partitions(&self) -> &HashSet<TopicPartition> {
        &self.metadata.partitions
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self) -> TransactionResult<()> {
        match self.metadata.state {
            TxnState::Empty | TxnState::CompleteCommit | TxnState::CompleteAbort => {
                self.transition_to(TxnState::Ongoing)?;
                self.metadata.partitions.clear();
                self.update_timestamp();

                info!(
                    producer_id = self.metadata.producer_id,
                    transactional_id = %self.metadata.transactional_id,
                    "Transaction begun"
                );

                Ok(())
            }
            _ => Err(TransactionError::InvalidState(format!(
                "Cannot begin transaction from state {:?}",
                self.metadata.state
            ))),
        }
    }

    /// Add partitions to ongoing transaction
    pub fn add_partitions(&mut self, partitions: Vec<TopicPartition>) -> TransactionResult<()> {
        if !self.metadata.state.can_add_partitions() {
            return Err(TransactionError::InvalidState(format!(
                "Cannot add partitions in state {:?}",
                self.metadata.state
            )));
        }

        let mut added_count = 0;
        for partition in partitions {
            if self.metadata.partitions.insert(partition.clone()) {
                added_count += 1;
                debug!(
                    producer_id = self.metadata.producer_id,
                    topic = %partition.topic,
                    partition = partition.partition,
                    "Partition added to transaction"
                );
            }
        }

        self.update_timestamp();

        info!(
            producer_id = self.metadata.producer_id,
            transactional_id = %self.metadata.transactional_id,
            added_partitions = added_count,
            total_partitions = self.metadata.partitions.len(),
            "Partitions added to transaction"
        );

        Ok(())
    }

    /// Prepare to commit transaction
    pub fn prepare_commit(&mut self) -> TransactionResult<()> {
        if !self.metadata.state.can_end() {
            return Err(TransactionError::InvalidState(format!(
                "Cannot prepare commit from state {:?}",
                self.metadata.state
            )));
        }

        self.transition_to(TxnState::PrepareCommit)?;
        self.update_timestamp();

        info!(
            producer_id = self.metadata.producer_id,
            transactional_id = %self.metadata.transactional_id,
            partition_count = self.metadata.partitions.len(),
            "Transaction prepared for commit"
        );

        Ok(())
    }

    /// Prepare to abort transaction
    pub fn prepare_abort(&mut self) -> TransactionResult<()> {
        if !self.metadata.state.can_end() {
            return Err(TransactionError::InvalidState(format!(
                "Cannot prepare abort from state {:?}",
                self.metadata.state
            )));
        }

        self.transition_to(TxnState::PrepareAbort)?;
        self.update_timestamp();

        info!(
            producer_id = self.metadata.producer_id,
            transactional_id = %self.metadata.transactional_id,
            partition_count = self.metadata.partitions.len(),
            "Transaction prepared for abort"
        );

        Ok(())
    }

    /// Complete commit transaction
    pub fn complete_commit(&mut self) -> TransactionResult<()> {
        match self.metadata.state {
            TxnState::PrepareCommit => {
                self.transition_to(TxnState::CompleteCommit)?;
                self.update_timestamp();

                info!(
                    producer_id = self.metadata.producer_id,
                    transactional_id = %self.metadata.transactional_id,
                    "Transaction committed successfully"
                );

                Ok(())
            }
            _ => Err(TransactionError::InvalidState(format!(
                "Cannot complete commit from state {:?}",
                self.metadata.state
            ))),
        }
    }

    /// Complete abort transaction
    pub fn complete_abort(&mut self) -> TransactionResult<()> {
        match self.metadata.state {
            TxnState::PrepareAbort => {
                self.transition_to(TxnState::CompleteAbort)?;
                self.update_timestamp();

                info!(
                    producer_id = self.metadata.producer_id,
                    transactional_id = %self.metadata.transactional_id,
                    "Transaction aborted successfully"
                );

                Ok(())
            }
            _ => Err(TransactionError::InvalidState(format!(
                "Cannot complete abort from state {:?}",
                self.metadata.state
            ))),
        }
    }

    /// Fence producer (increment epoch and set to dead state)
    pub fn fence_producer(&mut self) -> TransactionResult<()> {
        self.metadata.producer_epoch =
            self.metadata.producer_epoch.checked_add(1).ok_or_else(|| {
                TransactionError::InvalidProducerEpoch {
                    expected: self.metadata.producer_epoch,
                    actual: i16::MAX,
                }
            })?;

        self.transition_to(TxnState::Dead)?;
        self.update_timestamp();

        warn!(
            producer_id = self.metadata.producer_id,
            new_epoch = self.metadata.producer_epoch,
            transactional_id = %self.metadata.transactional_id,
            "Producer fenced"
        );

        Ok(())
    }

    /// Check if transaction has expired
    pub fn is_expired(&self) -> bool {
        if self.metadata.state == TxnState::Empty {
            return false;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let elapsed = now - self.metadata.txn_start_timestamp;
        elapsed > self.metadata.transaction_timeout_ms as i64
    }

    /// Validate producer epoch
    pub fn validate_epoch(&self, epoch: i16) -> TransactionResult<()> {
        if self.metadata.producer_epoch != epoch {
            return Err(TransactionError::InvalidProducerEpoch {
                expected: self.metadata.producer_epoch,
                actual: epoch,
            });
        }
        Ok(())
    }

    /// Check if transaction can accept new operations
    pub fn can_accept_operations(&self) -> bool {
        matches!(self.metadata.state, TxnState::Ongoing)
    }

    /// Check if transaction is in terminal state
    pub fn is_terminal(&self) -> bool {
        self.metadata.state.is_terminal()
    }

    /// Reset to empty state (after completion)
    pub fn reset(&mut self) -> TransactionResult<()> {
        if !self.metadata.state.is_terminal() {
            return Err(TransactionError::InvalidState(format!(
                "Cannot reset from non-terminal state {:?}",
                self.metadata.state
            )));
        }

        self.transition_to(TxnState::Empty)?;
        self.metadata.partitions.clear();
        self.update_timestamp();

        debug!(
            producer_id = self.metadata.producer_id,
            transactional_id = %self.metadata.transactional_id,
            "Transaction state machine reset"
        );

        Ok(())
    }

    /// Get transaction duration in milliseconds
    pub fn transaction_duration_ms(&self) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        now - self.metadata.txn_start_timestamp
    }

    /// Transition to new state with validation
    fn transition_to(&mut self, new_state: TxnState) -> TransactionResult<()> {
        if !self.is_valid_transition(self.metadata.state, new_state) {
            return Err(TransactionError::InvalidState(format!(
                "Invalid state transition from {:?} to {:?}",
                self.metadata.state, new_state
            )));
        }

        debug!(
            producer_id = self.metadata.producer_id,
            old_state = ?self.metadata.state,
            new_state = ?new_state,
            "Transaction state transition"
        );

        self.metadata.state = new_state;
        Ok(())
    }

    /// Check if state transition is valid
    fn is_valid_transition(&self, from: TxnState, to: TxnState) -> bool {
        match (from, to) {
            // From Empty
            (TxnState::Empty, TxnState::Ongoing) => true,
            (TxnState::Empty, TxnState::Dead) => true,

            // From Ongoing
            (TxnState::Ongoing, TxnState::PrepareCommit) => true,
            (TxnState::Ongoing, TxnState::PrepareAbort) => true,
            (TxnState::Ongoing, TxnState::Dead) => true,

            // From PrepareCommit
            (TxnState::PrepareCommit, TxnState::CompleteCommit) => true,
            (TxnState::PrepareCommit, TxnState::PrepareAbort) => true,
            (TxnState::PrepareCommit, TxnState::Dead) => true,

            // From PrepareAbort
            (TxnState::PrepareAbort, TxnState::CompleteAbort) => true,
            (TxnState::PrepareAbort, TxnState::Dead) => true,

            // From CompleteCommit
            (TxnState::CompleteCommit, TxnState::Empty) => true,
            (TxnState::CompleteCommit, TxnState::Ongoing) => true,
            (TxnState::CompleteCommit, TxnState::Dead) => true,

            // From CompleteAbort
            (TxnState::CompleteAbort, TxnState::Empty) => true,
            (TxnState::CompleteAbort, TxnState::Ongoing) => true,
            (TxnState::CompleteAbort, TxnState::Dead) => true,

            // From Dead (terminal)
            (TxnState::Dead, _) => false,

            // From PrepareEpochFence
            (TxnState::PrepareEpochFence, TxnState::Dead) => true,

            // Same state is always valid
            (a, b) if a == b => true,

            // All other transitions are invalid
            _ => false,
        }
    }

    /// Update last timestamp
    fn update_timestamp(&mut self) {
        self.metadata.txn_last_update_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
    }
}

/// Transaction state machine builder for testing
#[cfg(test)]
pub struct TransactionStateMachineBuilder {
    transactional_id: String,
    producer_id: ProducerId,
    producer_epoch: i16,
    transaction_timeout_ms: i32,
    initial_state: TxnState,
}

#[cfg(test)]
impl TransactionStateMachineBuilder {
    pub fn new() -> Self {
        Self {
            transactional_id: "test-txn".to_string(),
            producer_id: 1,
            producer_epoch: 0,
            transaction_timeout_ms: 60000,
            initial_state: TxnState::Empty,
        }
    }

    pub fn with_transactional_id(mut self, id: String) -> Self {
        self.transactional_id = id;
        self
    }

    pub fn with_producer_id(mut self, id: ProducerId) -> Self {
        self.producer_id = id;
        self
    }

    pub fn with_initial_state(mut self, state: TxnState) -> Self {
        self.initial_state = state;
        self
    }

    pub fn build(self) -> TransactionStateMachine {
        let mut sm = TransactionStateMachine::new(
            self.transactional_id,
            self.producer_id,
            self.producer_epoch,
            self.transaction_timeout_ms,
        );
        sm.metadata.state = self.initial_state;
        sm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let mut sm = TransactionStateMachine::new("test-txn".to_string(), 1, 0, 60000);

        // Initial state should be Empty
        assert_eq!(sm.current_state(), TxnState::Empty);

        // Begin transaction
        sm.begin_transaction().unwrap();
        assert_eq!(sm.current_state(), TxnState::Ongoing);

        // Add partitions
        let partitions = vec![
            TopicPartition::new("topic1".to_string(), 0),
            TopicPartition::new("topic1".to_string(), 1),
        ];
        sm.add_partitions(partitions).unwrap();
        assert_eq!(sm.partitions().len(), 2);

        // Prepare commit
        sm.prepare_commit().unwrap();
        assert_eq!(sm.current_state(), TxnState::PrepareCommit);

        // Complete commit
        sm.complete_commit().unwrap();
        assert_eq!(sm.current_state(), TxnState::CompleteCommit);

        // Reset
        sm.reset().unwrap();
        assert_eq!(sm.current_state(), TxnState::Empty);
        assert_eq!(sm.partitions().len(), 0);
    }

    #[test]
    fn test_transaction_abort() {
        let mut sm = TransactionStateMachine::new("test-txn".to_string(), 1, 0, 60000);

        sm.begin_transaction().unwrap();
        sm.prepare_abort().unwrap();
        assert_eq!(sm.current_state(), TxnState::PrepareAbort);

        sm.complete_abort().unwrap();
        assert_eq!(sm.current_state(), TxnState::CompleteAbort);
    }

    #[test]
    fn test_invalid_state_transitions() {
        let mut sm = TransactionStateMachine::new("test-txn".to_string(), 1, 0, 60000);

        // Cannot add partitions when empty
        let partitions = vec![TopicPartition::new("topic1".to_string(), 0)];
        assert!(sm.add_partitions(partitions).is_err());

        // Cannot prepare commit when empty
        assert!(sm.prepare_commit().is_err());

        // Cannot complete commit without prepare
        assert!(sm.complete_commit().is_err());
    }

    #[test]
    fn test_producer_fencing() {
        let mut sm = TransactionStateMachine::new("test-txn".to_string(), 1, 0, 60000);

        let initial_epoch = sm.producer_epoch();
        sm.fence_producer().unwrap();

        assert_eq!(sm.producer_epoch(), initial_epoch + 1);
        assert_eq!(sm.current_state(), TxnState::Dead);
        assert!(sm.is_terminal());
    }

    #[test]
    fn test_epoch_validation() {
        let sm = TransactionStateMachine::new("test-txn".to_string(), 1, 5, 60000);

        assert!(sm.validate_epoch(5).is_ok());
        assert!(sm.validate_epoch(4).is_err());
        assert!(sm.validate_epoch(6).is_err());
    }

    #[test]
    fn test_duplicate_partitions() {
        let mut sm = TransactionStateMachine::new("test-txn".to_string(), 1, 0, 60000);

        sm.begin_transaction().unwrap();

        let partitions = vec![
            TopicPartition::new("topic1".to_string(), 0),
            TopicPartition::new("topic1".to_string(), 0), // duplicate
        ];

        sm.add_partitions(partitions).unwrap();
        assert_eq!(sm.partitions().len(), 1);
    }

    #[test]
    fn test_builder_pattern() {
        let sm = TransactionStateMachineBuilder::new()
            .with_transactional_id("custom-txn".to_string())
            .with_producer_id(42)
            .with_initial_state(TxnState::Ongoing)
            .build();

        assert_eq!(sm.transactional_id(), "custom-txn");
        assert_eq!(sm.producer_id(), 42);
        assert_eq!(sm.current_state(), TxnState::Ongoing);
    }
}
