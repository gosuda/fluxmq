//! Producer ID Management Module
//!
//! This module manages producer IDs and epochs for transactional producers.
//! It ensures unique producer identification and proper epoch management for exactly-once semantics.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{TransactionError, TransactionResult, TxnState};

/// Unique producer identifier
pub type ProducerId = i64;

/// Producer metadata including epoch and transaction state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerMetadata {
    /// Transactional ID (nullable for non-transactional producers)
    pub transactional_id: Option<String>,
    /// Unique producer ID
    pub producer_id: ProducerId,
    /// Current producer epoch
    pub producer_epoch: i16,
    /// Last assigned timestamp
    pub last_update_timestamp: i64,
    /// Current transaction state
    pub txn_state: TxnState,
    /// Transaction timeout in milliseconds
    pub transaction_timeout_ms: i32,
    /// Whether this producer supports idempotence
    pub enable_idempotence: bool,
}

impl ProducerMetadata {
    /// Create new producer metadata
    pub fn new(
        transactional_id: Option<String>,
        producer_id: ProducerId,
        transaction_timeout_ms: i32,
    ) -> Self {
        Self {
            transactional_id,
            producer_id,
            producer_epoch: 0,
            last_update_timestamp: chrono::Utc::now().timestamp_millis(),
            txn_state: TxnState::Empty,
            transaction_timeout_ms,
            enable_idempotence: true,
        }
    }

    /// Increment producer epoch (used when producer is fenced)
    pub fn increment_epoch(&mut self) -> TransactionResult<()> {
        if self.producer_epoch == i16::MAX {
            return Err(TransactionError::InvalidProducerEpoch {
                expected: self.producer_epoch,
                actual: i16::MAX,
            });
        }

        self.producer_epoch += 1;
        self.last_update_timestamp = chrono::Utc::now().timestamp_millis();
        self.txn_state = TxnState::Empty;

        debug!(
            producer_id = self.producer_id,
            new_epoch = self.producer_epoch,
            "Producer epoch incremented"
        );

        Ok(())
    }

    /// Check if producer epoch is valid
    pub fn validate_epoch(&self, epoch: i16) -> bool {
        self.producer_epoch == epoch
    }

    /// Update transaction state
    pub fn update_state(&mut self, new_state: TxnState) {
        debug!(
            producer_id = self.producer_id,
            old_state = ?self.txn_state,
            new_state = ?new_state,
            "Producer transaction state updated"
        );

        self.txn_state = new_state;
        self.last_update_timestamp = chrono::Utc::now().timestamp_millis();
    }

    /// Check if transaction has timed out
    pub fn is_transaction_expired(&self) -> bool {
        if self.txn_state == TxnState::Empty {
            return false;
        }

        let now = chrono::Utc::now().timestamp_millis();
        let elapsed = now - self.last_update_timestamp;
        elapsed > self.transaction_timeout_ms as i64
    }
}

/// Producer ID manager for allocating and tracking producer IDs
#[derive(Debug)]
pub struct ProducerIdManager {
    /// Next producer ID to allocate
    next_producer_id: AtomicI64,
    /// Producer metadata by producer ID using RwLock (cold path - infrequent access)
    producers_by_id: Arc<RwLock<HashMap<ProducerId, ProducerMetadata>>>,
    /// Producer ID lookup by transactional ID using RwLock (cold path - infrequent access)
    producers_by_txn_id: Arc<RwLock<HashMap<String, ProducerId>>>,
    /// Producer ID allocation range
    producer_id_range: (i64, i64),
}

impl ProducerIdManager {
    /// Create new producer ID manager
    pub fn new() -> Self {
        Self {
            next_producer_id: AtomicI64::new(1),
            producers_by_id: Arc::new(RwLock::new(HashMap::new())),
            producers_by_txn_id: Arc::new(RwLock::new(HashMap::new())),
            producer_id_range: (1, i64::MAX),
        }
    }

    /// Create producer ID manager with custom range
    pub fn with_range(start: i64, end: i64) -> Self {
        Self {
            next_producer_id: AtomicI64::new(start),
            producers_by_id: Arc::new(RwLock::new(HashMap::new())),
            producers_by_txn_id: Arc::new(RwLock::new(HashMap::new())),
            producer_id_range: (start, end),
        }
    }

    /// Initialize or get producer ID for transactional producer
    pub async fn init_producer_id(
        &self,
        transactional_id: Option<String>,
        transaction_timeout_ms: i32,
        current_producer_id: Option<ProducerId>,
        current_producer_epoch: Option<i16>,
    ) -> TransactionResult<(ProducerId, i16)> {
        match &transactional_id {
            Some(txn_id) => {
                self.init_transactional_producer(
                    txn_id,
                    transaction_timeout_ms,
                    current_producer_id,
                    current_producer_epoch,
                )
                .await
            }
            None => self.init_idempotent_producer().await,
        }
    }

    /// Initialize transactional producer with transactional ID
    async fn init_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        current_producer_id: Option<ProducerId>,
        current_producer_epoch: Option<i16>,
    ) -> TransactionResult<(ProducerId, i16)> {
        // Check if transactional ID already exists
        let existing_producer_id = self
            .producers_by_txn_id
            .read()
            .get(transactional_id)
            .copied();

        if let Some(existing_producer_id) = existing_producer_id {
            let mut producers = self.producers_by_id.write();
            if let Some(metadata) = producers.get_mut(&existing_producer_id) {
                // Validate current producer ID and epoch if provided
                if let (Some(pid), Some(epoch)) = (current_producer_id, current_producer_epoch) {
                    if pid == existing_producer_id && metadata.validate_epoch(epoch) {
                        // Same producer, return current ID and epoch
                        info!(
                            producer_id = existing_producer_id,
                            epoch = metadata.producer_epoch,
                            transactional_id = transactional_id,
                            "Returning existing producer ID"
                        );
                        return Ok((existing_producer_id, metadata.producer_epoch));
                    }
                }

                // Fence the existing producer by incrementing epoch
                metadata.increment_epoch()?;
                metadata.transaction_timeout_ms = transaction_timeout_ms;

                info!(
                    producer_id = existing_producer_id,
                    new_epoch = metadata.producer_epoch,
                    transactional_id = transactional_id,
                    "Producer fenced and epoch incremented"
                );

                return Ok((existing_producer_id, metadata.producer_epoch));
            }
        }

        // Allocate new producer ID for this transactional ID
        let producer_id = self.allocate_producer_id();
        let metadata = ProducerMetadata::new(
            Some(transactional_id.to_string()),
            producer_id,
            transaction_timeout_ms,
        );

        self.producers_by_id.write().insert(producer_id, metadata);
        self.producers_by_txn_id
            .write()
            .insert(transactional_id.to_string(), producer_id);

        info!(
            producer_id = producer_id,
            transactional_id = transactional_id,
            "New transactional producer ID allocated"
        );

        Ok((producer_id, 0))
    }

    /// Initialize idempotent (non-transactional) producer
    async fn init_idempotent_producer(&self) -> TransactionResult<(ProducerId, i16)> {
        let producer_id = self.allocate_producer_id();
        let metadata = ProducerMetadata::new(None, producer_id, 0);

        self.producers_by_id.write().insert(producer_id, metadata);

        info!(
            producer_id = producer_id,
            "New idempotent producer ID allocated"
        );

        Ok((producer_id, 0))
    }

    /// Get producer metadata by ID
    pub async fn get_producer_metadata(&self, producer_id: ProducerId) -> Option<ProducerMetadata> {
        self.producers_by_id.read().get(&producer_id).cloned()
    }

    /// Update producer transaction state
    pub async fn update_producer_state(
        &self,
        producer_id: ProducerId,
        new_state: TxnState,
    ) -> TransactionResult<()> {
        if let Some(metadata) = self.producers_by_id.write().get_mut(&producer_id) {
            metadata.update_state(new_state);
            Ok(())
        } else {
            Err(TransactionError::ProducerIdNotFound(producer_id))
        }
    }

    /// Validate producer ID and epoch
    pub async fn validate_producer(
        &self,
        producer_id: ProducerId,
        producer_epoch: i16,
    ) -> TransactionResult<()> {
        let producers = self.producers_by_id.read();
        if let Some(metadata) = producers.get(&producer_id) {
            if metadata.validate_epoch(producer_epoch) {
                Ok(())
            } else {
                Err(TransactionError::InvalidProducerEpoch {
                    expected: metadata.producer_epoch,
                    actual: producer_epoch,
                })
            }
        } else {
            Err(TransactionError::ProducerIdNotFound(producer_id))
        }
    }

    /// Remove expired producers (cleanup task)
    pub async fn cleanup_expired_producers(&self) -> usize {
        let expired_producers: Vec<_> = {
            let producers = self.producers_by_id.read();
            producers
                .iter()
                .filter(|(_, metadata)| {
                    metadata.txn_state.is_terminal() || metadata.is_transaction_expired()
                })
                .map(|(id, metadata)| (*id, metadata.transactional_id.clone()))
                .collect()
        };

        {
            let mut producers = self.producers_by_id.write();
            let mut txn_ids = self.producers_by_txn_id.write();
            for (producer_id, transactional_id) in &expired_producers {
                producers.remove(producer_id);
                if let Some(txn_id) = transactional_id {
                    txn_ids.remove(txn_id);
                }
            }
        }

        let count = expired_producers.len();
        if count > 0 {
            info!(cleaned_up = count, "Cleaned up expired producers");
        }

        count
    }

    /// Get producer statistics
    pub async fn get_stats(&self) -> ProducerIdStats {
        let producers = self.producers_by_id.read();
        let mut state_counts = HashMap::new();
        let mut transactional_count = 0;
        let mut idempotent_count = 0;

        for metadata in producers.values() {
            *state_counts.entry(metadata.txn_state).or_insert(0) += 1;

            if metadata.transactional_id.is_some() {
                transactional_count += 1;
            } else {
                idempotent_count += 1;
            }
        }

        ProducerIdStats {
            total_producers: producers.len(),
            transactional_producers: transactional_count,
            idempotent_producers: idempotent_count,
            next_producer_id: self.next_producer_id.load(Ordering::Relaxed),
            state_counts,
        }
    }

    /// Allocate next available producer ID
    fn allocate_producer_id(&self) -> ProducerId {
        let producer_id = self.next_producer_id.fetch_add(1, Ordering::SeqCst);

        if producer_id >= self.producer_id_range.1 {
            warn!(
                current_id = producer_id,
                max_id = self.producer_id_range.1,
                "Producer ID approaching maximum value"
            );
        }

        producer_id
    }
}

impl Default for ProducerIdManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Producer ID manager statistics
#[derive(Debug, Clone)]
pub struct ProducerIdStats {
    pub total_producers: usize,
    pub transactional_producers: usize,
    pub idempotent_producers: usize,
    pub next_producer_id: i64,
    pub state_counts: HashMap<TxnState, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init_transactional_producer() {
        let manager = ProducerIdManager::new();

        // First initialization
        let (producer_id, epoch) = manager
            .init_producer_id(Some("test-txn".to_string()), 60000, None, None)
            .await
            .unwrap();

        assert_eq!(producer_id, 1);
        assert_eq!(epoch, 0);

        // Second initialization with same transactional ID should fence
        let (producer_id2, epoch2) = manager
            .init_producer_id(Some("test-txn".to_string()), 60000, None, None)
            .await
            .unwrap();

        assert_eq!(producer_id2, producer_id);
        assert_eq!(epoch2, 1);
    }

    #[tokio::test]
    async fn test_init_idempotent_producer() {
        let manager = ProducerIdManager::new();

        let (producer_id1, epoch1) = manager.init_producer_id(None, 0, None, None).await.unwrap();

        let (producer_id2, epoch2) = manager.init_producer_id(None, 0, None, None).await.unwrap();

        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 0);
        assert_ne!(producer_id1, producer_id2);
    }

    #[tokio::test]
    async fn test_validate_producer() {
        let manager = ProducerIdManager::new();

        let (producer_id, epoch) = manager
            .init_producer_id(Some("test-txn".to_string()), 60000, None, None)
            .await
            .unwrap();

        // Valid producer and epoch
        assert!(manager.validate_producer(producer_id, epoch).await.is_ok());

        // Invalid epoch
        assert!(manager
            .validate_producer(producer_id, epoch + 1)
            .await
            .is_err());

        // Invalid producer ID
        assert!(manager
            .validate_producer(producer_id + 100, epoch)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_producer_state_management() {
        let manager = ProducerIdManager::new();

        let (producer_id, _) = manager
            .init_producer_id(Some("test-txn".to_string()), 60000, None, None)
            .await
            .unwrap();

        // Update state
        manager
            .update_producer_state(producer_id, TxnState::Ongoing)
            .await
            .unwrap();

        let metadata = manager.get_producer_metadata(producer_id).await.unwrap();
        assert_eq!(metadata.txn_state, TxnState::Ongoing);
    }

    #[tokio::test]
    async fn test_producer_id_allocation() {
        let manager = ProducerIdManager::new();

        let mut producer_ids = Vec::new();
        for _i in 0..10 {
            let (producer_id, _) = manager.init_producer_id(None, 0, None, None).await.unwrap();
            producer_ids.push(producer_id);
        }

        // All producer IDs should be unique and sequential
        for i in 0..10 {
            assert_eq!(producer_ids[i], (i + 1) as i64);
        }
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = ProducerIdManager::new();

        // Create some producers
        manager
            .init_producer_id(Some("txn1".to_string()), 60000, None, None)
            .await
            .unwrap();
        manager
            .init_producer_id(Some("txn2".to_string()), 60000, None, None)
            .await
            .unwrap();
        manager.init_producer_id(None, 0, None, None).await.unwrap();

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_producers, 3);
        assert_eq!(stats.transactional_producers, 2);
        assert_eq!(stats.idempotent_producers, 1);
        assert_eq!(stats.next_producer_id, 4);
    }
}
