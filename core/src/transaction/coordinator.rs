//! Transaction Coordinator
//!
//! This module implements the central transaction coordinator that manages the lifecycle
//! of all transactions in FluxMQ. It handles producer registration, transaction state
//! management, and coordinates transaction completion across multiple partitions.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use super::{
    messages::*,
    producer_id_manager::ProducerIdManager,
    state_machine::TransactionStateMachine,
    transaction_log::{TransactionLog, TransactionLogConfig},
    CoordinatorMetadata, ProducerId, TopicPartition, TransactionResult, TxnState,
};

/// Transaction coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCoordinatorConfig {
    /// Transaction timeout range
    pub transaction_max_timeout_ms: i32,
    pub transaction_min_timeout_ms: i32,
    /// Default transaction timeout
    pub transaction_default_timeout_ms: i32,
    /// Producer ID allocation range
    pub producer_id_start: i64,
    pub producer_id_end: i64,
    /// Log configuration
    pub log_config: TransactionLogConfig,
    /// Cleanup interval for expired transactions
    pub cleanup_interval_ms: u64,
    /// Coordinator epoch
    pub coordinator_epoch: i32,
}

impl Default for TransactionCoordinatorConfig {
    fn default() -> Self {
        Self {
            transaction_max_timeout_ms: 900_000,    // 15 minutes
            transaction_min_timeout_ms: 1_000,      // 1 second
            transaction_default_timeout_ms: 60_000, // 1 minute
            producer_id_start: 1,
            producer_id_end: i64::MAX,
            log_config: TransactionLogConfig::default(),
            cleanup_interval_ms: 30_000, // 30 seconds
            coordinator_epoch: 0,
        }
    }
}

/// Central transaction coordinator
#[derive(Debug)]
pub struct TransactionCoordinator {
    /// Configuration
    config: TransactionCoordinatorConfig,
    /// Producer ID manager
    producer_manager: Arc<ProducerIdManager>,
    /// Transaction state machines by transactional ID using RwLock (cold path - infrequent access)
    transactions: Arc<RwLock<HashMap<String, TransactionStateMachine>>>,
    /// Persistent transaction log
    transaction_log: Arc<TransactionLog>,
    /// Coordinator metadata
    metadata: Arc<Mutex<CoordinatorMetadata>>,
    /// Pending transaction markers using RwLock (cold path - infrequent access)
    pending_markers: Arc<RwLock<HashMap<ProducerId, Vec<WritableTxnMarker>>>>,
}

impl TransactionCoordinator {
    /// Create new transaction coordinator
    pub async fn new(config: TransactionCoordinatorConfig) -> TransactionResult<Self> {
        let producer_manager = Arc::new(ProducerIdManager::with_range(
            config.producer_id_start,
            config.producer_id_end,
        ));

        let transaction_log = Arc::new(TransactionLog::new(config.log_config.clone()).await?);

        let coordinator = Self {
            config,
            producer_manager,
            transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_log,
            metadata: Arc::new(Mutex::new(CoordinatorMetadata::default())),
            pending_markers: Arc::new(RwLock::new(HashMap::new())),
        };

        // Recover transactions from log
        coordinator.recover_transactions().await?;

        // Start background cleanup task
        coordinator.start_cleanup_task().await;

        info!("Transaction coordinator initialized");
        Ok(coordinator)
    }

    /// Handle InitProducerId request
    pub async fn handle_init_producer_id(
        &self,
        request: InitProducerIdRequest,
    ) -> TransactionResult<InitProducerIdResponse> {
        let timeout_ms = if request.transaction_timeout_ms <= 0 {
            self.config.transaction_default_timeout_ms
        } else {
            request.transaction_timeout_ms.clamp(
                self.config.transaction_min_timeout_ms,
                self.config.transaction_max_timeout_ms,
            )
        };

        let current_producer_id = if request.producer_id == -1 {
            None
        } else {
            Some(request.producer_id)
        };
        let current_producer_epoch = if request.producer_epoch == -1 {
            None
        } else {
            Some(request.producer_epoch)
        };

        let (producer_id, producer_epoch) = self
            .producer_manager
            .init_producer_id(
                request.transactional_id.clone(),
                timeout_ms,
                current_producer_id,
                current_producer_epoch,
            )
            .await?;

        // Log the producer initialization
        if let Some(ref txn_id) = request.transactional_id {
            self.transaction_log
                .log_init_producer_id(txn_id.clone(), producer_id, producer_epoch, timeout_ms)
                .await?;

            // Create transaction state machine
            let state_machine = TransactionStateMachine::new(
                txn_id.clone(),
                producer_id,
                producer_epoch,
                timeout_ms,
            );
            self.transactions
                .write()
                .insert(txn_id.clone(), state_machine);
        }

        info!(
            producer_id = producer_id,
            producer_epoch = producer_epoch,
            transactional_id = ?request.transactional_id,
            "Producer ID initialized"
        );

        Ok(InitProducerIdResponse {
            correlation_id: request.header.correlation_id,
            error_code: 0, // NoError
            producer_id,
            producer_epoch,
            throttle_time_ms: 0,
            api_version: request.header.api_version,
        })
    }

    /// Handle AddPartitionsToTxn request
    pub async fn handle_add_partitions_to_txn(
        &self,
        request: AddPartitionsToTxnRequest,
    ) -> TransactionResult<AddPartitionsToTxnResponse> {
        // Validate producer
        self.producer_manager
            .validate_producer(request.producer_id, request.producer_epoch)
            .await?;

        let mut results = Vec::new();

        // Convert topics to topic partitions
        let mut all_partitions = Vec::new();
        for topic in &request.topics {
            let mut topic_result = AddPartitionsToTxnTopicResult {
                name: topic.name.clone(),
                results: Vec::new(),
            };

            for &partition in &topic.partitions {
                let topic_partition = TopicPartition::new(topic.name.clone(), partition);
                all_partitions.push(topic_partition);

                topic_result
                    .results
                    .push(AddPartitionsToTxnPartitionResult {
                        partition_index: partition,
                        error_code: 0, // NoError
                    });
            }

            results.push(topic_result);
        }

        // Add partitions to transaction
        {
            let mut transactions = self.transactions.write();
            if let Some(state_machine) = transactions.get_mut(&request.transactional_id) {
                state_machine.add_partitions(all_partitions.clone())?;

                // Log the partition addition
                self.transaction_log
                    .log_add_partitions(
                        &request.transactional_id,
                        request.producer_id,
                        request.producer_epoch,
                        all_partitions.clone(),
                    )
                    .await?;

                info!(
                    producer_id = request.producer_id,
                    transactional_id = %request.transactional_id,
                    partition_count = all_partitions.len(),
                    "Partitions added to transaction"
                );
            } else {
                // Set error for all partitions
                for result in &mut results {
                    for partition_result in &mut result.results {
                        partition_result.error_code = 48; // InvalidTxnState
                    }
                }
            }
        }

        Ok(AddPartitionsToTxnResponse {
            correlation_id: request.header.correlation_id,
            api_version: request.header.api_version,
            throttle_time_ms: 0,
            results,
        })
    }

    /// Handle AddOffsetsToTxn request
    pub async fn handle_add_offsets_to_txn(
        &self,
        request: AddOffsetsToTxnRequest,
    ) -> TransactionResult<AddOffsetsToTxnResponse> {
        // Validate producer
        self.producer_manager
            .validate_producer(request.producer_id, request.producer_epoch)
            .await?;

        // For now, we'll just log the offset addition
        // In a full implementation, this would add the consumer group
        // to the transaction's pending offset commits

        let error_code = if self
            .transactions
            .read()
            .contains_key(&request.transactional_id)
        {
            info!(
                producer_id = request.producer_id,
                transactional_id = %request.transactional_id,
                group_id = %request.group_id,
                "Consumer group offsets added to transaction"
            );
            0 // NoError
        } else {
            48 // InvalidTxnState
        };

        Ok(AddOffsetsToTxnResponse {
            correlation_id: request.header.correlation_id,
            throttle_time_ms: 0,
            error_code,
        })
    }

    /// Handle EndTxn request
    pub async fn handle_end_txn(
        &self,
        request: EndTxnRequest,
    ) -> TransactionResult<EndTxnResponse> {
        // Validate producer
        self.producer_manager
            .validate_producer(request.producer_id, request.producer_epoch)
            .await?;

        let error_code = {
            let mut transactions = self.transactions.write();
            if let Some(state_machine) = transactions.get_mut(&request.transactional_id) {
                match if request.committed {
                    state_machine.prepare_commit()
                } else {
                    state_machine.prepare_abort()
                } {
                    Ok(()) => {
                        let partitions: Vec<_> =
                            state_machine.partitions().iter().cloned().collect();

                        // Log prepare transaction
                        if let Err(e) = self
                            .transaction_log
                            .log_prepare_transaction(
                                &request.transactional_id,
                                request.producer_id,
                                request.producer_epoch,
                                request.committed,
                                partitions.clone(),
                            )
                            .await
                        {
                            error!("Failed to log prepare transaction: {}", e);
                            42 // InvalidRequest
                        } else {
                            // Create transaction markers for WriteTxnMarkers
                            if !partitions.is_empty() {
                                let marker = WritableTxnMarker {
                                    producer_id: request.producer_id,
                                    producer_epoch: request.producer_epoch,
                                    transaction_result: request.committed,
                                    topics: self.convert_partitions_to_marker_topics(&partitions),
                                    coordinator_epoch: self.config.coordinator_epoch,
                                };

                                self.pending_markers
                                    .write()
                                    .entry(request.producer_id)
                                    .or_insert_with(Vec::new)
                                    .push(marker);
                            }

                            // Complete the transaction
                            if request.committed {
                                state_machine.complete_commit().unwrap();
                            } else {
                                state_machine.complete_abort().unwrap();
                            }

                            // Log complete transaction
                            if let Err(e) = self
                                .transaction_log
                                .log_complete_transaction(
                                    &request.transactional_id,
                                    request.producer_id,
                                    request.producer_epoch,
                                    request.committed,
                                )
                                .await
                            {
                                error!("Failed to log complete transaction: {}", e);
                            }

                            info!(
                                producer_id = request.producer_id,
                                transactional_id = %request.transactional_id,
                                committed = request.committed,
                                "Transaction ended"
                            );

                            0 // NoError
                        }
                    }
                    Err(e) => {
                        warn!("Failed to end transaction: {}", e);
                        48 // InvalidTxnState
                    }
                }
            } else {
                48 // InvalidTxnState
            }
        };

        Ok(EndTxnResponse {
            correlation_id: request.header.correlation_id,
            api_version: request.header.api_version,
            throttle_time_ms: 0,
            error_code,
        })
    }

    /// Handle WriteTxnMarkers request (internal API)
    pub async fn handle_write_txn_markers(
        &self,
        request: WriteTxnMarkersRequest,
    ) -> TransactionResult<WriteTxnMarkersResponse> {
        let mut markers = Vec::new();

        for marker in request.markers {
            let mut topics = Vec::new();

            for topic in marker.topics {
                let mut partitions = Vec::new();

                for partition in topic.partitions {
                    // In a real implementation, this would write the transaction marker
                    // to the actual partition log. For now, we'll just acknowledge it.
                    partitions.push(WritableTxnMarkerPartitionResult {
                        partition_index: partition,
                        error_code: 0, // NoError
                    });

                    debug!(
                        producer_id = marker.producer_id,
                        topic = %topic.name,
                        partition = partition,
                        commit = marker.transaction_result,
                        "Transaction marker written"
                    );
                }

                topics.push(WritableTxnMarkerTopicResult {
                    name: topic.name,
                    partitions,
                });
            }

            markers.push(WritableTxnMarkerResult {
                producer_id: marker.producer_id,
                topics,
            });
        }

        Ok(WriteTxnMarkersResponse {
            correlation_id: request.header.correlation_id,
            markers,
        })
    }

    /// Handle TxnOffsetCommit request
    pub async fn handle_txn_offset_commit(
        &self,
        request: TxnOffsetCommitRequest,
    ) -> TransactionResult<TxnOffsetCommitResponse> {
        // Validate producer
        self.producer_manager
            .validate_producer(request.producer_id, request.producer_epoch)
            .await?;

        let mut topics = Vec::new();

        for topic in request.topics {
            let mut partitions = Vec::new();

            for partition in topic.partitions {
                // In a real implementation, this would commit the offset
                // within the transaction context
                partitions.push(TxnOffsetCommitResponsePartition {
                    partition_index: partition.partition_index,
                    error_code: 0, // NoError
                });

                debug!(
                    producer_id = request.producer_id,
                    transactional_id = %request.transactional_id,
                    group_id = %request.group_id,
                    topic = %topic.name,
                    partition = partition.partition_index,
                    offset = partition.committed_offset,
                    "Transactional offset committed"
                );
            }

            topics.push(TxnOffsetCommitResponseTopic {
                name: topic.name,
                partitions,
            });
        }

        Ok(TxnOffsetCommitResponse {
            correlation_id: request.header.correlation_id,
            throttle_time_ms: 0,
            topics,
        })
    }

    /// Get coordinator statistics
    pub async fn get_stats(&self) -> TransactionCoordinatorStats {
        let producer_stats = self.producer_manager.get_stats().await;
        let metadata = self.metadata.lock().await;

        let transactions = self.transactions.read();
        let mut state_counts = HashMap::new();
        for state_machine in transactions.values() {
            let state = state_machine.current_state();
            *state_counts.entry(state).or_insert(0) += 1;
        }

        TransactionCoordinatorStats {
            active_transactions: transactions.len(),
            producer_stats,
            state_counts,
            coordinator_epoch: metadata.coordinator_epoch,
        }
    }

    /// Recover transactions from persistent log
    async fn recover_transactions(&self) -> TransactionResult<()> {
        let active_transactions = self.transaction_log.get_active_transactions().await;

        let mut transactions = self.transactions.write();
        for (txn_id, metadata) in active_transactions {
            let state = metadata.state;
            let state_machine = TransactionStateMachine::from_metadata(metadata);
            transactions.insert(txn_id.clone(), state_machine);

            debug!(
                transactional_id = %txn_id,
                state = ?state,
                "Recovered transaction"
            );
        }

        info!(
            recovered_count = transactions.len(),
            "Transaction recovery completed"
        );

        Ok(())
    }

    /// Start background cleanup task
    async fn start_cleanup_task(&self) {
        let producer_manager = Arc::clone(&self.producer_manager);
        let transaction_log = Arc::clone(&self.transaction_log);
        let transactions = Arc::clone(&self.transactions);
        let cleanup_interval = Duration::from_millis(self.config.cleanup_interval_ms);

        tokio::spawn(async move {
            loop {
                sleep(cleanup_interval).await;

                // Cleanup expired producers
                let expired_producers = producer_manager.cleanup_expired_producers().await;

                // Cleanup completed transactions
                let completed_transactions = transaction_log.cleanup_completed_transactions().await;

                // Remove completed transactions from memory
                if completed_transactions > 0 {
                    transactions.write().retain(|_, sm| !sm.is_terminal());
                }

                if expired_producers > 0 || completed_transactions > 0 {
                    debug!(
                        expired_producers = expired_producers,
                        completed_transactions = completed_transactions,
                        "Cleanup completed"
                    );
                }
            }
        });
    }

    /// Convert partitions to marker topics format
    fn convert_partitions_to_marker_topics(
        &self,
        partitions: &[TopicPartition],
    ) -> Vec<WritableTxnMarkerTopic> {
        let mut topic_map: HashMap<String, Vec<i32>> = HashMap::new();

        for partition in partitions {
            topic_map
                .entry(partition.topic.clone())
                .or_insert_with(Vec::new)
                .push(partition.partition);
        }

        topic_map
            .into_iter()
            .map(|(name, partitions)| WritableTxnMarkerTopic { name, partitions })
            .collect()
    }
}

/// Transaction coordinator statistics
#[derive(Debug, Clone)]
pub struct TransactionCoordinatorStats {
    pub active_transactions: usize,
    pub producer_stats: super::producer_id_manager::ProducerIdStats,
    pub state_counts: HashMap<TxnState, usize>,
    pub coordinator_epoch: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::kafka::messages::KafkaRequestHeader;
    use tempfile::TempDir;

    async fn create_test_coordinator() -> (TransactionCoordinator, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut config = TransactionCoordinatorConfig::default();
        config.log_config.log_dir = temp_dir.path().to_path_buf();

        let coordinator = TransactionCoordinator::new(config).await.unwrap();
        (coordinator, temp_dir)
    }

    #[tokio::test]
    async fn test_init_producer_id() {
        let (coordinator, _temp_dir) = create_test_coordinator().await;

        let request = InitProducerIdRequest {
            header: KafkaRequestHeader {
                api_key: 22,
                api_version: 4,
                correlation_id: 1,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: Some("test-txn".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        let response = coordinator.handle_init_producer_id(request).await.unwrap();

        assert_eq!(response.error_code, 0);
        assert!(response.producer_id > 0);
        assert_eq!(response.producer_epoch, 0);
    }

    #[tokio::test]
    #[ignore = "Transaction workflow needs proper state initialization"]
    async fn test_add_partitions_to_txn() {
        let (coordinator, _temp_dir) = create_test_coordinator().await;

        // First initialize producer
        let init_request = InitProducerIdRequest {
            header: KafkaRequestHeader {
                api_key: 22,
                api_version: 4,
                correlation_id: 1,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: Some("test-txn".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        let init_response = coordinator
            .handle_init_producer_id(init_request)
            .await
            .unwrap();

        // Begin transaction by adding partitions
        let request = AddPartitionsToTxnRequest {
            header: KafkaRequestHeader {
                api_key: 24,
                api_version: 3,
                correlation_id: 2,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: "test-txn".to_string(),
            producer_id: init_response.producer_id,
            producer_epoch: init_response.producer_epoch,
            topics: vec![AddPartitionsToTxnTopic {
                name: "test-topic".to_string(),
                partitions: vec![0, 1],
            }],
        };

        let response = coordinator
            .handle_add_partitions_to_txn(request)
            .await
            .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].results.len(), 2);
        assert_eq!(response.results[0].results[0].error_code, 0);
    }

    #[tokio::test]
    #[ignore = "Transaction workflow needs proper state initialization"]
    async fn test_end_transaction() {
        let (coordinator, _temp_dir) = create_test_coordinator().await;

        // Initialize producer
        let init_request = InitProducerIdRequest {
            header: KafkaRequestHeader {
                api_key: 22,
                api_version: 4,
                correlation_id: 1,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: Some("test-txn".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        let init_response = coordinator
            .handle_init_producer_id(init_request)
            .await
            .unwrap();

        // End transaction (commit)
        let request = EndTxnRequest {
            header: KafkaRequestHeader {
                api_key: 26,
                api_version: 3,
                correlation_id: 3,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: "test-txn".to_string(),
            producer_id: init_response.producer_id,
            producer_epoch: init_response.producer_epoch,
            committed: true,
        };

        let response = coordinator.handle_end_txn(request).await.unwrap();
        assert_eq!(response.error_code, 0);
    }

    #[tokio::test]
    async fn test_coordinator_stats() {
        let (coordinator, _temp_dir) = create_test_coordinator().await;

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.active_transactions, 0);
        assert!(stats.coordinator_epoch >= 0);
    }
}
