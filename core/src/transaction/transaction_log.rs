//! Transaction Log Module
//!
//! This module implements persistent transaction logging for crash recovery and
//! distributed coordination. It maintains the authoritative state of all transactions
//! and enables exactly-once guarantees across broker restarts.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, info, warn};

use super::{ProducerId, TopicPartition, TransactionMetadata, TransactionResult, TxnState};

/// Transaction log entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionLogEntry {
    /// Producer initialization or epoch bump
    InitProducerId {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        transaction_timeout_ms: i32,
        timestamp: i64,
    },
    /// Transaction begun
    BeginTransaction {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        timestamp: i64,
    },
    /// Partitions added to transaction
    AddPartitions {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        partitions: Vec<TopicPartition>,
        timestamp: i64,
    },
    /// Consumer group offsets added to transaction
    AddOffsets {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        group_id: String,
        timestamp: i64,
    },
    /// Transaction prepare phase (commit or abort)
    PrepareTransaction {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
        partitions: Vec<TopicPartition>,
        timestamp: i64,
    },
    /// Transaction completion (commit or abort)
    CompleteTransaction {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
        timestamp: i64,
    },
    /// Producer fenced
    FenceProducer {
        transactional_id: String,
        old_producer_id: ProducerId,
        old_producer_epoch: i16,
        new_producer_epoch: i16,
        timestamp: i64,
    },
    /// Pending transaction markers to be written (persisted for crash recovery)
    PendingMarkers {
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
        /// (topic_name, partition_ids)
        marker_topics: Vec<(String, Vec<i32>)>,
        timestamp: i64,
    },
}

impl TransactionLogEntry {
    /// Get timestamp of the log entry
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::InitProducerId { timestamp, .. } => *timestamp,
            Self::BeginTransaction { timestamp, .. } => *timestamp,
            Self::AddPartitions { timestamp, .. } => *timestamp,
            Self::AddOffsets { timestamp, .. } => *timestamp,
            Self::PrepareTransaction { timestamp, .. } => *timestamp,
            Self::CompleteTransaction { timestamp, .. } => *timestamp,
            Self::FenceProducer { timestamp, .. } => *timestamp,
            Self::PendingMarkers { timestamp, .. } => *timestamp,
        }
    }

    /// Get transactional ID
    pub fn transactional_id(&self) -> &str {
        match self {
            Self::InitProducerId {
                transactional_id, ..
            } => transactional_id,
            Self::BeginTransaction {
                transactional_id, ..
            } => transactional_id,
            Self::AddPartitions {
                transactional_id, ..
            } => transactional_id,
            Self::AddOffsets {
                transactional_id, ..
            } => transactional_id,
            Self::PrepareTransaction {
                transactional_id, ..
            } => transactional_id,
            Self::CompleteTransaction {
                transactional_id, ..
            } => transactional_id,
            Self::FenceProducer {
                transactional_id, ..
            } => transactional_id,
            Self::PendingMarkers {
                transactional_id, ..
            } => transactional_id,
        }
    }

    /// Get producer ID
    pub fn producer_id(&self) -> ProducerId {
        match self {
            Self::InitProducerId { producer_id, .. } => *producer_id,
            Self::BeginTransaction { producer_id, .. } => *producer_id,
            Self::AddPartitions { producer_id, .. } => *producer_id,
            Self::AddOffsets { producer_id, .. } => *producer_id,
            Self::PrepareTransaction { producer_id, .. } => *producer_id,
            Self::CompleteTransaction { producer_id, .. } => *producer_id,
            Self::FenceProducer {
                old_producer_id, ..
            } => *old_producer_id,
            Self::PendingMarkers { producer_id, .. } => *producer_id,
        }
    }
}

/// Transaction log for persistent transaction state
#[derive(Debug)]
pub struct TransactionLog {
    /// Log file path
    log_path: PathBuf,
    /// In-memory transaction states using RwLock (cold path - infrequent access)
    transaction_states: Arc<RwLock<HashMap<String, TransactionMetadata>>>,
    /// Log entry sequence number
    next_sequence_number: Arc<std::sync::atomic::AtomicU64>,
    /// Configuration
    config: TransactionLogConfig,
    /// Persistent file handle (avoids re-opening on every write)
    log_file: TokioMutex<tokio::fs::File>,
}

/// Transaction log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLogConfig {
    /// Log file path
    pub log_dir: PathBuf,
    /// Maximum log file size before rotation
    pub max_log_size: u64,
    /// Number of log files to retain
    pub log_retention_count: usize,
    /// Sync to disk after every write
    pub fsync_on_write: bool,
    /// Compact log when it reaches this ratio of deleted entries
    pub compaction_threshold: f64,
}

impl Default for TransactionLogConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./data/transaction_log"),
            max_log_size: 256 * 1024 * 1024, // 256MB
            log_retention_count: 5,
            fsync_on_write: true,
            compaction_threshold: 0.5,
        }
    }
}

impl TransactionLog {
    /// Create new transaction log
    pub async fn new(config: TransactionLogConfig) -> TransactionResult<Self> {
        // Ensure log directory exists
        fs::create_dir_all(&config.log_dir).await?;

        let log_path = config.log_dir.join("transaction.log");

        // Open persistent file handle (create if not exists, append mode)
        let log_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .await?;

        let transaction_log = Self {
            log_path,
            transaction_states: Arc::new(RwLock::new(HashMap::new())),
            next_sequence_number: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            config,
            log_file: TokioMutex::new(log_file),
        };

        // Load existing log entries
        transaction_log.recover().await?;

        info!(
            log_path = ?transaction_log.log_path,
            "Transaction log initialized"
        );

        Ok(transaction_log)
    }

    /// Log producer initialization
    pub async fn log_init_producer_id(
        &self,
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        transaction_timeout_ms: i32,
    ) -> TransactionResult<()> {
        let entry = TransactionLogEntry::InitProducerId {
            transactional_id: transactional_id.clone(),
            producer_id,
            producer_epoch,
            transaction_timeout_ms,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.append_log_entry(entry).await?;

        // Update in-memory state
        let metadata = TransactionMetadata {
            transactional_id: transactional_id.clone(),
            producer_id,
            producer_epoch,
            state: TxnState::Empty,
            partitions: std::collections::HashSet::new(),
            transaction_timeout_ms,
            txn_start_timestamp: chrono::Utc::now().timestamp_millis(),
            txn_last_update_timestamp: chrono::Utc::now().timestamp_millis(),
        };
        self.transaction_states
            .write()
            .insert(transactional_id, metadata);

        debug!(
            producer_id = producer_id,
            producer_epoch = producer_epoch,
            "Logged producer initialization"
        );

        Ok(())
    }

    /// Log transaction begin
    pub async fn log_begin_transaction(
        &self,
        transactional_id: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
    ) -> TransactionResult<()> {
        let entry = TransactionLogEntry::BeginTransaction {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.append_log_entry(entry).await?;

        // Update in-memory state
        if let Some(metadata) = self.transaction_states.write().get_mut(transactional_id) {
            metadata.state = TxnState::Ongoing;
            metadata.txn_start_timestamp = chrono::Utc::now().timestamp_millis();
            metadata.txn_last_update_timestamp = chrono::Utc::now().timestamp_millis();
            metadata.partitions.clear();
        }

        debug!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            "Logged transaction begin"
        );

        Ok(())
    }

    /// Log partitions added to transaction
    pub async fn log_add_partitions(
        &self,
        transactional_id: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        partitions: Vec<TopicPartition>,
    ) -> TransactionResult<()> {
        let entry = TransactionLogEntry::AddPartitions {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            partitions: partitions.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.append_log_entry(entry).await?;

        let partition_count = partitions.len();

        // Update in-memory state
        if let Some(metadata) = self.transaction_states.write().get_mut(transactional_id) {
            for partition in partitions {
                metadata.partitions.insert(partition);
            }
            metadata.txn_last_update_timestamp = chrono::Utc::now().timestamp_millis();
        }

        debug!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            partition_count = partition_count,
            "Logged add partitions"
        );

        Ok(())
    }

    /// Log prepare transaction (commit or abort)
    pub async fn log_prepare_transaction(
        &self,
        transactional_id: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
        partitions: Vec<TopicPartition>,
    ) -> TransactionResult<()> {
        let entry = TransactionLogEntry::PrepareTransaction {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            commit,
            partitions,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.append_log_entry(entry).await?;

        // Update in-memory state
        if let Some(metadata) = self.transaction_states.write().get_mut(transactional_id) {
            metadata.state = if commit {
                TxnState::PrepareCommit
            } else {
                TxnState::PrepareAbort
            };
            metadata.txn_last_update_timestamp = chrono::Utc::now().timestamp_millis();
        }

        debug!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            commit = commit,
            "Logged prepare transaction"
        );

        Ok(())
    }

    /// Log pending transaction markers (persisted for crash recovery)
    pub async fn log_pending_markers(
        &self,
        transactional_id: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
        marker_topics: Vec<(String, Vec<i32>)>,
    ) -> TransactionResult<()> {
        let entry = TransactionLogEntry::PendingMarkers {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            commit,
            marker_topics,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.append_log_entry(entry).await?;

        debug!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            commit = commit,
            "Logged pending transaction markers"
        );

        Ok(())
    }

    /// Batch: log prepare transaction + pending markers in a single fsync.
    /// Reduces 2 file opens + 2 fsyncs → 1 write + 1 fsync.
    pub async fn log_prepare_and_pending_markers(
        &self,
        transactional_id: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
        partitions: Vec<TopicPartition>,
        marker_topics: Vec<(String, Vec<i32>)>,
    ) -> TransactionResult<()> {
        let now = chrono::Utc::now().timestamp_millis();

        let entries = vec![
            TransactionLogEntry::PrepareTransaction {
                transactional_id: transactional_id.to_string(),
                producer_id,
                producer_epoch,
                commit,
                partitions,
                timestamp: now,
            },
            TransactionLogEntry::PendingMarkers {
                transactional_id: transactional_id.to_string(),
                producer_id,
                producer_epoch,
                commit,
                marker_topics,
                timestamp: now,
            },
        ];

        self.append_log_entries_batch(entries).await?;

        // Update in-memory state (same as log_prepare_transaction)
        if let Some(metadata) = self.transaction_states.write().get_mut(transactional_id) {
            metadata.state = if commit {
                TxnState::PrepareCommit
            } else {
                TxnState::PrepareAbort
            };
            metadata.txn_last_update_timestamp = now;
        }

        debug!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            commit = commit,
            "Logged prepare + pending markers (batched, single fsync)"
        );

        Ok(())
    }

    /// Log complete transaction (commit or abort)
    pub async fn log_complete_transaction(
        &self,
        transactional_id: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        commit: bool,
    ) -> TransactionResult<()> {
        let entry = TransactionLogEntry::CompleteTransaction {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            commit,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.append_log_entry(entry).await?;

        // Update in-memory state
        if let Some(metadata) = self.transaction_states.write().get_mut(transactional_id) {
            metadata.state = if commit {
                TxnState::CompleteCommit
            } else {
                TxnState::CompleteAbort
            };
            metadata.txn_last_update_timestamp = chrono::Utc::now().timestamp_millis();
        }

        info!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            commit = commit,
            "Logged complete transaction"
        );

        Ok(())
    }

    /// Get transaction metadata by transactional ID
    pub async fn get_transaction_metadata(
        &self,
        transactional_id: &str,
    ) -> Option<TransactionMetadata> {
        self.transaction_states
            .read()
            .get(transactional_id)
            .cloned()
    }

    /// Get all active transactions
    pub async fn get_active_transactions(&self) -> HashMap<String, TransactionMetadata> {
        self.transaction_states
            .read()
            .iter()
            .filter(|(_, metadata)| !metadata.state.is_terminal())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Get pending markers that need recovery (PrepareTransaction without CompleteTransaction)
    pub async fn get_pending_markers_for_recovery(
        &self,
    ) -> TransactionResult<Vec<TransactionLogEntry>> {
        if !self.log_path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&self.log_path).await?;
        let mut pending_markers = Vec::new();
        let mut completed_txns: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // First pass: collect completed transaction IDs
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_str::<LogRecord>(line) {
                if let TransactionLogEntry::CompleteTransaction {
                    transactional_id, ..
                } = &record.entry
                {
                    completed_txns.insert(transactional_id.clone());
                }
            }
        }

        // Second pass: collect PendingMarkers whose transaction hasn't completed
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_str::<LogRecord>(line) {
                if let TransactionLogEntry::PendingMarkers {
                    ref transactional_id,
                    ..
                } = record.entry
                {
                    if !completed_txns.contains(transactional_id) {
                        pending_markers.push(record.entry);
                    }
                }
            }
        }

        Ok(pending_markers)
    }

    /// Remove completed transactions from memory
    pub async fn cleanup_completed_transactions(&self) -> usize {
        let mut states = self.transaction_states.write();
        let initial_count = states.len();

        states.retain(|_, metadata| !metadata.state.is_terminal());

        let cleaned_up = initial_count - states.len();
        if cleaned_up > 0 {
            info!(cleaned_up = cleaned_up, "Cleaned up completed transactions");
        }

        cleaned_up
    }

    /// Append log entry to persistent log (uses persistent file handle)
    async fn append_log_entry(&self, entry: TransactionLogEntry) -> TransactionResult<()> {
        let sequence_number = self
            .next_sequence_number
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let log_record = LogRecord {
            sequence_number,
            entry,
        };

        let serialized = serde_json::to_string(&log_record)?;
        let log_line = format!("{}\n", serialized);

        let mut file = self.log_file.lock().await;
        file.write_all(log_line.as_bytes()).await?;

        if self.config.fsync_on_write {
            file.sync_all().await?;
        }

        debug!(
            sequence_number = sequence_number,
            "Appended transaction log entry"
        );

        Ok(())
    }

    /// Append multiple log entries with a single fsync (reduces I/O overhead)
    async fn append_log_entries_batch(
        &self,
        entries: Vec<TransactionLogEntry>,
    ) -> TransactionResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut buffer = String::new();
        let count = entries.len();

        for entry in entries {
            let sequence_number = self
                .next_sequence_number
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let log_record = LogRecord {
                sequence_number,
                entry,
            };
            let serialized = serde_json::to_string(&log_record)?;
            buffer.push_str(&serialized);
            buffer.push('\n');
        }

        let mut file = self.log_file.lock().await;
        file.write_all(buffer.as_bytes()).await?;

        if self.config.fsync_on_write {
            file.sync_all().await?;
        }

        debug!(count = count, "Appended batch of transaction log entries");

        Ok(())
    }

    /// Recover transaction state from log
    async fn recover(&self) -> TransactionResult<()> {
        if !self.log_path.exists() {
            info!("No existing transaction log found, starting fresh");
            return Ok(());
        }

        let content = fs::read_to_string(&self.log_path).await?;
        let mut recovered_transactions = HashMap::new();
        let mut max_sequence = 0u64;

        for (line_num, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<LogRecord>(line) {
                Ok(record) => {
                    max_sequence = max_sequence.max(record.sequence_number);
                    self.apply_log_entry(record.entry, &mut recovered_transactions);
                }
                Err(e) => {
                    warn!(
                        line_number = line_num + 1,
                        error = %e,
                        "Failed to parse log entry, skipping"
                    );
                }
            }
        }

        // Update in-memory state
        {
            let mut states = self.transaction_states.write();
            states.clear();
            for (key, value) in recovered_transactions {
                states.insert(key, value);
            }
        }

        self.next_sequence_number
            .store(max_sequence + 1, std::sync::atomic::Ordering::SeqCst);

        info!(
            recovered_transactions = self.transaction_states.read().len(),
            max_sequence = max_sequence,
            "Transaction log recovery completed"
        );

        Ok(())
    }

    /// Apply log entry during recovery
    fn apply_log_entry(
        &self,
        entry: TransactionLogEntry,
        states: &mut HashMap<String, TransactionMetadata>,
    ) {
        match entry {
            TransactionLogEntry::InitProducerId {
                transactional_id,
                producer_id,
                producer_epoch,
                transaction_timeout_ms,
                timestamp,
            } => {
                let metadata = TransactionMetadata {
                    transactional_id: transactional_id.clone(),
                    producer_id,
                    producer_epoch,
                    state: TxnState::Empty,
                    partitions: std::collections::HashSet::new(),
                    transaction_timeout_ms,
                    txn_start_timestamp: timestamp,
                    txn_last_update_timestamp: timestamp,
                };
                states.insert(transactional_id, metadata);
            }
            TransactionLogEntry::BeginTransaction {
                transactional_id,
                timestamp,
                ..
            } => {
                if let Some(metadata) = states.get_mut(&transactional_id) {
                    metadata.state = TxnState::Ongoing;
                    metadata.txn_start_timestamp = timestamp;
                    metadata.txn_last_update_timestamp = timestamp;
                    metadata.partitions.clear();
                }
            }
            TransactionLogEntry::AddPartitions {
                transactional_id,
                partitions,
                timestamp,
                ..
            } => {
                if let Some(metadata) = states.get_mut(&transactional_id) {
                    for partition in partitions {
                        metadata.partitions.insert(partition);
                    }
                    metadata.txn_last_update_timestamp = timestamp;
                }
            }
            TransactionLogEntry::PrepareTransaction {
                transactional_id,
                commit,
                timestamp,
                ..
            } => {
                if let Some(metadata) = states.get_mut(&transactional_id) {
                    metadata.state = if commit {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    };
                    metadata.txn_last_update_timestamp = timestamp;
                }
            }
            TransactionLogEntry::CompleteTransaction {
                transactional_id,
                commit,
                timestamp,
                ..
            } => {
                if let Some(metadata) = states.get_mut(&transactional_id) {
                    metadata.state = if commit {
                        TxnState::CompleteCommit
                    } else {
                        TxnState::CompleteAbort
                    };
                    metadata.txn_last_update_timestamp = timestamp;
                }
            }
            TransactionLogEntry::FenceProducer {
                transactional_id,
                new_producer_epoch,
                timestamp,
                ..
            } => {
                if let Some(metadata) = states.get_mut(&transactional_id) {
                    metadata.producer_epoch = new_producer_epoch;
                    metadata.state = TxnState::Dead;
                    metadata.txn_last_update_timestamp = timestamp;
                }
            }
            TransactionLogEntry::AddOffsets {
                transactional_id,
                timestamp,
                ..
            } => {
                if let Some(metadata) = states.get_mut(&transactional_id) {
                    metadata.txn_last_update_timestamp = timestamp;
                }
            }
            TransactionLogEntry::PendingMarkers { .. } => {
                // PendingMarkers are recovered by the TransactionCoordinator,
                // not by the log replay itself (they affect pending_markers, not transaction state)
            }
        }
    }
}

/// Log record with sequence number
#[derive(Debug, Serialize, Deserialize)]
struct LogRecord {
    sequence_number: u64,
    entry: TransactionLogEntry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_log() -> (TransactionLog, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let log = TransactionLog::new(config).await.unwrap();
        (log, temp_dir)
    }

    #[tokio::test]
    async fn test_log_init_producer_id() {
        let (log, _temp_dir) = create_test_log().await;

        log.log_init_producer_id("test-txn".to_string(), 1, 0, 60000)
            .await
            .unwrap();

        let metadata = log.get_transaction_metadata("test-txn").await.unwrap();
        assert_eq!(metadata.producer_id, 1);
        assert_eq!(metadata.producer_epoch, 0);
        assert_eq!(metadata.state, TxnState::Empty);
    }

    #[tokio::test]
    async fn test_transaction_lifecycle_logging() {
        let (log, _temp_dir) = create_test_log().await;

        // Initialize producer
        log.log_init_producer_id("test-txn".to_string(), 1, 0, 60000)
            .await
            .unwrap();

        // Begin transaction
        log.log_begin_transaction("test-txn", 1, 0).await.unwrap();
        let metadata = log.get_transaction_metadata("test-txn").await.unwrap();
        assert_eq!(metadata.state, TxnState::Ongoing);

        // Add partitions
        let partitions = vec![TopicPartition::new("topic1".to_string(), 0)];
        log.log_add_partitions("test-txn", 1, 0, partitions)
            .await
            .unwrap();
        let metadata = log.get_transaction_metadata("test-txn").await.unwrap();
        assert_eq!(metadata.partitions.len(), 1);

        // Prepare commit
        log.log_prepare_transaction("test-txn", 1, 0, true, vec![])
            .await
            .unwrap();
        let metadata = log.get_transaction_metadata("test-txn").await.unwrap();
        assert_eq!(metadata.state, TxnState::PrepareCommit);

        // Complete commit
        log.log_complete_transaction("test-txn", 1, 0, true)
            .await
            .unwrap();
        let metadata = log.get_transaction_metadata("test-txn").await.unwrap();
        assert_eq!(metadata.state, TxnState::CompleteCommit);
    }

    #[tokio::test]
    async fn test_log_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        // Create log and add some entries
        {
            let log = TransactionLog::new(config.clone()).await.unwrap();
            log.log_init_producer_id("test-txn".to_string(), 1, 0, 60000)
                .await
                .unwrap();
            log.log_begin_transaction("test-txn", 1, 0).await.unwrap();
        }

        // Create new log instance to test recovery
        let log = TransactionLog::new(config).await.unwrap();
        let metadata = log.get_transaction_metadata("test-txn").await.unwrap();
        assert_eq!(metadata.producer_id, 1);
        assert_eq!(metadata.state, TxnState::Ongoing);
    }

    #[tokio::test]
    async fn test_cleanup_completed_transactions() {
        let (log, _temp_dir) = create_test_log().await;

        // Add completed transaction
        log.log_init_producer_id("txn1".to_string(), 1, 0, 60000)
            .await
            .unwrap();
        log.log_complete_transaction("txn1", 1, 0, true)
            .await
            .unwrap();

        // Add ongoing transaction
        log.log_init_producer_id("txn2".to_string(), 2, 0, 60000)
            .await
            .unwrap();
        log.log_begin_transaction("txn2", 2, 0).await.unwrap();

        assert_eq!(log.cleanup_completed_transactions().await, 1);

        // Only ongoing transaction should remain
        assert!(log.get_transaction_metadata("txn1").await.is_none());
        assert!(log.get_transaction_metadata("txn2").await.is_some());
    }
}
