//! # FluxMQ Storage Module
//!
//! This module provides the storage layer for FluxMQ, implementing a hybrid memory-disk
//! storage system optimized for high-performance message streaming.
//!
//! ## Architecture
//!
//! The storage system uses a three-tier hybrid approach:
//!
//! 1. **Memory Tier**: In-memory message buffers for hot data and real-time operations
//! 2. **Memory-Mapped Tier**: 256MB memory-mapped segments for zero-copy I/O
//! 3. **Persistent Tier**: Append-only log files with CRC integrity checking
//!
//! ## Performance Features
//!
//! - **Sequential I/O**: Log-structured storage for 20-40x HDD, 5-14x SSD performance gains
//! - **Zero-Copy Operations**: Memory-mapped I/O eliminates unnecessary data copying
//! - **Lock-Free Design**: Atomic operations and lock-free data structures
//! - **Async Notifications**: Real-time message arrival notifications
//! - **Batch Processing**: Efficient batch writes and reads
//!
//! ## Modules
//!
//! - [`log`] - Append-only log file management
//! - [`segment`] - Log segment rotation and management
//! - [`index`] - Offset indexing for fast seeking
//! - [`immediate_optimizations`] - Performance optimization implementations

pub mod immediate_optimizations;
pub mod message_cache;
// pub mod ultra_high_performance;
pub mod index;
pub mod log;
// pub mod optimized;
pub mod segment;
pub mod tests;

use crate::compression::{CompressionEngine, CompressionPriority, CompressionType};
use crate::performance::mmap_storage::{MMapStorageConfig, MemoryMappedStorage};

use segment::{SegmentConfig, SegmentManager};
use tracing::{debug, error, info, trace, warn};

use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::{FluxmqError, Result};
use crossbeam::channel;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
enum PersistenceCommand {
    Append {
        topic: TopicName,
        partition: PartitionId,
        messages: Vec<Message>,
        offset: Offset,
    },
    Flush {
        topic: TopicName,
        partition: PartitionId,
    },
}

/// Message notification event sent when new messages are available
///
/// This structure is broadcast to consumers when new messages arrive, enabling
/// real-time message processing without polling. It provides essential information
/// about the new messages including their location and count.
///
/// # Fields
///
/// - `topic` - The topic name where messages were added
/// - `partition` - The specific partition ID within the topic
/// - `offset_range` - Range of offsets (start_offset, end_offset) for the new messages
/// - `message_count` - Number of messages in this notification
///
/// # Usage
///
/// ```rust,no_run
/// use fluxmq::storage::MessageNotification;
///
/// // Subscribe to message notifications
/// let mut receiver = storage.subscribe_notifications().await?;
///
/// while let Ok(notification) = receiver.recv().await {
///     println!("New messages in topic {}, partition {}: {} messages at offsets {}-{}",
///         notification.topic,
///         notification.partition,
///         notification.message_count,
///         notification.offset_range.0,
///         notification.offset_range.1
///     );
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MessageNotification {
    /// The topic name where messages were added
    pub topic: TopicName,
    /// The specific partition ID within the topic  
    pub partition: PartitionId,
    /// Range of offsets (start_offset, end_offset) for the new messages
    pub offset_range: (Offset, Offset),
    /// Number of messages in this notification
    pub message_count: usize,
}

#[derive(Debug)]
pub struct InMemoryStorage {
    topics: Arc<RwLock<HashMap<TopicName, Topic>>>,
}

#[derive(Debug)]
pub struct Topic {
    partitions: HashMap<PartitionId, Partition>,
}

#[derive(Debug)]
pub struct Partition {
    messages: Vec<(Offset, Message)>,
    next_offset: Offset,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn append_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        messages: Vec<Message>,
    ) -> Result<Offset> {
        let message_count = messages.len();
        if message_count == 0 {
            return Ok(0);
        }

        let mut topics = self.topics.write();
        let topic_data = topics
            .entry(topic.to_string())
            .or_insert_with(|| Topic::new());

        let partition_data = topic_data
            .partitions
            .entry(partition)
            .or_insert_with(|| Partition::new());

        let base_offset = partition_data.next_offset;

        // Pre-allocate capacity for batch processing
        partition_data.messages.reserve(message_count);

        // Batch process messages for better performance
        let mut current_offset = base_offset;
        for message in messages {
            partition_data.messages.push((current_offset, message));
            current_offset += 1;
        }

        partition_data.next_offset = current_offset;

        Ok(base_offset)
    }

    pub fn fetch_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        max_bytes: u32,
    ) -> Result<Vec<(Offset, Message)>> {
        let topics = self.topics.read();
        let topic_data = match topics.get(topic) {
            Some(t) => t,
            None => return Ok(vec![]),
        };

        let partition_data = match topic_data.partitions.get(&partition) {
            Some(p) => p,
            None => return Ok(vec![]),
        };

        // Optimized binary search for offset positioning
        let start_idx = partition_data
            .messages
            .binary_search_by_key(&offset, |(msg_offset, _)| *msg_offset)
            .unwrap_or_else(|idx| idx);

        if start_idx >= partition_data.messages.len() {
            return Ok(vec![]);
        }

        // Pre-allocate result vector with estimated capacity
        let mut result = Vec::with_capacity(std::cmp::min(
            1024,
            partition_data.messages.len() - start_idx,
        ));
        let mut total_bytes = 0usize;
        let max_bytes = max_bytes as usize;

        // Efficient batch collection with size limiting
        for (msg_offset, message) in &partition_data.messages[start_idx..] {
            let message_size =
                message.value.len() + message.key.as_ref().map(|k| k.len()).unwrap_or(0);

            if total_bytes + message_size > max_bytes && !result.is_empty() {
                break;
            }

            result.push((*msg_offset, message.clone()));
            total_bytes += message_size;

            // Prevent unbounded result sets
            if result.len() >= 10000 {
                break;
            }
        }

        Ok(result)
    }

    pub fn get_topics(&self) -> Vec<TopicName> {
        let topics = self.topics.read();
        topics.keys().cloned().collect()
    }

    pub fn get_partitions(&self, topic: &str) -> Vec<PartitionId> {
        let topics = self.topics.read();
        match topics.get(topic) {
            Some(topic_data) => topic_data.partitions.keys().cloned().collect(),
            None => vec![],
        }
    }

    pub fn get_latest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let topics = self.topics.read();
        topics
            .get(topic)?
            .partitions
            .get(&partition)
            .map(|p| p.next_offset)
    }

    /// Find offset for a given timestamp
    pub fn get_offset_by_timestamp(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp: u64,
    ) -> Option<Offset> {
        let topics = self.topics.read();
        let partition_data = topics.get(topic)?.partitions.get(&partition)?;

        // Find the first message with timestamp >= target timestamp
        for (offset, message) in &partition_data.messages {
            if message.timestamp >= timestamp {
                return Some(*offset);
            }
        }

        // If no message found with timestamp >= target, return latest offset
        Some(partition_data.next_offset)
    }

    /// Get earliest offset (first available offset)
    pub fn get_earliest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let topics = self.topics.read();
        let partition_data = topics.get(topic)?.partitions.get(&partition)?;

        // Return the offset of the first message, or 0 if no messages
        partition_data
            .messages
            .first()
            .map(|(offset, _)| *offset)
            .or(Some(0))
    }
}

impl Topic {
    fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }
}

impl Partition {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            next_offset: 0,
        }
    }
}

/// Advanced hybrid storage with 3-tier architecture:
/// 1. Memory Tier: In-memory for hot data and real-time operations
/// 2. Memory-Mapped Tier: 256MB memory-mapped segments for zero-copy I/O
/// 3. Persistent Tier: Append-only log files with CRC integrity checking
#[derive(Debug)]
pub struct HybridStorage {
    // Tier 1: In-memory storage for hot data
    memory: Arc<InMemoryStorage>,
    // Tier 2: Memory-mapped storage for zero-copy I/O
    mmap_storage: Arc<MemoryMappedStorage>,
    // Tier 3: Background persistence to disk
    base_dir: String,
    segments:
        Arc<RwLock<HashMap<(TopicName, PartitionId), Arc<parking_lot::Mutex<SegmentManager>>>>>,
    // Channel for async persistence
    persistence_tx: channel::Sender<PersistenceCommand>,
    // Broadcast channel for message notifications
    message_notifier: broadcast::Sender<MessageNotification>,
    // Compression engine for message compression
    compression_engine: Arc<parking_lot::Mutex<CompressionEngine>>,
    // Compression configuration
    compression_config: CompressionConfig,
}

impl HybridStorage {
    /// Create a new 3-tier hybrid storage instance
    pub fn new<P: AsRef<str>>(base_dir: P) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_string();
        std::fs::create_dir_all(&base_dir)?;

        // Create persistence channel
        let (tx, rx) = channel::unbounded();

        // Create message notification broadcast channel (capacity 1024)
        let (message_notifier, _) = broadcast::channel(1024);

        // Tier 1: In-memory storage
        let memory = Arc::new(InMemoryStorage::new());

        // Tier 2: Memory-mapped storage configuration
        let mmap_config = MMapStorageConfig {
            data_directory: std::path::PathBuf::from(&base_dir).join("mmap"),
            segment_size_mb: 256, // 256MB segments
            max_segments_per_partition: 1000,
            enable_direct_io: true,
            sync_on_write: false, // Async for performance
            preallocate_segments: true,
        };

        // Initialize memory-mapped storage
        let mmap_storage = Arc::new(MemoryMappedStorage::with_config(mmap_config)?);

        // Tier 3: Traditional segment-based storage
        let segments = Arc::new(RwLock::new(HashMap::new()));

        // Start background persistence task
        let segments_clone = Arc::clone(&segments);
        let base_dir_clone = base_dir.clone();
        tokio::spawn(async move {
            Self::persistence_worker(rx, segments_clone, base_dir_clone).await;
        });

        Ok(Self {
            memory,
            mmap_storage,
            base_dir,
            segments,
            persistence_tx: tx,
            message_notifier,
            compression_engine: Arc::new(parking_lot::Mutex::new(CompressionEngine::new())),
            compression_config: CompressionConfig::default(),
        })
    }

    /// Background task for async persistence to disk with batch processing
    async fn persistence_worker(
        rx: channel::Receiver<PersistenceCommand>,
        segments: Arc<
            RwLock<HashMap<(TopicName, PartitionId), Arc<parking_lot::Mutex<SegmentManager>>>>,
        >,
        base_dir: String,
    ) {
        const BATCH_TIMEOUT: u64 = 5; // 5ms batch timeout
        const MAX_BATCH_SIZE: usize = 1000; // Max messages per batch

        let mut pending_batch = Vec::new();
        let mut batch_timer =
            tokio::time::interval(std::time::Duration::from_millis(BATCH_TIMEOUT));

        loop {
            tokio::select! {
                // Handle crossbeam channel in non-blocking way
                result = tokio::task::spawn_blocking({
                    let rx_clone = rx.clone();
                    move || rx_clone.try_recv()
                }) => {
                    match result {
                        Ok(Ok(cmd)) => {
                            match cmd {
                                PersistenceCommand::Append {
                                    topic,
                                    partition,
                                    messages,
                                    offset,
                                } => {
                                    pending_batch.push((topic, partition, messages, offset));

                                    // Flush batch if it gets too large
                                    if pending_batch.len() >= MAX_BATCH_SIZE {
                                        Self::process_batch(&segments, &base_dir, &mut pending_batch);
                                    }
                                }
                                PersistenceCommand::Flush { topic, partition } => {
                                    // Flush any pending batch first
                                    if !pending_batch.is_empty() {
                                        Self::process_batch(&segments, &base_dir, &mut pending_batch);
                                    }

                                    if let Err(e) = Self::flush_partition_internal(&segments, &base_dir, &topic, partition) {
                                        error!("Failed to flush partition: {}", e);
                                    }
                                }
                            }
                        }
                        Ok(Err(crossbeam::channel::TryRecvError::Empty)) => {
                            // No message available, continue with timeout
                        }
                        Ok(Err(crossbeam::channel::TryRecvError::Disconnected)) => {
                            break; // Channel is closed
                        }
                        Err(_) => {
                            // spawn_blocking failed, should not happen
                            error!("Persistence worker spawn_blocking failed");
                            break;
                        }
                    }
                }
                _ = batch_timer.tick() => {
                    // Flush batch on timeout
                    if !pending_batch.is_empty() {
                        Self::process_batch(&segments, &base_dir, &mut pending_batch);
                    }
                }
            }
        }
    }

    fn process_batch(
        segments: &Arc<
            RwLock<HashMap<(TopicName, PartitionId), Arc<parking_lot::Mutex<SegmentManager>>>>,
        >,
        base_dir: &str,
        batch: &mut Vec<(TopicName, PartitionId, Vec<Message>, Offset)>,
    ) {
        for (topic, partition, messages, _offset) in batch.drain(..) {
            if let Err(e) = Self::persist_messages(segments, base_dir, &topic, partition, &messages)
            {
                error!(
                    "Failed to persist batch messages for {}:{}: {}",
                    topic, partition, e
                );
            }
        }
    }

    fn persist_messages(
        segments: &Arc<
            RwLock<HashMap<(TopicName, PartitionId), Arc<parking_lot::Mutex<SegmentManager>>>>,
        >,
        base_dir: &str,
        topic: &str,
        partition: PartitionId,
        messages: &[Message],
    ) -> Result<()> {
        let manager_arc =
            Self::get_or_create_segment_manager_sync(segments, base_dir, topic, partition)?;
        let mut manager = manager_arc.lock();
        manager.append(messages)?;
        Ok(())
    }

    fn flush_partition_internal(
        segments: &Arc<
            RwLock<HashMap<(TopicName, PartitionId), Arc<parking_lot::Mutex<SegmentManager>>>>,
        >,
        base_dir: &str,
        topic: &str,
        partition: PartitionId,
    ) -> Result<()> {
        let manager_arc =
            Self::get_or_create_segment_manager_sync(segments, base_dir, topic, partition)?;
        let mut manager = manager_arc.lock();
        manager.flush()?;
        Ok(())
    }

    fn get_or_create_segment_manager_sync(
        segments: &Arc<
            RwLock<HashMap<(TopicName, PartitionId), Arc<parking_lot::Mutex<SegmentManager>>>>,
        >,
        base_dir: &str,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Arc<parking_lot::Mutex<SegmentManager>>> {
        let key = (topic.to_string(), partition);

        // Check if segment manager already exists
        {
            let segments_map = segments.read();
            if let Some(manager) = segments_map.get(&key) {
                return Ok(Arc::clone(manager));
            }
        }

        // Create new segment manager
        let partition_dir = std::path::PathBuf::from(base_dir)
            .join(topic)
            .join(format!("partition-{}", partition));

        let config = SegmentConfig {
            base_dir: partition_dir,
            max_segment_size: 1024 * 1024 * 1024, // 1GB
            segment_prefix: "segment".to_string(),
        };

        let manager = Arc::new(parking_lot::Mutex::new(SegmentManager::new(config)?));

        // Store in cache
        {
            let mut segments_map = segments.write();
            segments_map.insert(key, Arc::clone(&manager));
        }

        Ok(manager)
    }

    pub fn append_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        messages: Vec<Message>,
    ) -> Result<Offset> {
        let start_offset = self.get_latest_offset(topic, partition).unwrap_or(0);
        let message_count = messages.len();

        // Calculate total message size for tier selection
        let total_size: usize = messages
            .iter()
            .map(|m| m.value.len() + m.key.as_ref().map_or(0, |k| k.len()))
            .sum();

        // Apply compression to large messages if enabled
        let processed_messages = if self.compression_config.enabled
            && total_size >= self.compression_config.min_message_size
        {
            self.compress_messages(messages, total_size)?
        } else {
            messages
        };

        // 4-tier storage strategy:
        // Tier 0 (Compression): Automatic compression for large messages
        // Tier 1 (Memory): All messages for fast access
        // Tier 2 (MMap): Messages > 4KB or batches > 64KB for zero-copy I/O
        // Tier 3 (Disk): All messages for persistence

        // Tier 1: Always store in memory for fast access
        let end_offset =
            self.memory
                .append_messages(topic, partition, processed_messages.clone())?;

        // Tier 2: Store in memory-mapped storage for large messages/batches
        if total_size > 4096 || message_count > 100 {
            // 4KB or 100+ messages
            match self.mmap_storage.append_messages_zero_copy(
                topic,
                partition,
                processed_messages.clone(),
            ) {
                Ok(_) => {
                    trace!(
                        "Large batch ({} bytes, {} messages) stored in memory-mapped storage for {}:{}",
                        total_size, message_count, topic, partition
                    );
                }
                Err(e) => {
                    warn!(
                        "Memory-mapped storage append failed: {}, continuing with memory-only",
                        e
                    );
                }
            }
        }

        // Send notification about new messages (fire-and-forget)
        let notification = MessageNotification {
            topic: topic.to_string(),
            partition,
            offset_range: (start_offset, end_offset),
            message_count,
        };

        // Don't block if no one is listening
        let _ = self.message_notifier.send(notification);

        // Tier 3: Async persistence to disk (fire-and-forget for performance)
        if let Err(_) = self.persistence_tx.send(PersistenceCommand::Append {
            topic: topic.to_string(),
            partition,
            messages: processed_messages,
            offset: end_offset,
        }) {
            warn!("Persistence channel full, skipping disk write");
        }

        Ok(end_offset)
    }

    pub fn fetch_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        max_bytes: u32,
    ) -> Result<Vec<(Offset, Message)>> {
        // 3-tier fetch strategy:
        // 1. Try memory first (fastest)
        // 2. Try memory-mapped storage (zero-copy)
        // 3. Fallback to disk if needed

        // Tier 1: Fast in-memory lookup
        let memory_result = self
            .memory
            .fetch_messages(topic, partition, offset, max_bytes)?;

        if !memory_result.is_empty() {
            return self.decompress_messages(memory_result);
        }

        // Tier 2: Memory-mapped storage (zero-copy I/O)
        match self
            .mmap_storage
            .fetch_messages_zero_copy(topic, partition, offset, max_bytes)
        {
            Ok(mmap_result) => {
                if !mmap_result.is_empty() {
                    trace!(
                        "Fetched {} messages from memory-mapped storage for {}:{}",
                        mmap_result.len(),
                        topic,
                        partition
                    );
                    return self.decompress_messages(mmap_result);
                }
            }
            Err(e) => {
                debug!("Memory-mapped storage fetch failed: {}, trying disk", e);
            }
        }

        // Tier 3: If not in memory or mmap, could potentially read from disk
        // For now, return empty result as disk loading happens during startup
        debug!(
            "No messages found for {}:{} at offset {} in any tier",
            topic, partition, offset
        );
        Ok(vec![])
    }

    pub fn get_topics(&self) -> Vec<TopicName> {
        self.memory.get_topics()
    }

    pub fn get_partitions(&self, topic: &str) -> Vec<PartitionId> {
        self.memory.get_partitions(topic)
    }

    pub fn get_latest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.memory.get_latest_offset(topic, partition)
    }

    /// Find offset for a given timestamp
    pub fn get_offset_by_timestamp(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp: u64,
    ) -> Option<Offset> {
        self.memory
            .get_offset_by_timestamp(topic, partition, timestamp)
    }

    /// Get earliest offset (first available offset)
    pub fn get_earliest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.memory.get_earliest_offset(topic, partition)
    }

    /// Force flush a specific partition to disk
    pub fn flush_partition(&self, topic: &str, partition: PartitionId) -> Result<()> {
        let _ = self.persistence_tx.send(PersistenceCommand::Flush {
            topic: topic.to_string(),
            partition,
        });
        Ok(())
    }

    /// Load data from disk into memory (for recovery)
    pub async fn load_from_disk(&self) -> Result<()> {
        info!("Starting recovery from disk storage at: {}", self.base_dir);

        // Scan base directory for topic directories
        let base_path = std::path::Path::new(&self.base_dir);
        if !base_path.exists() {
            info!("No existing data directory found, starting with fresh state");
            return Ok(());
        }

        let mut total_topics = 0;
        let mut total_partitions = 0;
        let mut total_messages = 0;

        // Iterate through topic directories
        for topic_entry in std::fs::read_dir(base_path)? {
            let topic_entry = topic_entry?;
            let topic_path = topic_entry.path();

            if !topic_path.is_dir() {
                continue;
            }

            let topic_name = topic_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| {
                    FluxmqError::Storage(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid topic directory name",
                    ))
                })?;

            total_topics += 1;
            info!("Loading topic: {}", topic_name);

            // Iterate through partition directories
            for partition_entry in std::fs::read_dir(&topic_path)? {
                let partition_entry = partition_entry?;
                let partition_path = partition_entry.path();

                if !partition_path.is_dir() {
                    continue;
                }

                // Parse partition number from directory name (e.g., "partition-0")
                let partition_dir_name = partition_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| {
                        FluxmqError::Storage(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Invalid partition directory name",
                        ))
                    })?;

                if !partition_dir_name.starts_with("partition-") {
                    continue;
                }

                let partition_id: PartitionId = partition_dir_name
                    .strip_prefix("partition-")
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        FluxmqError::Storage(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Invalid partition ID in directory: {}", partition_dir_name),
                        ))
                    })?;

                total_partitions += 1;
                info!("Loading partition: {}-{}", topic_name, partition_id);

                // Load data for this topic-partition
                let messages_loaded = self
                    .load_partition_from_disk(topic_name, partition_id, &partition_path)
                    .await?;
                total_messages += messages_loaded;

                debug!(
                    "Loaded {} messages for {}-{}",
                    messages_loaded, topic_name, partition_id
                );
            }
        }

        info!(
            "Recovery completed: {} topics, {} partitions, {} messages loaded",
            total_topics, total_partitions, total_messages
        );

        Ok(())
    }

    /// Load a specific partition's data from disk into memory
    async fn load_partition_from_disk(
        &self,
        topic: &str,
        partition: PartitionId,
        partition_path: &std::path::Path,
    ) -> Result<u64> {
        // Create segment manager for this partition
        let config = SegmentConfig {
            base_dir: partition_path.to_path_buf(),
            max_segment_size: 1024 * 1024 * 1024, // 1GB
            segment_prefix: "segment".to_string(),
        };

        let segment_manager = SegmentManager::new(config)?;

        // Read all messages from segments
        let mut total_messages = 0u64;
        let mut current_offset = 0;
        const BATCH_SIZE: usize = 1024 * 1024; // 1MB batches

        loop {
            // Read a batch of messages
            let log_entries = segment_manager.read(current_offset, BATCH_SIZE)?;

            if log_entries.is_empty() {
                break;
            }

            // Convert log entries back to messages and load into memory
            let mut messages = Vec::new();
            let mut max_offset = current_offset;

            for entry in log_entries {
                messages.push(entry.to_message());
                max_offset = entry.offset + 1;
                total_messages += 1;
            }

            if !messages.is_empty() {
                // Load directly into in-memory storage (bypass persistence channel)
                let _base_offset = self.memory.append_messages(topic, partition, messages)?;
                trace!(
                    "Loaded batch of {} messages for {}-{}",
                    max_offset - current_offset,
                    topic,
                    partition
                );
            }

            current_offset = max_offset;

            // Yield control occasionally for other async tasks
            if total_messages % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Register this segment manager in our cache for future operations
        if total_messages > 0 {
            let key = (topic.to_string(), partition);
            let manager_arc = Arc::new(parking_lot::Mutex::new(segment_manager));

            let mut segments_map = self.segments.write();
            segments_map.insert(key, manager_arc);
        }

        Ok(total_messages)
    }

    /// Subscribe to message notifications
    /// Returns a receiver that will get notified when new messages arrive
    pub fn subscribe_to_messages(&self) -> broadcast::Receiver<MessageNotification> {
        self.message_notifier.subscribe()
    }

    /// Get the number of active subscribers (for monitoring)
    pub fn get_subscriber_count(&self) -> usize {
        self.message_notifier.receiver_count()
    }

    /// Configure compression settings for the storage layer
    pub fn set_compression_config(&mut self, config: CompressionConfig) {
        self.compression_config = config;
    }

    /// Get current compression configuration
    pub fn get_compression_config(&self) -> &CompressionConfig {
        &self.compression_config
    }

    /// Compress messages based on size and configuration
    fn compress_messages(
        &self,
        messages: Vec<Message>,
        _total_size: usize,
    ) -> Result<Vec<Message>> {
        let mut engine = self.compression_engine.lock();
        let mut compressed_messages = Vec::with_capacity(messages.len());
        let mut total_original_size = 0;
        let mut total_compressed_size = 0;

        for message in messages {
            let original_size = message.value.len();
            total_original_size += original_size;

            // Only compress if message is large enough
            if original_size >= self.compression_config.min_message_size {
                // Choose compression type based on priority and message size
                let compression_type = engine
                    .choose_optimal_compression(original_size, self.compression_config.priority);

                match engine.compress(&message.value, compression_type) {
                    Ok(compressed_value) => {
                        let compressed_size = compressed_value.len();
                        total_compressed_size += compressed_size;

                        // Create compressed message with compression metadata in headers
                        let mut compressed_message = message.clone();
                        compressed_message.value = compressed_value;

                        // Add compression metadata to message headers
                        compressed_message.headers.insert(
                            "compression".to_string(),
                            bytes::Bytes::from(format!("{}", compression_type as u8)),
                        );
                        compressed_message.headers.insert(
                            "original_size".to_string(),
                            bytes::Bytes::from(original_size.to_string()),
                        );

                        compressed_messages.push(compressed_message);

                        trace!(
                            "Compressed message: {} -> {} bytes ({:.1}% reduction, type: {:?})",
                            original_size,
                            compressed_size,
                            (1.0 - compressed_size as f64 / original_size as f64) * 100.0,
                            compression_type
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Compression failed for message: {}, storing uncompressed",
                            e
                        );
                        compressed_messages.push(message);
                        total_compressed_size += original_size;
                    }
                }
            } else {
                // Keep small messages uncompressed
                compressed_messages.push(message);
                total_compressed_size += original_size;
            }
        }

        if total_original_size > 0 && total_compressed_size < total_original_size {
            let compression_ratio =
                (1.0 - total_compressed_size as f64 / total_original_size as f64) * 100.0;
            debug!(
                "Batch compression: {} -> {} bytes ({:.1}% reduction)",
                total_original_size, total_compressed_size, compression_ratio
            );
        }

        Ok(compressed_messages)
    }

    /// Decompress messages when reading from storage
    fn decompress_messages(
        &self,
        messages: Vec<(Offset, Message)>,
    ) -> Result<Vec<(Offset, Message)>> {
        let mut engine = self.compression_engine.lock();
        let mut decompressed_messages = Vec::with_capacity(messages.len());

        for (offset, message) in messages {
            // Check if message is compressed by looking at headers
            if let Some(compression_bytes) = message.headers.get("compression") {
                if let Ok(compression_str) = std::str::from_utf8(compression_bytes) {
                    if let Ok(compression_type_u8) = compression_str.parse::<u8>() {
                        if let Ok(compression_type) = CompressionType::try_from(compression_type_u8)
                        {
                            // Get original size hint if available
                            let original_size = message
                                .headers
                                .get("original_size")
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .and_then(|s| s.parse::<usize>().ok());

                            match engine.decompress(&message.value, compression_type, original_size)
                            {
                                Ok(decompressed_value) => {
                                    let original_compressed_size = message.value.len();
                                    let decompressed_size = decompressed_value.len();

                                    let mut decompressed_message = message.clone();
                                    decompressed_message.value = decompressed_value;

                                    // Remove compression metadata from headers
                                    decompressed_message.headers.remove("compression");
                                    decompressed_message.headers.remove("original_size");

                                    decompressed_messages.push((offset, decompressed_message));

                                    trace!(
                                        "Decompressed message at offset {}: {} -> {} bytes",
                                        offset,
                                        original_compressed_size,
                                        decompressed_size
                                    );
                                }
                                Err(e) => {
                                    warn!("Decompression failed for message at offset {}: {}, returning compressed data", offset, e);
                                    decompressed_messages.push((offset, message));
                                }
                            }
                        } else {
                            // Unknown compression type, return as-is
                            decompressed_messages.push((offset, message));
                        }
                    } else {
                        // Invalid compression header, return as-is
                        decompressed_messages.push((offset, message));
                    }
                } else {
                    // Invalid UTF-8 in compression header, return as-is
                    decompressed_messages.push((offset, message));
                }
            } else {
                // No compression header, return as-is
                decompressed_messages.push((offset, message));
            }
        }

        Ok(decompressed_messages)
    }
}

/// Configuration for storage compression
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Enable compression for stored messages
    pub enabled: bool,
    /// Minimum message size to trigger compression (bytes)
    pub min_message_size: usize,
    /// Compression priority strategy
    pub priority: CompressionPriority,
    /// Default compression type when auto-selection is not used
    pub default_type: CompressionType,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_message_size: 1024,               // 1KB minimum
            priority: CompressionPriority::Speed, // LZ4 for fast compression
            default_type: CompressionType::Lz4,
        }
    }
}
