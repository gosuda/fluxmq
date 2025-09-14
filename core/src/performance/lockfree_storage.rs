#![allow(dead_code)]
use crate::protocol::{Message, Offset, PartitionId};
use crate::Result;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free message storage with partitioned segments
///
/// This replaces the centralized HashMap with distributed, lock-free structures
/// to eliminate contention and enable true parallel processing.
pub struct LockFreeMessageStorage {
    // Partitioned storage: Each partition has its own segment
    partitions: Arc<DashMap<(String, PartitionId), PartitionSegment>>,

    // Global offset tracking per partition
    next_offsets: Arc<DashMap<(String, PartitionId), AtomicU64>>,

    // Performance counters (not clonable, but shared via Arc)
    total_appends: Arc<AtomicU64>,
    total_fetches: Arc<AtomicU64>,
}

/// Individual partition storage segment
struct PartitionSegment {
    // Lock-free message queue for high-throughput appends
    message_queue: SegQueue<(Offset, Message)>,

    // In-memory index for fast random access
    message_index: RwLock<Vec<(Offset, usize)>>, // (offset, queue_position)

    // Segment metadata
    segment_id: u64,
    creation_timestamp: u64,
}

impl LockFreeMessageStorage {
    pub fn new() -> Self {
        Self {
            partitions: Arc::new(DashMap::new()),
            next_offsets: Arc::new(DashMap::new()),
            total_appends: Arc::new(AtomicU64::new(0)),
            total_fetches: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Ultra-fast message append with lock-free operations
    pub fn append_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        messages: Vec<Message>,
    ) -> Result<Offset> {
        if messages.is_empty() {
            return Ok(0);
        }

        let key = (topic.to_string(), partition);
        let message_count = messages.len() as u64;

        // Get or create partition segment
        let partition_segment = self
            .partitions
            .entry(key.clone())
            .or_insert_with(|| PartitionSegment::new(0));

        // Atomically get next offset range
        let base_offset = self
            .next_offsets
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(message_count, Ordering::SeqCst);

        // Lock-free message append
        let mut index_updates = Vec::with_capacity(messages.len());

        for (i, message) in messages.into_iter().enumerate() {
            let msg_offset = base_offset + i as u64;

            // Push to lock-free queue
            partition_segment.message_queue.push((msg_offset, message));

            // Prepare index update
            index_updates.push((msg_offset, partition_segment.message_queue.len()));
        }

        // Batch update index (minimal locking)
        {
            let mut index = partition_segment.message_index.write();
            index.extend(index_updates);

            // Keep index sorted for binary search
            if index.len() > 1000 && !index.windows(2).all(|w| w[0].0 <= w[1].0) {
                index.sort_by_key(|(offset, _)| *offset);
            }
        }

        // Update performance counters
        self.total_appends
            .fetch_add(message_count, Ordering::Relaxed);

        Ok(base_offset)
    }

    /// Lock-free message fetch with zero-copy optimization
    pub fn fetch_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        max_bytes: u32,
    ) -> Result<Vec<(Offset, Message)>> {
        let key = (topic.to_string(), partition);

        let Some(partition_segment) = self.partitions.get(&key) else {
            return Ok(Vec::new());
        };

        // Convert queue to vec for efficient access
        let messages = self.collect_messages_from_queue(&partition_segment.message_queue);

        let mut result = Vec::new();
        let mut total_bytes = 0usize;
        let max_bytes = max_bytes as usize;

        // Binary search for starting position
        let start_idx = messages
            .binary_search_by_key(&offset, |(o, _)| *o)
            .unwrap_or_else(|idx| idx);

        // Zero-copy message selection
        for (msg_offset, message) in messages.into_iter().skip(start_idx) {
            let message_size =
                message.value.len() + message.key.as_ref().map(|k| k.len()).unwrap_or(0);

            if total_bytes + message_size > max_bytes && !result.is_empty() {
                break;
            }

            result.push((msg_offset, message));
            total_bytes += message_size;

            if result.len() >= 10000 {
                break;
            }
        }

        self.total_fetches.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Collect messages from lock-free queue efficiently
    fn collect_messages_from_queue(
        &self,
        queue: &SegQueue<(Offset, Message)>,
    ) -> Vec<(Offset, Message)> {
        let mut messages = Vec::new();

        // Drain the queue efficiently
        while let Some(message) = queue.pop() {
            messages.push(message);
        }

        // Sort by offset for correct ordering
        messages.sort_by_key(|(offset, _)| *offset);
        messages
    }

    /// Get partition topics
    pub fn get_topics(&self) -> Vec<String> {
        self.partitions
            .iter()
            .map(|entry| entry.key().0.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get partitions for topic
    pub fn get_partitions(&self, topic: &str) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .filter(|entry| &entry.key().0 == topic)
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Get latest offset for partition
    pub fn get_latest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.next_offsets
            .get(&key)
            .map(|atomic_offset| atomic_offset.load(Ordering::SeqCst))
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> LockFreeStorageStats {
        LockFreeStorageStats {
            total_partitions: self.partitions.len(),
            total_appends: self.total_appends.load(Ordering::Relaxed),
            total_fetches: self.total_fetches.load(Ordering::Relaxed),
            avg_queue_size: self.calculate_avg_queue_size(),
        }
    }

    fn calculate_avg_queue_size(&self) -> f64 {
        if self.partitions.is_empty() {
            return 0.0;
        }

        let total_size: usize = self
            .partitions
            .iter()
            .map(|entry| entry.value().message_queue.len())
            .sum();

        total_size as f64 / self.partitions.len() as f64
    }
}

impl PartitionSegment {
    fn new(segment_id: u64) -> Self {
        Self {
            message_queue: SegQueue::new(),
            message_index: RwLock::new(Vec::new()),
            segment_id,
            creation_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LockFreeStorageStats {
    pub total_partitions: usize,
    pub total_appends: u64,
    pub total_fetches: u64,
    pub avg_queue_size: f64,
}

impl LockFreeStorageStats {
    pub fn report(&self) -> String {
        format!(
            "LockFree Storage - Partitions: {}, Appends: {}, Fetches: {}, Avg Queue: {:.1}",
            self.total_partitions, self.total_appends, self.total_fetches, self.avg_queue_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_lockfree_append_and_fetch() {
        let storage = LockFreeMessageStorage::new();

        let messages = vec![
            Message {
                key: Some(Bytes::from("key1")),
                value: Bytes::from("value1"),
                timestamp: 1000,
                headers: std::collections::HashMap::new(),
            },
            Message {
                key: Some(Bytes::from("key2")),
                value: Bytes::from("value2"),
                timestamp: 2000,
                headers: std::collections::HashMap::new(),
            },
        ];

        // Test append
        let offset = storage.append_messages("test_topic", 0, messages).unwrap();
        assert_eq!(offset, 0);

        // Test fetch
        let fetched = storage.fetch_messages("test_topic", 0, 0, 1024).unwrap();
        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].0, 0);
        assert_eq!(fetched[1].0, 1);
    }

    #[test]
    fn test_concurrent_operations() {
        use std::thread;

        let storage = Arc::new(LockFreeMessageStorage::new());
        let mut handles = Vec::new();

        // Spawn multiple producer threads
        for i in 0..10 {
            let storage_clone = storage.clone();
            let handle = thread::spawn(move || {
                let messages = vec![Message {
                    key: Some(Bytes::from(format!("key_{}", i))),
                    value: Bytes::from(format!("value_{}", i)),
                    timestamp: i as u64,
                    headers: std::collections::HashMap::new(),
                }];
                storage_clone
                    .append_messages("concurrent_topic", 0, messages)
                    .unwrap()
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all messages were stored
        let latest_offset = storage.get_latest_offset("concurrent_topic", 0).unwrap();
        assert_eq!(latest_offset, 10);
    }
}
