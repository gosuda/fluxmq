use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
/// Quick performance wins that can be immediately applied
///
/// These optimizations provide immediate performance improvements
/// without requiring major architectural changes.
// use std::sync::Arc;

/// Quick optimization for storage with minimal Arc usage
pub struct QuickOptimizedStorage {
    // Instead of Arc<RwLock<HashMap<...>>>, use RwLock<HashMap<..., Box<...>>>
    // Box is cheaper than Arc when not sharing across threads
    topics: RwLock<HashMap<TopicName, Box<QuickTopic>>>,
}

/// Optimized topic structure with stack allocation for small partitions
pub struct QuickTopic {
    partitions: HashMap<PartitionId, QuickPartition>,
}

/// High-performance partition with pre-allocated capacity
pub struct QuickPartition {
    messages: Vec<(Offset, Message)>,
    next_offset: Offset,
}

impl QuickOptimizedStorage {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }

    /// Optimized append with minimal allocations
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
            .or_insert_with(|| Box::new(QuickTopic::new()));

        let partition_data = topic_data
            .partitions
            .entry(partition)
            .or_insert_with(|| QuickPartition::with_capacity(1000));

        let base_offset = partition_data.next_offset;

        // Optimize: Reserve exact capacity needed
        if partition_data.messages.capacity() < partition_data.messages.len() + message_count {
            let new_capacity = (partition_data.messages.len() + message_count).next_power_of_two();
            partition_data
                .messages
                .reserve(new_capacity - partition_data.messages.len());
        }

        // Optimize: Batch process with minimal overhead
        let mut current_offset = base_offset;
        for message in messages {
            partition_data.messages.push((current_offset, message));
            current_offset += 1;
        }

        partition_data.next_offset = current_offset;
        Ok(base_offset)
    }

    /// Fast fetch with binary search optimization (already implemented)
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

        // Fast binary search (already optimized in existing code)
        let start_idx = partition_data
            .messages
            .binary_search_by_key(&offset, |(msg_offset, _)| *msg_offset)
            .unwrap_or_else(|idx| idx);

        if start_idx >= partition_data.messages.len() {
            return Ok(vec![]);
        }

        // Optimize: Smart capacity allocation based on typical fetch sizes
        let estimated_count = std::cmp::min(
            (max_bytes / 100) as usize, // Assume ~100 bytes per message
            partition_data.messages.len() - start_idx,
        );
        let mut result = Vec::with_capacity(estimated_count.max(64));

        let mut total_bytes = 0usize;
        let max_bytes = max_bytes as usize;

        // Optimize: Process in chunks for better cache locality
        const CHUNK_SIZE: usize = 32;
        for chunk in partition_data.messages[start_idx..].chunks(CHUNK_SIZE) {
            for (msg_offset, message) in chunk {
                let message_size =
                    message.value.len() + message.key.as_ref().map(|k| k.len()).unwrap_or(0);

                if total_bytes + message_size > max_bytes && !result.is_empty() {
                    return Ok(result);
                }

                result.push((*msg_offset, message.clone()));
                total_bytes += message_size;

                if result.len() >= 10000 {
                    return Ok(result);
                }
            }
        }

        Ok(result)
    }

    /// Get topics list efficiently
    pub fn get_topics(&self) -> Vec<TopicName> {
        let topics = self.topics.read();
        topics.keys().cloned().collect()
    }

    /// Get latest offset efficiently
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

    pub fn get_earliest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let topics = self.topics.read();
        let partition_data = topics.get(topic)?.partitions.get(&partition)?;

        if partition_data.messages.is_empty() {
            Some(partition_data.next_offset)
        } else {
            Some(partition_data.messages[0].0)
        }
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> QuickStats {
        let topics = self.topics.read();
        let topic_count = topics.len();
        let partition_count: usize = topics.values().map(|t| t.partitions.len()).sum();
        let total_messages: usize = topics
            .values()
            .flat_map(|t| t.partitions.values())
            .map(|p| p.messages.len())
            .sum();

        QuickStats {
            topic_count,
            partition_count,
            total_messages,
        }
    }
}

impl QuickTopic {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }
}

impl QuickPartition {
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            next_offset: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
            next_offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuickStats {
    pub topic_count: usize,
    pub partition_count: usize,
    pub total_messages: usize,
}

/// Memory allocator optimization using mimalloc
// #[cfg(feature = "high-performance-allocator")]
// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Compiler hint optimizations (stable alternatives)
#[inline(always)]
pub fn likely(b: bool) -> bool {
    // Using stable alternative instead of unstable intrinsics
    b
}

#[inline(always)]
pub fn unlikely(b: bool) -> bool {
    // Using stable alternative instead of unstable intrinsics
    !b
}

/// CPU cache line optimization helpers
#[repr(align(64))]
pub struct CacheAligned<T>(pub T);

/// Performance monitoring utilities
pub struct PerformanceMonitor {
    start_time: std::time::Instant,
    samples: Vec<std::time::Duration>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            samples: Vec::with_capacity(10000),
        }
    }

    pub fn start_measurement(&mut self) {
        self.start_time = std::time::Instant::now();
    }

    pub fn end_measurement(&mut self) {
        let duration = self.start_time.elapsed();
        if self.samples.len() < self.samples.capacity() {
            self.samples.push(duration);
        }
    }

    pub fn get_stats(&self) -> PerfStats {
        if self.samples.is_empty() {
            return PerfStats {
                count: 0,
                avg_micros: 0.0,
                min_micros: 0.0,
                max_micros: 0.0,
                p95_micros: 0.0,
            };
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort();

        let avg = sorted_samples.iter().sum::<std::time::Duration>() / sorted_samples.len() as u32;
        let min = sorted_samples[0];
        let max = sorted_samples[sorted_samples.len() - 1];
        let p95_idx = (sorted_samples.len() * 95) / 100;
        let p95 = sorted_samples[p95_idx];

        PerfStats {
            count: sorted_samples.len(),
            avg_micros: avg.as_micros() as f64,
            min_micros: min.as_micros() as f64,
            max_micros: max.as_micros() as f64,
            p95_micros: p95.as_micros() as f64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerfStats {
    pub count: usize,
    pub avg_micros: f64,
    pub min_micros: f64,
    pub max_micros: f64,
    pub p95_micros: f64,
}

impl PerfStats {
    pub fn messages_per_second(&self) -> f64 {
        if self.avg_micros == 0.0 {
            0.0
        } else {
            1_000_000.0 / self.avg_micros
        }
    }
}
