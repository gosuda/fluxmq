use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::Result;
/// Immediate performance optimizations for existing storage
///
/// These are drop-in replacements that provide immediate performance gains
/// without breaking existing interfaces.
use parking_lot::RwLock;
use std::collections::HashMap;

/// Optimized storage that uses Box instead of Arc where possible
pub struct OptimizedInMemoryStorage {
    // Changed from Arc<RwLock<HashMap<...>>> to RwLock<HashMap<..., Box<...>>>
    // Box is much cheaper than Arc when we don't need to share across threads
    topics: RwLock<HashMap<TopicName, Box<OptimizedTopic>>>,
}

/// Topic structure optimized for better cache locality
pub struct OptimizedTopic {
    partitions: HashMap<PartitionId, OptimizedPartition>,
}

/// Partition with pre-allocated capacity and optimized data layout
pub struct OptimizedPartition {
    // Separate storage for better cache performance
    messages: Vec<(Offset, Message)>,
    next_offset: Offset,
    // Track capacity to optimize future allocations
    last_capacity_increase: usize,
}

impl OptimizedInMemoryStorage {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }

    /// High-performance message append with optimized memory allocation
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
            .or_insert_with(|| Box::new(OptimizedTopic::new()));

        let partition_data = topic_data
            .partitions
            .entry(partition)
            .or_insert_with(|| OptimizedPartition::with_smart_capacity());

        let base_offset = partition_data.next_offset;

        // Smart capacity management - grow exponentially but cap the growth
        let current_len = partition_data.messages.len();
        let required_capacity = current_len + message_count;
        let current_capacity = partition_data.messages.capacity();

        if required_capacity > current_capacity {
            // Smart growth: double capacity but don't exceed reasonable limits
            let new_capacity = if current_capacity < 1024 {
                required_capacity.next_power_of_two()
            } else {
                // For large partitions, grow by 50% to avoid excessive memory usage
                (required_capacity * 3) / 2
            };

            partition_data
                .messages
                .reserve(new_capacity - current_capacity);
            partition_data.last_capacity_increase = new_capacity;
        }

        // Batch process messages with minimal overhead
        for message in messages {
            partition_data
                .messages
                .push((partition_data.next_offset, message));
            partition_data.next_offset += 1;
        }

        Ok(base_offset)
    }

    /// Optimized message fetch that reuses existing binary search optimization
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

        // Reuse existing optimized binary search
        let start_idx = partition_data
            .messages
            .binary_search_by_key(&offset, |(msg_offset, _)| *msg_offset)
            .unwrap_or_else(|idx| idx);

        if start_idx >= partition_data.messages.len() {
            return Ok(vec![]);
        }

        // Smart result capacity based on historical patterns
        let estimated_messages = std::cmp::min(
            1024, // Reasonable upper bound
            partition_data.messages.len() - start_idx,
        );

        let mut result = Vec::with_capacity(estimated_messages);
        let mut total_bytes = 0usize;
        let max_bytes = max_bytes as usize;

        // Efficient processing with early termination
        for (msg_offset, message) in &partition_data.messages[start_idx..] {
            let message_size =
                message.value.len() + message.key.as_ref().map(|k| k.len()).unwrap_or(0);

            if total_bytes + message_size > max_bytes && !result.is_empty() {
                break;
            }

            result.push((*msg_offset, message.clone()));
            total_bytes += message_size;

            if result.len() >= 10000 {
                break;
            }
        }

        Ok(result)
    }

    /// Other existing methods with same interface
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

    pub fn get_earliest_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let topics = self.topics.read();
        let partition = topics.get(topic)?.partitions.get(&partition)?;

        if partition.messages.is_empty() {
            Some(partition.next_offset)
        } else {
            Some(partition.messages[0].0)
        }
    }

    /// Performance statistics
    pub fn get_performance_stats(&self) -> OptimizationStats {
        let topics = self.topics.read();
        let topic_count = topics.len();
        let partition_count: usize = topics.values().map(|t| t.partitions.len()).sum();
        let total_messages: usize = topics
            .values()
            .flat_map(|t| t.partitions.values())
            .map(|p| p.messages.len())
            .sum();

        let total_capacity: usize = topics
            .values()
            .flat_map(|t| t.partitions.values())
            .map(|p| p.messages.capacity())
            .sum();

        let avg_messages_per_partition = if partition_count > 0 {
            total_messages as f64 / partition_count as f64
        } else {
            0.0
        };

        let capacity_utilization = if total_capacity > 0 {
            total_messages as f64 / total_capacity as f64
        } else {
            1.0
        };

        OptimizationStats {
            topic_count,
            partition_count,
            total_messages,
            total_capacity,
            avg_messages_per_partition,
            capacity_utilization,
        }
    }
}

impl OptimizedTopic {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }
}

impl OptimizedPartition {
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            next_offset: 0,
            last_capacity_increase: 0,
        }
    }

    /// Create partition with smart initial capacity
    pub fn with_smart_capacity() -> Self {
        // Start with a reasonable default that works well for most use cases
        Self {
            messages: Vec::with_capacity(100),
            next_offset: 0,
            last_capacity_increase: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptimizationStats {
    pub topic_count: usize,
    pub partition_count: usize,
    pub total_messages: usize,
    pub total_capacity: usize,
    pub avg_messages_per_partition: f64,
    pub capacity_utilization: f64,
}

impl OptimizationStats {
    /// Calculate efficiency score (0.0 to 1.0)
    pub fn efficiency_score(&self) -> f64 {
        // Higher utilization is better (less wasted memory)
        // But not too high to avoid frequent reallocations
        let optimal_utilization = 0.75;
        let utilization_score = if self.capacity_utilization <= optimal_utilization {
            self.capacity_utilization / optimal_utilization
        } else {
            // Penalize over-utilization less than under-utilization
            1.0 - (self.capacity_utilization - optimal_utilization) * 0.5
        };

        utilization_score.max(0.0).min(1.0)
    }

    /// Generate human-readable report
    pub fn generate_report(&self) -> String {
        format!(
            "Storage Optimization Report:\n\
             - Topics: {}\n\
             - Partitions: {}\n\
             - Messages: {}\n\
             - Capacity: {}\n\
             - Avg messages/partition: {:.1}\n\
             - Capacity utilization: {:.1}%\n\
             - Efficiency score: {:.1}%\n",
            self.topic_count,
            self.partition_count,
            self.total_messages,
            self.total_capacity,
            self.avg_messages_per_partition,
            self.capacity_utilization * 100.0,
            self.efficiency_score() * 100.0
        )
    }
}
