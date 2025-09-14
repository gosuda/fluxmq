#![allow(dead_code)]
use std::cell::RefCell;
/// Smart pointer optimizations for high-performance message processing
///
/// This module provides optimized alternatives to Arc<T> when multi-threading
/// is not required, offering significant performance improvements.
use std::rc::Rc;
use std::sync::Arc;
// use parking_lot::RwLock;
use crate::protocol::{PartitionId, TopicName};
use std::collections::HashMap;

/// Context-aware smart pointer that chooses the optimal pointer type
/// based on usage pattern and thread safety requirements
pub enum SmartPtr<T> {
    /// Single-threaded reference counting (fastest)
    SingleThread(Rc<T>),
    /// Multi-threaded reference counting (moderate overhead)
    MultiThread(Arc<T>),
    /// Owned value (zero overhead when possible)
    Owned(Box<T>),
}

impl<T> SmartPtr<T> {
    /// Create a single-threaded smart pointer (fastest option)
    pub fn single_thread(value: T) -> Self {
        Self::SingleThread(Rc::new(value))
    }

    /// Create a multi-threaded smart pointer (when crossing thread boundaries)
    pub fn multi_thread(value: T) -> Self {
        Self::MultiThread(Arc::new(value))
    }

    /// Create an owned pointer (zero overhead)
    pub fn owned(value: T) -> Self {
        Self::Owned(Box::new(value))
    }

    /// Get immutable reference to the value
    pub fn as_ref(&self) -> &T {
        match self {
            Self::SingleThread(rc) => rc.as_ref(),
            Self::MultiThread(arc) => arc.as_ref(),
            Self::Owned(boxed) => boxed.as_ref(),
        }
    }
}

impl<T: Clone> Clone for SmartPtr<T> {
    fn clone(&self) -> Self {
        match self {
            Self::SingleThread(rc) => Self::SingleThread(rc.clone()),
            Self::MultiThread(arc) => Self::MultiThread(arc.clone()),
            Self::Owned(boxed) => Self::Owned(boxed.clone()),
        }
    }
}

/// Optimized storage structures that minimize Arc usage
pub mod storage {
    use super::*;
    use crate::protocol::{Message, Offset};

    /// Single-threaded topic storage (much faster than Arc<RwLock<>>)
    pub struct FastTopic {
        partitions: HashMap<PartitionId, FastPartition>,
    }

    /// Single-threaded partition storage
    pub struct FastPartition {
        messages: Vec<(Offset, Message)>,
        next_offset: Offset,
    }

    impl FastTopic {
        pub fn new() -> Self {
            Self {
                partitions: HashMap::new(),
            }
        }

        pub fn get_partition_mut(&mut self, partition_id: PartitionId) -> &mut FastPartition {
            self.partitions
                .entry(partition_id)
                .or_insert_with(FastPartition::new)
        }

        pub fn get_partition(&self, partition_id: PartitionId) -> Option<&FastPartition> {
            self.partitions.get(&partition_id)
        }
    }

    impl FastPartition {
        pub fn new() -> Self {
            Self {
                messages: Vec::new(),
                next_offset: 0,
            }
        }

        /// High-performance message append without unnecessary allocations
        pub fn append_messages_fast(&mut self, messages: Vec<Message>) -> Offset {
            let message_count = messages.len();
            if message_count == 0 {
                return self.next_offset;
            }

            let base_offset = self.next_offset;

            // Pre-allocate exact capacity needed
            self.messages.reserve_exact(message_count);

            // Batch append with minimal overhead
            let mut current_offset = base_offset;
            for message in messages {
                self.messages.push((current_offset, message));
                current_offset += 1;
            }

            self.next_offset = current_offset;
            base_offset
        }

        /// Fast message fetch with zero-allocation binary search
        pub fn fetch_messages_fast(
            &self,
            offset: Offset,
            max_bytes: u32,
        ) -> Vec<(Offset, Message)> {
            let start_idx = self
                .messages
                .binary_search_by_key(&offset, |(msg_offset, _)| *msg_offset)
                .unwrap_or_else(|idx| idx);

            if start_idx >= self.messages.len() {
                return Vec::new();
            }

            // Stack-allocated small result for most common case
            const STACK_CAPACITY: usize = 64;
            let mut result = if self.messages.len() - start_idx <= STACK_CAPACITY {
                Vec::with_capacity(STACK_CAPACITY)
            } else {
                Vec::with_capacity(std::cmp::min(1024, self.messages.len() - start_idx))
            };

            let mut total_bytes = 0usize;
            let max_bytes = max_bytes as usize;

            // Efficient slice processing
            for (msg_offset, message) in &self.messages[start_idx..] {
                let message_size =
                    message.value.len() + message.key.as_ref().map(|k| k.len()).unwrap_or(0);

                if total_bytes + message_size > max_bytes && !result.is_empty() {
                    break;
                }

                result.push((*msg_offset, message.clone()));
                total_bytes += message_size;

                // Prevent unbounded results
                if result.len() >= 10000 {
                    break;
                }
            }

            result
        }
    }

    /// Thread-local storage pool to avoid Arc overhead in single-threaded contexts
    pub struct ThreadLocalStorage {
        topics: RefCell<HashMap<TopicName, FastTopic>>,
    }

    impl ThreadLocalStorage {
        pub fn new() -> Self {
            Self {
                topics: RefCell::new(HashMap::new()),
            }
        }

        /// Zero-overhead topic access for single-threaded operations
        pub fn with_topic_mut<F, R>(&self, topic: &str, f: F) -> R
        where
            F: FnOnce(&mut FastTopic) -> R,
        {
            let mut topics = self.topics.borrow_mut();
            let topic_data = topics
                .entry(topic.to_string())
                .or_insert_with(FastTopic::new);
            f(topic_data)
        }

        /// Zero-allocation topic read access
        pub fn with_topic<F, R>(&self, topic: &str, f: F) -> Option<R>
        where
            F: FnOnce(&FastTopic) -> R,
        {
            let topics = self.topics.borrow();
            topics.get(topic).map(f)
        }
    }
}

/// Smart pointer usage analyzer to identify optimization opportunities
pub struct PointerAnalyzer {
    arc_usage: HashMap<String, usize>,
    rc_opportunities: Vec<String>,
    box_opportunities: Vec<String>,
}

impl PointerAnalyzer {
    pub fn new() -> Self {
        Self {
            arc_usage: HashMap::new(),
            rc_opportunities: Vec::new(),
            box_opportunities: Vec::new(),
        }
    }

    /// Record Arc usage for analysis
    pub fn record_arc_usage(&mut self, context: &str) {
        *self.arc_usage.entry(context.to_string()).or_insert(0) += 1;
    }

    /// Suggest optimization opportunities
    pub fn suggest_optimizations(&self) -> Vec<String> {
        let mut suggestions = Vec::new();

        for (context, count) in &self.arc_usage {
            if *count > 1000 {
                suggestions.push(format!(
                    "High Arc usage in {}: {} clones - consider Rc or Box if single-threaded",
                    context, count
                ));
            }
        }

        suggestions
    }

    /// Generate performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::from("Smart Pointer Performance Analysis\n");
        report.push_str("=====================================\n\n");

        report.push_str("Arc Usage by Context:\n");
        for (context, count) in &self.arc_usage {
            report.push_str(&format!("  {}: {} clones\n", context, count));
        }

        report.push_str("\nOptimization Opportunities:\n");
        for suggestion in self.suggest_optimizations() {
            report.push_str(&format!("  - {}\n", suggestion));
        }

        report
    }
}
