/// Simplified high-performance network optimizations for FluxMQ
///
/// This module focuses on practical optimizations that can be implemented immediately
use crate::protocol::{Message, Offset, PartitionId};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Simple batch processor for improved throughput
pub struct SimpleBatchProcessor {
    // Batch configuration
    max_batch_size: usize,

    // Simple message storage
    batches: Arc<RwLock<HashMap<String, Vec<Message>>>>,

    // Performance counters
    processed_messages: AtomicUsize,
    processed_batches: AtomicUsize,
}

impl SimpleBatchProcessor {
    pub fn new() -> Self {
        Self {
            max_batch_size: 5000, // Reasonable batch size
            batches: Arc::new(RwLock::new(HashMap::new())),
            processed_messages: AtomicUsize::new(0),
            processed_batches: AtomicUsize::new(0),
        }
    }

    /// Add message to batch
    pub fn add_message(&self, topic: &str, _partition: PartitionId, message: Message) {
        let mut batches = self.batches.write();
        let key = topic.to_string();

        let batch = batches.entry(key).or_insert_with(Vec::new);
        batch.push(message);

        // Auto-flush large batches
        if batch.len() >= self.max_batch_size {
            self.processed_messages
                .fetch_add(batch.len(), Ordering::Relaxed);
            self.processed_batches.fetch_add(1, Ordering::Relaxed);
            batch.clear(); // Process and clear
        }
    }

    /// Get processing statistics
    pub fn get_stats(&self) -> SimpleBatchStats {
        let processed_messages = self.processed_messages.load(Ordering::Relaxed);
        let processed_batches = self.processed_batches.load(Ordering::Relaxed);

        SimpleBatchStats {
            processed_messages,
            processed_batches,
            avg_batch_size: if processed_batches > 0 {
                processed_messages as f64 / processed_batches as f64
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimpleBatchStats {
    pub processed_messages: usize,
    pub processed_batches: usize,
    pub avg_batch_size: f64,
}

/// Simple buffer manager for reducing allocations
pub struct SimpleBufferManager {
    // Pre-allocated buffers
    small_buffers: Arc<RwLock<Vec<BytesMut>>>,
    large_buffers: Arc<RwLock<Vec<BytesMut>>>,

    // Usage statistics
    allocations: AtomicUsize,
    reuses: AtomicUsize,
}

impl SimpleBufferManager {
    pub fn new() -> Self {
        let manager = Self {
            small_buffers: Arc::new(RwLock::new(Vec::with_capacity(100))),
            large_buffers: Arc::new(RwLock::new(Vec::with_capacity(50))),
            allocations: AtomicUsize::new(0),
            reuses: AtomicUsize::new(0),
        };

        // Pre-allocate some buffers
        {
            let mut small = manager.small_buffers.write();
            for _ in 0..50 {
                small.push(BytesMut::with_capacity(1024));
            }
        }

        {
            let mut large = manager.large_buffers.write();
            for _ in 0..20 {
                large.push(BytesMut::with_capacity(16384));
            }
        }

        manager
    }

    /// Get a buffer of appropriate size
    pub fn get_buffer(&self, size: usize) -> BytesMut {
        self.allocations.fetch_add(1, Ordering::Relaxed);

        if size <= 1024 {
            if let Some(buf) = self.small_buffers.write().pop() {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                return buf;
            }
        } else if size <= 16384 {
            if let Some(buf) = self.large_buffers.write().pop() {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                return buf;
            }
        }

        BytesMut::with_capacity(size)
    }

    /// Return buffer to pool
    pub fn return_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();

        let capacity = buffer.capacity();

        if capacity >= 900 && capacity <= 1100 {
            if self.small_buffers.read().len() < 100 {
                self.small_buffers.write().push(buffer);
            }
        } else if capacity >= 15000 && capacity <= 17000 {
            if self.large_buffers.read().len() < 50 {
                self.large_buffers.write().push(buffer);
            }
        }
    }

    /// Get buffer statistics
    pub fn get_stats(&self) -> SimpleBufferStats {
        let allocations = self.allocations.load(Ordering::Relaxed);
        let reuses = self.reuses.load(Ordering::Relaxed);

        SimpleBufferStats {
            allocations,
            reuses,
            reuse_rate: if allocations > 0 {
                reuses as f64 / allocations as f64
            } else {
                0.0
            },
            small_pool_size: self.small_buffers.read().len(),
            large_pool_size: self.large_buffers.read().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimpleBufferStats {
    pub allocations: usize,
    pub reuses: usize,
    pub reuse_rate: f64,
    pub small_pool_size: usize,
    pub large_pool_size: usize,
}

/// Simple connection tracking
pub struct SimpleConnectionTracker {
    active_connections: AtomicUsize,
    total_connections: AtomicUsize,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

impl SimpleConnectionTracker {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            total_connections: AtomicUsize::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    /// Track new connection
    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Track connection closure
    pub fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Track data sent
    pub fn bytes_sent(&self, bytes: usize) {
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Track data received
    pub fn bytes_received(&self, bytes: usize) {
        self.bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Get connection statistics
    pub fn get_stats(&self) -> SimpleConnectionStats {
        SimpleConnectionStats {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimpleConnectionStats {
    pub active_connections: usize,
    pub total_connections: usize,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

/// Simple response cache for common responses
pub struct SimpleResponseCache {
    cache: Arc<RwLock<HashMap<String, Bytes>>>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
}

impl SimpleResponseCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
        }
    }

    /// Cache a response
    pub fn cache_response(&self, key: &str, response: Bytes) {
        self.cache.write().insert(key.to_string(), response);
    }

    /// Get cached response
    pub fn get_cached_response(&self, key: &str) -> Option<Bytes> {
        if let Some(response) = self.cache.read().get(key).cloned() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(response)
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Fast serialize Produce response
    pub fn fast_produce_response(
        &self,
        correlation_id: i32,
        topic: &str,
        partition: PartitionId,
        base_offset: Offset,
    ) -> Bytes {
        let cache_key = format!("produce_{}_{}", topic, partition);

        if let Some(cached) = self.get_cached_response(&cache_key) {
            // Patch correlation ID
            let mut response = BytesMut::from(&cached[..]);
            (&mut response[4..8]).put_i32(correlation_id);
            return response.freeze();
        }

        // Build response
        let mut buf = BytesMut::with_capacity(100);
        buf.put_u32(20 + topic.len() as u32); // Response length
        buf.put_i32(correlation_id);
        buf.put_u16(topic.len() as u16);
        buf.put_slice(topic.as_bytes());
        buf.put_u32(partition);
        buf.put_u64(base_offset);
        buf.put_i16(0); // No error

        let response = buf.freeze();

        // Cache template (will be patched with correlation ID)
        self.cache_response(&cache_key, response.clone());

        response
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> SimpleCacheStats {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);

        SimpleCacheStats {
            cache_hits: hits,
            cache_misses: misses,
            hit_rate: if hits + misses > 0 {
                hits as f64 / (hits + misses) as f64
            } else {
                0.0
            },
            cache_size: self.cache.read().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimpleCacheStats {
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub hit_rate: f64,
    pub cache_size: usize,
}

/// Combined simple network optimization manager
pub struct SimpleNetworkOptimizer {
    pub batch_processor: SimpleBatchProcessor,
    pub buffer_manager: SimpleBufferManager,
    pub connection_tracker: SimpleConnectionTracker,
    pub response_cache: SimpleResponseCache,
}

impl SimpleNetworkOptimizer {
    pub fn new() -> Self {
        Self {
            batch_processor: SimpleBatchProcessor::new(),
            buffer_manager: SimpleBufferManager::new(),
            connection_tracker: SimpleConnectionTracker::new(),
            response_cache: SimpleResponseCache::new(),
        }
    }

    /// Get comprehensive statistics
    pub fn get_stats(&self) -> SimpleNetworkStats {
        SimpleNetworkStats {
            batch_stats: self.batch_processor.get_stats(),
            buffer_stats: self.buffer_manager.get_stats(),
            connection_stats: self.connection_tracker.get_stats(),
            cache_stats: self.response_cache.get_stats(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimpleNetworkStats {
    pub batch_stats: SimpleBatchStats,
    pub buffer_stats: SimpleBufferStats,
    pub connection_stats: SimpleConnectionStats,
    pub cache_stats: SimpleCacheStats,
}

impl SimpleNetworkStats {
    pub fn report(&self) -> String {
        format!(
            "Simple Network Optimization Stats:\n\
             Batching: {:.1} avg size, {} batches\n\
             Buffers: {:.1}% reuse rate, {} pools\n\
             Connections: {} active, {} total\n\
             Cache: {:.1}% hit rate, {} entries",
            self.batch_stats.avg_batch_size,
            self.batch_stats.processed_batches,
            self.buffer_stats.reuse_rate * 100.0,
            self.buffer_stats.small_pool_size + self.buffer_stats.large_pool_size,
            self.connection_stats.active_connections,
            self.connection_stats.total_connections,
            self.cache_stats.hit_rate * 100.0,
            self.cache_stats.cache_size
        )
    }
}
