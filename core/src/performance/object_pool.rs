use crate::protocol::{FetchRequest, FetchResponse, Message, ProduceRequest, ProduceResponse};
use crossbeam::queue::SegQueue;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
/// High-performance object pooling for reducing allocations
///
/// Object pools eliminate the overhead of frequent allocations/deallocations
/// for commonly used objects, providing significant performance improvements.
use std::sync::Arc;

/// Generic object pool with lock-free queue for maximum performance
pub struct ObjectPool<T> {
    pool: SegQueue<T>,
    factory: Box<dyn Fn() -> T + Send + Sync>,
    max_size: usize,
    current_size: AtomicUsize,
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl<T> ObjectPool<T> {
    /// Create a new object pool with specified capacity and factory function
    pub fn new<F>(max_size: usize, factory: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        let pool = SegQueue::new();

        // Pre-populate pool with initial objects
        let initial_size = std::cmp::min(max_size / 4, 16);
        for _ in 0..initial_size {
            pool.push(factory());
        }

        Self {
            pool,
            factory: Box::new(factory),
            max_size,
            current_size: AtomicUsize::new(initial_size),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    /// Get an object from the pool or create a new one
    pub fn get(&self) -> PooledObject<T> {
        match self.pool.pop() {
            Some(object) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                PooledObject::new(object, self)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                let object = (self.factory)();
                PooledObject::new(object, self)
            }
        }
    }

    /// Return an object to the pool (called automatically by PooledObject)
    fn return_object(&self, object: T) {
        let current = self.current_size.load(Ordering::Relaxed);
        if current < self.max_size {
            self.pool.push(object);
            self.current_size.fetch_add(1, Ordering::Relaxed);
        }
        // If pool is full, just drop the object
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        PoolStats {
            hits,
            misses,
            hit_ratio: if hits + misses > 0 {
                hits as f64 / (hits + misses) as f64
            } else {
                0.0
            },
            pool_size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_size,
        }
    }
}

/// RAII wrapper that automatically returns objects to pool when dropped
pub struct PooledObject<T> {
    object: Option<T>,
    pool: *const ObjectPool<T>,
}

impl<T> PooledObject<T> {
    fn new(object: T, pool: &ObjectPool<T>) -> Self {
        Self {
            object: Some(object),
            pool: pool as *const _,
        }
    }
}

impl<T> Drop for PooledObject<T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            unsafe {
                (*self.pool).return_object(object);
            }
        }
    }
}

impl<T> std::ops::Deref for PooledObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.object.as_ref().unwrap()
    }
}

impl<T> std::ops::DerefMut for PooledObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.as_mut().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub hits: usize,
    pub misses: usize,
    pub hit_ratio: f64,
    pub pool_size: usize,
    pub max_size: usize,
}

/// Specialized pools for FluxMQ message types
pub struct MessagePools {
    pub produce_request: Arc<ObjectPool<ProduceRequest>>,
    pub fetch_request: Arc<ObjectPool<FetchRequest>>,
    pub produce_response: Arc<ObjectPool<ProduceResponse>>,
    pub fetch_response: Arc<ObjectPool<FetchResponse>>,
    pub messages: Arc<ObjectPool<Vec<Message>>>,
    pub byte_buffers: Arc<ObjectPool<Vec<u8>>>,
}

impl MessagePools {
    /// Create optimized pools for FluxMQ message types
    pub fn new() -> Self {
        Self {
            produce_request: Arc::new(ObjectPool::new(1000, || ProduceRequest {
                topic: String::new(),
                partition: 0,
                messages: Vec::new(),
                correlation_id: 0,
                acks: 1,
                timeout_ms: 1000,
            })),

            fetch_request: Arc::new(ObjectPool::new(1000, || FetchRequest {
                topic: String::new(),
                partition: 0,
                offset: 0,
                max_bytes: 0,
                timeout_ms: 0,
                correlation_id: 0,
            })),

            produce_response: Arc::new(ObjectPool::new(1000, || ProduceResponse {
                correlation_id: 0,
                topic: String::new(),
                partition: 0,
                base_offset: 0,
                error_code: 0,
                error_message: None,
            })),

            fetch_response: Arc::new(ObjectPool::new(1000, || FetchResponse {
                correlation_id: 0,
                topic: String::new(),
                partition: 0,
                messages: Vec::new(),
                error_code: 0,
                error_message: None,
            })),

            messages: Arc::new(ObjectPool::new(2000, || Vec::with_capacity(100))),

            byte_buffers: Arc::new(ObjectPool::new(5000, || Vec::with_capacity(8192))),
        }
    }

    /// Get comprehensive statistics for all pools
    pub fn get_stats(&self) -> MessagePoolStats {
        MessagePoolStats {
            produce_request: self.produce_request.stats(),
            fetch_request: self.fetch_request.stats(),
            produce_response: self.produce_response.stats(),
            fetch_response: self.fetch_response.stats(),
            messages: self.messages.stats(),
            byte_buffers: self.byte_buffers.stats(),
        }
    }

    /// Generate performance report
    pub fn performance_report(&self) -> String {
        let stats = self.get_stats();
        let mut report = String::from("Message Pools Performance Report\n");
        report.push_str("================================\n\n");

        macro_rules! add_pool_stats {
            ($name:expr, $stats:expr) => {
                report.push_str(&format!(
                    "{}: {} hits, {} misses, {:.2}% hit ratio, {}/{} objects\n",
                    $name,
                    $stats.hits,
                    $stats.misses,
                    $stats.hit_ratio * 100.0,
                    $stats.pool_size,
                    $stats.max_size
                ));
            };
        }

        add_pool_stats!("ProduceRequest", stats.produce_request);
        add_pool_stats!("FetchRequest", stats.fetch_request);
        add_pool_stats!("ProduceResponse", stats.produce_response);
        add_pool_stats!("FetchResponse", stats.fetch_response);
        add_pool_stats!("Messages", stats.messages);
        add_pool_stats!("ByteBuffers", stats.byte_buffers);

        let overall_hits: usize = [
            stats.produce_request.hits,
            stats.fetch_request.hits,
            stats.produce_response.hits,
            stats.fetch_response.hits,
            stats.messages.hits,
            stats.byte_buffers.hits,
        ]
        .iter()
        .sum();

        let overall_misses: usize = [
            stats.produce_request.misses,
            stats.fetch_request.misses,
            stats.produce_response.misses,
            stats.fetch_response.misses,
            stats.messages.misses,
            stats.byte_buffers.misses,
        ]
        .iter()
        .sum();

        let overall_hit_ratio = if overall_hits + overall_misses > 0 {
            overall_hits as f64 / (overall_hits + overall_misses) as f64
        } else {
            0.0
        };

        report.push_str(&format!(
            "\nOverall: {} hits, {} misses, {:.2}% hit ratio\n",
            overall_hits,
            overall_misses,
            overall_hit_ratio * 100.0
        ));

        report
    }
}

#[derive(Debug, Clone)]
pub struct MessagePoolStats {
    pub produce_request: PoolStats,
    pub fetch_request: PoolStats,
    pub produce_response: PoolStats,
    pub fetch_response: PoolStats,
    pub messages: PoolStats,
    pub byte_buffers: PoolStats,
}

/// Arena-based memory allocator for temporary objects
pub struct MemoryArena {
    chunks: Mutex<Vec<Vec<u8>>>,
    current_chunk: AtomicUsize,
    chunk_size: usize,
    allocated_bytes: AtomicUsize,
}

impl MemoryArena {
    /// Create a new memory arena with specified chunk size
    pub fn new(chunk_size: usize) -> Self {
        let mut chunks = Vec::new();
        chunks.push(vec![0u8; chunk_size]);

        Self {
            chunks: Mutex::new(chunks),
            current_chunk: AtomicUsize::new(0),
            chunk_size,
            allocated_bytes: AtomicUsize::new(0),
        }
    }

    /// Allocate memory from the arena (fast path)
    pub fn allocate(&self, size: usize) -> Option<*mut u8> {
        if size > self.chunk_size / 2 {
            // Large allocation, fallback to heap
            return None;
        }

        let mut chunks = self.chunks.lock();
        let current = self.current_chunk.load(Ordering::Relaxed);

        if current >= chunks.len() {
            // Need new chunk
            chunks.push(vec![0u8; self.chunk_size]);
        }

        // Simple bump allocator within chunk
        let allocated = self.allocated_bytes.fetch_add(size, Ordering::Relaxed);
        if allocated + size <= self.chunk_size {
            Some(chunks[current].as_mut_ptr().wrapping_add(allocated))
        } else {
            // Current chunk full, try next chunk
            self.current_chunk.fetch_add(1, Ordering::Relaxed);
            self.allocated_bytes.store(size, Ordering::Relaxed);
            if current + 1 >= chunks.len() {
                chunks.push(vec![0u8; self.chunk_size]);
            }
            Some(chunks[current + 1].as_mut_ptr())
        }
    }

    /// Reset the arena (reuse all chunks)
    pub fn reset(&self) {
        self.current_chunk.store(0, Ordering::Relaxed);
        self.allocated_bytes.store(0, Ordering::Relaxed);
    }

    /// Get memory usage statistics
    pub fn usage_stats(&self) -> ArenaStats {
        let chunks = self.chunks.lock();
        ArenaStats {
            total_chunks: chunks.len(),
            current_chunk: self.current_chunk.load(Ordering::Relaxed),
            allocated_bytes: self.allocated_bytes.load(Ordering::Relaxed),
            total_capacity: chunks.len() * self.chunk_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArenaStats {
    pub total_chunks: usize,
    pub current_chunk: usize,
    pub allocated_bytes: usize,
    pub total_capacity: usize,
}

impl ArenaStats {
    pub fn utilization(&self) -> f64 {
        if self.total_capacity == 0 {
            0.0
        } else {
            self.allocated_bytes as f64 / self.total_capacity as f64
        }
    }
}
