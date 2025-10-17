use crate::protocol::{Message, Offset, PartitionId};
use parking_lot::Mutex;
/// High-performance memory management for FluxMQ
///
/// This module provides optimized memory allocation strategies to minimize
/// allocations and improve cache locality for maximum throughput.
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Stack-allocated buffer for small messages to avoid heap allocations
const SMALL_BUFFER_SIZE: usize = 1024;
const MEDIUM_BUFFER_SIZE: usize = 8192;

/// Smart buffer that uses stack allocation for small data and heap for large data
pub enum SmartBuffer {
    Small([u8; SMALL_BUFFER_SIZE], usize), // Buffer + used size
    Medium(Box<[u8; MEDIUM_BUFFER_SIZE]>, usize),
    Large(Vec<u8>),
}

impl SmartBuffer {
    /// Create a new smart buffer based on estimated size
    pub fn new(estimated_size: usize) -> Self {
        if estimated_size <= SMALL_BUFFER_SIZE {
            Self::Small([0; SMALL_BUFFER_SIZE], 0)
        } else if estimated_size <= MEDIUM_BUFFER_SIZE {
            Self::Medium(Box::new([0; MEDIUM_BUFFER_SIZE]), 0)
        } else {
            Self::Large(Vec::with_capacity(estimated_size))
        }
    }

    /// Write data to the buffer
    pub fn write(&mut self, data: &[u8]) -> Result<usize, &'static str> {
        match self {
            Self::Small(buffer, used) => {
                if *used + data.len() <= SMALL_BUFFER_SIZE {
                    buffer[*used..*used + data.len()].copy_from_slice(data);
                    *used += data.len();
                    Ok(data.len())
                } else {
                    Err("Buffer overflow")
                }
            }
            Self::Medium(buffer, used) => {
                if *used + data.len() <= MEDIUM_BUFFER_SIZE {
                    buffer[*used..*used + data.len()].copy_from_slice(data);
                    *used += data.len();
                    Ok(data.len())
                } else {
                    Err("Buffer overflow")
                }
            }
            Self::Large(vec) => {
                vec.extend_from_slice(data);
                Ok(data.len())
            }
        }
    }

    /// Get the current data as a slice
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Small(buffer, used) => &buffer[..*used],
            Self::Medium(buffer, used) => &buffer[..*used],
            Self::Large(vec) => vec.as_slice(),
        }
    }

    /// Get the length of data in the buffer
    pub fn len(&self) -> usize {
        match self {
            Self::Small(_, used) => *used,
            Self::Medium(_, used) => *used,
            Self::Large(vec) => vec.len(),
        }
    }

    /// Clear the buffer for reuse
    pub fn clear(&mut self) {
        match self {
            Self::Small(_, used) => *used = 0,
            Self::Medium(_, used) => *used = 0,
            Self::Large(vec) => vec.clear(),
        }
    }
}

/// High-performance message storage with optimized memory layout
pub struct OptimizedMessageStorage {
    // Cache-friendly data structure
    messages: Vec<OptimizedMessage>,
    // Separate vectors for better cache locality
    offsets: Vec<Offset>,
    keys: Vec<Option<Vec<u8>>>,
    values: Vec<Vec<u8>>,
    // Metadata
    next_offset: Offset,
    capacity: usize,
}

/// Cache-optimized message representation
#[repr(C, align(64))] // Align to cache line
pub struct OptimizedMessage {
    pub offset: Offset,
    pub timestamp: u64,
    pub key_index: Option<u32>, // Index into keys array
    pub value_index: u32,       // Index into values array
    pub key_len: u16,
    pub value_len: u32,
}

impl OptimizedMessageStorage {
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    pub fn append_batch(
        &self,
        _topic: &str,
        _partition: PartitionId,
        messages: &[Message],
    ) -> crate::Result<Offset> {
        // Convert slice to Vec and delegate to existing method
        let _messages_vec = messages.to_vec();
        // For now, return a placeholder offset - would need proper implementation
        Ok(0)
    }

    pub fn fetch_range(
        &self,
        _topic: &str,
        _partition: PartitionId,
        _offset: Offset,
        _max_bytes: u32,
    ) -> crate::Result<Vec<(Offset, Message)>> {
        // Delegate to existing fetch method
        // For now, return empty result - would need proper implementation
        Ok(Vec::new())
    }
    /// Create new optimized storage with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            next_offset: 0,
            capacity,
        }
    }

    /// High-performance batch append with minimal allocations
    pub fn append_messages_optimized(&mut self, messages: Vec<Message>) -> Offset {
        let message_count = messages.len();
        if message_count == 0 {
            return self.next_offset;
        }

        let base_offset = self.next_offset;

        // Reserve exactly what we need
        self.messages.reserve(message_count);
        self.offsets.reserve(message_count);
        self.keys.reserve(message_count);
        self.values.reserve(message_count);

        // Process messages in batches for better cache usage
        const BATCH_SIZE: usize = 64;
        for chunk in messages.chunks(BATCH_SIZE) {
            for message in chunk {
                let key_index = if let Some(ref key) = message.key {
                    self.keys.push(Some(key.to_vec()));
                    Some((self.keys.len() - 1) as u32)
                } else {
                    self.keys.push(None);
                    None
                };

                self.values.push(message.value.to_vec());
                let value_index = (self.values.len() - 1) as u32;

                self.offsets.push(self.next_offset);

                self.messages.push(OptimizedMessage {
                    offset: self.next_offset,
                    timestamp: message.timestamp,
                    key_index,
                    value_index,
                    key_len: message.key.as_ref().map(|k| k.len() as u16).unwrap_or(0),
                    value_len: message.value.len() as u32,
                });

                self.next_offset += 1;
            }
        }

        base_offset
    }

    /// Fast message retrieval with optimized cache access patterns
    pub fn fetch_messages_optimized(
        &self,
        offset: Offset,
        max_bytes: u32,
    ) -> Vec<(Offset, Message)> {
        // Fast binary search on separate offset array
        let start_idx = self
            .offsets
            .binary_search(&offset)
            .unwrap_or_else(|idx| idx);

        if start_idx >= self.messages.len() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut total_bytes = 0u32;

        // Process in cache-friendly manner
        for i in start_idx..self.messages.len() {
            let msg = &self.messages[i];
            let message_size = msg.key_len as u32 + msg.value_len;

            if total_bytes + message_size > max_bytes && !result.is_empty() {
                break;
            }

            // Reconstruct message from optimized storage
            let key = msg
                .key_index
                .map(|idx| self.keys[idx as usize].clone())
                .flatten()
                .map(|k| k.into());

            let value = self.values[msg.value_index as usize].clone().into();

            let message = Message {
                key,
                value,
                timestamp: msg.timestamp,
                headers: std::collections::HashMap::new(),
            };

            result.push((msg.offset, message));
            total_bytes += message_size;

            if result.len() >= 10000 {
                break;
            }
        }

        result
    }

    /// Get memory usage statistics
    pub fn memory_stats(&self) -> MemoryStats {
        let messages_size = self.messages.len() * std::mem::size_of::<OptimizedMessage>();
        let offsets_size = self.offsets.len() * std::mem::size_of::<Offset>();
        let keys_size: usize = self
            .keys
            .iter()
            .map(|k| k.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum();
        let values_size: usize = self.values.iter().map(|v| v.len()).sum();

        MemoryStats {
            messages_size,
            offsets_size,
            keys_size,
            values_size,
            total_size: messages_size + offsets_size + keys_size + values_size,
            message_count: self.messages.len(),
            capacity: self.capacity,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub messages_size: usize,
    pub offsets_size: usize,
    pub keys_size: usize,
    pub values_size: usize,
    pub total_size: usize,
    pub message_count: usize,
    pub capacity: usize,
}

impl MemoryStats {
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.message_count as f64 / self.capacity as f64
        }
    }

    pub fn average_message_size(&self) -> f64 {
        if self.message_count == 0 {
            0.0
        } else {
            self.total_size as f64 / self.message_count as f64
        }
    }
}

/// Custom allocator that uses memory pools for common sizes
pub struct PooledAllocator {
    small_pool: Arc<Mutex<Vec<NonNull<u8>>>>,  // 64-1024 bytes
    medium_pool: Arc<Mutex<Vec<NonNull<u8>>>>, // 1-8KB
    large_pool: Arc<Mutex<Vec<NonNull<u8>>>>,  // 8-64KB
    allocations: AtomicUsize,
    deallocations: AtomicUsize,
}

impl PooledAllocator {
    pub fn new() -> Self {
        Self {
            small_pool: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            medium_pool: Arc::new(Mutex::new(Vec::with_capacity(500))),
            large_pool: Arc::new(Mutex::new(Vec::with_capacity(100))),
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
        }
    }

    /// Allocate memory from appropriate pool
    pub fn allocate(&self, size: usize) -> Option<NonNull<u8>> {
        self.allocations.fetch_add(1, Ordering::Relaxed);

        let pool = if size <= 1024 {
            &self.small_pool
        } else if size <= 8192 {
            &self.medium_pool
        } else if size <= 65536 {
            &self.large_pool
        } else {
            // Too large for pool, use system allocator
            return None;
        };

        // Try to get from pool first
        if let Some(ptr) = pool.lock().pop() {
            return Some(ptr);
        }

        // Allocate new memory
        let layout = Layout::from_size_align(size, 8).ok()?;
        let ptr = unsafe { alloc(layout) };
        NonNull::new(ptr)
    }

    /// Return memory to appropriate pool
    pub fn deallocate(&self, ptr: NonNull<u8>, size: usize) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);

        let pool = if size <= 1024 {
            &self.small_pool
        } else if size <= 8192 {
            &self.medium_pool
        } else if size <= 65536 {
            &self.large_pool
        } else {
            // Too large for pool, use system deallocator
            let layout = Layout::from_size_align(size, 8).unwrap();
            unsafe { dealloc(ptr.as_ptr(), layout) };
            return;
        };

        let mut pool_guard = pool.lock();
        if pool_guard.len() < pool_guard.capacity() {
            pool_guard.push(ptr);
        } else {
            // Pool is full, deallocate normally
            let layout = Layout::from_size_align(size, 8).unwrap();
            unsafe { dealloc(ptr.as_ptr(), layout) };
        }
    }

    /// Get allocator statistics
    pub fn stats(&self) -> AllocatorStats {
        AllocatorStats {
            allocations: self.allocations.load(Ordering::Relaxed),
            deallocations: self.deallocations.load(Ordering::Relaxed),
            small_pool_size: self.small_pool.lock().len(),
            medium_pool_size: self.medium_pool.lock().len(),
            large_pool_size: self.large_pool.lock().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocatorStats {
    pub allocations: usize,
    pub deallocations: usize,
    pub small_pool_size: usize,
    pub medium_pool_size: usize,
    pub large_pool_size: usize,
}

impl AllocatorStats {
    pub fn memory_efficiency(&self) -> f64 {
        if self.allocations == 0 {
            1.0
        } else {
            self.deallocations as f64 / self.allocations as f64
        }
    }

    pub fn total_pool_objects(&self) -> usize {
        self.small_pool_size + self.medium_pool_size + self.large_pool_size
    }
}
