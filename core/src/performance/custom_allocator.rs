use crate::performance::numa_allocator::{init_thread_local_numa_allocator, NumaAwareAllocator};

// Global allocator configuration for maximum performance
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
/// Custom high-performance allocators for FluxMQ
///
/// Implements specialized allocators to achieve 400k+ msg/sec throughput:
/// - Arena allocator for message batches
/// - Ring buffer allocator for streaming data
/// - Stack allocator for temporary operations
/// - Slab allocator for fixed-size objects
use std::alloc::Layout;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

/// High-performance arena allocator for batch message processing
pub struct ArenaAllocator {
    chunks: Vec<ArenaChunk>,
    current_chunk: AtomicUsize,
    chunk_size: usize,
    total_allocated: AtomicUsize,
    allocations: AtomicUsize,
}

struct ArenaChunk {
    memory: NonNull<u8>,
    size: usize,
    offset: AtomicUsize,
}

unsafe impl Send for ArenaChunk {}
unsafe impl Sync for ArenaChunk {}

impl ArenaAllocator {
    pub fn new(chunk_size: usize, num_chunks: usize) -> Self {
        let mut chunks = Vec::with_capacity(num_chunks);

        for _ in 0..num_chunks {
            let layout = Layout::from_size_align(chunk_size, 64).unwrap(); // 64-byte aligned
            let memory = unsafe {
                let ptr = std::alloc::alloc(layout);
                if ptr.is_null() {
                    std::alloc::handle_alloc_error(layout);
                }
                NonNull::new_unchecked(ptr)
            };

            chunks.push(ArenaChunk {
                memory,
                size: chunk_size,
                offset: AtomicUsize::new(0),
            });
        }

        Self {
            chunks,
            current_chunk: AtomicUsize::new(0),
            chunk_size,
            total_allocated: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
        }
    }

    pub fn allocate(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
        let aligned_size = (size + align - 1) & !(align - 1);

        for _ in 0..self.chunks.len() {
            let chunk_idx = self.current_chunk.load(Ordering::Relaxed) % self.chunks.len();
            let chunk = &self.chunks[chunk_idx];

            let mut offset = chunk.offset.load(Ordering::Relaxed);
            loop {
                let aligned_offset = (offset + align - 1) & !(align - 1);
                let new_offset = aligned_offset + aligned_size;

                if new_offset > chunk.size {
                    // Chunk full, try next chunk
                    self.current_chunk.fetch_add(1, Ordering::Relaxed);
                    break;
                }

                match chunk.offset.compare_exchange_weak(
                    offset,
                    new_offset,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let ptr = unsafe { chunk.memory.as_ptr().add(aligned_offset) };
                        self.total_allocated
                            .fetch_add(aligned_size, Ordering::Relaxed);
                        self.allocations.fetch_add(1, Ordering::Relaxed);
                        return NonNull::new(ptr);
                    }
                    Err(actual) => offset = actual,
                }
            }
        }

        None // All chunks exhausted
    }

    pub fn reset(&self) {
        for chunk in &self.chunks {
            chunk.offset.store(0, Ordering::Relaxed);
        }
        self.current_chunk.store(0, Ordering::Relaxed);
        // Note: Don't reset total_allocated and allocations for statistics
    }

    pub fn get_stats(&self) -> ArenaStats {
        let total_capacity = self.chunks.len() * self.chunk_size;
        let used_bytes = self
            .chunks
            .iter()
            .map(|chunk| chunk.offset.load(Ordering::Relaxed))
            .sum::<usize>();

        ArenaStats {
            total_capacity,
            used_bytes,
            num_chunks: self.chunks.len(),
            chunk_size: self.chunk_size,
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            allocations: self.allocations.load(Ordering::Relaxed),
            utilization: used_bytes as f64 / total_capacity as f64,
        }
    }
}

impl Drop for ArenaAllocator {
    fn drop(&mut self) {
        for chunk in &self.chunks {
            unsafe {
                let layout = Layout::from_size_align(chunk.size, 64).unwrap();
                std::alloc::dealloc(chunk.memory.as_ptr(), layout);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArenaStats {
    pub total_capacity: usize,
    pub used_bytes: usize,
    pub num_chunks: usize,
    pub chunk_size: usize,
    pub total_allocated: usize,
    pub allocations: usize,
    pub utilization: f64,
}

/// Ring buffer allocator for streaming message data
pub struct RingBufferAllocator {
    buffer: NonNull<u8>,
    size: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
    allocations: AtomicUsize,
    deallocations: AtomicUsize,
}

unsafe impl Send for RingBufferAllocator {}
unsafe impl Sync for RingBufferAllocator {}

impl RingBufferAllocator {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap(); // Page-aligned
        let buffer = unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            NonNull::new_unchecked(ptr)
        };

        Self {
            buffer,
            size,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
        }
    }

    pub fn allocate(&self, size: usize) -> Option<NonNull<u8>> {
        let aligned_size = (size + 7) & !7; // 8-byte aligned

        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let available = if tail <= head {
                self.size - head + tail
            } else {
                tail - head
            };

            if available < aligned_size + 8 {
                // Need space for size header
                return None; // Ring buffer full
            }

            let new_head = (head + aligned_size + 8) % self.size;

            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let ptr = unsafe { self.buffer.as_ptr().add(head) };

                    // Store size header for deallocation
                    unsafe {
                        ptr::write(ptr as *mut usize, aligned_size);
                    }

                    let data_ptr = unsafe { ptr.add(8) };
                    self.allocations.fetch_add(1, Ordering::Relaxed);
                    return NonNull::new(data_ptr);
                }
                Err(actual) => head = actual,
            }
        }
    }

    pub fn deallocate(&self, ptr: NonNull<u8>) -> bool {
        let data_ptr = ptr.as_ptr();
        let header_ptr = unsafe { data_ptr.sub(8) };

        // Verify this is the next item to deallocate
        let tail = self.tail.load(Ordering::Acquire);
        let expected_ptr = unsafe { self.buffer.as_ptr().add(tail) };

        if header_ptr != expected_ptr {
            return false; // Can only deallocate in order
        }

        let size = unsafe { ptr::read(header_ptr as *const usize) };
        let new_tail = (tail + size + 8) % self.size;

        self.tail.store(new_tail, Ordering::Release);
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn get_stats(&self) -> RingBufferStats {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        let used_bytes = if head >= tail {
            head - tail
        } else {
            self.size - tail + head
        };

        RingBufferStats {
            size: self.size,
            used_bytes,
            available_bytes: self.size - used_bytes,
            allocations: self.allocations.load(Ordering::Relaxed),
            deallocations: self.deallocations.load(Ordering::Relaxed),
            utilization: used_bytes as f64 / self.size as f64,
        }
    }
}

impl Drop for RingBufferAllocator {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align(self.size, 4096).unwrap();
            std::alloc::dealloc(self.buffer.as_ptr(), layout);
        }
    }
}

#[derive(Debug, Clone)]
pub struct RingBufferStats {
    pub size: usize,
    pub used_bytes: usize,
    pub available_bytes: usize,
    pub allocations: usize,
    pub deallocations: usize,
    pub utilization: f64,
}

/// Stack allocator for temporary operations (LIFO allocation)
pub struct StackAllocator {
    memory: NonNull<u8>,
    size: usize,
    top: AtomicUsize,
    allocations: AtomicUsize,
    peak_usage: AtomicUsize,
}

unsafe impl Send for StackAllocator {}
unsafe impl Sync for StackAllocator {}

impl StackAllocator {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 64).unwrap(); // Cache-line aligned
        let memory = unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            NonNull::new_unchecked(ptr)
        };

        Self {
            memory,
            size,
            top: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            peak_usage: AtomicUsize::new(0),
        }
    }

    pub fn allocate(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
        let aligned_size = (size + align - 1) & !(align - 1);

        let mut current_top = self.top.load(Ordering::Relaxed);
        loop {
            let aligned_top = (current_top + align - 1) & !(align - 1);
            let new_top = aligned_top + aligned_size;

            if new_top > self.size {
                return None; // Stack overflow
            }

            match self.top.compare_exchange_weak(
                current_top,
                new_top,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let ptr = unsafe { self.memory.as_ptr().add(aligned_top) };
                    self.allocations.fetch_add(1, Ordering::Relaxed);

                    // Update peak usage
                    let mut peak = self.peak_usage.load(Ordering::Relaxed);
                    while new_top > peak {
                        match self.peak_usage.compare_exchange_weak(
                            peak,
                            new_top,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => peak = actual,
                        }
                    }

                    return NonNull::new(ptr);
                }
                Err(actual) => current_top = actual,
            }
        }
    }

    pub fn reset(&self) {
        self.top.store(0, Ordering::Release);
    }

    pub fn set_checkpoint(&self) -> StackCheckpoint {
        StackCheckpoint {
            position: self.top.load(Ordering::Acquire),
        }
    }

    pub fn reset_to_checkpoint(&self, checkpoint: StackCheckpoint) {
        self.top.store(checkpoint.position, Ordering::Release);
    }

    pub fn get_stats(&self) -> StackStats {
        let current_top = self.top.load(Ordering::Acquire);
        let peak = self.peak_usage.load(Ordering::Relaxed);

        StackStats {
            size: self.size,
            current_usage: current_top,
            peak_usage: peak,
            allocations: self.allocations.load(Ordering::Relaxed),
            utilization: current_top as f64 / self.size as f64,
            peak_utilization: peak as f64 / self.size as f64,
        }
    }
}

impl Drop for StackAllocator {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align(self.size, 64).unwrap();
            std::alloc::dealloc(self.memory.as_ptr(), layout);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StackCheckpoint {
    position: usize,
}

#[derive(Debug, Clone)]
pub struct StackStats {
    pub size: usize,
    pub current_usage: usize,
    pub peak_usage: usize,
    pub allocations: usize,
    pub utilization: f64,
    pub peak_utilization: f64,
}

/// Slab allocator for fixed-size objects (messages, connections, etc.)
pub struct SlabAllocator {
    slabs: Vec<SlabPool>,
    size_classes: Vec<usize>,
}

struct SlabPool {
    object_size: usize,
    objects_per_slab: usize,
    slabs: Vec<Slab>,
    free_list: AtomicPtr<FreeNode>,
    total_objects: AtomicUsize,
    allocated_objects: AtomicUsize,
}

struct Slab {
    memory: NonNull<u8>,
    size: usize,
}

#[repr(C)]
struct FreeNode {
    next: *mut FreeNode,
}

unsafe impl Send for SlabPool {}
unsafe impl Sync for SlabPool {}
unsafe impl Send for Slab {}
unsafe impl Sync for Slab {}

impl SlabAllocator {
    pub fn new() -> Self {
        // Common size classes for FluxMQ objects
        let size_classes = vec![
            32,   // Small message headers
            64,   // Connection metadata
            128,  // Message metadata
            256,  // Small message data
            512,  // Medium message data
            1024, // Large message data
            2048, // Batch headers
            4096, // Large payloads
        ];

        let mut slabs = Vec::new();

        for &size in &size_classes {
            slabs.push(SlabPool::new(size));
        }

        Self {
            slabs,
            size_classes,
        }
    }

    pub fn allocate(&self, size: usize) -> Option<NonNull<u8>> {
        // Find appropriate size class
        let size_class = self
            .size_classes
            .iter()
            .find(|&&class_size| class_size >= size)
            .copied()?;

        // Find corresponding slab pool
        let pool_index = self
            .size_classes
            .iter()
            .position(|&class_size| class_size == size_class)?;

        self.slabs[pool_index].allocate()
    }

    pub fn deallocate(&self, ptr: NonNull<u8>, size: usize) -> bool {
        // Find appropriate size class
        let size_class = self
            .size_classes
            .iter()
            .find(|&&class_size| class_size >= size)
            .copied();

        if let Some(size_class) = size_class {
            let pool_index = self
                .size_classes
                .iter()
                .position(|&class_size| class_size == size_class);

            if let Some(index) = pool_index {
                self.slabs[index].deallocate(ptr);
                return true;
            }
        }

        false
    }

    pub fn get_stats(&self) -> Vec<SlabPoolStats> {
        self.slabs
            .iter()
            .enumerate()
            .map(|(i, pool)| pool.get_stats(self.size_classes[i]))
            .collect()
    }
}

impl SlabPool {
    fn new(object_size: usize) -> Self {
        let objects_per_slab = 4096 / object_size.max(32); // At least 32 bytes per object

        Self {
            object_size,
            objects_per_slab,
            slabs: Vec::new(),
            free_list: AtomicPtr::new(ptr::null_mut()),
            total_objects: AtomicUsize::new(0),
            allocated_objects: AtomicUsize::new(0),
        }
    }

    fn allocate(&self) -> Option<NonNull<u8>> {
        // Try to get from free list first
        let mut head = self.free_list.load(Ordering::Acquire);
        loop {
            if head.is_null() {
                // Free list empty, need new slab
                self.allocate_new_slab();
                head = self.free_list.load(Ordering::Acquire);
                if head.is_null() {
                    return None; // Failed to allocate new slab
                }
                continue;
            }

            let next = unsafe { (*head).next };

            match self.free_list.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.allocated_objects.fetch_add(1, Ordering::Relaxed);
                    return NonNull::new(head as *mut u8);
                }
                Err(actual) => head = actual,
            }
        }
    }

    fn deallocate(&self, ptr: NonNull<u8>) {
        let node = ptr.as_ptr() as *mut FreeNode;

        let mut head = self.free_list.load(Ordering::Acquire);
        loop {
            unsafe { (*node).next = head };

            match self.free_list.compare_exchange_weak(
                head,
                node,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.allocated_objects.fetch_sub(1, Ordering::Relaxed);
                    break;
                }
                Err(actual) => head = actual,
            }
        }
    }

    fn allocate_new_slab(&self) {
        let slab_size = self.object_size * self.objects_per_slab;
        let layout = Layout::from_size_align(slab_size, 64).unwrap();

        let memory = unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                return; // Allocation failed
            }
            NonNull::new_unchecked(ptr)
        };

        // Initialize free list for this slab
        for i in 0..self.objects_per_slab {
            let obj_ptr = unsafe { memory.as_ptr().add(i * self.object_size) as *mut FreeNode };

            let next_ptr = if i < self.objects_per_slab - 1 {
                unsafe { memory.as_ptr().add((i + 1) * self.object_size) as *mut FreeNode }
            } else {
                self.free_list.load(Ordering::Acquire)
            };

            unsafe { (*obj_ptr).next = next_ptr };
        }

        // Update free list head
        let new_head = memory.as_ptr() as *mut FreeNode;
        self.free_list.store(new_head, Ordering::Release);
        self.total_objects
            .fetch_add(self.objects_per_slab, Ordering::Relaxed);

        // Note: We're not storing the slab in the vector for simplicity
        // In a production version, you'd want to track slabs for proper cleanup
    }

    fn get_stats(&self, size_class: usize) -> SlabPoolStats {
        let total = self.total_objects.load(Ordering::Relaxed);
        let allocated = self.allocated_objects.load(Ordering::Relaxed);

        SlabPoolStats {
            size_class,
            object_size: self.object_size,
            total_objects: total,
            allocated_objects: allocated,
            free_objects: total.saturating_sub(allocated),
            utilization: if total > 0 {
                allocated as f64 / total as f64
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlabPoolStats {
    pub size_class: usize,
    pub object_size: usize,
    pub total_objects: usize,
    pub allocated_objects: usize,
    pub free_objects: usize,
    pub utilization: f64,
}

/// Combined custom allocator manager
pub struct CustomAllocatorManager {
    arena: Arc<ArenaAllocator>,
    ring_buffer: Arc<RingBufferAllocator>,
    stack: Arc<StackAllocator>,
    slab: Arc<SlabAllocator>,
    numa_allocator: Arc<NumaAwareAllocator>,
}

impl CustomAllocatorManager {
    pub fn new() -> Self {
        Self {
            arena: Arc::new(ArenaAllocator::new(1024 * 1024, 8)), // 8 × 1MB chunks
            ring_buffer: Arc::new(RingBufferAllocator::new(2 * 1024 * 1024)), // 2MB ring
            stack: Arc::new(StackAllocator::new(512 * 1024)),     // 512KB stack
            slab: Arc::new(SlabAllocator::new()),
            numa_allocator: init_thread_local_numa_allocator(),
        }
    }

    pub fn get_comprehensive_stats(&self) -> CustomAllocatorStats {
        CustomAllocatorStats {
            arena: self.arena.get_stats(),
            ring_buffer: self.ring_buffer.get_stats(),
            stack: self.stack.get_stats(),
            slab_pools: self.slab.get_stats(),
            numa: Default::default(),
        }
    }

    pub fn reset_allocators(&self) {
        self.arena.reset();
        self.stack.reset();
        // Note: Ring buffer and slab allocators maintain their own state
    }
}

/// Get information about the currently active global allocator
pub fn get_global_allocator_info() -> &'static str {
    #[cfg(feature = "jemalloc")]
    return "jemalloc (TiKV) - High-performance allocator optimized for server workloads";

    #[cfg(feature = "mimalloc")]
    return "mimalloc (Microsoft) - Fast allocator with excellent security features";

    #[cfg(not(any(feature = "jemalloc", feature = "mimalloc")))]
    return "System default (glibc malloc/macOS malloc) - Standard system allocator";
}

#[derive(Debug, Clone)]
pub struct CustomAllocatorStats {
    pub arena: ArenaStats,
    pub ring_buffer: RingBufferStats,
    pub stack: StackStats,
    pub slab_pools: Vec<SlabPoolStats>,
    pub numa: crate::performance::numa_allocator::NumaStats,
}

impl CustomAllocatorStats {
    pub fn report(&self) -> String {
        let mut report = String::new();

        report.push_str("Custom Allocator Statistics:\n\n");

        // Global allocator information
        report.push_str(&format!(
            "Global Allocator: {}\n",
            get_global_allocator_info()
        ));
        report.push_str("\n");

        // Arena allocator
        report.push_str(&format!(
            "Arena: {:.1}% utilization, {} allocations, {}/{}MB used\n",
            self.arena.utilization * 100.0,
            self.arena.allocations,
            self.arena.used_bytes / (1024 * 1024),
            self.arena.total_capacity / (1024 * 1024)
        ));

        // Ring buffer allocator
        report.push_str(&format!(
            "Ring Buffer: {:.1}% utilization, {} allocs, {} deallocs, {}/{}MB\n",
            self.ring_buffer.utilization * 100.0,
            self.ring_buffer.allocations,
            self.ring_buffer.deallocations,
            self.ring_buffer.used_bytes / (1024 * 1024),
            self.ring_buffer.size / (1024 * 1024)
        ));

        // Stack allocator
        report.push_str(&format!(
            "Stack: {:.1}% current, {:.1}% peak, {} allocations, {}/{}KB\n",
            self.stack.utilization * 100.0,
            self.stack.peak_utilization * 100.0,
            self.stack.allocations,
            self.stack.current_usage / 1024,
            self.stack.size / 1024
        ));

        // Slab allocators
        report.push_str("Slab Pools:\n");
        for slab_stat in &self.slab_pools {
            report.push_str(&format!(
                "  {}B: {:.1}% util, {}/{} objects allocated\n",
                slab_stat.size_class,
                slab_stat.utilization * 100.0,
                slab_stat.allocated_objects,
                slab_stat.total_objects
            ));
        }

        // NUMA allocator summary
        report.push_str(&format!(
            "\nNUMA: {:.1}% locality, {:.1}% pool efficiency\n",
            self.numa.allocation_stats.numa_locality_rate() * 100.0,
            self.numa.allocation_stats.pool_efficiency() * 100.0
        ));

        report
    }
}

/// Configure jemalloc for optimal FluxMQ performance
#[cfg(feature = "jemalloc")]
pub fn configure_jemalloc_optimizations() {
    // jemalloc can be tuned via environment variables or mallctl calls
    // For production FluxMQ deployments, consider these optimizations:

    tracing::info!("🚀 jemalloc global allocator active - optimized for server workloads");
    tracing::info!("💡 For optimal performance, consider setting:");
    tracing::info!("   MALLOC_CONF=background_thread:true,metadata_thp:auto,dirty_decay_ms:5000");

    // Note: In practice, you might use mallctl() calls here to configure jemalloc
    // programmatically instead of relying on environment variables
}

/// Configure mimalloc for optimal FluxMQ performance
#[cfg(feature = "mimalloc")]
pub fn configure_mimalloc_optimizations() {
    tracing::info!("🚀 mimalloc global allocator active - Microsoft's high-performance allocator");
    tracing::info!(
        "💡 mimalloc provides excellent out-of-the-box performance for messaging workloads"
    );

    // mimalloc generally doesn't need manual tuning - it's designed to be fast by default
}

/// Initialize global allocator optimizations if available
pub fn initialize_global_allocator_optimizations() {
    #[cfg(feature = "jemalloc")]
    configure_jemalloc_optimizations();

    #[cfg(feature = "mimalloc")]
    configure_mimalloc_optimizations();

    #[cfg(not(any(feature = "jemalloc", feature = "mimalloc")))]
    tracing::info!("📦 Using system default allocator - consider enabling 'jemalloc' or 'mimalloc' feature for better performance");
}
