
/// Consumer Group arena allocator for high-efficiency rebalancing operations
///
/// This module provides specialized arena memory allocation for consumer group
/// coordination operations, particularly during rebalancing where many temporary
/// data structures (assignments, topic_partitions, member lists) are created
/// and destroyed rapidly.
///
/// Performance targets:
/// - 50-70% reduction in rebalancing latency
/// - Zero-cost session cleanup (arena drop)
/// - Memory locality for better cache performance
use crate::consumer::{ConsumerId, TopicPartition};
use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

/// Consumer group arena optimized for rebalancing sessions
#[repr(C, align(64))] // Cache-line aligned for optimal performance
pub struct ConsumerArena {
    /// Raw memory region for the arena
    memory: NonNull<u8>,
    /// Total size of the arena
    size: usize,
    /// Current allocation position (atomic for thread safety)
    position: AtomicUsize,
    /// Arena identifier for debugging and metrics
    arena_id: u32,
    /// Number of allocations made from this arena
    allocation_count: AtomicUsize,
    /// Session ID this arena is associated with
    session_id: String,
    /// Group ID this arena serves
    group_id: String,
}

impl ConsumerArena {
    /// Create a new consumer arena for a rebalancing session
    /// Typical size: 128KB for most consumer groups, 512KB for large groups
    pub fn new(
        size: usize,
        arena_id: u32,
        group_id: String,
        session_id: String,
    ) -> std::result::Result<Self, String> {
        if size == 0 || size > 8 * 1024 * 1024 {
            // Max 8MB per arena
            return Err(format!("Invalid consumer arena size: {} bytes", size));
        }

        // Allocate aligned memory for the arena
        let layout = Layout::from_size_align(size, 64)
            .map_err(|e| format!("Failed to create layout: {}", e))?;

        let memory = unsafe { alloc(layout) };
        if memory.is_null() {
            return Err("Failed to allocate consumer arena memory".to_string());
        }

        let arena = Self {
            memory: unsafe { NonNull::new_unchecked(memory) },
            size,
            position: AtomicUsize::new(0),
            arena_id,
            allocation_count: AtomicUsize::new(0),
            session_id: session_id.clone(),
            group_id: group_id.clone(),
        };

        debug!(
            "👥 CONSUMER ARENA: Created arena {} for group '{}' session '{}' ({}KB capacity)",
            arena_id,
            group_id,
            session_id,
            size / 1024
        );

        Ok(arena)
    }

    /// Fast pointer-bump allocation with alignment
    pub fn allocate(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
        if size == 0 {
            return None;
        }

        let mut current_pos = self.position.load(Ordering::Relaxed);

        loop {
            // Calculate aligned position
            let aligned_pos = (current_pos + align - 1) & !(align - 1);
            let new_pos = aligned_pos + size;

            // Check if allocation fits
            if new_pos > self.size {
                warn!(
                    "⚠️ CONSUMER ARENA: Arena {} exhausted during rebalancing for group '{}' ({} / {} bytes used)",
                    self.arena_id,
                    self.group_id,
                    current_pos,
                    self.size
                );
                return None;
            }

            // Try to claim this space atomically
            match self.position.compare_exchange_weak(
                current_pos,
                new_pos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully allocated
                    self.allocation_count.fetch_add(1, Ordering::Relaxed);

                    let ptr = unsafe { self.memory.as_ptr().add(aligned_pos) };
                    return Some(unsafe { NonNull::new_unchecked(ptr) });
                }
                Err(actual) => {
                    // Another thread updated position, retry with new value
                    current_pos = actual;
                }
            }
        }
    }

    /// Allocate HashMap with specific initial capacity for assignments
    pub fn allocate_assignment_map(
        &self,
        capacity: usize,
    ) -> Option<HashMap<ConsumerId, Vec<TopicPartition>>> {
        // Estimate memory needed for HashMap
        let estimated_size = capacity
            * (std::mem::size_of::<ConsumerId>() + std::mem::size_of::<Vec<TopicPartition>>() + 64);

        if !self.can_allocate(estimated_size, 8) {
            return None;
        }

        // For simplicity, return a regular HashMap but track the allocation
        self.allocation_count.fetch_add(1, Ordering::Relaxed);
        Some(HashMap::with_capacity(capacity))
    }

    /// Allocate Vec for topic partitions
    pub fn allocate_partition_vec(&self, capacity: usize) -> Option<Vec<TopicPartition>> {
        let size = capacity * std::mem::size_of::<TopicPartition>();
        let align = std::mem::align_of::<TopicPartition>();

        if capacity == 0 {
            return Some(Vec::new());
        }

        self.allocate(size, align).map(|ptr| {
            // Create Vec with arena-allocated memory
            // Safety: We've allocated sufficient aligned memory
            unsafe { Vec::from_raw_parts(ptr.as_ptr() as *mut TopicPartition, 0, capacity) }
        })
    }

    /// Allocate Vec for consumer IDs
    pub fn allocate_consumer_vec(&self, capacity: usize) -> Option<Vec<ConsumerId>> {
        let size = capacity * std::mem::size_of::<ConsumerId>();
        let align = std::mem::align_of::<ConsumerId>();

        if capacity == 0 {
            return Some(Vec::new());
        }

        self.allocate(size, align).map(|ptr| {
            // Create Vec with arena-allocated memory
            // Safety: We've allocated sufficient aligned memory
            unsafe { Vec::from_raw_parts(ptr.as_ptr() as *mut ConsumerId, 0, capacity) }
        })
    }

    /// Allocate memory for topic partition HashMap
    pub fn allocate_topic_partition_map(
        &self,
        capacity: usize,
    ) -> Option<HashMap<String, Vec<TopicPartition>>> {
        // Estimate memory needed
        let estimated_size = capacity * (64 + std::mem::size_of::<Vec<TopicPartition>>() + 128); // String + Vec + overhead

        if !self.can_allocate(estimated_size, 8) {
            return None;
        }

        // Track the allocation
        self.allocation_count.fetch_add(1, Ordering::Relaxed);
        Some(HashMap::with_capacity(capacity))
    }

    /// Get current arena utilization statistics
    pub fn get_stats(&self) -> ConsumerArenaStats {
        let used = self.position.load(Ordering::Relaxed);
        let allocations = self.allocation_count.load(Ordering::Relaxed);

        ConsumerArenaStats {
            arena_id: self.arena_id,
            group_id: self.group_id.clone(),
            session_id: self.session_id.clone(),
            total_size: self.size,
            used_bytes: used,
            allocation_count: allocations,
            utilization: (used as f64 / self.size as f64) * 100.0,
        }
    }

    /// Check if arena has sufficient space for allocation
    pub fn can_allocate(&self, size: usize, align: usize) -> bool {
        let current_pos = self.position.load(Ordering::Relaxed);
        let aligned_pos = (current_pos + align - 1) & !(align - 1);
        aligned_pos + size <= self.size
    }

    /// Reset arena for session reuse
    pub fn reset_for_new_session(&self, new_session_id: String) {
        self.position.store(0, Ordering::Relaxed);
        self.allocation_count.store(0, Ordering::Relaxed);

        debug!(
            "🔄 CONSUMER ARENA: Reset arena {} for new session '{}' in group '{}'",
            self.arena_id, new_session_id, self.group_id
        );
    }

    /// Get group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get session ID  
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

impl Drop for ConsumerArena {
    fn drop(&mut self) {
        let stats = self.get_stats();
        debug!(
            "🏁 CONSUMER ARENA: Dropping arena {} for group '{}' - {} allocations, {:.1}% utilization",
            self.arena_id,
            self.group_id,
            stats.allocation_count,
            stats.utilization
        );

        // Deallocate arena memory
        let layout = Layout::from_size_align(self.size, 64).unwrap();
        unsafe {
            dealloc(self.memory.as_ptr(), layout);
        }
    }
}

// Arena is safe to send between threads
unsafe impl Send for ConsumerArena {}
unsafe impl Sync for ConsumerArena {}

/// Consumer arena utilization statistics
#[derive(Debug, Clone)]
pub struct ConsumerArenaStats {
    pub arena_id: u32,
    pub group_id: String,
    pub session_id: String,
    pub total_size: usize,
    pub used_bytes: usize,
    pub allocation_count: usize,
    pub utilization: f64,
}

impl ConsumerArenaStats {
    pub fn is_efficient(&self) -> bool {
        self.utilization > 60.0 && self.allocation_count > 3
    }

    pub fn report(&self) -> String {
        format!(
            "Consumer Arena {}: {:.1}% used ({} / {}KB), {} allocations for group '{}'",
            self.arena_id,
            self.utilization,
            self.used_bytes / 1024,
            self.total_size / 1024,
            self.allocation_count,
            self.group_id
        )
    }
}

/// Manager for consumer group arenas across different groups and sessions
pub struct ConsumerArenaManager {
    /// Active arenas per group ID
    group_arenas: HashMap<String, Arc<ConsumerArena>>,
    /// Arena pool for reuse
    arena_pool: std::collections::VecDeque<Arc<ConsumerArena>>,
    /// Next arena ID for debugging
    next_arena_id: AtomicUsize,
}

impl ConsumerArenaManager {
    pub fn new() -> Self {
        Self {
            group_arenas: HashMap::new(),
            arena_pool: std::collections::VecDeque::new(),
            next_arena_id: AtomicUsize::new(1),
        }
    }

    /// Get or create arena for a consumer group rebalancing session
    pub fn get_arena_for_group(
        &mut self,
        group_id: String,
        session_id: String,
        estimated_members: usize,
    ) -> std::result::Result<Arc<ConsumerArena>, String> {
        // Calculate arena size based on estimated group size
        let arena_size = match estimated_members {
            0..=5 => 65536,     // 64KB for small groups
            6..=20 => 262144,   // 256KB for medium groups
            21..=100 => 524288, // 512KB for large groups
            _ => 1048576,       // 1MB for very large groups
        };

        // Check if we have a reusable arena
        if let Some(arena) = self.arena_pool.pop_front() {
            if arena.size >= arena_size {
                arena.reset_for_new_session(session_id);
                self.group_arenas.insert(group_id, arena.clone());
                return Ok(arena);
            }
        }

        // Create new arena
        let arena_id = self.next_arena_id.fetch_add(1, Ordering::Relaxed) as u32;
        let arena = Arc::new(ConsumerArena::new(
            arena_size,
            arena_id,
            group_id.clone(),
            session_id,
        )?);

        debug!(
            "🆕 CONSUMER ARENA: Created new {}KB arena {} for group '{}' ({} estimated members)",
            arena_size / 1024,
            arena_id,
            group_id,
            estimated_members
        );

        self.group_arenas.insert(group_id, arena.clone());
        Ok(arena)
    }

    /// Release arena after rebalancing completion
    pub fn release_arena_for_group(&mut self, group_id: &str) {
        if let Some(arena) = self.group_arenas.remove(group_id) {
            let stats = arena.get_stats();

            debug!(
                "🔚 CONSUMER ARENA: Released arena {} for group '{}' - {:.1}% utilization",
                stats.arena_id, group_id, stats.utilization
            );

            // Return to pool if it's still useful and has only one reference
            if Arc::strong_count(&arena) == 1 && stats.utilization < 95.0 {
                self.arena_pool.push_back(arena);
            }
            // Otherwise arena will be dropped when Arc count reaches 0
        }
    }

    /// Get arena for specific group (if active)
    pub fn get_active_arena(&self, group_id: &str) -> Option<Arc<ConsumerArena>> {
        self.group_arenas.get(group_id).cloned()
    }

    /// Get manager statistics
    pub fn get_manager_stats(&self) -> ManagerStats {
        let active_groups: Vec<String> = self.group_arenas.keys().cloned().collect();

        ManagerStats {
            active_groups: active_groups.len(),
            pooled_arenas: self.arena_pool.len(),
            total_managed_arenas: active_groups.len() + self.arena_pool.len(),
            group_ids: active_groups,
            next_arena_id: self.next_arena_id.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct ManagerStats {
    pub active_groups: usize,
    pub pooled_arenas: usize,
    pub total_managed_arenas: usize,
    pub group_ids: Vec<String>,
    pub next_arena_id: usize,
}

impl ManagerStats {
    pub fn report(&self) -> String {
        format!(
            "Consumer Arena Manager: {} active groups, {} pooled arenas, {} total managed (next_id: {})",
            self.active_groups,
            self.pooled_arenas,
            self.total_managed_arenas,
            self.next_arena_id
        )
    }
}

/// RAII wrapper for consumer group rebalancing session
pub struct ConsumerRebalanceSession {
    arena: Arc<ConsumerArena>,
    group_id: String,
    session_start: std::time::Instant,
}

impl ConsumerRebalanceSession {
    pub fn new(arena: Arc<ConsumerArena>, group_id: String) -> Self {
        debug!(
            "🚀 CONSUMER REBALANCE: Started session '{}' for group '{}' with arena {}",
            arena.session_id(),
            group_id,
            arena.arena_id
        );

        Self {
            arena,
            group_id,
            session_start: std::time::Instant::now(),
        }
    }

    /// Access the underlying arena
    pub fn arena(&self) -> &ConsumerArena {
        &self.arena
    }

    /// Get session duration so far
    pub fn session_duration(&self) -> std::time::Duration {
        self.session_start.elapsed()
    }

    /// Allocate assignment map for rebalancing
    pub fn allocate_assignments(
        &self,
        consumer_count: usize,
    ) -> Option<HashMap<ConsumerId, Vec<TopicPartition>>> {
        self.arena.allocate_assignment_map(consumer_count)
    }

    /// Allocate topic partitions map
    pub fn allocate_topic_partitions(
        &self,
        topic_count: usize,
    ) -> Option<HashMap<String, Vec<TopicPartition>>> {
        self.arena.allocate_topic_partition_map(topic_count)
    }

    /// Allocate consumer list
    pub fn allocate_consumer_list(&self, capacity: usize) -> Option<Vec<ConsumerId>> {
        self.arena.allocate_consumer_vec(capacity)
    }

    /// Allocate partition list  
    pub fn allocate_partition_list(&self, capacity: usize) -> Option<Vec<TopicPartition>> {
        self.arena.allocate_partition_vec(capacity)
    }
}

impl Drop for ConsumerRebalanceSession {
    fn drop(&mut self) {
        let duration = self.session_duration();
        let stats = self.arena.get_stats();

        debug!(
            "🏁 CONSUMER REBALANCE: Completed session '{}' for group '{}' in {:.2}ms - {} allocations, {:.1}% arena utilization",
            self.arena.session_id(),
            self.group_id,
            duration.as_secs_f64() * 1000.0,
            stats.allocation_count,
            stats.utilization
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_arena_basic_allocation() {
        let arena =
            ConsumerArena::new(8192, 1, "test-group".to_string(), "session-1".to_string()).unwrap();

        // Test basic allocation
        let ptr = arena.allocate(100, 1).unwrap();
        assert!(!ptr.as_ptr().is_null());

        // Test stats
        let stats = arena.get_stats();
        assert_eq!(stats.allocation_count, 1);
        assert!(stats.used_bytes >= 100);
        assert_eq!(stats.group_id, "test-group");
    }

    #[test]
    fn test_consumer_arena_assignment_map() {
        let arena = ConsumerArena::new(32768, 2, "test-group".to_string(), "session-2".to_string())
            .unwrap();

        // Allocate assignment map
        let assignments = arena.allocate_assignment_map(10).unwrap();
        assert!(assignments.capacity() >= 10);

        let stats = arena.get_stats();
        assert_eq!(stats.allocation_count, 1);
    }

    #[test]
    fn test_consumer_arena_manager() {
        let mut manager = ConsumerArenaManager::new();

        // Get arena for group
        let arena = manager
            .get_arena_for_group("test-group".to_string(), "session-1".to_string(), 5)
            .unwrap();
        assert_eq!(arena.group_id(), "test-group");

        // Manager should track the active group
        let stats = manager.get_manager_stats();
        assert_eq!(stats.active_groups, 1);
        assert!(stats.group_ids.contains(&"test-group".to_string()));

        // Release the arena
        manager.release_arena_for_group("test-group");
        let stats_after = manager.get_manager_stats();
        assert_eq!(stats_after.active_groups, 0);
    }

    #[test]
    fn test_consumer_rebalance_session() {
        let arena = Arc::new(
            ConsumerArena::new(16384, 3, "test-group".to_string(), "session-3".to_string())
                .unwrap(),
        );
        let session = ConsumerRebalanceSession::new(arena.clone(), "test-group".to_string());

        // Test allocation through session
        let assignments = session.allocate_assignments(5).unwrap();
        assert!(assignments.capacity() >= 5);

        // Test session duration
        assert!(session.session_duration().as_nanos() > 0);
    }
}
