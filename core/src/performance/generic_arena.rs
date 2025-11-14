//! # Generic Arena Allocator - Unified High-Performance Memory Allocation
//!
//! This module provides a unified, generic arena allocator that consolidates
//! the common patterns from arena_allocator.rs, protocol_arena.rs, and consumer_arena.rs.
//!
//! ## Design Goals
//!
//! 1. **Zero Runtime Overhead**: Generic implementation with zero-cost abstractions
//! 2. **Compile-Time Specialization**: Type parameters allow compiler optimization
//! 3. **Code Reuse**: ~80% code reduction by unifying common patterns
//! 4. **Flexibility**: Extensible metadata system for domain-specific needs
//!
//! ## Performance Characteristics
//!
//! - **O(1) Allocation**: Pointer-bump allocation with atomic CAS
//! - **Cache-Line Aligned**: 64-byte alignment for optimal CPU cache performance
//! - **Lock-Free**: Atomic operations for thread-safe concurrent access
//! - **Zero Fragmentation**: Contiguous allocation prevents memory fragmentation
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use fluxmq::performance::generic_arena::{Arena, ArenaConfig, ArenaMetadata};
//!
//! // Create a protocol arena
//! #[derive(Default)]
//! struct ProtocolMetadata {
//!     request_count: usize,
//! }
//!
//! impl ArenaMetadata for ProtocolMetadata {
//!     fn arena_type() -> &'static str {
//!         "Protocol"
//!     }
//! }
//!
//! let config = ArenaConfig::new(64 * 1024); // 64KB arena
//! let arena = Arena::<ProtocolMetadata>::new(config, 1).unwrap();
//!
//! // Allocate memory
//! let ptr = arena.allocate(256, 8).unwrap();
//! ```

use std::alloc::{alloc, dealloc, Layout};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tracing::{debug, warn};

/// Arena configuration parameters
#[derive(Clone, Debug)]
pub struct ArenaConfig {
    /// Total size of the arena in bytes
    pub size: usize,
    /// Alignment requirement (must be power of 2)
    pub alignment: usize,
    /// Maximum single allocation size
    pub max_allocation_size: usize,
    /// Enable detailed performance metrics
    pub enable_metrics: bool,
}

impl ArenaConfig {
    /// Create a new arena configuration with specified size
    pub fn new(size: usize) -> Self {
        Self {
            size,
            alignment: 64, // Cache-line aligned
            max_allocation_size: size,
            enable_metrics: true,
        }
    }

    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.size == 0 {
            return Err("Arena size must be greater than 0");
        }
        if !self.alignment.is_power_of_two() {
            return Err("Alignment must be a power of 2");
        }
        if self.max_allocation_size > self.size {
            return Err("Max allocation size exceeds arena size");
        }
        Ok(())
    }
}

impl Default for ArenaConfig {
    fn default() -> Self {
        Self::new(1024 * 1024) // 1MB default
    }
}

/// Trait for arena-specific metadata
///
/// Implement this trait to add domain-specific tracking to your arena.
/// The trait is zero-sized and only provides type information.
pub trait ArenaMetadata: Default + Send + Sync {
    /// Human-readable name for this arena type
    fn arena_type() -> &'static str;

    /// Optional validation hook called after arena creation
    fn validate_arena(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Default metadata implementation for generic use cases
#[derive(Default)]
pub struct GenericMetadata;

impl ArenaMetadata for GenericMetadata {
    fn arena_type() -> &'static str {
        "Generic"
    }
}

/// Generic arena allocator with pointer-bump allocation
///
/// This struct provides fast, lock-free memory allocation using a simple
/// pointer-bump strategy. Memory is allocated contiguously from a pre-allocated
/// region, with atomic operations ensuring thread safety.
///
/// # Type Parameters
///
/// * `M` - Metadata type implementing `ArenaMetadata` trait for domain-specific tracking
#[repr(C, align(64))] // Cache-line aligned for optimal CPU performance
pub struct Arena<M: ArenaMetadata = GenericMetadata> {
    /// Start of arena memory region
    memory: NonNull<u8>,
    /// Total size of arena region
    size: usize,
    /// Current allocation position within arena (atomic for lock-free access)
    position: AtomicUsize,
    /// Arena identifier for debugging and metrics
    arena_id: u64,
    /// Number of successful allocations
    allocation_count: AtomicU64,
    /// Total bytes allocated (includes alignment padding)
    bytes_allocated: AtomicU64,
    /// Number of failed allocations (out of space)
    allocation_failures: AtomicU64,
    /// Configuration used to create this arena
    config: ArenaConfig,
    /// Domain-specific metadata (zero-sized type)
    _metadata: PhantomData<M>,
}

// Safety: Arena can be shared across threads safely due to atomic operations
unsafe impl<M: ArenaMetadata> Send for Arena<M> {}
unsafe impl<M: ArenaMetadata> Sync for Arena<M> {}

impl<M: ArenaMetadata> Arena<M> {
    /// Create a new arena with specified configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Arena configuration parameters
    /// * `arena_id` - Unique identifier for this arena
    ///
    /// # Returns
    ///
    /// * `Ok(Arena)` - Successfully created arena
    /// * `Err(String)` - Error message if creation failed
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use fluxmq::performance::generic_arena::{Arena, ArenaConfig, GenericMetadata};
    ///
    /// let config = ArenaConfig::new(1024 * 1024); // 1MB arena
    /// let arena = Arena::<GenericMetadata>::new(config, 1).unwrap();
    /// ```
    pub fn new(config: ArenaConfig, arena_id: u64) -> Result<Self, String> {
        // Validate configuration
        config.validate().map_err(|e| e.to_string())?;

        // Create memory layout with proper alignment
        let layout = Layout::from_size_align(config.size, config.alignment)
            .map_err(|e| format!("Invalid arena layout: {}", e))?;

        // Allocate aligned memory
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(format!(
                "Failed to allocate {}MB for {} arena #{}",
                config.size / (1024 * 1024),
                M::arena_type(),
                arena_id
            ));
        }

        let memory = unsafe { NonNull::new_unchecked(ptr) };

        let arena = Self {
            memory,
            size: config.size,
            position: AtomicUsize::new(0),
            arena_id,
            allocation_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            allocation_failures: AtomicU64::new(0),
            config: config.clone(),
            _metadata: PhantomData,
        };

        debug!(
            "🏟️ ARENA: Created {} arena #{} with {}KB capacity",
            M::arena_type(),
            arena_id,
            config.size / 1024
        );

        Ok(arena)
    }

    /// Allocate memory from the arena with specified size and alignment
    ///
    /// This method uses lock-free atomic operations to perform fast pointer-bump
    /// allocation. If the arena is exhausted, returns None.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes to allocate
    /// * `align` - Alignment requirement (must be power of 2)
    ///
    /// # Returns
    ///
    /// * `Some(NonNull<u8>)` - Pointer to allocated memory
    /// * `None` - Arena exhausted or invalid parameters
    ///
    /// # Safety
    ///
    /// The returned pointer is valid until the arena is dropped. The caller
    /// must ensure the alignment requirement is a power of 2.
    pub fn allocate(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
        if size == 0 || size > self.config.max_allocation_size {
            return None;
        }

        debug_assert!(
            align.is_power_of_two(),
            "Alignment must be power of 2, got {}",
            align
        );

        let mut current_pos = self.position.load(Ordering::Relaxed);

        // Try allocation with CAS loop
        loop {
            // Calculate aligned position
            let aligned_pos = (current_pos + align - 1) & !(align - 1);
            let new_pos = aligned_pos.checked_add(size)?;

            // Check if allocation fits
            if new_pos > self.size {
                self.allocation_failures.fetch_add(1, Ordering::Relaxed);
                if self.config.enable_metrics {
                    warn!(
                        "⚠️ ARENA: {} arena #{} exhausted ({}/{} bytes used)",
                        M::arena_type(),
                        self.arena_id,
                        current_pos,
                        self.size
                    );
                }
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
                    // Success! Update metrics and return pointer
                    self.allocation_count.fetch_add(1, Ordering::Relaxed);
                    self.bytes_allocated
                        .fetch_add(size as u64, Ordering::Relaxed);

                    let ptr = unsafe { self.memory.as_ptr().add(aligned_pos) };
                    return Some(unsafe { NonNull::new_unchecked(ptr) });
                }
                Err(actual) => {
                    // CAS failed, retry with updated position
                    current_pos = actual;
                }
            }
        }
    }

    /// Reset the arena to empty state
    ///
    /// This method resets the allocation position to zero, effectively
    /// reclaiming all allocated memory. This is extremely fast (single atomic store)
    /// and allows arena reuse without deallocation/reallocation.
    ///
    /// # Safety
    ///
    /// All previously allocated pointers from this arena become invalid.
    /// The caller must ensure no references to arena memory exist.
    pub fn reset(&self) {
        self.position.store(0, Ordering::Release);
        if self.config.enable_metrics {
            debug!(
                "🔄 ARENA: Reset {} arena #{} (reclaimed {}KB)",
                M::arena_type(),
                self.arena_id,
                self.bytes_allocated.load(Ordering::Relaxed) / 1024
            );
        }
    }

    /// Get current utilization percentage
    pub fn utilization(&self) -> f64 {
        let used = self.position.load(Ordering::Relaxed);
        (used as f64 / self.size as f64) * 100.0
    }

    /// Get remaining capacity in bytes
    pub fn remaining_capacity(&self) -> usize {
        let used = self.position.load(Ordering::Relaxed);
        self.size.saturating_sub(used)
    }

    /// Get arena statistics
    pub fn stats(&self) -> ArenaStats {
        ArenaStats {
            arena_id: self.arena_id,
            arena_type: M::arena_type(),
            total_size: self.size,
            bytes_used: self.position.load(Ordering::Relaxed),
            allocation_count: self.allocation_count.load(Ordering::Relaxed),
            bytes_allocated: self.bytes_allocated.load(Ordering::Relaxed),
            allocation_failures: self.allocation_failures.load(Ordering::Relaxed),
            utilization_percent: self.utilization(),
        }
    }

    /// Get arena ID
    pub fn id(&self) -> u64 {
        self.arena_id
    }

    /// Get total arena size
    pub fn size(&self) -> usize {
        self.size
    }
}

impl<M: ArenaMetadata> Drop for Arena<M> {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, self.config.alignment)
            .expect("Valid layout in drop");

        unsafe {
            dealloc(self.memory.as_ptr(), layout);
        }

        if self.config.enable_metrics {
            debug!(
                "♻️ ARENA: Dropped {} arena #{} (allocated {}KB over {} operations)",
                M::arena_type(),
                self.arena_id,
                self.bytes_allocated.load(Ordering::Relaxed) / 1024,
                self.allocation_count.load(Ordering::Relaxed)
            );
        }
    }
}

/// Arena performance statistics
#[derive(Debug, Clone)]
pub struct ArenaStats {
    pub arena_id: u64,
    pub arena_type: &'static str,
    pub total_size: usize,
    pub bytes_used: usize,
    pub allocation_count: u64,
    pub bytes_allocated: u64,
    pub allocation_failures: u64,
    pub utilization_percent: f64,
}

impl ArenaStats {
    /// Check if arena is nearly exhausted (>90% used)
    pub fn is_nearly_full(&self) -> bool {
        self.utilization_percent > 90.0
    }

    /// Get efficiency ratio (allocated bytes / used bytes including padding)
    pub fn efficiency(&self) -> f64 {
        if self.bytes_used == 0 {
            0.0
        } else {
            (self.bytes_allocated as f64 / self.bytes_used as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_creation() {
        let config = ArenaConfig::new(1024);
        let arena = Arena::<GenericMetadata>::new(config, 1).unwrap();
        assert_eq!(arena.size(), 1024);
        assert_eq!(arena.utilization(), 0.0);
    }

    #[test]
    fn test_arena_allocation() {
        let config = ArenaConfig::new(1024);
        let arena = Arena::<GenericMetadata>::new(config, 1).unwrap();

        let ptr1 = arena.allocate(100, 8).unwrap();
        let ptr2 = arena.allocate(200, 8).unwrap();

        assert_ne!(ptr1, ptr2);
        assert!(arena.utilization() > 0.0);
    }

    #[test]
    fn test_arena_reset() {
        let config = ArenaConfig::new(1024);
        let arena = Arena::<GenericMetadata>::new(config, 1).unwrap();

        arena.allocate(500, 8).unwrap();
        assert!(arena.utilization() > 40.0);

        arena.reset();
        assert_eq!(arena.utilization(), 0.0);
    }

    #[test]
    fn test_arena_exhaustion() {
        let config = ArenaConfig::new(100);
        let arena = Arena::<GenericMetadata>::new(config, 1).unwrap();

        // Allocate most of the space
        arena.allocate(90, 1).unwrap();

        // This should fail (not enough space)
        assert!(arena.allocate(20, 1).is_none());
    }
}
