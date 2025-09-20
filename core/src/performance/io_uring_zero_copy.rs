#![allow(dead_code)]
/// Ultra-high performance networking using Linux io_uring
///
/// This module implements kernel bypass networking using io_uring,
/// achieving the highest possible I/O performance on Linux systems.
///
/// Features:
/// - Kernel bypass networking with io_uring
/// - Zero-copy file-to-socket transfers
/// - Batch I/O operations for maximum efficiency
/// - Async completion without system call overhead
/// - Support for SQPOLL mode for ultra-low latency
use crate::Result;
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use io_uring::{opcode, types, CompletionQueue, IoUring, SubmissionQueue};

/// io_uring based ultra-high performance network handler
pub struct IoUringNetworkHandler {
    // io_uring instance
    #[cfg(target_os = "linux")]
    ring: IoUring,

    // Performance tracking
    operations_submitted: AtomicU64,
    operations_completed: AtomicU64,
    bytes_transferred: AtomicU64,

    // Configuration
    config: IoUringConfig,

    // Active operations tracking
    active_operations: HashMap<u64, IoUringOperation>,
    next_operation_id: AtomicU64,
}

/// Configuration for io_uring optimizations
#[derive(Debug, Clone)]
pub struct IoUringConfig {
    /// Queue depth (number of concurrent operations)
    pub queue_depth: u32,

    /// Enable SQPOLL mode for ultra-low latency
    pub sqpoll_mode: bool,

    /// CPU affinity for SQPOLL thread
    pub sqpoll_cpu: Option<u32>,

    /// Enable IOPOLL for storage devices that support polling
    pub iopoll_mode: bool,

    /// Batch size for submission queue
    pub batch_size: u32,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            queue_depth: 1024,   // 1K concurrent operations
            sqpoll_mode: true,   // Enable kernel polling for ultra-low latency
            sqpoll_cpu: Some(0), // Pin SQPOLL to CPU 0
            iopoll_mode: false,  // Disable by default (needs NVMe support)
            batch_size: 32,      // Submit 32 operations at once
        }
    }
}

/// Represents an active io_uring operation
#[derive(Debug)]
struct IoUringOperation {
    operation_id: u64,
    operation_type: IoUringOperationType,
    file_fd: Option<RawFd>,
    socket_fd: RawFd,
    bytes_expected: usize,
}

#[derive(Debug)]
pub enum IoUringOperationType {
    Sendfile { offset: u64, length: usize },
    Write { buffer_addr: u64, length: usize },
    Read { buffer_addr: u64, length: usize },
    Accept,
    Close,
}

impl IoUringNetworkHandler {
    /// Create new io_uring network handler
    #[cfg(target_os = "linux")]
    pub fn new() -> Result<Self> {
        Self::with_config(IoUringConfig::default())
    }

    #[cfg(target_os = "linux")]
    pub fn with_config(config: IoUringConfig) -> Result<Self> {
        // Create io_uring instance
        let mut builder = IoUring::builder();
        builder.setup_count(config.queue_depth);

        // Configure SQPOLL mode for ultra-low latency
        if config.sqpoll_mode {
            builder.setup_sqpoll(1000); // 1ms idle timeout
            if let Some(cpu) = config.sqpoll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
        }

        // Configure IOPOLL for high-performance storage
        if config.iopoll_mode {
            builder.setup_iopoll();
        }

        let ring = builder.build(config.queue_depth).map_err(|e| {
            crate::FluxmqError::Network(format!("Failed to create io_uring: {}", e))
        })?;

        tracing::info!(
            "Created io_uring with queue_depth={}, sqpoll={}, iopoll={}",
            config.queue_depth,
            config.sqpoll_mode,
            config.iopoll_mode
        );

        Ok(Self {
            ring,
            operations_submitted: AtomicU64::new(0),
            operations_completed: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            config,
            active_operations: HashMap::new(),
            next_operation_id: AtomicU64::new(1),
        })
    }

    /// Fallback constructor for non-Linux platforms
    #[cfg(not(target_os = "linux"))]
    pub fn new() -> Result<Self> {
        Err(crate::FluxmqError::Network(
            "io_uring is only supported on Linux".to_string(),
        ))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn with_config(_config: IoUringConfig) -> Result<Self> {
        Self::new()
    }

    /// Submit sendfile operation using io_uring
    #[cfg(target_os = "linux")]
    pub fn submit_sendfile(
        &mut self,
        file: &File,
        socket_fd: RawFd,
        offset: u64,
        length: usize,
    ) -> Result<u64> {
        let operation_id = self.next_operation_id.fetch_add(1, Ordering::Relaxed);
        let file_fd = file.as_raw_fd();

        // Create sendfile operation
        let sendfile_op = opcode::Sendfile::new(types::Fd(socket_fd), types::Fd(file_fd), offset)
            .len(length as u32)
            .build()
            .user_data(operation_id);

        // Submit to submission queue
        let submission_queue = self.ring.submission();
        unsafe {
            submission_queue.push(&sendfile_op).map_err(|e| {
                crate::FluxmqError::Network(format!("Failed to submit sendfile: {}", e))
            })?;
        }

        // Track operation
        self.active_operations.insert(
            operation_id,
            IoUringOperation {
                operation_id,
                operation_type: IoUringOperationType::Sendfile { offset, length },
                file_fd: Some(file_fd),
                socket_fd,
                bytes_expected: length,
            },
        );

        self.operations_submitted.fetch_add(1, Ordering::Relaxed);

        tracing::trace!(
            "Submitted io_uring sendfile: op_id={}, file_fd={}, socket_fd={}, offset={}, length={}",
            operation_id,
            file_fd,
            socket_fd,
            offset,
            length
        );

        Ok(operation_id)
    }

    /// Submit write operation using io_uring
    #[cfg(target_os = "linux")]
    pub fn submit_write(&mut self, socket_fd: RawFd, buffer: &[u8]) -> Result<u64> {
        let operation_id = self.next_operation_id.fetch_add(1, Ordering::Relaxed);

        // Create write operation
        let write_op =
            opcode::Write::new(types::Fd(socket_fd), buffer.as_ptr(), buffer.len() as u32)
                .build()
                .user_data(operation_id);

        // Submit to submission queue
        let submission_queue = self.ring.submission();
        unsafe {
            submission_queue.push(&write_op).map_err(|e| {
                crate::FluxmqError::Network(format!("Failed to submit write: {}", e))
            })?;
        }

        // Track operation
        self.active_operations.insert(
            operation_id,
            IoUringOperation {
                operation_id,
                operation_type: IoUringOperationType::Write {
                    buffer_addr: buffer.as_ptr() as u64,
                    length: buffer.len(),
                },
                file_fd: None,
                socket_fd,
                bytes_expected: buffer.len(),
            },
        );

        self.operations_submitted.fetch_add(1, Ordering::Relaxed);

        tracing::trace!(
            "Submitted io_uring write: op_id={}, socket_fd={}, length={}",
            operation_id,
            socket_fd,
            buffer.len()
        );

        Ok(operation_id)
    }

    /// Submit read operation using io_uring
    #[cfg(target_os = "linux")]
    pub fn submit_read(&mut self, socket_fd: RawFd, buffer: &mut [u8]) -> Result<u64> {
        let operation_id = self.next_operation_id.fetch_add(1, Ordering::Relaxed);

        // Create read operation
        let read_op = opcode::Read::new(
            types::Fd(socket_fd),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
        .build()
        .user_data(operation_id);

        // Submit to submission queue
        let submission_queue = self.ring.submission();
        unsafe {
            submission_queue.push(&read_op).map_err(|e| {
                crate::FluxmqError::Network(format!("Failed to submit read: {}", e))
            })?;
        }

        // Track operation
        self.active_operations.insert(
            operation_id,
            IoUringOperation {
                operation_id,
                operation_type: IoUringOperationType::Read {
                    buffer_addr: buffer.as_ptr() as u64,
                    length: buffer.len(),
                },
                file_fd: None,
                socket_fd,
                bytes_expected: buffer.len(),
            },
        );

        self.operations_submitted.fetch_add(1, Ordering::Relaxed);

        tracing::trace!(
            "Submitted io_uring read: op_id={}, socket_fd={}, length={}",
            operation_id,
            socket_fd,
            buffer.len()
        );

        Ok(operation_id)
    }

    /// Process completion queue and return completed operations
    #[cfg(target_os = "linux")]
    pub fn process_completions(&mut self) -> Result<Vec<IoUringCompletion>> {
        let mut completions = Vec::new();
        let mut completion_queue = self.ring.completion();

        // Process all available completions
        while let Some(completion) = completion_queue.next() {
            let operation_id = completion.user_data();
            let result = completion.result();

            if let Some(operation) = self.active_operations.remove(&operation_id) {
                let bytes_transferred = if result >= 0 { result as u64 } else { 0 };

                self.operations_completed.fetch_add(1, Ordering::Relaxed);
                self.bytes_transferred
                    .fetch_add(bytes_transferred, Ordering::Relaxed);

                completions.push(IoUringCompletion {
                    operation_id,
                    operation_type: operation.operation_type,
                    result: result as i64,
                    bytes_transferred: bytes_transferred as usize,
                });

                tracing::trace!(
                    "Completed io_uring operation: op_id={}, result={}, bytes={}",
                    operation_id,
                    result,
                    bytes_transferred
                );
            } else {
                tracing::warn!(
                    "Received completion for unknown operation: {}",
                    operation_id
                );
            }
        }

        // Submit pending operations if queue not full
        if !completions.is_empty() {
            let _ = self.ring.submit();
        }

        Ok(completions)
    }

    /// Submit all pending operations to kernel
    #[cfg(target_os = "linux")]
    pub fn submit_and_wait(&mut self, min_complete: usize) -> Result<usize> {
        let submitted = self.ring.submit_and_wait(min_complete).map_err(|e| {
            crate::FluxmqError::Network(format!("io_uring submit_and_wait failed: {}", e))
        })?;

        tracing::trace!(
            "io_uring submitted {} operations, waiting for {} completions",
            submitted,
            min_complete
        );

        Ok(submitted)
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> IoUringStats {
        IoUringStats {
            operations_submitted: self.operations_submitted.load(Ordering::Relaxed),
            operations_completed: self.operations_completed.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            active_operations: self.active_operations.len(),
            queue_depth: self.config.queue_depth,
            sqpoll_enabled: self.config.sqpoll_mode,
        }
    }
}

/// Result of a completed io_uring operation
#[derive(Debug)]
pub struct IoUringCompletion {
    pub operation_id: u64,
    pub operation_type: IoUringOperationType,
    pub result: i64,
    pub bytes_transferred: usize,
}

/// Performance statistics for io_uring operations
#[derive(Debug, Clone)]
pub struct IoUringStats {
    pub operations_submitted: u64,
    pub operations_completed: u64,
    pub bytes_transferred: u64,
    pub active_operations: usize,
    pub queue_depth: u32,
    pub sqpoll_enabled: bool,
}

impl IoUringStats {
    pub fn completion_rate(&self) -> f64 {
        if self.operations_submitted > 0 {
            (self.operations_completed as f64) / (self.operations_submitted as f64)
        } else {
            0.0
        }
    }

    pub fn throughput_mbps(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            (self.bytes_transferred as f64 / duration_secs) / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }

    pub fn report(&self) -> String {
        format!(
            "io_uring Stats - Submitted: {}, Completed: {}, Transferred: {:.2} MB, Active: {}, Completion Rate: {:.1}%",
            self.operations_submitted,
            self.operations_completed,
            self.bytes_transferred as f64 / 1_000_000.0,
            self.active_operations,
            self.completion_rate() * 100.0
        )
    }
}

/// High-level interface for io_uring based message transfer
pub struct IoUringMessageTransfer {
    handler: IoUringNetworkHandler,
    transfer_stats: AtomicU64,
}

impl IoUringMessageTransfer {
    pub fn new() -> Result<Self> {
        Ok(Self {
            handler: IoUringNetworkHandler::new()?,
            transfer_stats: AtomicU64::new(0),
        })
    }

    /// Transfer messages from file to socket using io_uring sendfile
    #[cfg(target_os = "linux")]
    pub fn transfer_messages_async(
        &mut self,
        file: &File,
        socket_fd: RawFd,
        segments: Vec<(u64, usize)>, // (offset, length) pairs
    ) -> Result<Vec<u64>> {
        let mut operation_ids = Vec::with_capacity(segments.len());

        // Submit all sendfile operations
        for (offset, length) in segments {
            let op_id = self
                .handler
                .submit_sendfile(file, socket_fd, offset, length)?;
            operation_ids.push(op_id);
        }

        tracing::debug!(
            "Submitted {} io_uring sendfile operations to socket_fd={}",
            operation_ids.len(),
            socket_fd
        );

        Ok(operation_ids)
    }

    /// Wait for operations to complete and return results
    #[cfg(target_os = "linux")]
    pub fn wait_for_completions(&mut self, min_complete: usize) -> Result<Vec<IoUringCompletion>> {
        // Submit pending and wait for completions
        self.handler.submit_and_wait(min_complete)?;

        // Process completions
        let completions = self.handler.process_completions()?;

        self.transfer_stats
            .fetch_add(completions.len() as u64, Ordering::Relaxed);

        Ok(completions)
    }

    /// Get transfer statistics
    pub fn get_stats(&self) -> String {
        format!(
            "Message Transfer - Operations: {}, {}",
            self.transfer_stats.load(Ordering::Relaxed),
            self.handler.get_stats().report()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_uring_config() {
        let config = IoUringConfig::default();
        assert_eq!(config.queue_depth, 1024);
        assert!(config.sqpoll_mode);
        assert_eq!(config.sqpoll_cpu, Some(0));
        assert!(!config.iopoll_mode);
        assert_eq!(config.batch_size, 32);
    }

    #[test]
    fn test_io_uring_stats() {
        let stats = IoUringStats {
            operations_submitted: 100,
            operations_completed: 95,
            bytes_transferred: 1024 * 1024, // 1MB
            active_operations: 5,
            queue_depth: 1024,
            sqpoll_enabled: true,
        };

        assert_eq!(stats.completion_rate(), 0.95);
        assert_eq!(stats.throughput_mbps(1.0), 1.0); // 1MB in 1 second = 1 MB/s

        let report = stats.report();
        assert!(report.contains("Submitted: 100"));
        assert!(report.contains("Completed: 95"));
        assert!(report.contains("Completion Rate: 95.0%"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_io_uring_creation() {
        // This test requires Linux and may fail in some environments
        if let Ok(handler) = IoUringNetworkHandler::new() {
            let stats = handler.get_stats();
            assert_eq!(stats.operations_submitted, 0);
            assert_eq!(stats.operations_completed, 0);
            assert!(stats.sqpoll_enabled);
        }
    }
}
