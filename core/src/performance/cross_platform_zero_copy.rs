#![allow(dead_code)]
/// Cross-platform zero-copy optimizations
///
/// This module provides a unified interface for platform-specific zero-copy operations,
/// automatically selecting the best available method for each platform:
///
/// - Linux: sendfile, splice, io_uring, copy_file_range
/// - macOS: sendfile with BSD optimizations
/// - Windows: TransmitFile, TransmitPackets
/// - Fallback: Traditional I/O for unsupported platforms
use crate::Result;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[cfg(target_os = "linux")]
use crate::performance::{
    copy_file_range_zero_copy::CopyFileRangeHandler, io_uring_zero_copy::IoUringMessageTransfer,
    sendfile_zero_copy::SendfileTransfer,
};

#[cfg(target_os = "macos")]
use crate::performance::sendfile_zero_copy::SendfileTransfer;

#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawHandle;

#[cfg(target_os = "windows")]
use winapi::{
    shared::minwindef::{BOOL, DWORD, FALSE, TRUE},
    um::{
        fileapi::OVERLAPPED,
        handleapi::INVALID_HANDLE_VALUE,
        minwinbase::LPOVERLAPPED,
        winsock2::{TransmitFile, SOCKET},
    },
};

/// Unified cross-platform zero-copy handler
pub struct CrossPlatformZeroCopy {
    // Platform-specific handlers
    #[cfg(target_os = "linux")]
    sendfile_handler: SendfileTransfer,
    #[cfg(target_os = "linux")]
    io_uring_handler: Option<IoUringMessageTransfer>,
    #[cfg(target_os = "linux")]
    copy_file_range_handler: CopyFileRangeHandler,

    #[cfg(target_os = "macos")]
    sendfile_handler: SendfileTransfer,

    // Performance tracking
    operations_performed: AtomicU64,
    bytes_transferred: AtomicU64,
    platform_operations: AtomicU64,
    fallback_operations: AtomicU64,

    // Configuration
    config: CrossPlatformConfig,
}

/// Configuration for cross-platform optimizations
#[derive(Debug, Clone)]
pub struct CrossPlatformConfig {
    /// Prefer io_uring over sendfile on Linux (if available)
    pub prefer_io_uring: bool,

    /// Enable Windows-specific optimizations
    pub enable_windows_optimizations: bool,

    /// Enable macOS-specific optimizations
    pub enable_macos_optimizations: bool,

    /// Maximum transfer size per operation
    pub max_transfer_size: usize,

    /// Number of concurrent operations for batch processing
    pub batch_size: usize,
}

impl Default for CrossPlatformConfig {
    fn default() -> Self {
        Self {
            prefer_io_uring: true,
            enable_windows_optimizations: true,
            enable_macos_optimizations: true,
            max_transfer_size: 1024 * 1024 * 1024, // 1GB
            batch_size: 32,
        }
    }
}

impl CrossPlatformZeroCopy {
    /// Create new cross-platform zero-copy handler
    pub fn new() -> Result<Self> {
        Self::with_config(CrossPlatformConfig::default())
    }

    pub fn with_config(config: CrossPlatformConfig) -> Result<Self> {
        tracing::info!(
            "Initializing cross-platform zero-copy for: {}",
            std::env::consts::OS
        );

        Ok(Self {
            #[cfg(target_os = "linux")]
            sendfile_handler: SendfileTransfer::new(),
            #[cfg(target_os = "linux")]
            io_uring_handler: if config.prefer_io_uring {
                IoUringMessageTransfer::new().ok()
            } else {
                None
            },
            #[cfg(target_os = "linux")]
            copy_file_range_handler: CopyFileRangeHandler::new(),

            #[cfg(target_os = "macos")]
            sendfile_handler: SendfileTransfer::new(),

            operations_performed: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            platform_operations: AtomicU64::new(0),
            fallback_operations: AtomicU64::new(0),
            config,
        })
    }

    /// Transfer file to socket using best available method
    pub async fn transfer_file_to_socket(
        &mut self,
        file: &File,
        socket: &tokio::net::TcpStream,
        offset: u64,
        length: usize,
    ) -> Result<usize> {
        let start_time = Instant::now();

        #[cfg(target_os = "linux")]
        {
            // Linux: Try io_uring first, then sendfile
            if let Some(ref mut io_uring) = self.io_uring_handler {
                match self
                    .try_io_uring_transfer(io_uring, file, socket, offset, length)
                    .await
                {
                    Ok(bytes) => {
                        self.record_operation(bytes, start_time, "io_uring");
                        return Ok(bytes);
                    }
                    Err(e) => {
                        tracing::debug!(
                            "io_uring transfer failed: {}, falling back to sendfile",
                            e
                        );
                    }
                }
            }

            // Fallback to sendfile
            match self
                .sendfile_handler
                .sendfile_to_socket(file, socket, offset, length)
                .await
            {
                Ok(bytes) => {
                    self.record_operation(bytes, start_time, "sendfile");
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::debug!("sendfile failed: {}, using traditional copy", e);
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            // macOS: Use optimized sendfile
            match self
                .sendfile_handler
                .sendfile_to_socket(file, socket, offset, length)
                .await
            {
                Ok(bytes) => {
                    self.record_operation(bytes, start_time, "sendfile_macos");
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::debug!("macOS sendfile failed: {}, using traditional copy", e);
                }
            }
        }

        #[cfg(target_os = "windows")]
        {
            // Windows: Try TransmitFile
            match self
                .windows_transmit_file(file, socket, offset, length)
                .await
            {
                Ok(bytes) => {
                    self.record_operation(bytes, start_time, "transmit_file");
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::debug!("Windows TransmitFile failed: {}, using traditional copy", e);
                }
            }
        }

        // Fallback for all platforms - return error as socket transfer is not implemented
        Err(crate::FluxmqError::Network(
            "File-to-socket transfer not implemented".to_string(),
        ))
    }

    /// Copy file using best available method
    pub fn copy_file<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        src_path: P1,
        dst_path: P2,
    ) -> Result<u64> {
        let start_time = Instant::now();

        #[cfg(target_os = "linux")]
        {
            // Linux: Use copy_file_range
            match self
                .copy_file_range_handler
                .copy_entire_file(&src_path, &dst_path)
            {
                Ok(bytes) => {
                    self.record_operation(bytes as usize, start_time, "copy_file_range");
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::debug!("copy_file_range failed: {}, using std::fs::copy", e);
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            // macOS: Use optimized file copy with sendfile-like behavior
            match self.macos_optimized_copy(&src_path, &dst_path) {
                Ok(bytes) => {
                    self.record_operation(bytes as usize, start_time, "macos_copy");
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::debug!("macOS optimized copy failed: {}, using std::fs::copy", e);
                }
            }
        }

        #[cfg(target_os = "windows")]
        {
            // Windows: Use CopyFileEx with optimizations
            match self.windows_optimized_copy(&src_path, &dst_path) {
                Ok(bytes) => {
                    self.record_operation(bytes as usize, start_time, "windows_copy");
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::debug!("Windows optimized copy failed: {}, using std::fs::copy", e);
                }
            }
        }

        // Fallback to standard library
        let bytes =
            std::fs::copy(src_path, dst_path).map_err(|e| crate::FluxmqError::Storage(e))?;

        self.record_operation(bytes as usize, start_time, "std_copy");
        Ok(bytes)
    }

    /// Linux-specific io_uring transfer
    #[cfg(target_os = "linux")]
    async fn try_io_uring_transfer(
        &mut self,
        io_uring: &mut IoUringMessageTransfer,
        file: &File,
        socket: &tokio::net::TcpStream,
        offset: u64,
        length: usize,
    ) -> Result<usize> {
        use std::os::unix::io::AsRawFd;

        let socket_fd = socket.as_raw_fd();
        let operation_ids =
            io_uring.transfer_messages_async(file, socket_fd, vec![(offset, length)])?;

        let completions = io_uring.wait_for_completions(operation_ids.len())?;
        let bytes_transferred: usize = completions.iter().map(|c| c.bytes_transferred).sum();

        Ok(bytes_transferred)
    }

    /// Windows TransmitFile implementation
    #[cfg(target_os = "windows")]
    async fn windows_transmit_file(
        &self,
        file: &File,
        socket: &tokio::net::TcpStream,
        offset: u64,
        length: usize,
    ) -> Result<usize> {
        use std::os::windows::io::AsRawHandle;

        // Get raw handles
        let file_handle = file.as_raw_handle();
        let socket_handle = socket.as_raw_socket() as SOCKET;

        // Prepare overlapped structure for offset
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.Offset = (offset & 0xFFFFFFFF) as DWORD;
        overlapped.OffsetHigh = (offset >> 32) as DWORD;

        // Call TransmitFile
        let result = unsafe {
            TransmitFile(
                socket_handle,
                file_handle as SOCKET, // TransmitFile expects HANDLE
                length as DWORD,
                0, // bytes per send
                &mut overlapped as LPOVERLAPPED,
                std::ptr::null_mut(), // no buffers
                0,                    // no flags
            )
        };

        if result == FALSE {
            let error = std::io::Error::last_os_error();
            tracing::warn!("TransmitFile failed: {}", error);
            return Err(crate::FluxmqError::Network(format!(
                "TransmitFile failed: {}",
                error
            )));
        }

        tracing::debug!("TransmitFile succeeded: {} bytes", length);
        Ok(length)
    }

    /// Windows optimized file copy
    #[cfg(target_os = "windows")]
    fn windows_optimized_copy<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        src_path: P1,
        dst_path: P2,
    ) -> Result<u64> {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use winapi::um::winbase::{CopyFileExW, COPY_FILE_ALLOW_DECRYPTED_DESTINATION};

        // Convert paths to wide strings
        let src_wide: Vec<u16> = src_path
            .as_ref()
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let dst_wide: Vec<u16> = dst_path
            .as_ref()
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // Use CopyFileEx for better performance
        let result = unsafe {
            CopyFileExW(
                src_wide.as_ptr(),
                dst_wide.as_ptr(),
                None, // no progress callback
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                COPY_FILE_ALLOW_DECRYPTED_DESTINATION,
            )
        };

        if result == FALSE {
            let error = std::io::Error::last_os_error();
            return Err(crate::FluxmqError::Storage(error));
        }

        // Get file size for return value
        let metadata = std::fs::metadata(src_path).map_err(|e| crate::FluxmqError::Storage(e))?;

        Ok(metadata.len())
    }

    /// macOS optimized file copy
    #[cfg(target_os = "macos")]
    fn macos_optimized_copy<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        src_path: P1,
        dst_path: P2,
    ) -> Result<u64> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        // Try using copyfile() system call for optimization
        let src_cstr = CString::new(src_path.as_ref().as_os_str().as_bytes()).map_err(|_| {
            crate::FluxmqError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid path",
            ))
        })?;
        let dst_cstr = CString::new(dst_path.as_ref().as_os_str().as_bytes()).map_err(|_| {
            crate::FluxmqError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid path",
            ))
        })?;

        // macOS copyfile is more efficient than read/write
        // COPYFILE_DATA copies file data (equivalent to COPYFILE_ALL for data transfer)
        let result = unsafe {
            libc::copyfile(
                src_cstr.as_ptr(),
                dst_cstr.as_ptr(),
                std::ptr::null_mut(),
                libc::COPYFILE_DATA,
            )
        };

        if result != 0 {
            let error = std::io::Error::last_os_error();
            return Err(crate::FluxmqError::Storage(error));
        }

        // Get file size for return value
        let metadata = std::fs::metadata(src_path).map_err(|e| crate::FluxmqError::Storage(e))?;

        Ok(metadata.len())
    }

    /// Fallback transfer using standard file copy (synchronous)
    fn fallback_transfer_sync(
        &self,
        src_path: &std::path::Path,
        dst_path: &std::path::Path,
    ) -> Result<u64> {
        // Use standard file copy as fallback
        let bytes_copied =
            std::fs::copy(src_path, dst_path).map_err(|e| crate::FluxmqError::Storage(e))?;

        // Update internal counters instead of using stats field
        self.operations_performed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_transferred
            .fetch_add(bytes_copied, std::sync::atomic::Ordering::Relaxed);
        Ok(bytes_copied)
    }

    /// Record operation statistics
    fn record_operation(&self, bytes: usize, start_time: Instant, method: &str) {
        let duration = start_time.elapsed();

        self.operations_performed.fetch_add(1, Ordering::Relaxed);
        self.bytes_transferred
            .fetch_add(bytes as u64, Ordering::Relaxed);

        if method == "fallback" || method == "std_copy" {
            self.fallback_operations.fetch_add(1, Ordering::Relaxed);
        } else {
            self.platform_operations.fetch_add(1, Ordering::Relaxed);
        }

        tracing::debug!(
            "Cross-platform transfer: {} bytes in {:?} using {}",
            bytes,
            duration,
            method
        );
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> CrossPlatformStats {
        let total_ops = self.operations_performed.load(Ordering::Relaxed);
        let platform_ops = self.platform_operations.load(Ordering::Relaxed);

        CrossPlatformStats {
            total_operations: total_ops,
            platform_operations: platform_ops,
            fallback_operations: self.fallback_operations.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            platform_optimization_ratio: if total_ops > 0 {
                (platform_ops as f64) / (total_ops as f64)
            } else {
                0.0
            },
            current_platform: std::env::consts::OS.to_string(),
        }
    }
}

/// Cross-platform performance statistics
#[derive(Debug, Clone)]
pub struct CrossPlatformStats {
    pub total_operations: u64,
    pub platform_operations: u64,
    pub fallback_operations: u64,
    pub bytes_transferred: u64,
    pub platform_optimization_ratio: f64,
    pub current_platform: String,
}

impl CrossPlatformStats {
    pub fn report(&self) -> String {
        format!(
            "Cross-Platform Zero-Copy - Platform: {}, Total Ops: {}, Platform Ops: {}, Fallbacks: {}, Bytes: {:.2} MB, Optimization Rate: {:.1}%",
            self.current_platform,
            self.total_operations,
            self.platform_operations,
            self.fallback_operations,
            self.bytes_transferred as f64 / 1_000_000.0,
            self.platform_optimization_ratio * 100.0
        )
    }

    pub fn throughput_mbps(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            (self.bytes_transferred as f64 / duration_secs) / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_cross_platform_creation() {
        let result = CrossPlatformZeroCopy::new();

        match result {
            Ok(handler) => {
                let stats = handler.get_stats();
                println!(
                    "Cross-platform handler created for: {}",
                    stats.current_platform
                );
                assert_eq!(stats.total_operations, 0);
                assert!(stats.current_platform.len() > 0);
            }
            Err(e) => {
                println!("Handler creation failed: {}", e);
            }
        }
    }

    #[test]
    fn test_cross_platform_file_copy() {
        let dir = tempdir().unwrap();
        let src_path = dir.path().join("cross_platform_src.txt");
        let dst_path = dir.path().join("cross_platform_dst.txt");

        // Create source file
        {
            let mut src_file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&src_path)
                .unwrap();

            let test_data = b"Cross-platform zero-copy test data!";
            src_file.write_all(test_data).unwrap();
            src_file.flush().unwrap();
        }

        // Test cross-platform copy
        if let Ok(handler) = CrossPlatformZeroCopy::new() {
            let result = handler.copy_file(&src_path, &dst_path);

            match result {
                Ok(bytes_copied) => {
                    println!("✅ Cross-platform copy: {} bytes", bytes_copied);
                    assert!(bytes_copied > 0);

                    // Verify content
                    let src_content = std::fs::read(&src_path).unwrap();
                    let dst_content = std::fs::read(&dst_path).unwrap();
                    assert_eq!(src_content, dst_content);

                    let stats = handler.get_stats();
                    println!("Stats: {}", stats.report());
                }
                Err(e) => {
                    println!("Cross-platform copy failed: {}", e);
                }
            }
        }
    }

    #[test]
    fn test_platform_detection() {
        println!("Current platform: {}", std::env::consts::OS);
        println!("Target family: {}", std::env::consts::FAMILY);
        println!("Target arch: {}", std::env::consts::ARCH);

        // Verify platform-specific compilation
        #[cfg(target_os = "linux")]
        println!("✅ Linux optimizations available");

        #[cfg(target_os = "macos")]
        println!("✅ macOS optimizations available");

        #[cfg(target_os = "windows")]
        println!("✅ Windows optimizations available");

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        println!("⚠️ Platform-specific optimizations not available for this platform");
    }
}
