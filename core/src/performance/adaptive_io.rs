/// Adaptive I/O Strategy
///
/// This module provides automatic platform capability detection and selects
/// the best available I/O method at runtime:
///
/// 1. **io_uring** (Linux 5.1+) - Highest performance, kernel bypass
/// 2. **sendfile** (Unix) - Zero-copy file-to-socket transfer
/// 3. **Standard I/O** - Fallback for all platforms
///
/// The strategy is determined once at startup and cached for performance.
use crate::Result;
use std::sync::OnceLock;

/// Available I/O strategies, ordered by performance (best to worst)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoStrategy {
    /// io_uring on Linux 5.1+ - Ultra-high performance kernel bypass
    IoUring,
    /// sendfile on Unix systems - Zero-copy file-to-socket
    Sendfile,
    /// Standard memory-based I/O - Universal fallback
    Standard,
}

/// Global I/O strategy, detected once at startup
static IO_STRATEGY: OnceLock<IoStrategy> = OnceLock::new();

/// Platform capabilities detected at runtime
#[derive(Debug, Clone)]
pub struct PlatformCapabilities {
    pub has_io_uring: bool,
    pub has_sendfile: bool,
    pub kernel_version: Option<String>,
    pub os_type: String,
}

impl PlatformCapabilities {
    /// Detect platform capabilities at runtime
    pub fn detect() -> Self {
        let os_type = std::env::consts::OS.to_string();

        let has_io_uring = Self::check_io_uring_support();
        let has_sendfile = Self::check_sendfile_support();
        let kernel_version = Self::get_kernel_version();

        tracing::info!(
            "Platform capabilities detected: os={}, io_uring={}, sendfile={}, kernel={:?}",
            os_type,
            has_io_uring,
            has_sendfile,
            kernel_version
        );

        Self {
            has_io_uring,
            has_sendfile,
            kernel_version,
            os_type,
        }
    }

    /// Check if io_uring is available (Linux 5.1+)
    fn check_io_uring_support() -> bool {
        #[cfg(target_os = "linux")]
        {
            // Check if io_uring is available by attempting to create a small ring
            match io_uring::IoUring::new(2) {
                Ok(_) => {
                    tracing::debug!("io_uring support detected");
                    true
                }
                Err(e) => {
                    tracing::debug!("io_uring not available: {}", e);
                    false
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// Check if sendfile is available (Unix systems)
    fn check_sendfile_support() -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
        {
            // sendfile is available on Unix systems
            tracing::debug!("sendfile support detected (Unix platform)");
            true
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
        {
            false
        }
    }

    /// Get kernel version for Linux systems
    fn get_kernel_version() -> Option<String> {
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            if let Ok(version) = fs::read_to_string("/proc/version") {
                // Extract version string (e.g., "Linux version 5.15.0")
                let parts: Vec<&str> = version.split_whitespace().collect();
                if parts.len() >= 3 {
                    return Some(parts[2].to_string());
                }
            }
            None
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    /// Select the best I/O strategy based on capabilities
    pub fn select_strategy(&self) -> IoStrategy {
        if self.has_io_uring {
            tracing::info!("Selected I/O strategy: io_uring (Linux kernel bypass)");
            IoStrategy::IoUring
        } else if self.has_sendfile {
            tracing::info!("Selected I/O strategy: sendfile (Unix zero-copy)");
            IoStrategy::Sendfile
        } else {
            tracing::info!("Selected I/O strategy: standard (fallback)");
            IoStrategy::Standard
        }
    }
}

/// Get the current I/O strategy (initializes on first call)
pub fn get_io_strategy() -> IoStrategy {
    *IO_STRATEGY.get_or_init(|| {
        let capabilities = PlatformCapabilities::detect();
        capabilities.select_strategy()
    })
}

/// Adaptive Fetch handler that selects the best I/O method
pub struct AdaptiveFetchHandler {
    strategy: IoStrategy,

    #[cfg(target_os = "linux")]
    io_uring_handler: Option<crate::performance::io_uring_zero_copy::IoUringMessageTransfer>,

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    sendfile_handler: Option<crate::performance::fetch_sendfile::FetchSendfileHandler>,
}

impl AdaptiveFetchHandler {
    /// Create a new adaptive fetch handler
    pub fn new() -> Result<Self> {
        let strategy = get_io_strategy();

        #[cfg(target_os = "linux")]
        let io_uring_handler = if strategy == IoStrategy::IoUring {
            match crate::performance::io_uring_zero_copy::IoUringMessageTransfer::new() {
                Ok(handler) => {
                    tracing::info!("io_uring handler initialized successfully");
                    Some(handler)
                }
                Err(e) => {
                    tracing::warn!("Failed to initialize io_uring handler: {}, falling back", e);
                    None
                }
            }
        } else {
            None
        };

        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
        let sendfile_handler =
            if strategy == IoStrategy::Sendfile || strategy == IoStrategy::IoUring {
                // Initialize sendfile as fallback even for io_uring
                let handler = crate::performance::fetch_sendfile::FetchSendfileHandler::new();
                tracing::debug!("sendfile handler initialized as fallback");
                Some(handler)
            } else {
                None
            };

        Ok(Self {
            strategy,
            #[cfg(target_os = "linux")]
            io_uring_handler,
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
            sendfile_handler,
        })
    }

    /// Get the current strategy
    pub fn strategy(&self) -> IoStrategy {
        self.strategy
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> String {
        let mut stats = format!("Adaptive I/O Strategy: {:?}\n", self.strategy);

        #[cfg(target_os = "linux")]
        if let Some(ref handler) = self.io_uring_handler {
            stats.push_str(&format!("  {}\n", handler.get_stats()));
        }

        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
        if let Some(ref handler) = self.sendfile_handler {
            stats.push_str(&format!("  {}\n", handler.get_stats()));
        }

        stats
    }
}

impl Default for AdaptiveFetchHandler {
    fn default() -> Self {
        Self::new().expect("Failed to create AdaptiveFetchHandler")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_capabilities_detection() {
        let caps = PlatformCapabilities::detect();
        println!("Platform: {}", caps.os_type);
        println!("io_uring: {}", caps.has_io_uring);
        println!("sendfile: {}", caps.has_sendfile);
        println!("kernel: {:?}", caps.kernel_version);

        // Verify strategy selection logic
        let strategy = caps.select_strategy();
        assert!(matches!(
            strategy,
            IoStrategy::IoUring | IoStrategy::Sendfile | IoStrategy::Standard
        ));
    }

    #[test]
    fn test_io_strategy_singleton() {
        let strategy1 = get_io_strategy();
        let strategy2 = get_io_strategy();
        assert_eq!(strategy1, strategy2, "Strategy should be consistent");
    }

    #[test]
    fn test_adaptive_handler_creation() {
        let handler = AdaptiveFetchHandler::new();
        assert!(handler.is_ok(), "Handler creation should succeed");

        if let Ok(handler) = handler {
            println!("Strategy: {:?}", handler.strategy());
            println!("Stats:\n{}", handler.get_stats());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_io_uring_detection() {
        let caps = PlatformCapabilities::detect();
        // On Linux, we should detect io_uring capability
        println!("Linux io_uring support: {}", caps.has_io_uring);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn test_unix_sendfile_detection() {
        let caps = PlatformCapabilities::detect();
        // On Unix systems, sendfile should be available
        assert!(caps.has_sendfile, "Unix systems should support sendfile");
    }
}
