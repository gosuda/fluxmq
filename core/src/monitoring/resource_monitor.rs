//! System Resource Monitor
//!
//! This module provides real-time system resource monitoring including:
//! - CPU utilization and core-specific metrics
//! - Memory usage (RSS, VSZ, heap, stack)
//! - Disk I/O statistics and performance
//! - Network I/O bandwidth and packet rates
//! - File descriptor usage and limits

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// System resource metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemResourceSnapshot {
    pub timestamp: u64,
    pub cpu: CpuMetrics,
    pub memory: MemoryMetrics,
    pub disk: DiskMetrics,
    pub network: NetworkMetrics,
    pub process: ProcessMetrics,
    pub system: SystemMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuMetrics {
    pub total_usage_percent: f64,
    pub per_core_usage: Vec<f64>,
    pub load_average_1m: f64,
    pub load_average_5m: f64,
    pub load_average_15m: f64,
    pub context_switches_per_sec: u64,
    pub interrupts_per_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub total_mb: u64,
    pub available_mb: u64,
    pub used_mb: u64,
    pub used_percent: f64,
    pub cached_mb: u64,
    pub buffers_mb: u64,
    pub swap_total_mb: u64,
    pub swap_used_mb: u64,
    pub page_faults_per_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMetrics {
    pub read_bytes_per_sec: u64,
    pub write_bytes_per_sec: u64,
    pub read_ops_per_sec: u64,
    pub write_ops_per_sec: u64,
    pub avg_read_latency_ms: f64,
    pub avg_write_latency_ms: f64,
    pub disk_usage_percent: f64,
    pub available_space_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub rx_bytes_per_sec: u64,
    pub tx_bytes_per_sec: u64,
    pub rx_packets_per_sec: u64,
    pub tx_packets_per_sec: u64,
    pub rx_errors_per_sec: u64,
    pub tx_errors_per_sec: u64,
    pub connections_established: u64,
    pub connections_time_wait: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessMetrics {
    pub pid: u32,
    pub memory_rss_mb: u64,
    pub memory_vsz_mb: u64,
    pub cpu_user_percent: f64,
    pub cpu_system_percent: f64,
    pub open_files: u64,
    pub threads: u64,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub hostname: String,
    pub platform: String,
    pub kernel_version: String,
    pub uptime_seconds: u64,
    pub boot_time: u64,
    pub users_logged_in: u64,
}

/// Configuration for resource monitoring
#[derive(Debug, Clone)]
pub struct ResourceMonitorConfig {
    pub update_interval_ms: u64,
    pub history_retention_minutes: usize,
    pub enable_detailed_metrics: bool,
    pub enable_process_monitoring: bool,
}

impl Default for ResourceMonitorConfig {
    fn default() -> Self {
        Self {
            update_interval_ms: 5000, // 5 second updates
            history_retention_minutes: 60,
            enable_detailed_metrics: true,
            enable_process_monitoring: true,
        }
    }
}

/// System resource monitor
pub struct SystemResourceMonitor {
    config: ResourceMonitorConfig,
    current_snapshot: Arc<RwLock<Option<SystemResourceSnapshot>>>,
    history: Arc<RwLock<Vec<SystemResourceSnapshot>>>,
    process_id: u32,
}

impl SystemResourceMonitor {
    pub fn new(config: ResourceMonitorConfig) -> Self {
        Self {
            config,
            current_snapshot: Arc::new(RwLock::new(None)),
            history: Arc::new(RwLock::new(Vec::new())),
            process_id: std::process::id(),
        }
    }

    /// Start resource monitoring background task
    pub async fn start_monitoring(self: Arc<Self>) {
        let monitor = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(monitor.config.update_interval_ms));

            loop {
                interval.tick().await;
                if let Err(e) = monitor.collect_resource_metrics().await {
                    warn!("Failed to collect resource metrics: {}", e);
                }
            }
        });

        debug!(
            "System resource monitoring started with {}ms interval",
            self.config.update_interval_ms
        );
    }

    /// Collect comprehensive resource metrics
    async fn collect_resource_metrics(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let snapshot = SystemResourceSnapshot {
            timestamp,
            cpu: self.collect_cpu_metrics().await?,
            memory: self.collect_memory_metrics().await?,
            disk: self.collect_disk_metrics().await?,
            network: self.collect_network_metrics().await?,
            process: self.collect_process_metrics().await?,
            system: self.collect_system_metrics().await?,
        };

        // Update current snapshot
        {
            let mut current = self.current_snapshot.write().await;
            *current = Some(snapshot.clone());
        }

        // Add to history
        {
            let mut history = self.history.write().await;
            history.push(snapshot);

            // Trim old history
            let cutoff_time = timestamp - (self.config.history_retention_minutes as u64 * 60);
            history.retain(|s| s.timestamp > cutoff_time);
        }

        Ok(())
    }

    /// Collect CPU metrics (simplified implementation)
    async fn collect_cpu_metrics(
        &self,
    ) -> Result<CpuMetrics, Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would read from /proc/stat, /proc/loadavg, etc.
        // For now, providing simulated metrics
        Ok(CpuMetrics {
            total_usage_percent: 25.0,
            per_core_usage: vec![23.0, 27.0, 24.0, 26.0, 25.0, 24.0, 28.0, 22.0], // 8 cores
            load_average_1m: 1.5,
            load_average_5m: 1.8,
            load_average_15m: 2.1,
            context_switches_per_sec: 5000,
            interrupts_per_sec: 3000,
        })
    }

    /// Collect memory metrics (simplified implementation)
    async fn collect_memory_metrics(
        &self,
    ) -> Result<MemoryMetrics, Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would read from /proc/meminfo
        Ok(MemoryMetrics {
            total_mb: 16384,
            available_mb: 12288,
            used_mb: 4096,
            used_percent: 25.0,
            cached_mb: 2048,
            buffers_mb: 512,
            swap_total_mb: 4096,
            swap_used_mb: 0,
            page_faults_per_sec: 100,
        })
    }

    /// Collect disk I/O metrics (simplified implementation)
    async fn collect_disk_metrics(
        &self,
    ) -> Result<DiskMetrics, Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would read from /proc/diskstats
        Ok(DiskMetrics {
            read_bytes_per_sec: 50 * 1024 * 1024,  // 50 MB/s
            write_bytes_per_sec: 30 * 1024 * 1024, // 30 MB/s
            read_ops_per_sec: 1000,
            write_ops_per_sec: 500,
            avg_read_latency_ms: 2.5,
            avg_write_latency_ms: 5.0,
            disk_usage_percent: 45.0,
            available_space_gb: 500,
        })
    }

    /// Collect network I/O metrics (simplified implementation)
    async fn collect_network_metrics(
        &self,
    ) -> Result<NetworkMetrics, Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would read from /proc/net/dev, /proc/net/tcp
        Ok(NetworkMetrics {
            rx_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            tx_bytes_per_sec: 80 * 1024 * 1024,  // 80 MB/s
            rx_packets_per_sec: 50000,
            tx_packets_per_sec: 40000,
            rx_errors_per_sec: 0,
            tx_errors_per_sec: 0,
            connections_established: 1000,
            connections_time_wait: 50,
        })
    }

    /// Collect process-specific metrics (simplified implementation)
    async fn collect_process_metrics(
        &self,
    ) -> Result<ProcessMetrics, Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would read from /proc/[pid]/stat, /proc/[pid]/status
        Ok(ProcessMetrics {
            pid: self.process_id,
            memory_rss_mb: 256,
            memory_vsz_mb: 512,
            cpu_user_percent: 15.0,
            cpu_system_percent: 10.0,
            open_files: 100,
            threads: 20,
            uptime_seconds: 3600, // 1 hour
        })
    }

    /// Collect system-wide metrics (simplified implementation)
    async fn collect_system_metrics(
        &self,
    ) -> Result<SystemMetrics, Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would use system APIs
        Ok(SystemMetrics {
            hostname: "fluxmq-server".to_string(),
            platform: std::env::consts::OS.to_string(),
            kernel_version: "6.1.0".to_string(),
            uptime_seconds: 86400, // 1 day
            boot_time: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() - 86400,
            users_logged_in: 2,
        })
    }

    /// Get the current resource snapshot
    pub async fn get_current_snapshot(&self) -> Option<SystemResourceSnapshot> {
        let current = self.current_snapshot.read().await;
        current.clone()
    }

    /// Get historical resource data
    pub async fn get_history(&self) -> Vec<SystemResourceSnapshot> {
        let history = self.history.read().await;
        history.clone()
    }

    /// Get resource utilization summary
    pub async fn get_utilization_summary(&self) -> ResourceUtilizationSummary {
        let snapshot = self.get_current_snapshot().await;

        match snapshot {
            Some(s) => ResourceUtilizationSummary {
                cpu_usage_percent: s.cpu.total_usage_percent,
                memory_usage_percent: s.memory.used_percent,
                disk_usage_percent: s.disk.disk_usage_percent,
                network_utilization_percent: self.calculate_network_utilization(&s.network),
                critical_alerts: self.check_critical_thresholds(&s),
            },
            None => ResourceUtilizationSummary::default(),
        }
    }

    /// Calculate network utilization as a percentage
    fn calculate_network_utilization(&self, network: &NetworkMetrics) -> f64 {
        // Assume 1 Gbps network interface = 125 MB/s
        let max_bandwidth_bytes = 125 * 1024 * 1024;
        let total_usage = network.rx_bytes_per_sec + network.tx_bytes_per_sec;
        (total_usage as f64 / max_bandwidth_bytes as f64) * 100.0
    }

    /// Check for critical resource thresholds
    fn check_critical_thresholds(&self, snapshot: &SystemResourceSnapshot) -> Vec<String> {
        let mut alerts = Vec::new();

        if snapshot.cpu.total_usage_percent > 80.0 {
            alerts.push("High CPU usage".to_string());
        }
        if snapshot.memory.used_percent > 85.0 {
            alerts.push("High memory usage".to_string());
        }
        if snapshot.disk.disk_usage_percent > 90.0 {
            alerts.push("High disk usage".to_string());
        }
        if snapshot.process.open_files > 800 {
            alerts.push("High file descriptor usage".to_string());
        }

        alerts
    }
}

/// Resource utilization summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilizationSummary {
    pub cpu_usage_percent: f64,
    pub memory_usage_percent: f64,
    pub disk_usage_percent: f64,
    pub network_utilization_percent: f64,
    pub critical_alerts: Vec<String>,
}

impl Default for ResourceUtilizationSummary {
    fn default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_percent: 0.0,
            disk_usage_percent: 0.0,
            network_utilization_percent: 0.0,
            critical_alerts: Vec::new(),
        }
    }
}
