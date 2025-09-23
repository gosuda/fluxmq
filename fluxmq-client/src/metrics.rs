//! Metrics collection for FluxMQ client

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Client metrics collector
#[derive(Debug)]
pub struct ClientMetrics {
    // Producer metrics
    pub records_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub send_errors: AtomicU64,
    pub send_latency_sum: AtomicU64,
    pub send_latency_count: AtomicU64,

    // Consumer metrics
    pub records_consumed: AtomicU64,
    pub bytes_consumed: AtomicU64,
    pub consume_errors: AtomicU64,
    pub fetch_latency_sum: AtomicU64,
    pub fetch_latency_count: AtomicU64,

    // Connection metrics
    pub connections_created: AtomicU64,
    pub connections_failed: AtomicU64,
    pub connection_errors: AtomicU64,
}

impl Default for ClientMetrics {
    fn default() -> Self {
        Self {
            records_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
            send_latency_sum: AtomicU64::new(0),
            send_latency_count: AtomicU64::new(0),
            records_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            consume_errors: AtomicU64::new(0),
            fetch_latency_sum: AtomicU64::new(0),
            fetch_latency_count: AtomicU64::new(0),
            connections_created: AtomicU64::new(0),
            connections_failed: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }
}

impl ClientMetrics {
    /// Record a successful send operation
    pub fn record_send(&self, record_count: u64, byte_count: u64, latency: Duration) {
        self.records_sent.fetch_add(record_count, Ordering::Relaxed);
        self.bytes_sent.fetch_add(byte_count, Ordering::Relaxed);
        self.send_latency_sum
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.send_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a send error
    pub fn record_send_error(&self) {
        self.send_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful consume operation
    pub fn record_consume(&self, record_count: u64, byte_count: u64, latency: Duration) {
        self.records_consumed
            .fetch_add(record_count, Ordering::Relaxed);
        self.bytes_consumed.fetch_add(byte_count, Ordering::Relaxed);
        self.fetch_latency_sum
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.fetch_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a consume error
    pub fn record_consume_error(&self) {
        self.consume_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful connection
    pub fn record_connection_created(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed connection
    pub fn record_connection_failed(&self) {
        self.connections_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection error
    pub fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get average send latency in microseconds
    pub fn average_send_latency_us(&self) -> f64 {
        let sum = self.send_latency_sum.load(Ordering::Relaxed);
        let count = self.send_latency_count.load(Ordering::Relaxed);

        if count == 0 {
            0.0
        } else {
            sum as f64 / count as f64
        }
    }

    /// Get average fetch latency in microseconds  
    pub fn average_fetch_latency_us(&self) -> f64 {
        let sum = self.fetch_latency_sum.load(Ordering::Relaxed);
        let count = self.fetch_latency_count.load(Ordering::Relaxed);

        if count == 0 {
            0.0
        } else {
            sum as f64 / count as f64
        }
    }

    /// Get snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            records_sent: self.records_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            average_send_latency_us: self.average_send_latency_us(),
            records_consumed: self.records_consumed.load(Ordering::Relaxed),
            bytes_consumed: self.bytes_consumed.load(Ordering::Relaxed),
            consume_errors: self.consume_errors.load(Ordering::Relaxed),
            average_fetch_latency_us: self.average_fetch_latency_us(),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_failed: self.connections_failed.load(Ordering::Relaxed),
            connection_errors: self.connection_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub records_sent: u64,
    pub bytes_sent: u64,
    pub send_errors: u64,
    pub average_send_latency_us: f64,
    pub records_consumed: u64,
    pub bytes_consumed: u64,
    pub consume_errors: u64,
    pub average_fetch_latency_us: f64,
    pub connections_created: u64,
    pub connections_failed: u64,
    pub connection_errors: u64,
}

/// Timing helper for measuring operation latency
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(self) -> Duration {
        self.start.elapsed()
    }
}

/// Global metrics instance
static GLOBAL_METRICS: once_cell::sync::Lazy<Arc<ClientMetrics>> =
    once_cell::sync::Lazy::new(|| Arc::new(ClientMetrics::default()));

/// Get the global metrics instance
pub fn global_metrics() -> Arc<ClientMetrics> {
    GLOBAL_METRICS.clone()
}
