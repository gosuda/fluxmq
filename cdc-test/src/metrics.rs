use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
pub struct Metrics {
    total_messages: Arc<AtomicU64>,
    total_errors: Arc<AtomicU64>,
    total_latency_ms: Arc<AtomicU64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            total_messages: Arc::new(AtomicU64::new(0)),
            total_errors: Arc::new(AtomicU64::new(0)),
            total_latency_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_success(&self, latency: Duration) {
        self.total_messages.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_messages(&self) -> u64 {
        self.total_messages.load(Ordering::Relaxed)
    }

    pub fn total_errors(&self) -> u64 {
        self.total_errors.load(Ordering::Relaxed)
    }

    pub fn avg_latency_ms(&self) -> f64 {
        let total = self.total_messages.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }

        let latency = self.total_latency_ms.load(Ordering::Relaxed);
        latency as f64 / total as f64
    }

    /// Start periodic metrics reporter
    pub fn start_reporter(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_count = 0u64;

            loop {
                interval.tick().await;

                let current = self.total_messages();
                let errors = self.total_errors();
                let rate = (current - last_count) / 10; // Messages per second
                let avg_latency = self.avg_latency_ms();

                info!(
                    total_messages = current,
                    total_errors = errors,
                    messages_per_sec = rate,
                    avg_latency_ms = format!("{:.2}", avg_latency),
                    "Metrics report"
                );

                last_count = current;
            }
        })
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
