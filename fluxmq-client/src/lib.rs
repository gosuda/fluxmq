//! # FluxMQ Client Library
//!
//! A high-performance, async Rust client for FluxMQ message broker.
//!
//! ## Features
//!
//! - **High Performance**: Zero-copy operations with bytes::Bytes
//! - **Async/Await**: Built on tokio for non-blocking I/O
//! - **Connection Pooling**: Efficient connection reuse and management
//! - **Auto-Partitioning**: Hash-based and round-robin partition assignment
//! - **Batch Operations**: Efficient message batching for throughput
//! - **Type Safety**: Strong typing with comprehensive error handling
//! - **Observability**: Built-in metrics and tracing support
//!
//! ## Quick Start
//!
//! ### Producer Example
//!
//! ```rust,no_run
//! use fluxmq_client::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let producer = ProducerBuilder::new()
//!         .brokers(vec!["localhost:9092"])
//!         .build()
//!         .await?;
//!     
//!     let record = ProduceRecord::builder()
//!         .topic("my-topic")
//!         .key("user-123")
//!         .value("Hello FluxMQ!")
//!         .build();
//!     
//!     let metadata = producer.send(record).await?;
//!     println!("Message sent to partition {} at offset {}",
//!              metadata.partition, metadata.offset);
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Consumer Example
//!
//! ```rust,no_run  
//! use fluxmq_client::*;
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let consumer = ConsumerBuilder::new()
//!         .brokers(vec!["localhost:9092"])
//!         .group_id("my-consumer-group")
//!         .topics(vec!["my-topic"])
//!         .build()
//!         .await?;
//!     
//!     let mut stream = consumer.stream();
//!     while let Some(record) = stream.next().await {
//!         match record {
//!             Ok(record) => {
//!                 println!("Received: key={:?}, value={}",
//!                          record.key, String::from_utf8_lossy(&record.value));
//!                 consumer.commit_sync().await?;
//!             }
//!             Err(e) => eprintln!("Error receiving message: {}", e),
//!         }
//!     }
//!     
//!     Ok(())
//! }
//! ```

pub mod admin;
pub mod client;
pub mod config;
pub mod connection;
pub mod consumer;
pub mod error;
pub mod metrics;
pub mod producer;
pub mod protocol;

pub use admin::*;
pub use client::*;
pub use config::*;
pub use consumer::*;
pub use error::*;
pub use producer::*;
pub use protocol::{ConsumeRecord, ProduceRecord, TopicPartition};

/// Client library result type
pub type Result<T> = std::result::Result<T, FluxmqClientError>;

/// Client library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
