//! # FluxMQ Broker Module
//!
//! This module provides the core message broker functionality including TCP server
//! implementation, request handling, and client connection management.
//!
//! ## Architecture
//!
//! The broker module is built around two main components:
//!
//! - [`server`] - TCP server that handles incoming client connections and manages
//!   the async event loop for processing Kafka protocol requests
//! - [`handler`] - Message handler that processes individual Kafka API requests
//!   and coordinates with storage, consumer groups, and replication systems
//!
//! ## Key Features
//!
//! - **High-Performance Async I/O**: Built on tokio for non-blocking operations
//! - **Kafka Protocol Compatibility**: Full wire protocol support for 20 Kafka APIs
//! - **Connection Management**: Efficient handling of thousands of concurrent connections
//! - **Request Processing**: Optimized request routing and response generation
//! - **Error Handling**: Comprehensive error recovery and client notification
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use fluxmq::{BrokerServer, BrokerConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = BrokerConfig {
//!         port: 9092,
//!         host: "0.0.0.0".to_string(),
//!         enable_consumer_groups: true,
//!         enable_tls: false,
//!         data_dir: Some("./data".into()),
//!         ..Default::default()
//!     };
//!     
//!     println!("Starting FluxMQ broker on {}:{}", config.host, config.port);
//!     let server = BrokerServer::new(config).await?;
//!     
//!     // This will run until the server is stopped
//!     server.run().await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Performance Characteristics
//!
//! The broker is optimized for high throughput and low latency:
//!
//! - **Throughput**: 600,000+ messages/second with advanced optimizations
//! - **Latency**: Sub-millisecond response times for most operations
//! - **Concurrency**: Supports 10,000+ concurrent client connections
//! - **Memory Usage**: Bounded memory with efficient buffer management
//!
//! ## Client Compatibility
//!
//! The broker is compatible with all major Kafka client libraries:
//!
//! - Java: `org.apache.kafka:kafka-clients`
//! - Python: `kafka-python`
//! - Node.js: `kafkajs`
//! - Go: `sarama`, `confluent-kafka-go`
//! - Scala: Native Kafka clients
//!
//! ## Configuration
//!
//! The broker supports extensive configuration options through [`BrokerConfig`]:
//!
//! ```rust,no_run
//! use fluxmq::BrokerConfig;
//!
//! let config = BrokerConfig {
//!     port: 9092,
//!     host: "localhost".to_string(),
//!     
//!     // Enable advanced features
//!     enable_consumer_groups: true,
//!     enable_tls: true,
//!     enable_acl: true,
//!     
//!     // TLS configuration
//!     tls_cert: Some("server.crt".into()),
//!     tls_key: Some("server.key".into()),
//!     
//!     // Storage configuration
//!     data_dir: Some("./fluxmq-data".into()),
//!     segment_size: 1024 * 1024 * 1024, // 1GB segments
//!     
//!     // Performance tuning
//!     max_connections: 10000,
//!     buffer_size: 64 * 1024, // 64KB buffers
//!     
//!     ..Default::default()
//! };
//! ```
//!
//! ## Monitoring and Metrics
//!
//! The broker exposes comprehensive metrics for operational monitoring:
//!
//! ```rust,no_run
//! use fluxmq::{BrokerServer, HttpMetricsServer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let broker = BrokerServer::new(Default::default()).await?;
//!     
//!     // Start HTTP metrics endpoint
//!     let metrics_server = HttpMetricsServer::new(8080, broker.metrics()).await?;
//!     
//!     // Metrics available at http://localhost:8080/metrics
//!     tokio::spawn(async move {
//!         metrics_server.run().await
//!     });
//!     
//!     broker.run().await?;
//!     Ok(())
//! }
//! ```

pub mod handler;
pub mod server;

pub use handler::*;
pub use server::*;
