//! # FluxMQ Protocol Module
//!
//! This module implements the Kafka wire protocol and FluxMQ's native protocol,
//! providing 100% compatibility with Kafka clients while maintaining high performance.
//!
//! ## Protocol Support
//!
//! FluxMQ implements **20 Kafka APIs** with full wire protocol compatibility:
//!
//! ### Core APIs
//! - **API 0**: Produce - Message publishing with batching and compression
//! - **API 1**: Fetch - Message consumption with async notifications  
//! - **API 2**: ListOffsets - Offset querying with timestamp support
//! - **API 3**: Metadata - Topic/partition discovery and broker information
//!
//! ### Consumer Group APIs (8-16)
//! - **API 8**: OffsetCommit - Save consumer position
//! - **API 9**: OffsetFetch - Retrieve saved positions
//! - **API 10**: FindCoordinator - Locate group coordinator
//! - **API 11**: JoinGroup - Join consumer group with rebalancing
//! - **API 12**: Heartbeat - Maintain group membership
//! - **API 13**: LeaveGroup - Gracefully leave group
//! - **API 14**: SyncGroup - Synchronize partition assignments
//! - **API 15**: DescribeGroups - Get detailed group information
//! - **API 16**: ListGroups - List all consumer groups
//!
//! ### Security & Admin APIs
//! - **API 17**: SaslHandshake - SASL mechanism negotiation
//! - **API 18**: ApiVersions - Protocol version negotiation (KIP-482 support)
//! - **API 19**: CreateTopics - Programmatic topic creation
//! - **API 20**: DeleteTopics - Topic deletion and cleanup
//! - **API 32**: DescribeConfigs - Configuration inspection
//! - **API 33**: AlterConfigs - Dynamic configuration updates
//! - **API 36**: SaslAuthenticate - Complete SASL authentication
//!
//! ## Performance Features
//!
//! - **Zero-Copy Parsing**: Direct buffer manipulation without allocations
//! - **Batch Processing**: Handle multiple requests in single pass
//! - **Protocol Version Negotiation**: Optimal encoding based on client capabilities
//! - **High-Performance Codec**: Streaming decoder for large messages
//! - **KIP-482 Flexible Versions**: Tagged fields and compact encoding support
//!
//! ## Modules
//!
//! - [`messages`] - Protocol message definitions and types
//! - [`high_performance_codec`] - Optimized codec for maximum throughput
//! - [`kafka`] - Kafka-specific protocol implementation and compatibility layer

pub mod high_performance_codec;
pub mod kafka;
pub mod messages;
pub mod tests;

pub use messages::*;
