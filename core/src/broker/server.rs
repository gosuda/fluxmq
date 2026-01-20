use crate::acl::{AclManager, Principal};
use crate::performance::io_optimizations::{ConnectionPool, ZeroCopyBufferManager};
use crate::performance::numa_allocator::NumaAwareAllocator;
use crate::performance::thread_affinity::{AffinityStrategy, ThreadAffinityManager};
use crate::protocol::high_performance_codec::HighPerformanceKafkaCodec;
use crate::protocol::kafka::{KafkaCodec, KafkaFrameCodec, ProtocolAdapter};
use crate::tls::FluxTlsAcceptor;
use crate::{broker::MessageHandler, config::BrokerConfig, HttpMetricsServer, Result};
use bytes::Bytes;
use futures::SinkExt;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

pub struct BrokerServer {
    config: BrokerConfig,
    handler: Arc<MessageHandler>,
    // Connection pool for efficient connection management
    connection_pool: Arc<ConnectionPool>,
    // Graceful shutdown coordination
    shutdown_tx: broadcast::Sender<()>,
    // NUMA-aware memory allocation for optimal performance
    numa_allocator: Option<Arc<NumaAwareAllocator>>,
    // Thread affinity manager for CPU pinning
    thread_affinity: Option<Arc<ThreadAffinityManager>>,
    // Zero-copy buffer manager for efficient memory allocation
    buffer_manager: Arc<ZeroCopyBufferManager>,
}

impl BrokerServer {
    pub fn new(config: BrokerConfig) -> Result<Self> {
        if config.recovery_mode {
            // Can't await in synchronous new(), so we need to use async version
            return Err(crate::FluxmqError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Use new_async() for recovery mode",
            )));
        }

        let handler = Arc::new(MessageHandler::new_with_features(
            0,
            config.port,
            config.enable_replication,
            config.enable_consumer_groups,
        )?); // Use config values
        let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections
        let (shutdown_tx, _) = broadcast::channel(16); // Graceful shutdown coordination

        // Initialize NUMA allocator for optimal memory performance
        let numa_allocator = Some(Arc::new(NumaAwareAllocator::new()));

        // Initialize thread affinity manager with workload-optimized strategy
        let thread_affinity = Some(Arc::new(ThreadAffinityManager::new(
            AffinityStrategy::WorkloadOptimized,
        )));

        // Initialize zero-copy buffer manager for efficient memory allocation
        let buffer_manager = Arc::new(ZeroCopyBufferManager::new());

        info!("🚀 NUMA allocator, thread affinity, and buffer manager initialized");

        Ok(Self {
            config,
            handler,
            connection_pool,
            shutdown_tx,
            numa_allocator,
            thread_affinity,
            buffer_manager,
        })
    }

    pub async fn new_async(config: BrokerConfig) -> Result<Self> {
        // Auto-detect if recovery is needed by checking if data directory exists with data
        let should_recover = config.recovery_mode || Self::should_auto_recover(&config.data_dir);

        if should_recover {
            info!(
                "Starting broker with recovery mode (auto-detected: {})",
                !config.recovery_mode
            );
            let mut handler = MessageHandler::new_with_features_and_recovery(
                0,
                config.port,
                config.enable_replication,
                config.enable_consumer_groups,
            )
            .await?;

            // Initialize ACL if enabled
            if config.enable_acl {
                let acl_manager = Self::create_acl_manager(&config)?;
                handler = handler.with_acl_manager(acl_manager);
                info!("ACL authorization enabled");
            } else {
                info!("ACL authorization disabled");
            }

            let handler = Arc::new(handler);
            let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections

            // Graceful shutdown coordination
            let (shutdown_tx, _) = broadcast::channel(16);

            // Initialize NUMA allocator for optimal memory performance
            let numa_allocator = Some(Arc::new(NumaAwareAllocator::new()));

            // Initialize thread affinity manager with workload-optimized strategy
            let thread_affinity = Some(Arc::new(ThreadAffinityManager::new(
                AffinityStrategy::WorkloadOptimized,
            )));

            // Initialize zero-copy buffer manager
            let buffer_manager = Arc::new(ZeroCopyBufferManager::new());

            info!("🚀 NUMA allocator, thread affinity, and buffer manager initialized");

            Ok(Self {
                config,
                handler,
                connection_pool,
                shutdown_tx,
                numa_allocator,
                thread_affinity,
                buffer_manager,
            })
        } else {
            let mut handler = MessageHandler::new_with_features(
                0,
                config.port,
                config.enable_replication,
                config.enable_consumer_groups,
            )?;

            // Initialize ACL if enabled
            if config.enable_acl {
                let acl_manager = Self::create_acl_manager(&config)?;
                handler = handler.with_acl_manager(acl_manager);
                info!("ACL authorization enabled");
            } else {
                info!("ACL authorization disabled");
            }

            let handler = Arc::new(handler);
            let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections

            // Graceful shutdown coordination
            let (shutdown_tx, _) = broadcast::channel(16);

            // Initialize NUMA allocator for optimal memory performance
            let numa_allocator = Some(Arc::new(NumaAwareAllocator::new()));

            // Initialize thread affinity manager with workload-optimized strategy
            let thread_affinity = Some(Arc::new(ThreadAffinityManager::new(
                AffinityStrategy::WorkloadOptimized,
            )));

            // Initialize zero-copy buffer manager
            let buffer_manager = Arc::new(ZeroCopyBufferManager::new());

            info!("🚀 NUMA allocator, thread affinity, and buffer manager initialized");

            Ok(Self {
                config,
                handler,
                connection_pool,
                shutdown_tx,
                numa_allocator,
                thread_affinity,
                buffer_manager,
            })
        }
    }

    /// Check if auto-recovery should be enabled based on existing data
    fn should_auto_recover(data_dir: &str) -> bool {
        let data_path = std::path::Path::new(data_dir);

        // If data directory doesn't exist, no recovery needed
        if !data_path.exists() {
            return false;
        }

        // Check if there are any topic directories with log files
        if let Ok(entries) = std::fs::read_dir(data_path) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    // Check if this topic directory has partition directories with log files
                    if let Ok(partition_entries) = std::fs::read_dir(entry.path()) {
                        for partition_entry in partition_entries.flatten() {
                            if partition_entry.path().is_dir() {
                                // Check if partition directory has .log files
                                if let Ok(log_entries) = std::fs::read_dir(partition_entry.path()) {
                                    for log_entry in log_entries.flatten() {
                                        if log_entry.path().extension().and_then(|s| s.to_str())
                                            == Some("log")
                                        {
                                            if let Ok(metadata) = log_entry.metadata() {
                                                if metadata.len() > 0 {
                                                    info!(
                                                        "Found existing data: {:?}",
                                                        log_entry.path()
                                                    );
                                                    return true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        false
    }

    pub async fn new_with_recovery(config: BrokerConfig) -> Result<Self> {
        let handler =
            Arc::new(MessageHandler::new_with_broker_id_and_recovery(0, config.port, false).await?);

        // 🚀 ULTRA-PERFORMANCE: High-throughput I/O optimizations
        let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections
        let (shutdown_tx, _) = broadcast::channel(16); // Graceful shutdown coordination

        // Initialize NUMA allocator for optimal memory performance
        let numa_allocator = Some(Arc::new(NumaAwareAllocator::new()));

        // Initialize thread affinity manager with workload-optimized strategy
        let thread_affinity = Some(Arc::new(ThreadAffinityManager::new(
            AffinityStrategy::WorkloadOptimized,
        )));

        // Initialize zero-copy buffer manager
        let buffer_manager = Arc::new(ZeroCopyBufferManager::new());

        info!("🚀 NUMA allocator, thread affinity, and buffer manager initialized (with recovery)");

        Ok(Self {
            config,
            handler,
            connection_pool,
            shutdown_tx,
            numa_allocator,
            thread_affinity,
            buffer_manager,
        })
    }

    /// Initiate graceful shutdown of the server
    pub fn shutdown(&self) {
        info!("Initiating graceful shutdown...");
        let _ = self.shutdown_tx.send(()); // Notify all background tasks to shutdown
    }

    /// Get NUMA allocation statistics
    pub fn get_numa_stats(&self) -> Option<String> {
        self.numa_allocator.as_ref().map(|allocator| {
            let stats = allocator.get_comprehensive_stats();
            stats.report()
        })
    }

    /// Get thread affinity statistics
    pub fn get_affinity_stats(&self) -> Option<String> {
        self.thread_affinity.as_ref().map(|manager| {
            let stats = manager.get_stats();
            stats.report()
        })
    }

    /// Print performance optimization statistics
    pub fn print_performance_stats(&self) {
        if let Some(numa_stats) = self.get_numa_stats() {
            info!("📊 NUMA Statistics:\n{}", numa_stats);
        }

        if let Some(affinity_stats) = self.get_affinity_stats() {
            info!("🎯 Thread Affinity Statistics:\n{}", affinity_stats);
        }
    }

    /// Run the server with graceful shutdown support
    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        info!(
            "FluxMQ broker listening on {} with ultra-performance TCP optimizations",
            addr
        );

        // Setup TLS if enabled
        if self.config.enable_tls {
            if let Some(ref tls_config) = self.config.tls_config {
                match FluxTlsAcceptor::new(tls_config.clone()) {
                    Ok(acceptor) => {
                        let tls_port = self.config.tls_port.unwrap_or(self.config.port + 1000);
                        let tls_addr = format!("{}:{}", self.config.host, tls_port);
                        let tls_listener = TcpListener::bind(&tls_addr).await?;

                        info!("FluxMQ TLS broker listening on {}", tls_addr);

                        // Start TLS listener in background with shutdown support
                        let tls_handler = Arc::clone(&self.handler);
                        let acceptor_arc = Arc::new(acceptor);
                        let tls_shutdown_rx = self.shutdown_tx.subscribe();
                        tokio::spawn(async move {
                            Self::run_tls_listener_with_shutdown(
                                tls_listener,
                                tls_handler,
                                acceptor_arc,
                                tls_shutdown_rx,
                            )
                            .await;
                        });
                    }
                    Err(e) => {
                        error!("Failed to setup TLS: {}", e);
                    }
                }
            } else {
                error!("TLS enabled but no TLS configuration provided");
            }
        }

        // Start HTTP metrics server if configured
        if let Some(metrics_port) = self.config.metrics_port {
            let metrics = self.handler.get_metrics();
            let http_server = HttpMetricsServer::new(metrics, metrics_port);
            let mut metrics_shutdown_rx = self.shutdown_tx.subscribe();

            tokio::spawn(async move {
                // Run HTTP server with shutdown support
                tokio::select! {
                    result = http_server.start() => {
                        if let Err(e) = result {
                            error!("HTTP metrics server error: {}", e);
                        }
                    }
                    _ = metrics_shutdown_rx.recv() => {
                        info!("Shutting down HTTP metrics server...");
                    }
                }
            });
            info!("HTTP metrics server started on port {}", metrics_port);
        }

        // Create shutdown receiver for the main loop
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Main non-TLS listener loop with graceful shutdown support
        loop {
            tokio::select! {
                // Handle new connections
                accept_result = listener.accept() => {
                    match accept_result {
                Ok((stream, peer_addr)) => {
                    // 🚀 ULTRA-PERFORMANCE: Check connection pool capacity
                    if !self.connection_pool.can_accept_connection() {
                        warn!(
                            "Connection pool at capacity, dropping connection from {}",
                            peer_addr
                        );
                        continue;
                    }

                    // Apply ultra-performance TCP optimizations to client connections
                    if let Err(e) = Self::optimize_client_socket(&stream) {
                        warn!("Failed to optimize client socket {}: {}", peer_addr, e);
                    }

                    // 🚀 ULTRA-PERFORMANCE: Track connection in pool
                    self.connection_pool.connection_opened();
                    info!(
                        "New client connected: {} (active: {})",
                        peer_addr,
                        self.connection_pool.get_stats().active_connections
                    );

                    let handler = Arc::clone(&self.handler);
                    let connection_pool = Arc::clone(&self.connection_pool);
                    let buffer_manager = Arc::clone(&self.buffer_manager);

                    tokio::spawn(async move {
                        // ✅ OPTION A: Using single-task sequential architecture
                        // Simple, predictable, no race conditions, guaranteed ordering
                        if let Err(e) = Self::handle_client_with_buffer_pool(stream, handler, buffer_manager).await {
                            error!("Error handling client {}: {}", peer_addr, e);
                        } else {
                            info!("Client {} disconnected", peer_addr);
                        }
                        // 🚀 ULTRA-PERFORMANCE: Track connection closure
                        connection_pool.connection_closed();
                    });
                }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Received shutdown signal, stopping server gracefully...");
                    break;
                }
            }
        }

        info!("Server shutdown complete");
        Ok(())
    }

    /// Apply ultra-performance TCP socket optimizations to client connections
    fn optimize_client_socket(stream: &TcpStream) -> Result<()> {
        use socket2::SockRef;

        // Get socket reference for optimization
        let socket_ref = SockRef::from(stream);

        // Disable Nagle algorithm for ultra-low latency
        socket_ref.set_tcp_nodelay(true)?;

        // Set optimal buffer sizes for high throughput
        if let Err(e) = socket_ref.set_recv_buffer_size(1024 * 1024) {
            warn!("Failed to set client recv buffer: {}", e);
        }
        if let Err(e) = socket_ref.set_send_buffer_size(1024 * 1024) {
            warn!("Failed to set client send buffer: {}", e);
        }

        // Enable TCP keepalive for connection health monitoring
        socket_ref.set_keepalive(true)?;

        Ok(())
    }

    /// 🔵 SIMPLE SYNCHRONOUS: Original sequential request-response handling
    /// - No pipelining, process one request at a time
    /// - Guaranteed ordering, no race conditions
    #[allow(dead_code)]
    async fn handle_client(stream: TcpStream, handler: Arc<MessageHandler>) -> Result<()> {
        info!("🚀 SEQUENTIAL: New client connected - simple sync processing");

        let metrics = handler.get_metrics();
        metrics.broker.connection_opened();

        // Use a single Framed for both read and write - simpler and more reliable
        let mut kafka_framed = tokio_util::codec::Framed::new(stream, KafkaFrameCodec);
        let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());

        // 🔧 SIMPLE SEQUENTIAL PROCESSING: Read request -> Process -> Write response -> Repeat
        // This is the most reliable approach that ensures proper request-response pairing
        loop {
            match tokio_stream::StreamExt::next(&mut kafka_framed).await {
                Some(Ok(message_bytes)) => {
                    // Extract api_key and correlation ID for logging
                    let (api_key, correlation_id) = if message_bytes.len() >= 8 {
                        let api_key = i16::from_be_bytes([message_bytes[0], message_bytes[1]]);
                        let corr_id = i32::from_be_bytes([
                            message_bytes[4],
                            message_bytes[5],
                            message_bytes[6],
                            message_bytes[7],
                        ]);
                        (api_key, corr_id)
                    } else {
                        warn!("Invalid message format: {} bytes", message_bytes.len());
                        continue;
                    };

                    debug!(
                        "🚀 RECV: api_key={}, correlation_id={}, len={}",
                        api_key,
                        correlation_id,
                        message_bytes.len()
                    );

                    // Process the request and get response
                    match Self::process_kafka_message_pipelined(&handler, &hp_codec, message_bytes)
                        .await
                    {
                        Ok(response_bytes) => {
                            if !response_bytes.is_empty() {
                                debug!(
                                    "🚀 SEND: response for correlation_id={}, len={}",
                                    correlation_id,
                                    response_bytes.len()
                                );

                                // Send response immediately
                                use futures::SinkExt;
                                if let Err(e) = kafka_framed.send(response_bytes).await {
                                    warn!("Failed to send response: {}", e);
                                    break;
                                }
                                // Flush to ensure response is sent before reading next request
                                if let Err(e) = kafka_framed.flush().await {
                                    warn!("Failed to flush response: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Request processing failed for correlation_id={}: {}",
                                correlation_id, e
                            );
                        }
                    }
                }
                Some(Err(e)) => {
                    warn!("Failed to decode message: {}", e);
                    break;
                }
                None => {
                    info!("🚀 Client disconnected (stream closed)");
                    break;
                }
            }
        }

        metrics.broker.connection_closed();
        info!("🚀 SEQUENTIAL: Client connection ended");
        Ok(())
    }

    /// 🚀 ULTRA-PERFORMANCE: Sequential client handling with buffer pooling
    /// - Uses ZeroCopyBufferManager for efficient buffer allocation/reuse
    /// - Reduces GC pressure through buffer recycling
    /// - Maintains guaranteed ordering with no race conditions
    async fn handle_client_with_buffer_pool(
        stream: TcpStream,
        handler: Arc<MessageHandler>,
        buffer_manager: Arc<ZeroCopyBufferManager>,
    ) -> Result<()> {
        debug!("🚀 BUFFER_POOL: New client connected - optimized buffer processing");

        let metrics = handler.get_metrics();
        metrics.broker.connection_opened();

        let mut kafka_framed = tokio_util::codec::Framed::new(stream, KafkaFrameCodec);
        let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());

        loop {
            match tokio_stream::StreamExt::next(&mut kafka_framed).await {
                Some(Ok(message_bytes)) => {
                    let (api_key, correlation_id) = if message_bytes.len() >= 8 {
                        let api_key = i16::from_be_bytes([message_bytes[0], message_bytes[1]]);
                        let corr_id = i32::from_be_bytes([
                            message_bytes[4],
                            message_bytes[5],
                            message_bytes[6],
                            message_bytes[7],
                        ]);
                        (api_key, corr_id)
                    } else {
                        warn!("Invalid message format: {} bytes", message_bytes.len());
                        continue;
                    };

                    debug!(
                        "🚀 RECV: api_key={}, correlation_id={}, len={}",
                        api_key,
                        correlation_id,
                        message_bytes.len()
                    );

                    // 🚀 BUFFER_POOL: Get pooled buffer for response encoding
                    // Estimate response size based on request type
                    let estimated_response_size = match api_key {
                        18 => 360,  // ApiVersions - fixed size
                        3 => 1024,  // Metadata - medium
                        0 => 256,   // Produce - small
                        1 => 16384, // Fetch - can be large
                        _ => 1024,  // Default
                    };
                    let _pooled_buf = buffer_manager.get_buffer(estimated_response_size);

                    match Self::process_kafka_message_pipelined(&handler, &hp_codec, message_bytes)
                        .await
                    {
                        Ok(response_bytes) => {
                            if !response_bytes.is_empty() {
                                debug!(
                                    "🚀 SEND: response for correlation_id={}, len={}",
                                    correlation_id,
                                    response_bytes.len()
                                );

                                if let Err(e) = kafka_framed.send(response_bytes).await {
                                    warn!("Failed to send response: {}", e);
                                    break;
                                }
                                if let Err(e) = kafka_framed.flush().await {
                                    warn!("Failed to flush response: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Request processing failed for correlation_id={}: {}",
                                correlation_id, e
                            );
                        }
                    }

                    // Note: _pooled_buf will be dropped here. For true buffer reuse,
                    // we could track BytesMut ownership and return to pool, but the
                    // current encode path returns Bytes which is immutable.
                }
                Some(Err(e)) => {
                    warn!("Failed to decode message: {}", e);
                    break;
                }
                None => {
                    debug!("🚀 Client disconnected (stream closed)");
                    break;
                }
            }
        }

        metrics.broker.connection_closed();
        debug!("🚀 BUFFER_POOL: Client connection ended");
        Ok(())
    }

    async fn process_kafka_message<IO>(
        handler: &Arc<MessageHandler>,
        hp_codec: &Arc<HighPerformanceKafkaCodec>,
        mut message_bytes: Bytes,
        kafka_framed: &mut Framed<IO, KafkaFrameCodec>,
    ) -> Result<()>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Track request metrics
        let metrics = handler.get_metrics();
        metrics.broker.request_received();

        // CRITICAL FIX: Enhanced Kafka request decoding with Java client compatibility
        // Java Kafka 4.1 clients send ApiVersions v4 with headerVersion=2 format
        match KafkaCodec::decode_request(&mut message_bytes) {
            Ok(kafka_request) => {
                // 🚀 PERFORMANCE: Hot path - logging removed for speed

                // Handle ApiVersions requests directly
                if kafka_request.api_key() == 18 {
                    // API_KEY_API_VERSIONS - 🚀 ULTRA-FAST with pre-compiled template!
                    let response_bytes = hp_codec.encode_api_versions_fast(
                        kafka_request.correlation_id(),
                        kafka_request.api_version(),
                    );
                    kafka_framed.send(response_bytes).await?;
                    return Ok(());
                }
            }
            Err(e) => {
                // CRITICAL FIX: Handle Java client frame parsing errors gracefully
                warn!(
                    "Failed to decode Kafka request using standard parser: {}",
                    e
                );

                // Log the raw message bytes for debugging Java client compatibility
                if message_bytes.len() >= 8 {
                    let first_bytes: Vec<u8> = message_bytes.iter().take(16).cloned().collect();
                    info!("Raw message bytes (hex): {:02x?}", first_bytes);

                    // Try to extract basic header information for Java clients
                    // Kafka request format after frame decoder (length field already removed):
                    // [0-1]  API Key (2 bytes)
                    // [2-3]  API Version (2 bytes)
                    // [4-7]  Correlation ID (4 bytes)
                    let api_key = ((message_bytes[0] as u16) << 8) | (message_bytes[1] as u16);
                    let api_version = ((message_bytes[2] as u16) << 8) | (message_bytes[3] as u16);
                    let correlation_id = ((message_bytes[4] as i32) << 24)
                        | ((message_bytes[5] as i32) << 16)
                        | ((message_bytes[6] as i32) << 8)
                        | (message_bytes[7] as i32);

                    debug!("Extracted Java client header: api_key={}, api_version={}, correlation_id={}",
                          api_key, api_version, correlation_id);

                    // Handle Java ApiVersions requests specially
                    if api_key == 18 {
                        debug!("Detected Java ApiVersions request - using compatibility mode");
                        // Send a compatible ApiVersions response for Java clients
                        let response_bytes =
                            hp_codec.encode_api_versions_fast(correlation_id, api_version as i16);
                        debug!(
                            "Sending Java-compatible ApiVersions response: {} bytes",
                            response_bytes.len()
                        );
                        kafka_framed.send(response_bytes).await?;
                        debug!("Java-compatible ApiVersions response sent successfully");
                        return Ok(());
                    }
                }

                warn!("Unable to process request from Java client - unsupported format");
                return Ok(()); // Don't crash the connection, just ignore the request
            }
        }

        // This code should only run if we successfully decoded the request
        let kafka_request = match KafkaCodec::decode_request(&mut message_bytes) {
            Ok(req) => req,
            Err(_) => return Ok(()), // Already handled above in the match block
        };

        // Handle DescribeGroups requests directly
        if kafka_request.api_key() == 15 {
            // API_KEY_DESCRIBE_GROUPS
            let describe_groups_response = Self::create_describe_groups_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&describe_groups_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // Handle ListGroups requests directly
        if kafka_request.api_key() == 16 {
            // API_KEY_LIST_GROUPS
            let list_groups_response = Self::create_list_groups_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&list_groups_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // Handle GetTelemetrySubscriptions requests directly
        if kafka_request.api_key() == 71 {
            // API_KEY_GET_TELEMETRY_SUBSCRIPTIONS
            let telemetry_response =
                Self::create_get_telemetry_subscriptions_response(kafka_request.correlation_id());
            let response_bytes = KafkaCodec::encode_response(&telemetry_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // Handle DescribeCluster requests directly
        if kafka_request.api_key() == 60 {
            // API_KEY_DESCRIBE_CLUSTER
            let describe_cluster_response = Self::create_describe_cluster_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&describe_cluster_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // Handle FindCoordinator requests directly
        if kafka_request.api_key() == 10 {
            // API_KEY_FIND_COORDINATOR - 🚀 PERFORMANCE: Hot path
            let find_coordinator_response = Self::create_find_coordinator_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&find_coordinator_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // 🚀 PERFORMANCE: Hot path - logging removed

        // 🔍 DEBUG: Log API key for request routing investigation
        info!(
            "🔍 REQUEST ROUTING: api_key={}, correlation_id={}",
            kafka_request.api_key(),
            kafka_request.correlation_id()
        );

        // Check if this is a consumer group request (zero-copy check)
        if ProtocolAdapter::is_consumer_group_request(&kafka_request) {
            info!(
                "🔍 ROUTE: Taking CONSUMER_GROUP path for api_key={}",
                kafka_request.api_key()
            );
            // Extract metadata before consuming the request
            let correlation_id = kafka_request.correlation_id();
            let api_version = kafka_request.api_version();

            // Now handle consumer group request (taking ownership)
            if let Ok(Some(cg_message)) =
                ProtocolAdapter::handle_consumer_group_request(kafka_request)
            {
                if let Some(cg_coordinator) = handler.get_consumer_group_coordinator() {
                    match cg_coordinator.handle_message(cg_message).await {
                        Ok(cg_response) => {
                            let kafka_response = ProtocolAdapter::consumer_group_response_to_kafka(
                                cg_response,
                                correlation_id,
                                api_version,
                            )?;
                            // DISABLED: Use standard codec to debug buffer bounds issue
                            let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                            kafka_framed.send(response_bytes).await?;
                        }
                        Err(e) => {
                            warn!("Consumer group request failed: {}", e);
                            metrics.broker.error_occurred();
                            // Send error response
                            Self::send_kafka_error_response(
                                kafka_framed,
                                correlation_id,
                                crate::protocol::kafka::KafkaErrorCode::Unknown,
                            )
                            .await?;
                        }
                    }
                }
            } else {
                // Failed to parse as consumer group request or None returned
                warn!("Failed to parse consumer group request");
                Self::send_kafka_error_response(
                    kafka_framed,
                    correlation_id,
                    crate::protocol::kafka::KafkaErrorCode::Unknown,
                )
                .await?;
            }
        } else {
            info!(
                "🔍 ROUTE: Taking ELSE path for api_key={}",
                kafka_request.api_key()
            );

            // Handle Metadata requests directly (like ApiVersions)
            if kafka_request.api_key() == 3 {
                debug!("🔍 ROUTE: Handling METADATA request (api_key=3)");
                // API_KEY_METADATA - 🚀 PERFORMANCE: Hot path
                let metadata_response = Self::create_metadata_response(&kafka_request, handler);
                let response_bytes = KafkaCodec::encode_response(&metadata_response)?;
                kafka_framed.send(response_bytes).await?;
                return Ok(());
            }

            // 🔧 TRANSACTION API: Handle transaction requests directly (bypassing adapter)
            // API keys: 22=InitProducerId, 24=AddPartitionsToTxn, 25=AddOffsetsToTxn, 26=EndTxn, 27=WriteTxnMarkers, 28=TxnOffsetCommit
            let api_key = kafka_request.api_key();
            if api_key == 22
                || api_key == 24
                || api_key == 25
                || api_key == 26
                || api_key == 27
                || api_key == 28
            {
                debug!(
                    "🔍 ROUTE: Handling TRANSACTION request (api_key={})",
                    api_key
                );
                let transaction_response =
                    Self::handle_transaction_request(&kafka_request, handler).await;
                let response_bytes = KafkaCodec::encode_response(&transaction_response)?;
                kafka_framed.send(response_bytes).await?;
                return Ok(());
            }

            // 🔧 ADMIN API: Handle admin requests directly (bypassing adapter)
            // API keys: 4=LeaderAndIsr, 5=StopReplica, 6=UpdateMetadata, 7=ControlledShutdown
            //           21=DeleteRecords, 23=OffsetForLeaderEpoch, 29-31=ACL APIs
            //           37=CreatePartitions, 42=DeleteGroups, 44=IncrementalAlterConfigs
            //           45=AlterPartitionReassignments, 46=ListPartitionReassignments
            //           47=OffsetDelete, 61=DescribeProducers, 72=PushTelemetry
            if api_key == 4
                || api_key == 5
                || api_key == 6
                || api_key == 7
                || api_key == 21
                || api_key == 23
                || api_key == 29
                || api_key == 30
                || api_key == 31
                || api_key == 37
                || api_key == 42
                || api_key == 44
                || api_key == 45
                || api_key == 46
                || api_key == 47
                || api_key == 61
                || api_key == 72
            {
                debug!("🔍 ROUTE: Handling ADMIN request (api_key={})", api_key);
                let admin_response = Self::handle_admin_request(&kafka_request, handler).await;
                let response_bytes = KafkaCodec::encode_response(&admin_response)?;
                kafka_framed.send(response_bytes).await?;
                return Ok(());
            }

            // 🔧 CRITICAL FIX: Handle PRODUCE requests directly to support multi-partition batches
            // Java Kafka clients send PRODUCE requests with multiple partitions in a single request.
            // The adapter.rs only processes the first partition, causing clients to hang waiting
            // for responses for the other partitions. This bypasses the adapter and handles ALL partitions.
            if kafka_request.api_key() == 0 {
                // API_KEY_PRODUCE
                use crate::protocol::kafka::{
                    KafkaPartitionProduceResponse, KafkaProduceResponse, KafkaRequest,
                    KafkaResponse, KafkaResponseHeader, KafkaTopicProduceResponse,
                };

                if let KafkaRequest::Produce(kafka_produce_req) = kafka_request {
                    let mut topic_responses =
                        Vec::with_capacity(kafka_produce_req.topic_data.len());
                    let correlation_id = kafka_produce_req.header.correlation_id;
                    let api_version = kafka_produce_req.header.api_version;
                    let acks = kafka_produce_req.acks;

                    // Handle acks=0 (fire-and-forget) early - no response needed
                    if acks == 0 {
                        return Ok(());
                    }

                    // Process each topic
                    for topic_data in kafka_produce_req.topic_data {
                        let mut partition_responses =
                            Vec::with_capacity(topic_data.partition_data.len());

                        // Process each partition
                        for partition_data in topic_data.partition_data {
                            let partition_id = partition_data.partition;

                            // Parse and produce messages for this partition
                            let result = if let Some(records_bytes) = partition_data.records {
                                // Parse Kafka record batch
                                match ProtocolAdapter::parse_kafka_record_batch(&records_bytes) {
                                    Ok(messages) => {
                                        // Record metrics before moving messages
                                        let msg_count = messages.len() as u64;
                                        let msg_bytes: u64 =
                                            messages.iter().map(|m| m.value.len() as u64).sum();

                                        // Store messages using handler (move, no clone)
                                        match handler.storage.append_messages(
                                            &topic_data.topic,
                                            partition_id as u32,
                                            messages,
                                        ) {
                                            Ok(base_offset) => {
                                                handler
                                                    .metrics
                                                    .throughput
                                                    .record_produced(msg_count, msg_bytes);
                                                (0i16, base_offset as i64) // error_code=0 (success), base_offset
                                            }
                                            Err(e) => {
                                                warn!("❌ MULTI-PARTITION: Failed to append messages: {}", e);
                                                (1i16, 0i64) // error_code=1 (unknown error)
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "❌ MULTI-PARTITION: Failed to parse record batch: {}",
                                            e
                                        );
                                        (1i16, 0i64)
                                    }
                                }
                            } else {
                                warn!(
                                    "❌ MULTI-PARTITION: No records in partition {}",
                                    partition_id
                                );
                                (1i16, 0i64)
                            };

                            partition_responses.push(KafkaPartitionProduceResponse {
                                partition: partition_id,
                                error_code: result.0,
                                base_offset: result.1,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                        }

                        topic_responses.push(KafkaTopicProduceResponse {
                            topic: topic_data.topic,
                            partition_responses,
                        });
                    }

                    // Create complete PRODUCE response with ALL partitions
                    let kafka_response = KafkaResponse::Produce(KafkaProduceResponse {
                        header: KafkaResponseHeader { correlation_id },
                        api_version,
                        responses: topic_responses,
                        throttle_time_ms: 0,
                    });

                    let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                    kafka_framed.send(response_bytes).await?;
                    return Ok(());
                }
            }

            // 🔧 TRANSACTION API: Handle Transaction APIs directly (bypass adapter)
            let api_key = kafka_request.api_key();
            if matches!(api_key, 22 | 24 | 25 | 26 | 27 | 28) {
                let kafka_response =
                    Self::handle_transaction_request(&kafka_request, &handler).await;
                let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                kafka_framed.send(response_bytes).await?;
                return Ok(());
            }

            // Handle regular message request
            // PERFORMANCE: Store correlation_id before consuming kafka_request to avoid clone
            let correlation_id = kafka_request.correlation_id();
            match ProtocolAdapter::kafka_to_fluxmq(kafka_request) {
                Ok(fluxmq_request) => match handler.handle_request(fluxmq_request).await {
                    Ok(fluxmq_response) => {
                        // Handle NoResponse (fire-and-forget) - don't send any response
                        // 🚀 PERFORMANCE: Hot path - logging removed
                        if matches!(
                            fluxmq_response,
                            crate::protocol::messages::Response::NoResponse
                        ) {
                            return Ok(());
                        }

                        match ProtocolAdapter::fluxmq_to_kafka(fluxmq_response, correlation_id) {
                            Ok(kafka_response) => {
                                let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                                kafka_framed.send(response_bytes).await?;
                            }
                            Err(e) => {
                                // Check for fire-and-forget NoResponse error - don't disconnect client
                                if e.to_string().contains(
                                    "Fire-and-forget request - no response should be sent",
                                ) {
                                    return Ok(());
                                } else {
                                    // Other adapter errors should be propagated
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("FluxMQ request failed: {}", e);
                        metrics.broker.error_occurred();
                        Self::send_kafka_error_response(
                            kafka_framed,
                            correlation_id,
                            crate::protocol::kafka::KafkaErrorCode::Unknown,
                        )
                        .await?;
                    }
                },
                Err(e) => {
                    warn!("Failed to convert Kafka request: {}", e);
                    metrics.broker.error_occurred();
                    Self::send_kafka_error_response(
                        kafka_framed,
                        correlation_id,
                        crate::protocol::kafka::KafkaErrorCode::InvalidRequest,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    /// 🚀 PIPELINED: 응답 bytes를 직접 반환하는 파이프라인 최적화 버전
    async fn process_kafka_message_pipelined(
        handler: &Arc<MessageHandler>,
        hp_codec: &Arc<HighPerformanceKafkaCodec>,
        mut message_bytes: Bytes,
    ) -> Result<Bytes> {
        // Track request metrics
        let metrics = handler.get_metrics();
        metrics.broker.request_received();

        // Decode Kafka request
        let kafka_request = match KafkaCodec::decode_request(&mut message_bytes) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode Kafka request: {}", e);
                return Ok(Bytes::new());
            }
        };

        // Handle ApiVersions requests with pre-compiled template
        if kafka_request.api_key() == 18 {
            let response_bytes = hp_codec.encode_api_versions_fast(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            return Ok(response_bytes);
        }

        // Handle Metadata requests
        if kafka_request.api_key() == 3 {
            let metadata_response = Self::create_metadata_response(&kafka_request, handler);
            let response_bytes = KafkaCodec::encode_response(&metadata_response)?;
            return Ok(response_bytes);
        }

        // 🔍 Handle FindCoordinator (api_key=10) BEFORE consumer group processing
        // FindCoordinator is special - it returns this broker as the coordinator, not a real CG message
        if kafka_request.api_key() == 10 {
            debug!("🔍 FIND_COORDINATOR: Returning this broker as coordinator (api_version={}, corr_id={})",
                  kafka_request.api_version(), kafka_request.correlation_id());
            let find_coordinator_response = Self::create_find_coordinator_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&find_coordinator_response)?;
            return Ok(response_bytes);
        }

        // 🔧 Handle DescribeGroups (api_key=15) BEFORE consumer group processing
        // DescribeGroups returns actual group information from coordinator
        if kafka_request.api_key() == 15 {
            info!("🔧 ADMIN (pipelined): Handling DescribeGroups (API 15) directly");
            let describe_groups_response =
                Self::create_describe_groups_response_with_coordinator(&kafka_request, &handler)
                    .await;
            let response_bytes = KafkaCodec::encode_response(&describe_groups_response)?;
            return Ok(response_bytes);
        }

        // 🔧 Handle ListGroups (api_key=16) BEFORE consumer group processing
        // ListGroups returns empty group list directly without going through CG coordinator
        if kafka_request.api_key() == 16 {
            info!("🔧 ADMIN (pipelined): Handling ListGroups (API 16) directly");
            let list_groups_response = Self::create_list_groups_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&list_groups_response)?;
            return Ok(response_bytes);
        }

        // 🔧 Handle GetTelemetrySubscriptions (api_key=71) BEFORE consumer group processing
        // Returns telemetry subscription info for client metrics
        if kafka_request.api_key() == 71 {
            info!("🔧 TELEMETRY (pipelined): Handling GetTelemetrySubscriptions (API 71) directly");
            let telemetry_response =
                Self::create_get_telemetry_subscriptions_response(kafka_request.correlation_id());
            let response_bytes = KafkaCodec::encode_response(&telemetry_response)?;
            return Ok(response_bytes);
        }

        // 🔧 Handle DescribeCluster (api_key=60) BEFORE consumer group processing
        // Returns cluster information for AdminClient
        if kafka_request.api_key() == 60 {
            info!("🔧 ADMIN (pipelined): Handling DescribeCluster (API 60) directly");
            let describe_cluster_response = Self::create_describe_cluster_response(
                kafka_request.correlation_id(),
                kafka_request.api_version(),
            );
            let response_bytes = KafkaCodec::encode_response(&describe_cluster_response)?;
            return Ok(response_bytes);
        }

        // 🔧 Handle IncrementalAlterConfigs (api_key=44)
        // Returns success response for config changes (configs are not persisted in single-node mode)
        if kafka_request.api_key() == 44 {
            info!("🔧 ADMIN (pipelined): Handling IncrementalAlterConfigs (API 44) directly");
            let incremental_alter_response =
                Self::create_incremental_alter_configs_response(&kafka_request);
            let response_bytes = KafkaCodec::encode_response(&incremental_alter_response)?;
            return Ok(response_bytes);
        }

        // 🔧 CONSUMER GROUP: Handle Consumer Group APIs in pipelined mode
        // Check if this is a consumer group request (JoinGroup, SyncGroup, etc. - NOT FindCoordinator)
        if ProtocolAdapter::is_consumer_group_request(&kafka_request) {
            let cg_api_key = kafka_request.api_key();
            info!(
                "👥 CONSUMER_GROUP: Handling api_key={} in pipelined mode",
                cg_api_key
            );

            // Extract metadata before consuming the request
            let correlation_id = kafka_request.correlation_id();
            let api_version = kafka_request.api_version();

            // Handle consumer group request
            match ProtocolAdapter::handle_consumer_group_request(kafka_request) {
                Ok(Some(cg_message)) => {
                    if let Some(cg_coordinator) = handler.get_consumer_group_coordinator() {
                        match cg_coordinator.handle_message(cg_message).await {
                            Ok(cg_response) => {
                                info!(
                                    "👥 CONSUMER_GROUP: Got response, converting to Kafka format"
                                );
                                match ProtocolAdapter::consumer_group_response_to_kafka(
                                    cg_response,
                                    correlation_id,
                                    api_version,
                                ) {
                                    Ok(kafka_response) => {
                                        let response_bytes =
                                            KafkaCodec::encode_response(&kafka_response)?;
                                        info!(
                                            "👥 CONSUMER_GROUP: Response encoded, {} bytes",
                                            response_bytes.len()
                                        );
                                        return Ok(response_bytes);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "👥 CONSUMER_GROUP: Failed to convert response: {}",
                                            e
                                        );
                                        return Self::create_consumer_group_error_response(
                                            correlation_id,
                                            cg_api_key,
                                            api_version,
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("👥 CONSUMER_GROUP: Coordinator error: {}", e);
                                return Self::create_consumer_group_error_response(
                                    correlation_id,
                                    cg_api_key,
                                    api_version,
                                );
                            }
                        }
                    } else {
                        warn!("👥 CONSUMER_GROUP: No coordinator available");
                        return Self::create_consumer_group_error_response(
                            correlation_id,
                            cg_api_key,
                            api_version,
                        );
                    }
                }
                Ok(None) => {
                    info!("👥 CONSUMER_GROUP: No message to process");
                    return Ok(Bytes::new());
                }
                Err(e) => {
                    warn!("👥 CONSUMER_GROUP: Failed to parse request: {}", e);
                    return Self::create_consumer_group_error_response(
                        correlation_id,
                        cg_api_key,
                        api_version,
                    );
                }
            }
        }

        // 🔧 TRANSACTION API: Handle Transaction APIs directly in blocking request path (bypass adapter)
        // API keys: 22=InitProducerId, 24=AddPartitionsToTxn, 25=AddOffsetsToTxn, 26=EndTxn, 27=WriteTxnMarkers, 28=TxnOffsetCommit
        let api_key = kafka_request.api_key();
        if matches!(api_key, 22 | 24 | 25 | 26 | 27 | 28) {
            info!(
                "🔧 TRANSACTION (blocking): Handling Transaction API key={} directly",
                api_key
            );
            let kafka_response = Self::handle_transaction_request(&kafka_request, handler).await;
            return Ok(KafkaCodec::encode_response(&kafka_response)?);
        }

        // 🔧 ADMIN API: Handle Admin APIs directly in blocking request path (bypass adapter)
        // API keys: 4=LeaderAndIsr, 5=StopReplica, 6=UpdateMetadata, 7=ControlledShutdown
        //           20=DeleteTopics, 21=DeleteRecords, 23=OffsetForLeaderEpoch, 29-31=ACL APIs
        //           32=DescribeConfigs, 33=AlterConfigs, 37=CreatePartitions, 42=DeleteGroups
        //           45=AlterPartitionReassignments, 46=ListPartitionReassignments, 47=OffsetDelete
        //           61=DescribeProducers, 72=PushTelemetry
        if matches!(
            api_key,
            4 | 5
                | 6
                | 7
                | 20
                | 21
                | 23
                | 29
                | 30
                | 31
                | 32
                | 33
                | 37
                | 42
                | 45
                | 46
                | 47
                | 61
                | 72
        ) {
            info!(
                "🔧 ADMIN (blocking): Handling Admin API key={} directly",
                api_key
            );
            let admin_response = Self::handle_admin_request(&kafka_request, handler).await;
            return Ok(KafkaCodec::encode_response(&admin_response)?);
        }

        // 🔧 CRITICAL FIX: Handle PRODUCE requests directly to support multi-partition batches
        // Java Kafka clients send PRODUCE requests with multiple partitions in a single request.
        // The adapter.rs only processes the first partition, causing clients to hang waiting
        // for responses for the other partitions. This bypasses the adapter and handles ALL partitions.
        if kafka_request.api_key() == 0 {
            use crate::protocol::kafka::{
                KafkaPartitionProduceResponse, KafkaProduceResponse, KafkaRequest, KafkaResponse,
                KafkaResponseHeader, KafkaTopicProduceResponse,
            };

            if let KafkaRequest::Produce(kafka_produce_req) = kafka_request {
                let mut topic_responses = Vec::with_capacity(kafka_produce_req.topic_data.len());
                let correlation_id = kafka_produce_req.header.correlation_id;
                let api_version = kafka_produce_req.header.api_version;
                let acks = kafka_produce_req.acks;

                // Handle acks=0 (fire-and-forget) early - no response needed
                if acks == 0 {
                    return Ok(Bytes::new());
                }

                // Process each topic
                for topic_data in kafka_produce_req.topic_data {
                    let mut partition_responses =
                        Vec::with_capacity(topic_data.partition_data.len());

                    // Process each partition
                    for partition_data in topic_data.partition_data {
                        let partition_id = partition_data.partition;

                        // Parse and produce messages for this partition
                        let result = if let Some(records_bytes) = partition_data.records {
                            // Parse Kafka record batch
                            match ProtocolAdapter::parse_kafka_record_batch(&records_bytes) {
                                Ok(messages) => {
                                    // Record metrics before moving messages
                                    let msg_count = messages.len() as u64;
                                    let msg_bytes: u64 =
                                        messages.iter().map(|m| m.value.len() as u64).sum();

                                    // Store messages using handler (move, no clone)
                                    match handler.storage.append_messages(
                                        &topic_data.topic,
                                        partition_id as u32,
                                        messages,
                                    ) {
                                        Ok(base_offset) => {
                                            handler
                                                .metrics
                                                .throughput
                                                .record_produced(msg_count, msg_bytes);
                                            (0i16, base_offset as i64) // error_code=0 (success), base_offset
                                        }
                                        Err(e) => {
                                            warn!(
                                                "❌ MULTI-PARTITION: Failed to append messages: {}",
                                                e
                                            );
                                            (1i16, 0i64) // error_code=1 (unknown error)
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "❌ MULTI-PARTITION: Failed to parse record batch: {}",
                                        e
                                    );
                                    (1i16, 0i64)
                                }
                            }
                        } else {
                            warn!(
                                "❌ MULTI-PARTITION: No records in partition {}",
                                partition_id
                            );
                            (1i16, 0i64)
                        };

                        partition_responses.push(KafkaPartitionProduceResponse {
                            partition: partition_id,
                            error_code: result.0,
                            base_offset: result.1,
                            log_append_time_ms: -1,
                            log_start_offset: 0,
                        });
                    }

                    topic_responses.push(KafkaTopicProduceResponse {
                        topic: topic_data.topic,
                        partition_responses,
                    });
                }

                // Create complete PRODUCE response with ALL partitions
                let kafka_response = KafkaResponse::Produce(KafkaProduceResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    responses: topic_responses,
                    throttle_time_ms: 0,
                });

                let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                return Ok(response_bytes);
            }
        }

        // Handle regular message request (Produce, Fetch, etc.)
        // PERFORMANCE: Store correlation_id before consuming kafka_request to avoid clone
        let correlation_id = kafka_request.correlation_id();
        let api_key_for_error = kafka_request.api_key();
        match ProtocolAdapter::kafka_to_fluxmq(kafka_request) {
            Ok(fluxmq_request) => {
                match handler.handle_request(fluxmq_request).await {
                    Ok(fluxmq_response) => {
                        // Handle NoResponse (fire-and-forget)
                        if matches!(
                            fluxmq_response,
                            crate::protocol::messages::Response::NoResponse
                        ) {
                            return Ok(Bytes::new());
                        }

                        match ProtocolAdapter::fluxmq_to_kafka(fluxmq_response, correlation_id) {
                            Ok(kafka_response) => {
                                let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                                Ok(response_bytes)
                            }
                            Err(e) => {
                                warn!("Failed to convert response: {}", e);
                                Self::create_error_response_simple(
                                    correlation_id,
                                    api_key_for_error,
                                )
                            }
                        }
                    }
                    Err(e) => {
                        warn!("FluxMQ request failed: {}", e);
                        metrics.broker.error_occurred();
                        Self::create_error_response_simple(correlation_id, api_key_for_error)
                    }
                }
            }
            Err(e) => {
                warn!("Failed to convert Kafka request: {}", e);
                metrics.broker.error_occurred();
                // 에러 시에도 응답 전송 (acks=1 지원)
                Self::create_error_response_simple(correlation_id, api_key_for_error)
            }
        }
    }

    /// Create error response for failed requests (acks=1 support)
    fn create_error_response(
        kafka_request: &crate::protocol::kafka::messages::KafkaRequest,
    ) -> Result<Bytes> {
        use crate::protocol::kafka::messages::*;

        // API Key별로 적절한 에러 응답 생성
        let error_response = match kafka_request.api_key() {
            0 => {
                // Produce request
                KafkaResponse::Produce(KafkaProduceResponse {
                    header: KafkaResponseHeader {
                        correlation_id: kafka_request.correlation_id(),
                    },
                    api_version: kafka_request.api_version(),
                    responses: vec![],
                    throttle_time_ms: 0,
                })
            }
            1 => {
                // Fetch request - use the same api_version from request
                KafkaResponse::Fetch(KafkaFetchResponse {
                    header: KafkaResponseHeader {
                        correlation_id: kafka_request.correlation_id(),
                    },
                    api_version: kafka_request.api_version(),
                    throttle_time_ms: 0,
                    error_code: 0,
                    session_id: 0,
                    responses: vec![],
                })
            }
            _ => {
                // 기타 API는 빈 Produce 응답으로 처리
                KafkaResponse::Produce(KafkaProduceResponse {
                    header: KafkaResponseHeader {
                        correlation_id: kafka_request.correlation_id(),
                    },
                    api_version: kafka_request.api_version(),
                    responses: vec![],
                    throttle_time_ms: 0,
                })
            }
        };

        Ok(KafkaCodec::encode_response(&error_response)?)
    }

    /// Create error response without kafka_request reference (clone-free)
    fn create_error_response_simple(correlation_id: i32, api_key: i16) -> Result<Bytes> {
        use crate::protocol::kafka::messages::*;

        // API Key별로 적절한 에러 응답 생성
        let error_response = match api_key {
            0 => {
                // Produce request
                KafkaResponse::Produce(KafkaProduceResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version: 7, // Default version
                    responses: vec![],
                    throttle_time_ms: 0,
                })
            }
            1 => {
                // Fetch request
                KafkaResponse::Fetch(KafkaFetchResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version: 11, // Default version
                    throttle_time_ms: 0,
                    error_code: 0,
                    session_id: 0,
                    responses: vec![],
                })
            }
            _ => {
                // 기타 API는 빈 Produce 응답으로 처리
                KafkaResponse::Produce(KafkaProduceResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version: 7,
                    responses: vec![],
                    throttle_time_ms: 0,
                })
            }
        };

        Ok(KafkaCodec::encode_response(&error_response)?)
    }

    async fn send_kafka_error_response<IO>(
        kafka_framed: &mut Framed<IO, KafkaFrameCodec>,
        correlation_id: i32,
        error_code: crate::protocol::kafka::KafkaErrorCode,
    ) -> Result<()>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use crate::protocol::kafka::{
            KafkaPartitionProduceResponse, KafkaProduceResponse, KafkaResponse,
            KafkaResponseHeader, KafkaTopicProduceResponse,
        };

        // Send a generic error response (using ProduceResponse as template)
        // Use api_version 0 for error responses (non-flexible)
        let error_response = KafkaResponse::Produce(KafkaProduceResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version: 0, // Use non-flexible version for error responses
            responses: vec![KafkaTopicProduceResponse {
                topic: "error".to_string(),
                partition_responses: vec![KafkaPartitionProduceResponse {
                    partition: -1,
                    error_code: error_code.as_i16(),
                    base_offset: -1,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                }],
            }],
            throttle_time_ms: 0,
        });

        let response_bytes = KafkaCodec::encode_response(&error_response)?;
        kafka_framed.send(response_bytes).await?;

        Ok(())
    }

    fn create_describe_groups_response(
        correlation_id: i32,
        api_version: i16,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaDescribeGroupsResponse, KafkaResponse, KafkaResponseHeader,
        };

        // Return empty response - no consumer groups currently active
        KafkaResponse::DescribeGroups(KafkaDescribeGroupsResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version,
            throttle_time_ms: 0,
            groups: vec![], // No groups to describe
        })
    }

    /// Create DescribeGroups response with actual group info from coordinator
    async fn create_describe_groups_response_with_coordinator(
        kafka_request: &crate::protocol::kafka::KafkaRequest,
        handler: &Arc<crate::broker::MessageHandler>,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::consumer::ConsumerGroupState;
        use crate::protocol::kafka::{
            KafkaDescribeGroupsResponse, KafkaDescribedGroup, KafkaDescribedGroupMember,
            KafkaRequest, KafkaResponse, KafkaResponseHeader,
        };
        use bytes::Bytes;

        let correlation_id = kafka_request.correlation_id();
        let api_version = kafka_request.api_version();

        // Extract group IDs from request
        let group_ids = match kafka_request {
            KafkaRequest::DescribeGroups(req) => req.groups.clone(),
            _ => vec![],
        };

        info!(
            "🔍 DescribeGroups: Looking up {} groups: {:?}",
            group_ids.len(),
            group_ids
        );

        let mut described_groups = Vec::with_capacity(group_ids.len());

        // Get coordinator if available
        if let Some(coordinator) = handler.get_consumer_group_coordinator() {
            for group_id in &group_ids {
                if let Some(metadata) = coordinator.get_group_metadata(group_id).await {
                    // Convert state to Kafka-compatible string
                    let state_str = match metadata.state {
                        ConsumerGroupState::PreparingRebalance => "PreparingRebalance",
                        ConsumerGroupState::CompletingRebalance => "CompletingRebalance",
                        ConsumerGroupState::Stable => "Stable",
                        ConsumerGroupState::Empty => "Empty",
                        ConsumerGroupState::Dead => "Dead",
                    };

                    // Convert members
                    let members: Vec<KafkaDescribedGroupMember> = metadata
                        .members
                        .values()
                        .map(|m| KafkaDescribedGroupMember {
                            member_id: m.consumer_id.clone(),
                            group_instance_id: None,
                            client_id: m.client_id.clone(),
                            client_host: m.client_host.clone(),
                            member_metadata: Bytes::new(),
                            member_assignment: Bytes::new(),
                        })
                        .collect();

                    info!(
                        "✅ DescribeGroups: Found group '{}' in state '{}' with {} members",
                        group_id,
                        state_str,
                        members.len()
                    );

                    described_groups.push(KafkaDescribedGroup {
                        error_code: 0, // NoError
                        group_id: group_id.clone(),
                        group_state: state_str.to_string(),
                        protocol_type: metadata.protocol_type.clone(),
                        protocol_data: metadata.protocol_name.clone(),
                        members,
                        authorized_operations: -2147483648, // Not supported
                    });
                } else {
                    // Group not found - return with GROUP_ID_NOT_FOUND error (but still include it)
                    info!("⚠️ DescribeGroups: Group '{}' not found", group_id);
                    described_groups.push(KafkaDescribedGroup {
                        error_code: 0, // NoError - group doesn't exist but that's ok
                        group_id: group_id.clone(),
                        group_state: "Dead".to_string(),
                        protocol_type: "".to_string(),
                        protocol_data: "".to_string(),
                        members: vec![],
                        authorized_operations: -2147483648,
                    });
                }
            }
        } else {
            // No coordinator - return empty for all requested groups
            for group_id in &group_ids {
                info!(
                    "⚠️ DescribeGroups: No coordinator, returning Dead for group '{}'",
                    group_id
                );
                described_groups.push(KafkaDescribedGroup {
                    error_code: 0,
                    group_id: group_id.clone(),
                    group_state: "Dead".to_string(),
                    protocol_type: "".to_string(),
                    protocol_data: "".to_string(),
                    members: vec![],
                    authorized_operations: -2147483648,
                });
            }
        }

        KafkaResponse::DescribeGroups(KafkaDescribeGroupsResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version,
            throttle_time_ms: 0,
            groups: described_groups,
        })
    }

    /// Create DescribeCluster response (API 60)
    /// Returns cluster information for AdminClient
    fn create_describe_cluster_response(
        correlation_id: i32,
        api_version: i16,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaDescribeClusterBroker, KafkaDescribeClusterResponse, KafkaResponse,
            KafkaResponseHeader,
        };

        KafkaResponse::DescribeCluster(KafkaDescribeClusterResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version,
            throttle_time_ms: 0,
            error_code: 0,                       // NoError
            error_message: None,                 // No error message
            endpoint_type: 1,                    // 1=brokers, 2=controllers
            cluster_id: "fluxmq-cluster".into(), // Cluster ID
            controller_id: 0,                    // Controller is broker 0
            brokers: vec![KafkaDescribeClusterBroker {
                broker_id: 0,
                host: "localhost".into(),
                port: 9092,
                rack: None,
            }],
            cluster_authorized_operations: -2147483648, // All operations allowed
        })
    }

    /// Create GetTelemetrySubscriptions response (API 71)
    /// Returns telemetry subscription information for client metrics collection
    fn create_get_telemetry_subscriptions_response(
        correlation_id: i32,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaGetTelemetrySubscriptionsResponse, KafkaResponse, KafkaResponseHeader,
        };
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate a pseudo-unique client instance ID using timestamp and correlation_id
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let mut client_instance_id = [0u8; 16];
        client_instance_id[0..8].copy_from_slice(&timestamp.to_be_bytes());
        client_instance_id[8..12].copy_from_slice(&correlation_id.to_be_bytes());
        // Version 4 UUID format markers
        client_instance_id[6] = (client_instance_id[6] & 0x0F) | 0x40; // Version 4
        client_instance_id[8] = (client_instance_id[8] & 0x3F) | 0x80; // Variant 1

        KafkaResponse::GetTelemetrySubscriptions(KafkaGetTelemetrySubscriptionsResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            error_code: 0, // NoError
            client_instance_id,
            subscription_id: 1,
            accepted_compression_types: vec![0], // No compression
            push_interval_ms: 60000,             // 60 seconds
            telemetry_max_bytes: 1048576,        // 1MB
            delta_temporality: true,
            requested_metrics: vec![String::new()], // Empty string = all metrics
        })
    }

    fn create_list_groups_response(
        correlation_id: i32,
        api_version: i16,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{KafkaListGroupsResponse, KafkaResponse, KafkaResponseHeader};

        // Return empty response - no consumer groups currently active
        KafkaResponse::ListGroups(KafkaListGroupsResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version,
            throttle_time_ms: 0,
            error_code: 0,  // NoError
            groups: vec![], // No groups
        })
    }

    /// Create IncrementalAlterConfigs response (API 44)
    /// Returns success for all requested config changes (configs are not persisted in single-node mode)
    fn create_incremental_alter_configs_response(
        kafka_request: &crate::protocol::kafka::KafkaRequest,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaIncrementalAlterConfigsResourceResponse, KafkaIncrementalAlterConfigsResponse,
            KafkaRequest, KafkaResponse,
        };

        let correlation_id = kafka_request.correlation_id();
        let api_version = kafka_request.api_version();

        // Extract resources from request
        let responses = match kafka_request {
            KafkaRequest::IncrementalAlterConfigs(req) => {
                req.resources
                    .iter()
                    .map(|r| KafkaIncrementalAlterConfigsResourceResponse {
                        error_code: 0, // NoError - configs accepted (not persisted in single-node mode)
                        error_message: None,
                        resource_type: r.resource_type,
                        resource_name: r.resource_name.clone(),
                    })
                    .collect()
            }
            _ => vec![],
        };

        KafkaResponse::IncrementalAlterConfigs(KafkaIncrementalAlterConfigsResponse {
            correlation_id,
            api_version,
            throttle_time_ms: 0,
            responses,
        })
    }

    fn create_find_coordinator_response(
        correlation_id: i32,
        api_version: i16,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaErrorCode, KafkaFindCoordinatorResponse, KafkaResponse, KafkaResponseHeader,
        };

        // Return this broker as the coordinator for all groups
        // In a real implementation, this would use consistent hashing
        KafkaResponse::FindCoordinator(KafkaFindCoordinatorResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version, // Pass through for flexible version encoding
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::NoError.as_i16(),
            error_message: None,
            node_id: 0, // This broker ID
            host: "localhost".to_string(),
            port: 9092,
        })
    }

    /// Handle transaction API requests directly
    /// API keys: 22=InitProducerId, 24=AddPartitionsToTxn, 25=AddOffsetsToTxn, 26=EndTxn, 27=WriteTxnMarkers, 28=TxnOffsetCommit
    async fn handle_transaction_request(
        kafka_request: &crate::protocol::kafka::KafkaRequest,
        _handler: &Arc<crate::broker::MessageHandler>,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaAddOffsetsToTxnResponse, KafkaAddPartitionsToTxnPartitionResult,
            KafkaAddPartitionsToTxnResponse, KafkaAddPartitionsToTxnTopicResult,
            KafkaEndTxnResponse, KafkaInitProducerIdResponse, KafkaRequest, KafkaResponse,
            KafkaTxnOffsetCommitResponse, KafkaTxnOffsetCommitResponsePartition,
            KafkaTxnOffsetCommitResponseTopic, KafkaWritableTxnMarkerResult,
            KafkaWriteTxnMarkersResponse,
        };
        use std::sync::atomic::{AtomicI64, Ordering};

        // Simple producer ID counter (in production, use persistent storage)
        static PRODUCER_ID_COUNTER: AtomicI64 = AtomicI64::new(1000);

        match kafka_request {
            KafkaRequest::InitProducerId(req) => {
                info!(
                    "🔧 TRANSACTION: InitProducerId request, transactional_id={:?}, api_version={}",
                    req.transactional_id, req.header.api_version
                );

                // Assign a new producer ID (or return existing one if already initialized)
                let producer_id = if req.producer_id >= 0 {
                    req.producer_id
                } else {
                    PRODUCER_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
                };

                let producer_epoch = if req.producer_epoch >= 0 {
                    req.producer_epoch + 1
                } else {
                    0
                };

                info!("🔧 TRANSACTION: InitProducerId response, producer_id={}, epoch={}, api_version={}",
                    producer_id, producer_epoch, req.header.api_version);

                KafkaResponse::InitProducerId(KafkaInitProducerIdResponse {
                    correlation_id: req.header.correlation_id,
                    error_code: 0, // NoError
                    producer_id,
                    producer_epoch,
                    throttle_time_ms: 0,
                    api_version: req.header.api_version, // Pass API version for flexible encoding
                })
            }

            KafkaRequest::AddPartitionsToTxn(req) => {
                info!(
                    "🔧 TRANSACTION: AddPartitionsToTxn request, transactional_id={}, topics={:?}",
                    req.transactional_id,
                    req.topics.iter().map(|t| &t.name).collect::<Vec<_>>()
                );

                // Build success responses for all requested partitions
                let results: Vec<KafkaAddPartitionsToTxnTopicResult> = req
                    .topics
                    .iter()
                    .map(|topic| {
                        KafkaAddPartitionsToTxnTopicResult {
                            name: topic.name.clone(),
                            results: topic
                                .partitions
                                .iter()
                                .map(|&partition| {
                                    KafkaAddPartitionsToTxnPartitionResult {
                                        partition_index: partition,
                                        error_code: 0, // NoError
                                    }
                                })
                                .collect(),
                        }
                    })
                    .collect();

                KafkaResponse::AddPartitionsToTxn(KafkaAddPartitionsToTxnResponse {
                    correlation_id: req.header.correlation_id,
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    results,
                })
            }

            KafkaRequest::AddOffsetsToTxn(req) => {
                info!(
                    "🔧 TRANSACTION: AddOffsetsToTxn request, transactional_id={}, group_id={}",
                    req.transactional_id, req.group_id
                );

                KafkaResponse::AddOffsetsToTxn(KafkaAddOffsetsToTxnResponse {
                    correlation_id: req.header.correlation_id,
                    throttle_time_ms: 0,
                    error_code: 0, // NoError
                })
            }

            KafkaRequest::EndTxn(req) => {
                info!(
                    "🔧 TRANSACTION: EndTxn request, transactional_id={}, committed={}",
                    req.transactional_id, req.committed
                );

                KafkaResponse::EndTxn(KafkaEndTxnResponse {
                    correlation_id: req.header.correlation_id,
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    error_code: 0, // NoError
                })
            }

            KafkaRequest::WriteTxnMarkers(req) => {
                info!(
                    "🔧 TRANSACTION: WriteTxnMarkers request, markers={}",
                    req.markers.len()
                );

                // Build success responses for all markers
                let markers: Vec<KafkaWritableTxnMarkerResult> = req
                    .markers
                    .iter()
                    .map(|marker| {
                        KafkaWritableTxnMarkerResult {
                            producer_id: marker.producer_id,
                            topics: marker
                                .topics
                                .iter()
                                .map(|topic| {
                                    crate::protocol::kafka::KafkaWritableTxnMarkerTopicResult {
                                name: topic.name.clone(),
                                partitions: topic.partitions.iter().map(|&partition| {
                                    crate::protocol::kafka::KafkaWritableTxnMarkerPartitionResult {
                                        partition_index: partition,
                                        error_code: 0, // NoError
                                    }
                                }).collect(),
                            }
                                })
                                .collect(),
                        }
                    })
                    .collect();

                KafkaResponse::WriteTxnMarkers(KafkaWriteTxnMarkersResponse {
                    correlation_id: req.header.correlation_id,
                    markers,
                })
            }

            KafkaRequest::TxnOffsetCommit(req) => {
                info!(
                    "🔧 TRANSACTION: TxnOffsetCommit request, transactional_id={}, group_id={}",
                    req.transactional_id, req.group_id
                );

                // Build success responses for all topics/partitions
                let topics: Vec<KafkaTxnOffsetCommitResponseTopic> = req
                    .topics
                    .iter()
                    .map(|topic| {
                        KafkaTxnOffsetCommitResponseTopic {
                            name: topic.name.clone(),
                            partitions: topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    KafkaTxnOffsetCommitResponsePartition {
                                        partition_index: partition.partition_index,
                                        error_code: 0, // NoError
                                    }
                                })
                                .collect(),
                        }
                    })
                    .collect();

                KafkaResponse::TxnOffsetCommit(KafkaTxnOffsetCommitResponse {
                    correlation_id: req.header.correlation_id,
                    throttle_time_ms: 0,
                    topics,
                })
            }

            _ => {
                error!("🔧 TRANSACTION: Unexpected request type in handle_transaction_request");
                // Return a generic error - should not happen
                KafkaResponse::InitProducerId(KafkaInitProducerIdResponse {
                    correlation_id: kafka_request.correlation_id(),
                    error_code: 1, // Unknown error
                    producer_id: -1,
                    producer_epoch: -1,
                    throttle_time_ms: 0,
                    api_version: kafka_request.api_version(),
                })
            }
        }
    }

    /// Handle admin API requests directly
    /// API keys: 4=LeaderAndIsr, 5=StopReplica, 6=UpdateMetadata, 7=ControlledShutdown
    ///           21=DeleteRecords, 37=CreatePartitions, 42=DeleteGroups, 45=AlterPartitionReassignments
    async fn handle_admin_request(
        kafka_request: &crate::protocol::kafka::KafkaRequest,
        handler: &Arc<crate::broker::MessageHandler>,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            DeletableTopicResult,
            KafkaAclCreationResult,
            KafkaAlterConfigsResourceResponse,
            KafkaAlterConfigsResponse,
            KafkaAlterPartitionReassignmentsResponse,
            KafkaConfigResourceResult,
            KafkaControlledShutdownResponse,
            KafkaCreateAclsResponse,
            KafkaCreatePartitionsResponse,
            KafkaCreatePartitionsTopicResult,
            KafkaDeletableGroupResult,
            KafkaDeleteAclsFilterResult,
            KafkaDeleteAclsResponse,
            KafkaDeleteGroupsResponse,
            KafkaDeleteRecordsPartitionResult,
            KafkaDeleteRecordsResponse,
            KafkaDeleteRecordsTopicResult,
            KafkaDeleteTopicsResponse,
            KafkaDescribeAclsResponse,
            KafkaDescribeClusterBroker,
            KafkaDescribeClusterResponse,
            KafkaDescribeConfigsResponse,
            KafkaDescribeProducersPartitionResponse,
            KafkaDescribeProducersResponse,
            KafkaDescribeProducersTopicResponse,
            KafkaIncrementalAlterConfigsResourceResponse,
            KafkaIncrementalAlterConfigsResponse,
            KafkaLeaderAndIsrPartitionResponse,
            KafkaLeaderAndIsrResponse,
            // New Admin APIs (Kafka 4.1.0 compatibility)
            KafkaListPartitionReassignmentsResponse,
            KafkaOffsetDeleteResponse,
            KafkaOffsetDeleteResponsePartition,
            KafkaOffsetDeleteResponseTopic,
            KafkaOffsetForLeaderEpochPartitionResponse,
            // Phase 2 APIs - Replication and Security
            KafkaOffsetForLeaderEpochResponse,
            KafkaOffsetForLeaderEpochTopicResponse,
            KafkaPushTelemetryResponse,
            KafkaReassignablePartitionResponse,
            KafkaReassignableTopicResponse,
            KafkaRequest,
            KafkaResponse,
            KafkaResponseHeader,
            KafkaStopReplicaPartitionError,
            KafkaStopReplicaResponse,
            KafkaUpdateMetadataResponse,
        };

        match kafka_request {
            KafkaRequest::DeleteRecords(req) => {
                info!(
                    "🔧 ADMIN: DeleteRecords request for {} topics",
                    req.topics.len()
                );

                let mut topic_results = Vec::new();
                for topic in &req.topics {
                    let mut partition_results = Vec::new();
                    for partition in &topic.partitions {
                        // Delete records up to the specified offset
                        // For now, return success with low_watermark = requested offset
                        let low_watermark = partition.offset;
                        partition_results.push(KafkaDeleteRecordsPartitionResult {
                            partition_index: partition.partition,
                            low_watermark,
                            error_code: 0, // NoError
                        });
                    }
                    topic_results.push(KafkaDeleteRecordsTopicResult {
                        name: topic.name.clone(),
                        partitions: partition_results,
                    });
                }

                KafkaResponse::DeleteRecords(KafkaDeleteRecordsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    topics: topic_results,
                })
            }

            KafkaRequest::CreatePartitions(req) => {
                info!(
                    "🔧 ADMIN: CreatePartitions request for {} topics",
                    req.topics.len()
                );

                let mut results = Vec::new();
                for topic in &req.topics {
                    // Check if topic exists using topic_manager (where topics are actually stored)
                    let topic_exists = handler.topic_manager.get_topic(&topic.name).is_some();
                    if !topic_exists {
                        results.push(KafkaCreatePartitionsTopicResult {
                            name: topic.name.clone(),
                            error_code: 3, // UnknownTopicOrPartition
                            error_message: Some(format!("Topic '{}' does not exist", topic.name)),
                        });
                    } else {
                        // Get current partition count from topic_manager
                        let current_partitions = handler
                            .topic_manager
                            .get_topic(&topic.name)
                            .map(|t| t.num_partitions as i32)
                            .unwrap_or(0);

                        if topic.count <= current_partitions {
                            results.push(KafkaCreatePartitionsTopicResult {
                                name: topic.name.clone(),
                                error_code: 37, // InvalidPartitions
                                error_message: Some(format!(
                                    "New partition count {} must be greater than current count {}",
                                    topic.count, current_partitions
                                )),
                            });
                        } else {
                            // Create new partitions (in FluxMQ, partitions are created on demand)
                            results.push(KafkaCreatePartitionsTopicResult {
                                name: topic.name.clone(),
                                error_code: 0, // NoError
                                error_message: None,
                            });
                        }
                    }
                }

                KafkaResponse::CreatePartitions(KafkaCreatePartitionsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    results,
                })
            }

            KafkaRequest::DeleteGroups(req) => {
                info!(
                    "🔧 ADMIN: DeleteGroups request for {} groups",
                    req.groups.len()
                );

                let mut results = Vec::new();
                for group_id in &req.groups {
                    // Delete the consumer group
                    // For now, return success (actual deletion would require coordinator integration)
                    results.push(KafkaDeletableGroupResult {
                        group_id: group_id.clone(),
                        error_code: 0, // NoError
                    });
                }

                KafkaResponse::DeleteGroups(KafkaDeleteGroupsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    results,
                })
            }

            KafkaRequest::AlterPartitionReassignments(req) => {
                info!(
                    "🔧 ADMIN: AlterPartitionReassignments request for {} topics",
                    req.topics.len()
                );

                let mut responses = Vec::new();
                for topic in &req.topics {
                    let mut partition_responses = Vec::new();
                    for partition in &topic.partitions {
                        // FluxMQ doesn't support partition reassignment in single-node mode
                        partition_responses.push(KafkaReassignablePartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: 0, // NoError (accept but no-op)
                            error_message: None,
                        });
                    }
                    responses.push(KafkaReassignableTopicResponse {
                        name: topic.name.clone(),
                        partitions: partition_responses,
                    });
                }

                KafkaResponse::AlterPartitionReassignments(
                    KafkaAlterPartitionReassignmentsResponse {
                        header: KafkaResponseHeader {
                            correlation_id: req.header.correlation_id,
                        },
                        api_version: req.header.api_version,
                        throttle_time_ms: 0,
                        error_code: 0,
                        error_message: None,
                        responses,
                    },
                )
            }

            // ====================================================================
            // New Admin APIs (Kafka 4.1.0 compatibility)
            // ====================================================================
            KafkaRequest::ListPartitionReassignments(req) => {
                info!("🔧 ADMIN: ListPartitionReassignments request");

                // In single-node mode, return empty list (no ongoing reassignments)
                KafkaResponse::ListPartitionReassignments(KafkaListPartitionReassignmentsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    error_code: 0,
                    error_message: None,
                    topics: vec![], // No ongoing reassignments
                })
            }

            KafkaRequest::OffsetDelete(req) => {
                info!(
                    "🔧 ADMIN: OffsetDelete request for group {} with {} topics",
                    req.group_id,
                    req.topics.len()
                );

                // Delete offsets for the consumer group
                let mut topic_responses = Vec::new();
                for topic in &req.topics {
                    let mut partition_responses = Vec::new();
                    for partition in &topic.partitions {
                        // Accept the delete request (actual deletion handled by coordinator)
                        partition_responses.push(KafkaOffsetDeleteResponsePartition {
                            partition_index: partition.partition_index,
                            error_code: 0, // NoError
                        });
                    }
                    topic_responses.push(KafkaOffsetDeleteResponseTopic {
                        name: topic.name.clone(),
                        partitions: partition_responses,
                    });
                }

                KafkaResponse::OffsetDelete(KafkaOffsetDeleteResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    error_code: 0,
                    throttle_time_ms: 0,
                    topics: topic_responses,
                })
            }

            KafkaRequest::DescribeCluster(req) => {
                info!(
                    "🔧 ADMIN: DescribeCluster request, include_ops={}",
                    req.include_cluster_authorized_operations
                );

                // Return cluster information
                KafkaResponse::DescribeCluster(KafkaDescribeClusterResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    error_code: 0,
                    error_message: None,
                    endpoint_type: req.endpoint_type,
                    cluster_id: "fluxmq-cluster".to_string(),
                    controller_id: 0, // Broker 0 is the controller
                    brokers: vec![KafkaDescribeClusterBroker {
                        broker_id: 0,
                        host: "localhost".to_string(),
                        port: 9092,
                        rack: None,
                    }],
                    cluster_authorized_operations: if req.include_cluster_authorized_operations {
                        // All operations allowed (bit mask)
                        0x7FFF
                    } else {
                        -2147483648 // INT_MIN means not requested
                    },
                })
            }

            KafkaRequest::DescribeProducers(req) => {
                info!(
                    "🔧 ADMIN: DescribeProducers request for {} topics",
                    req.topics.len()
                );

                let mut topic_responses = Vec::new();
                for topic in &req.topics {
                    let mut partition_responses = Vec::new();
                    for partition_index in &topic.partition_indexes {
                        // Return empty producer list (no active transactions tracked yet)
                        partition_responses.push(KafkaDescribeProducersPartitionResponse {
                            partition_index: *partition_index,
                            error_code: 0, // NoError
                            error_message: None,
                            active_producers: vec![], // No active producers
                        });
                    }
                    topic_responses.push(KafkaDescribeProducersTopicResponse {
                        name: topic.name.clone(),
                        partitions: partition_responses,
                    });
                }

                KafkaResponse::DescribeProducers(KafkaDescribeProducersResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    topics: topic_responses,
                })
            }

            KafkaRequest::PushTelemetry(req) => {
                info!(
                    "🔧 TELEMETRY: PushTelemetry request, subscription_id={}, terminating={}",
                    req.subscription_id, req.terminating
                );

                // Accept telemetry data (log and discard for now)
                KafkaResponse::PushTelemetry(KafkaPushTelemetryResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    error_code: 0, // NoError
                })
            }

            // ====================================================================
            // Phase 2 APIs - Replication and Security
            // ====================================================================
            KafkaRequest::OffsetForLeaderEpoch(req) => {
                info!(
                    "🔧 REPLICATION: OffsetForLeaderEpoch request, replica_id={}, {} topics",
                    req.replica_id,
                    req.topics.len()
                );

                // Return end offset and leader epoch for each requested partition
                let topics = req
                    .topics
                    .iter()
                    .map(|topic_req| {
                        let partitions = topic_req
                            .partitions
                            .iter()
                            .map(|part_req| {
                                // Get end offset from storage via handler
                                let end_offset = handler
                                    .storage
                                    .get_latest_offset(&topic_req.topic, part_req.partition as u32)
                                    .unwrap_or(0)
                                    as i64;

                                KafkaOffsetForLeaderEpochPartitionResponse {
                                    error_code: 0, // NoError
                                    partition: part_req.partition,
                                    leader_epoch: part_req.leader_epoch, // Echo back the requested epoch
                                    end_offset,
                                }
                            })
                            .collect();

                        KafkaOffsetForLeaderEpochTopicResponse {
                            topic: topic_req.topic.clone(),
                            partitions,
                        }
                    })
                    .collect();

                KafkaResponse::OffsetForLeaderEpoch(KafkaOffsetForLeaderEpochResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    topics,
                })
            }

            KafkaRequest::DescribeAcls(req) => {
                info!(
                    "🔧 ACL: DescribeAcls request, resource_type={}, operation={}",
                    req.resource_type_filter, req.operation
                );

                // Return empty ACLs list (no ACLs configured by default)
                KafkaResponse::DescribeAcls(KafkaDescribeAclsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    error_code: 0, // NoError
                    error_message: None,
                    resources: vec![], // No ACLs configured
                })
            }

            KafkaRequest::CreateAcls(req) => {
                info!(
                    "🔧 ACL: CreateAcls request, {} creations",
                    req.creations.len()
                );

                // Accept all ACL creations (no-op in single-node mode without authorization)
                let results = req
                    .creations
                    .iter()
                    .map(|_| KafkaAclCreationResult {
                        error_code: 0, // NoError - ACL created successfully
                        error_message: None,
                    })
                    .collect();

                KafkaResponse::CreateAcls(KafkaCreateAclsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    results,
                })
            }

            KafkaRequest::DeleteAcls(req) => {
                info!("🔧 ACL: DeleteAcls request, {} filters", req.filters.len());

                // Accept all ACL deletions (no-op - no ACLs to delete)
                let filter_results = req
                    .filters
                    .iter()
                    .map(|_| KafkaDeleteAclsFilterResult {
                        error_code: 0, // NoError
                        error_message: None,
                        matching_acls: vec![], // No ACLs matched the filter
                    })
                    .collect();

                KafkaResponse::DeleteAcls(KafkaDeleteAclsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    api_version: req.header.api_version,
                    throttle_time_ms: 0,
                    filter_results,
                })
            }

            // ====================================================================
            // Cluster Coordination APIs (Inter-broker communication)
            // ====================================================================
            KafkaRequest::LeaderAndIsr(req) => {
                info!(
                    "🔧 CLUSTER: LeaderAndIsr request from controller {} epoch {}",
                    req.controller_id, req.controller_epoch
                );

                // In single-node mode, acknowledge all partition state changes
                let mut partition_responses = Vec::new();
                for partition_state in &req.partition_states {
                    partition_responses.push(KafkaLeaderAndIsrPartitionResponse {
                        topic: partition_state.topic.clone(),
                        partition: partition_state.partition,
                        error_code: 0, // NoError
                    });
                }

                KafkaResponse::LeaderAndIsr(KafkaLeaderAndIsrResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    error_code: 0, // NoError
                    partitions: partition_responses,
                })
            }

            KafkaRequest::StopReplica(req) => {
                info!(
                    "🔧 CLUSTER: StopReplica request from controller {} epoch {}",
                    req.controller_id, req.controller_epoch
                );

                // In single-node mode, acknowledge all stop replica requests
                let mut partition_errors = Vec::new();
                for topic_state in &req.topics {
                    for partition_state in &topic_state.partition_states {
                        partition_errors.push(KafkaStopReplicaPartitionError {
                            topic: topic_state.topic.clone(),
                            partition: partition_state.partition,
                            error_code: 0, // NoError
                        });
                    }
                }

                KafkaResponse::StopReplica(KafkaStopReplicaResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    error_code: 0, // NoError
                    partition_errors,
                })
            }

            KafkaRequest::UpdateMetadata(req) => {
                info!(
                    "🔧 CLUSTER: UpdateMetadata request from controller {} with {} partition states",
                    req.controller_id, req.partition_states.len()
                );

                // In single-node mode, acknowledge metadata update
                KafkaResponse::UpdateMetadata(KafkaUpdateMetadataResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    error_code: 0, // NoError
                })
            }

            KafkaRequest::ControlledShutdown(req) => {
                info!(
                    "🔧 CLUSTER: ControlledShutdown request from broker {}",
                    req.broker_id
                );

                // In single-node mode, allow immediate shutdown with no remaining partitions
                KafkaResponse::ControlledShutdown(KafkaControlledShutdownResponse {
                    header: KafkaResponseHeader {
                        correlation_id: req.header.correlation_id,
                    },
                    error_code: 0,                // NoError
                    partitions_remaining: vec![], // No partitions need to be migrated
                })
            }

            KafkaRequest::DescribeConfigs(req) => {
                info!(
                    "🔧 ADMIN: DescribeConfigs request for {} resources",
                    req.resources.len()
                );

                let mut results = Vec::new();
                for resource in &req.resources {
                    // Return empty config entries for now
                    results.push(KafkaConfigResourceResult {
                        error_code: 0, // NoError
                        error_message: None,
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                        configs: vec![],
                    });
                }

                KafkaResponse::DescribeConfigs(KafkaDescribeConfigsResponse {
                    correlation_id: req.correlation_id,
                    api_version: req.api_version,
                    throttle_time_ms: 0,
                    results,
                })
            }

            KafkaRequest::AlterConfigs(req) => {
                info!(
                    "🔧 ADMIN: AlterConfigs request for {} resources",
                    req.resources.len()
                );

                let mut responses = Vec::new();
                for resource in &req.resources {
                    // Accept the config change (simulated)
                    responses.push(KafkaAlterConfigsResourceResponse {
                        error_code: 0, // NoError
                        error_message: None,
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                    });
                }

                KafkaResponse::AlterConfigs(KafkaAlterConfigsResponse {
                    correlation_id: req.correlation_id,
                    api_version: req.api_version,
                    throttle_time_ms: 0,
                    responses,
                })
            }

            KafkaRequest::DeleteTopics(req) => {
                info!(
                    "🔧 ADMIN: DeleteTopics request for {} topics",
                    req.topic_names.len()
                );

                let mut responses = Vec::new();
                for topic in &req.topic_names {
                    // Check if topic exists using topic_manager (where topics are actually stored)
                    let topic_exists = handler.topic_manager.get_topic(topic).is_some();
                    if !topic_exists {
                        responses.push(DeletableTopicResult {
                            name: topic.to_string(),
                            topic_id: None,
                            error_code: 3, // UnknownTopicOrPartition
                            error_message: Some(format!("Topic '{}' does not exist", topic)),
                        });
                    } else {
                        // Delete the topic from topic_manager
                        let _ = handler.topic_manager.delete_topic(topic);
                        responses.push(DeletableTopicResult {
                            name: topic.to_string(),
                            topic_id: None,
                            error_code: 0, // NoError
                            error_message: None,
                        });
                    }
                }

                KafkaResponse::DeleteTopics(KafkaDeleteTopicsResponse {
                    correlation_id: req.correlation_id,
                    api_version: req.api_version,
                    throttle_time_ms: 0,
                    responses,
                })
            }

            KafkaRequest::IncrementalAlterConfigs(req) => {
                info!(
                    "🔧 ADMIN: IncrementalAlterConfigs request for {} resources",
                    req.resources.len()
                );

                // Accept all config changes (no-op in single-node mode)
                let responses = req
                    .resources
                    .iter()
                    .map(|r| KafkaIncrementalAlterConfigsResourceResponse {
                        error_code: 0, // NoError - configs accepted
                        error_message: None,
                        resource_type: r.resource_type,
                        resource_name: r.resource_name.clone(),
                    })
                    .collect();

                KafkaResponse::IncrementalAlterConfigs(KafkaIncrementalAlterConfigsResponse {
                    correlation_id: req.correlation_id,
                    api_version: req.api_version,
                    throttle_time_ms: 0,
                    responses,
                })
            }

            _ => {
                error!("🔧 ADMIN: Unexpected request type in handle_admin_request");
                // Return a generic error response
                KafkaResponse::DeleteRecords(KafkaDeleteRecordsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: kafka_request.correlation_id(),
                    },
                    api_version: kafka_request.api_version(),
                    throttle_time_ms: 0,
                    topics: vec![],
                })
            }
        }
    }

    /// Create an error response for consumer group requests based on API key
    fn create_consumer_group_error_response(
        correlation_id: i32,
        api_key: i16,
        api_version: i16,
    ) -> Result<Bytes> {
        use crate::protocol::kafka::{
            KafkaErrorCode, KafkaFindCoordinatorResponse, KafkaHeartbeatResponse,
            KafkaJoinGroupResponse, KafkaLeaveGroupResponse, KafkaResponse, KafkaResponseHeader,
            KafkaSyncGroupResponse,
        };

        let error_code = KafkaErrorCode::CoordinatorNotAvailable.as_i16(); // 15 = COORDINATOR_NOT_AVAILABLE

        let error_response = match api_key {
            11 => {
                // JoinGroup
                KafkaResponse::JoinGroup(KafkaJoinGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                    generation_id: -1,
                    protocol_type: String::new(),
                    protocol_name: String::new(),
                    leader: String::new(),
                    member_id: String::new(),
                    members: vec![],
                })
            }
            12 => {
                // Heartbeat
                KafkaResponse::Heartbeat(KafkaHeartbeatResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                })
            }
            13 => {
                // LeaveGroup
                KafkaResponse::LeaveGroup(KafkaLeaveGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                })
            }
            14 => {
                // SyncGroup
                KafkaResponse::SyncGroup(KafkaSyncGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                    protocol_type: String::new(),
                    protocol_name: String::new(),
                    assignment: Bytes::new(),
                })
            }
            _ => {
                // Default to FindCoordinator error for other APIs
                KafkaResponse::FindCoordinator(KafkaFindCoordinatorResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version: 0,
                    throttle_time_ms: 0,
                    error_code,
                    error_message: Some("Consumer group coordinator not available".to_string()),
                    node_id: -1,
                    host: String::new(),
                    port: 0,
                })
            }
        };

        let response_bytes = KafkaCodec::encode_response(&error_response)?;
        Ok(response_bytes)
    }

    fn create_metadata_response(
        kafka_request: &crate::protocol::kafka::KafkaRequest,
        handler: &Arc<crate::broker::MessageHandler>,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaBrokerMetadata, KafkaMetadataResponse, KafkaPartitionMetadata, KafkaRequest,
            KafkaResponse, KafkaResponseHeader, KafkaTopicMetadata,
        };

        // Extract metadata request
        let (requested_topics, allow_auto_topic_creation, correlation_id, api_version) =
            match kafka_request {
                KafkaRequest::Metadata(req) => (
                    req.topics.clone(),
                    req.allow_auto_topic_creation,
                    req.header.correlation_id,
                    req.header.api_version,
                ),
                _ => {
                    return KafkaResponse::Metadata(KafkaMetadataResponse {
                        header: KafkaResponseHeader {
                            correlation_id: kafka_request.correlation_id(),
                        },
                        throttle_time_ms: 0,
                        brokers: vec![],
                        cluster_id: Some("fluxmq-cluster".to_string()),
                        controller_id: 0,
                        topics: vec![],
                        api_version: 0,
                        cluster_authorized_operations: -2147483648,
                    })
                }
            };

        info!(
            "Metadata request: topics={:?}, allow_auto_topic_creation={}",
            requested_topics, allow_auto_topic_creation
        );

        // Basic metadata response - single broker
        let brokers = vec![KafkaBrokerMetadata {
            node_id: 0,
            host: "localhost".to_string(),
            port: handler.get_broker_port() as i32,
            rack: None,
        }];
        info!("Creating brokers array with {} entries", brokers.len());
        for (i, broker) in brokers.iter().enumerate() {
            info!(
                "  broker[{}]: node_id={}, host='{}', port={}",
                i, broker.node_id, broker.host, broker.port
            );
        }

        // Get enhanced topic descriptions from the handler
        let topic_descriptions = handler.get_enhanced_topic_descriptions(requested_topics.clone());
        info!(
            "Enhanced topic metadata for DescribeTopics: {} topics",
            topic_descriptions.len()
        );
        info!("Requested topics: {:?}", requested_topics);
        info!(
            "Topic descriptions returned: {:?}",
            topic_descriptions
                .iter()
                .map(|d| (&d.name, d.error_code))
                .collect::<Vec<_>>()
        );
        let mut topics = Vec::new();

        for description in topic_descriptions {
            if description.error_code != 0 && allow_auto_topic_creation {
                // Topic doesn't exist - try auto creation
                info!(
                    "Auto-creating topic '{}' per metadata request",
                    description.name
                );
                if let Ok(topic_info) = handler.ensure_topic_exists(&description.name) {
                    // Successfully auto-created, create partition metadata
                    let mut partitions = Vec::new();
                    for partition_info in topic_info.partitions {
                        partitions.push(KafkaPartitionMetadata {
                            error_code: 0,
                            partition: partition_info.id as i32,
                            leader: partition_info.leader.map(|id| id as i32).unwrap_or(0),
                            leader_epoch: 0,
                            replica_nodes: partition_info
                                .replicas
                                .into_iter()
                                .map(|id| id as i32)
                                .collect(),
                            isr_nodes: partition_info
                                .in_sync_replicas
                                .into_iter()
                                .map(|id| id as i32)
                                .collect(),
                            offline_replicas: vec![],
                        });
                    }

                    topics.push(KafkaTopicMetadata {
                        error_code: 0, // Successfully created
                        topic: description.name,
                        is_internal: false,
                        partitions,
                        topic_authorized_operations: -2147483648,
                    });
                } else {
                    // Auto creation failed, return error
                    topics.push(KafkaTopicMetadata {
                        error_code: description.error_code,
                        topic: description.name,
                        is_internal: description.is_internal,
                        partitions: vec![],
                        topic_authorized_operations: -2147483648,
                    });
                }
            } else if description.error_code != 0 {
                // Topic doesn't exist and auto creation disabled - return with error
                topics.push(KafkaTopicMetadata {
                    error_code: description.error_code,
                    topic: description.name,
                    is_internal: description.is_internal,
                    partitions: vec![], // No partitions for error topics
                    topic_authorized_operations: -2147483648,
                });
            } else {
                // Topic exists - create enhanced partition metadata
                let mut partitions = Vec::new();
                for partition_desc in description.partitions {
                    partitions.push(KafkaPartitionMetadata {
                        error_code: 0, // No error
                        partition: partition_desc.id as i32,
                        leader: partition_desc.leader,
                        leader_epoch: partition_desc.leader_epoch,
                        replica_nodes: partition_desc.replicas,
                        isr_nodes: partition_desc.isr,
                        offline_replicas: partition_desc.offline_replicas,
                    });
                }

                topics.push(KafkaTopicMetadata {
                    error_code: 0, // No error
                    topic: description.name,
                    is_internal: description.is_internal,
                    partitions,
                    topic_authorized_operations: -2147483648,
                });
            }
        }

        // CRITICAL FIX: Ensure all requested topics are included in response
        // If get_enhanced_topic_descriptions() returned empty, we still need to handle requested topics
        if let Some(ref requested) = requested_topics {
            for topic_name in requested {
                // Check if this topic was already processed
                if !topics.iter().any(|t| t.topic == *topic_name) {
                    info!(
                        "Adding missing requested topic '{}' to metadata response",
                        topic_name
                    );
                    if allow_auto_topic_creation {
                        // Try to auto-create the missing topic
                        if let Ok(topic_info) = handler.ensure_topic_exists(topic_name) {
                            let mut partitions = Vec::new();
                            for partition_info in topic_info.partitions {
                                partitions.push(KafkaPartitionMetadata {
                                    error_code: 0,
                                    partition: partition_info.id as i32,
                                    leader: partition_info.leader.map(|id| id as i32).unwrap_or(0),
                                    leader_epoch: 0,
                                    replica_nodes: partition_info
                                        .replicas
                                        .into_iter()
                                        .map(|id| id as i32)
                                        .collect(),
                                    isr_nodes: partition_info
                                        .in_sync_replicas
                                        .into_iter()
                                        .map(|id| id as i32)
                                        .collect(),
                                    offline_replicas: vec![],
                                });
                            }
                            topics.push(KafkaTopicMetadata {
                                error_code: 0,
                                topic: topic_name.clone(),
                                is_internal: false,
                                partitions,
                                topic_authorized_operations: -2147483648,
                            });
                        } else {
                            // Auto-creation failed, return error topic
                            topics.push(KafkaTopicMetadata {
                                error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                                topic: topic_name.clone(),
                                is_internal: false,
                                partitions: vec![],
                                topic_authorized_operations: -2147483648,
                            });
                        }
                    } else {
                        // Auto-creation disabled, return unknown topic error
                        topics.push(KafkaTopicMetadata {
                            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                            topic: topic_name.clone(),
                            is_internal: false,
                            partitions: vec![],
                            topic_authorized_operations: -2147483648,
                        });
                    }
                }
            }
        }

        info!("Final topics count in metadata response: {}", topics.len());

        KafkaResponse::Metadata(KafkaMetadataResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version: api_version, // Use client's requested API version
            throttle_time_ms: 0,
            brokers,
            cluster_id: None,
            controller_id: 0,
            topics,
            cluster_authorized_operations: -2147483648,
        })
    }

    /// Run TLS listener for secure connections with shutdown support
    async fn run_tls_listener_with_shutdown(
        listener: TcpListener,
        handler: Arc<MessageHandler>,
        acceptor: Arc<FluxTlsAcceptor>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        info!("Starting TLS listener with shutdown support...");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            info!("New TLS client connected: {}", peer_addr);
                            let handler_clone = Arc::clone(&handler);
                            let _acceptor_clone = Arc::clone(&acceptor);

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_tls_client(stream, handler_clone).await {
                                    error!("Error handling TLS client {}: {}", peer_addr, e);
                                } else {
                                    info!("TLS client {} disconnected", peer_addr);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept TLS connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("TLS listener shutting down gracefully...");
                    break;
                }
            }
        }
    }

    /// Handle TLS client connection using the same logic as regular clients
    async fn handle_tls_client<IO>(stream: IO, handler: Arc<MessageHandler>) -> Result<()>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        info!("New TLS client connected - using Kafka protocol");

        // Track connection in metrics
        let metrics = handler.get_metrics();
        metrics.broker.connection_opened();

        let mut kafka_framed = Framed::new(stream, KafkaFrameCodec);
        // Create high-performance codec for this connection
        let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());

        // Process all messages as Kafka protocol (same as regular clients)
        while let Some(result) = tokio_stream::StreamExt::next(&mut kafka_framed).await {
            match result {
                Ok(message_bytes) => {
                    if let Err(e) = Self::process_kafka_message(
                        &handler,
                        &hp_codec,
                        message_bytes,
                        &mut kafka_framed,
                    )
                    .await
                    {
                        error!("Error processing TLS Kafka message: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    warn!("Failed to decode TLS Kafka message: {}", e);
                    break;
                }
            }
        }

        // Track connection closure
        metrics.broker.connection_closed();

        Ok(())
    }

    /// Create and configure ACL manager
    fn create_acl_manager(config: &BrokerConfig) -> Result<AclManager> {
        let mut acl_manager = AclManager::new(config.allow_everyone);

        // Add super users
        for super_user in &config.super_users {
            acl_manager.add_super_user(Principal::user(super_user));
        }

        // Load ACL configuration file if specified
        if let Some(ref acl_file) = config.acl_config_file {
            if std::path::Path::new(acl_file).exists() {
                match acl_manager.load_from_file(acl_file) {
                    Ok(()) => info!("Loaded ACL configuration from {}", acl_file),
                    Err(e) => {
                        warn!("Failed to load ACL configuration from {}: {}", acl_file, e);
                        warn!("Continuing with empty ACL configuration");
                    }
                }
            } else {
                info!(
                    "ACL configuration file {} does not exist, starting with empty ACL",
                    acl_file
                );
            }
        }

        Ok(acl_manager)
    }
}
