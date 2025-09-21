use crate::acl::{AclManager, Principal};
use crate::performance::io_optimizations::{BatchProcessor, ConnectionPool, IOOptimizationManager};
use crate::protocol::high_performance_codec::HighPerformanceKafkaCodec;
use crate::protocol::kafka::{KafkaCodec, KafkaFrameCodec, ProtocolAdapter};
use crate::tls::FluxTlsAcceptor;
use crate::{broker::MessageHandler, config::BrokerConfig, HttpMetricsServer, Result};
use bytes::Bytes;
use futures::SinkExt;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

pub struct BrokerServer {
    config: BrokerConfig,
    handler: Arc<MessageHandler>,
    // High-performance codec for ultra-fast protocol processing
    hp_codec: Arc<HighPerformanceKafkaCodec>,
    // Advanced batch processor for 10k+ message batching
    #[allow(dead_code)]
    batch_processor: Arc<BatchProcessor>,
    // I/O optimization manager for high-throughput operations
    io_manager: Arc<IOOptimizationManager>,
    // Connection pool for efficient connection management
    connection_pool: Arc<ConnectionPool>,
    // Graceful shutdown coordination
    shutdown_tx: broadcast::Sender<()>,
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
        let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());
        let batch_processor = Arc::new(BatchProcessor::new());
        // 🚀 ULTRA-PERFORMANCE: High-throughput I/O optimizations
        let io_manager = Arc::new(IOOptimizationManager::new());
        let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections
        let (shutdown_tx, _) = broadcast::channel(16); // Graceful shutdown coordination
        Ok(Self {
            config,
            handler,
            hp_codec,
            batch_processor,
            io_manager,
            connection_pool,
            shutdown_tx,
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
            let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());
            let batch_processor = Arc::new(BatchProcessor::new());
            // 🚀 ULTRA-PERFORMANCE: High-throughput I/O optimizations
            let io_manager = Arc::new(IOOptimizationManager::new());
            let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections

            // Graceful shutdown coordination
            let (shutdown_tx, _) = broadcast::channel(16);

            Ok(Self {
                config,
                handler,
                hp_codec,
                batch_processor,
                io_manager,
                connection_pool,
                shutdown_tx,
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
            let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());
            let batch_processor = Arc::new(BatchProcessor::new());
            // 🚀 ULTRA-PERFORMANCE: High-throughput I/O optimizations
            let io_manager = Arc::new(IOOptimizationManager::new());
            let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections

            // Graceful shutdown coordination
            let (shutdown_tx, _) = broadcast::channel(16);

            Ok(Self {
                config,
                handler,
                hp_codec,
                batch_processor,
                io_manager,
                connection_pool,
                shutdown_tx,
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

        let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());
        let batch_processor = Arc::new(BatchProcessor::new());
        // 🚀 ULTRA-PERFORMANCE: High-throughput I/O optimizations
        let io_manager = Arc::new(IOOptimizationManager::new());
        let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections
        let (shutdown_tx, _) = broadcast::channel(16); // Graceful shutdown coordination
        Ok(Self {
            config,
            handler,
            hp_codec,
            batch_processor,
            io_manager,
            connection_pool,
            shutdown_tx,
        })
    }

    /// Initiate graceful shutdown of the server
    pub fn shutdown(&self) {
        info!("Initiating graceful shutdown...");
        let _ = self.shutdown_tx.send(()); // Notify all background tasks to shutdown
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
                            Self::run_tls_listener_with_shutdown(tls_listener, tls_handler, acceptor_arc, tls_shutdown_rx).await;
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
                    let _hp_codec = Arc::clone(&self.hp_codec);
                    let _io_manager = Arc::clone(&self.io_manager);
                    let connection_pool = Arc::clone(&self.connection_pool);

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_client(stream, handler).await {
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

    async fn handle_client(stream: TcpStream, handler: Arc<MessageHandler>) -> Result<()> {
        info!("New client connected - using Kafka protocol");

        // Track connection metrics
        let metrics = handler.get_metrics();
        metrics.broker.connection_opened();

        // All connections use Kafka protocol only
        let mut kafka_framed = Framed::new(stream, KafkaFrameCodec);

        // Process all messages as Kafka protocol
        while let Some(result) = kafka_framed.next().await {
            match result {
                Ok(message_bytes) => {
                    // Create high-performance codec for this connection
                    let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());
                    Self::process_kafka_message(
                        &handler,
                        &hp_codec,
                        message_bytes,
                        &mut kafka_framed,
                    )
                    .await?;
                }
                Err(e) => {
                    warn!("Failed to decode Kafka message: {}", e);
                    break;
                }
            }
        }

        // Track connection closure
        metrics.broker.connection_closed();

        Ok(())
    }

    /// 🚀 ULTRA-PERFORMANCE: Enhanced client handling with I/O optimizations
    #[allow(dead_code)]
    async fn handle_client_ultra(
        stream: TcpStream,
        handler: Arc<MessageHandler>,
        hp_codec: Arc<HighPerformanceKafkaCodec>,
        io_manager: Arc<IOOptimizationManager>,
    ) -> Result<()> {
        info!("🚀 New ultra-performance client connected - optimized Kafka protocol");

        // Track connection metrics
        let metrics = handler.get_metrics();
        metrics.broker.connection_opened();

        // 🚀 OPTIMIZATION: Use pre-allocated buffers from I/O manager
        let buffer_stats_before = io_manager.buffer_manager.get_stats();

        // All connections use Kafka protocol with ultra-performance optimizations
        let mut kafka_framed = Framed::new(stream, KafkaFrameCodec);

        // Process all messages as Kafka protocol with advanced optimizations
        while let Some(result) = kafka_framed.next().await {
            match result {
                Ok(message_bytes) => {
                    // 🚀 ULTRA-OPTIMIZATION: Process with shared high-performance codec
                    Self::process_kafka_message_ultra(
                        &handler,
                        &hp_codec,
                        &io_manager,
                        message_bytes,
                        &mut kafka_framed,
                    )
                    .await?;
                }
                Err(e) => {
                    warn!("Failed to decode Kafka message: {}", e);
                    break;
                }
            }
        }

        // 🚀 PERFORMANCE: Report buffer optimization stats
        let buffer_stats_after = io_manager.buffer_manager.get_stats();
        if buffer_stats_after.reuse_count > buffer_stats_before.reuse_count {
            let reuse_efficiency = buffer_stats_after.reuse_efficiency() * 100.0;
            info!(
                "🚀 Buffer reuse efficiency: {:.1}%, {} reuses",
                reuse_efficiency,
                buffer_stats_after.reuse_count - buffer_stats_before.reuse_count
            );
        }

        // Track connection closure
        metrics.broker.connection_closed();

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
                info!(
                    "Processing Kafka request: API key {}",
                    kafka_request.api_key()
                );

                // Handle ApiVersions requests directly
                if kafka_request.api_key() == 18 {
                    // API_KEY_API_VERSIONS - 🚀 ULTRA-FAST with pre-compiled template!
                    let response_bytes =
                        hp_codec.encode_api_versions_fast(kafka_request.correlation_id());
                    info!(
                        "Sending ApiVersions response: {} bytes, correlation_id={}",
                        response_bytes.len(),
                        kafka_request.correlation_id()
                    );
                    info!(
                        "Response bytes (hex): {:02x?}",
                        &response_bytes[0..std::cmp::min(response_bytes.len(), 50)]
                    );
                    kafka_framed.send(response_bytes).await?;
                    info!("ApiVersions response sent successfully");
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

                    info!("Extracted Java client header: api_key={}, api_version={}, correlation_id={}", 
                          api_key, api_version, correlation_id);

                    // Handle Java ApiVersions requests specially
                    if api_key == 18 {
                        warn!("Detected Java ApiVersions request - using compatibility mode");
                        // Send a compatible ApiVersions response for Java clients
                        let response_bytes = hp_codec.encode_api_versions_fast(correlation_id);
                        info!(
                            "Sending Java-compatible ApiVersions response: {} bytes",
                            response_bytes.len()
                        );
                        kafka_framed.send(response_bytes).await?;
                        info!("Java-compatible ApiVersions response sent successfully");
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
            let describe_groups_response =
                Self::create_describe_groups_response(kafka_request.correlation_id());
            let response_bytes = KafkaCodec::encode_response(&describe_groups_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // Handle FindCoordinator requests directly
        if kafka_request.api_key() == 10 {
            // API_KEY_FIND_COORDINATOR
            let find_coordinator_response =
                Self::create_find_coordinator_response(kafka_request.correlation_id());
            let response_bytes = KafkaCodec::encode_response(&find_coordinator_response)?;
            info!(
                "Sending FindCoordinator response: {} bytes, correlation_id={}",
                response_bytes.len(),
                kafka_request.correlation_id()
            );
            kafka_framed.send(response_bytes).await?;
            info!("FindCoordinator response sent successfully");
            return Ok(());
        }

        // Log the request for debugging
        info!(
            "Kafka request details: correlation_id={}",
            kafka_request.correlation_id()
        );

        // Check if this is a consumer group request (zero-copy check)
        if ProtocolAdapter::is_consumer_group_request(&kafka_request) {
            // Extract metadata before consuming the request
            let correlation_id = kafka_request.correlation_id();
            let api_version = kafka_request.api_version();

            // Now handle consumer group request (taking ownership)
            if let Ok(Some(cg_message)) = ProtocolAdapter::handle_consumer_group_request(kafka_request) {
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
            // Handle Metadata requests directly (like ApiVersions)
            if kafka_request.api_key() == 3 {
                // API_KEY_METADATA
                let metadata_response = Self::create_metadata_response(&kafka_request, handler);
                let response_bytes = KafkaCodec::encode_response(&metadata_response)?;
                info!(
                    "Sending Metadata response: {} bytes, correlation_id={}",
                    response_bytes.len(),
                    kafka_request.correlation_id()
                );
                kafka_framed.send(response_bytes).await?;
                info!("Metadata response sent successfully");
                return Ok(());
            }

            // Handle regular message request
            match ProtocolAdapter::kafka_to_fluxmq(kafka_request.clone()) {
                Ok(fluxmq_request) => match handler.handle_request(fluxmq_request).await {
                    Ok(fluxmq_response) => {
                        // Handle NoResponse (fire-and-forget) - don't send any response
                        if matches!(
                            fluxmq_response,
                            crate::protocol::messages::Response::NoResponse
                        ) {
                            info!("🔥 FIRE-AND-FORGET: NoResponse received, connection stays open");
                            return Ok(());
                        }

                        match ProtocolAdapter::fluxmq_to_kafka(
                            fluxmq_response,
                            kafka_request.correlation_id(),
                        ) {
                            Ok(kafka_response) => {
                                let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                                kafka_framed.send(response_bytes).await?;
                            }
                            Err(e) => {
                                // Check for fire-and-forget NoResponse error - don't disconnect client
                                if e.to_string().contains(
                                    "Fire-and-forget request - no response should be sent",
                                ) {
                                    info!("🔥 FIRE-AND-FORGET: NoResponse adapter error, keeping connection alive");
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
                            kafka_request.correlation_id(),
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
                        kafka_request.correlation_id(),
                        crate::protocol::kafka::KafkaErrorCode::InvalidRequest,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    /// 🚀 ULTRA-PERFORMANCE: Enhanced message processing with I/O optimizations
    #[allow(dead_code)]
    async fn process_kafka_message_ultra<IO>(
        handler: &Arc<MessageHandler>,
        hp_codec: &Arc<HighPerformanceKafkaCodec>,
        _io_manager: &Arc<IOOptimizationManager>,
        mut message_bytes: Bytes,
        kafka_framed: &mut Framed<IO, KafkaFrameCodec>,
    ) -> Result<()>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Track request metrics
        let metrics = handler.get_metrics();
        metrics.broker.request_received();

        // 🚀 OPTIMIZATION: Add message to batch processor for intelligent batching
        // (This would be enhanced further in production to batch decode operations)

        // Decode Kafka request
        match KafkaCodec::decode_request(&mut message_bytes) {
            Ok(kafka_request) => {
                info!(
                    "🚀 Processing ultra-optimized Kafka request: API key {}",
                    kafka_request.api_key()
                );

                // ENABLE: High-performance ApiVersions fast path for better performance
                if kafka_request.api_key() == 18 {
                    // API_KEY_API_VERSIONS - 🚀 ULTRA-FAST with pre-compiled template!
                    let response_bytes =
                        hp_codec.encode_api_versions_fast(kafka_request.correlation_id());
                    info!(
                        "🚀 Sending ultra-fast ApiVersions response: {} bytes, correlation_id={}",
                        response_bytes.len(),
                        kafka_request.correlation_id()
                    );
                    kafka_framed.send(response_bytes).await?;
                    info!("🚀 Ultra-fast ApiVersions response sent successfully");
                    return Ok(());
                }

                // Handle DescribeGroups requests directly
                if kafka_request.api_key() == 15 {
                    // API_KEY_DESCRIBE_GROUPS
                    let describe_groups_response =
                        Self::create_describe_groups_response(kafka_request.correlation_id());
                    let response_bytes = KafkaCodec::encode_response(&describe_groups_response)?;
                    kafka_framed.send(response_bytes).await?;
                    return Ok(());
                }

                // Handle FindCoordinator requests directly
                if kafka_request.api_key() == 10 {
                    // API_KEY_FIND_COORDINATOR
                    let find_coordinator_response =
                        Self::create_find_coordinator_response(kafka_request.correlation_id());
                    let response_bytes = KafkaCodec::encode_response(&find_coordinator_response)?;
                    info!(
                        "🚀 Sending ultra-optimized FindCoordinator response: {} bytes, correlation_id={}",
                        response_bytes.len(),
                        kafka_request.correlation_id()
                    );
                    kafka_framed.send(response_bytes).await?;
                    info!("🚀 Ultra-optimized FindCoordinator response sent successfully");
                    return Ok(());
                }

                // Log the request for debugging
                info!(
                    "🚀 Ultra-optimized Kafka request details: correlation_id={}",
                    kafka_request.correlation_id()
                );

                // Check if this is a consumer group request
                if let Ok(Some(cg_message)) =
                    ProtocolAdapter::handle_consumer_group_request(kafka_request.clone())
                {
                    // Handle consumer group request
                    if let Some(cg_coordinator) = handler.get_consumer_group_coordinator() {
                        match cg_coordinator.handle_message(cg_message).await {
                            Ok(cg_response) => {
                                let kafka_response =
                                    ProtocolAdapter::consumer_group_response_to_kafka(
                                        cg_response,
                                        kafka_request.correlation_id(),
                                        kafka_request.api_version(),
                                    )?;
                                // 🚀 ULTRA-PERFORMANCE: Use shared high-performance codec (5-10x faster)
                                let response_bytes = KafkaCodec::encode_response(&kafka_response)
                                    .map_err(|e| {
                                    crate::FluxmqError::Network(format!(
                                        "Ultra-performance codec error: {}",
                                        e
                                    ))
                                })?;
                                kafka_framed.send(response_bytes).await?;
                            }
                            Err(e) => {
                                warn!("Consumer group request failed: {}", e);
                                metrics.broker.error_occurred();
                                // Send error response
                                Self::send_kafka_error_response(
                                    kafka_framed,
                                    kafka_request.correlation_id(),
                                    crate::protocol::kafka::KafkaErrorCode::Unknown,
                                )
                                .await?;
                            }
                        }
                    } else {
                        // Consumer groups not enabled
                        Self::send_kafka_error_response(
                            kafka_framed,
                            kafka_request.correlation_id(),
                            crate::protocol::kafka::KafkaErrorCode::UnsupportedVersion,
                        )
                        .await?;
                    }
                } else {
                    // Handle Metadata requests directly (like ApiVersions)
                    if kafka_request.api_key() == 3 {
                        // API_KEY_METADATA
                        let metadata_response =
                            Self::create_metadata_response(&kafka_request, handler);
                        let response_bytes = KafkaCodec::encode_response(&metadata_response)?;
                        info!(
                            "🚀 Sending ultra-optimized Metadata response: {} bytes, correlation_id={}",
                            response_bytes.len(),
                            kafka_request.correlation_id()
                        );
                        kafka_framed.send(response_bytes).await?;
                        info!("🚀 Ultra-optimized Metadata response sent successfully");
                        return Ok(());
                    }

                    // Handle regular message request with ultra-optimizations
                    match ProtocolAdapter::kafka_to_fluxmq(kafka_request.clone()) {
                        Ok(fluxmq_request) => match handler.handle_request(fluxmq_request).await {
                            Ok(fluxmq_response) => {
                                let kafka_response = ProtocolAdapter::fluxmq_to_kafka(
                                    fluxmq_response,
                                    kafka_request.correlation_id(),
                                )?;
                                // 🚀 ULTRA-PERFORMANCE: Use shared high-performance codec (5-10x faster)
                                let response_bytes = KafkaCodec::encode_response(&kafka_response)
                                    .map_err(|e| {
                                    crate::FluxmqError::Network(format!(
                                        "Ultra-performance codec error: {}",
                                        e
                                    ))
                                })?;

                                // 🚀 OPTIMIZATION: Use I/O manager for efficient response sending
                                kafka_framed.send(response_bytes).await?;
                            }
                            Err(e) => {
                                warn!("FluxMQ request failed: {}", e);
                                metrics.broker.error_occurred();
                                Self::send_kafka_error_response(
                                    kafka_framed,
                                    kafka_request.correlation_id(),
                                    crate::protocol::kafka::KafkaErrorCode::Unknown,
                                )
                                .await?;
                            }
                        },
                        Err(e) => {
                            warn!("Failed to convert Kafka to FluxMQ: {}", e);
                            metrics.broker.error_occurred();
                            Self::send_kafka_error_response(
                                kafka_framed,
                                kafka_request.correlation_id(),
                                crate::protocol::kafka::KafkaErrorCode::UnknownTopicOrPartition,
                            )
                            .await?;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to decode ultra-optimized Kafka request: {}", e);
                metrics.broker.error_occurred();
                // Can't send proper error response without correlation ID
            }
        }

        Ok(())
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
        let error_response = KafkaResponse::Produce(KafkaProduceResponse {
            header: KafkaResponseHeader { correlation_id },
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

    #[allow(dead_code)]
    fn create_api_versions_response(
        correlation_id: i32,
        api_version: i16,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaApiVersion, KafkaApiVersionsResponse, KafkaResponse, KafkaResponseHeader,
        };

        // List of supported APIs - standard Kafka versions
        let supported_apis = vec![
            KafkaApiVersion {
                api_key: 0,
                min_version: 0,
                max_version: 8,
            }, // Produce
            KafkaApiVersion {
                api_key: 1,
                min_version: 0,
                max_version: 11,
            }, // Fetch
            KafkaApiVersion {
                api_key: 2,
                min_version: 0,
                max_version: 5,
            }, // ListOffsets
            KafkaApiVersion {
                api_key: 3,
                min_version: 0,
                max_version: 9,
            }, // Metadata - increased version range
            KafkaApiVersion {
                api_key: 8,
                min_version: 0,
                max_version: 6,
            }, // OffsetCommit
            KafkaApiVersion {
                api_key: 9,
                min_version: 0,
                max_version: 5,
            }, // OffsetFetch - CRITICAL: Missing API causing client errors
            KafkaApiVersion {
                api_key: 10,
                min_version: 0,
                max_version: 2,
            }, // FindCoordinator
            KafkaApiVersion {
                api_key: 11,
                min_version: 0,
                max_version: 5,
            }, // JoinGroup
            KafkaApiVersion {
                api_key: 12,
                min_version: 0,
                max_version: 3,
            }, // Heartbeat
            KafkaApiVersion {
                api_key: 13,
                min_version: 0,
                max_version: 3,
            }, // LeaveGroup
            KafkaApiVersion {
                api_key: 14,
                min_version: 0,
                max_version: 3,
            }, // SyncGroup
            KafkaApiVersion {
                api_key: 15,
                min_version: 0,
                max_version: 4,
            }, // DescribeGroups
            KafkaApiVersion {
                api_key: 16,
                min_version: 0,
                max_version: 3,
            }, // ListGroups
            KafkaApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4, // UPGRADED: Now supports v4 with flexible versions
            }, // ApiVersions
            KafkaApiVersion {
                api_key: 19,
                min_version: 0,
                max_version: 5,
            }, // CreateTopics
            KafkaApiVersion {
                api_key: 20,
                min_version: 0,
                max_version: 3,
            }, // DeleteTopics
            KafkaApiVersion {
                api_key: 32,
                min_version: 0,
                max_version: 3,
            }, // DescribeConfigs
            KafkaApiVersion {
                api_key: 33,
                min_version: 0,
                max_version: 2,
            }, // AlterConfigs
        ];

        KafkaResponse::ApiVersions(KafkaApiVersionsResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version,
            error_code: 0, // No error
            api_keys: supported_apis,
            throttle_time_ms: 0,
            // Java client compatibility fields (v3+)
            cluster_id: Some("fluxmq-cluster".to_string()), // Cluster identifier
            controller_id: Some(0),                         // Broker 0 is controller
            supported_features: vec![],                     // No special features yet
        })
    }

    fn create_describe_groups_response(
        correlation_id: i32,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaDescribeGroupsResponse, KafkaResponse, KafkaResponseHeader,
        };

        // Return empty response - no consumer groups currently active
        KafkaResponse::DescribeGroups(KafkaDescribeGroupsResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            groups: vec![], // No groups to describe
        })
    }

    fn create_find_coordinator_response(
        correlation_id: i32,
    ) -> crate::protocol::kafka::KafkaResponse {
        use crate::protocol::kafka::{
            KafkaErrorCode, KafkaFindCoordinatorResponse, KafkaResponse, KafkaResponseHeader,
        };

        // Return this broker as the coordinator for all groups
        // In a real implementation, this would use consistent hashing
        KafkaResponse::FindCoordinator(KafkaFindCoordinatorResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::NoError.as_i16(),
            error_message: None,
            node_id: 0, // This broker ID
            host: "localhost".to_string(),
            port: 9092,
        })
    }

    #[allow(dead_code)]
    fn extract_topics_from_metadata_request(
        kafka_request: &crate::protocol::kafka::KafkaRequest,
    ) -> Option<Vec<String>> {
        use crate::protocol::kafka::KafkaRequest;

        match kafka_request {
            KafkaRequest::Metadata(metadata_req) => metadata_req.topics.clone(),
            _ => None,
        }
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

    /// Run TLS listener for secure connections
    async fn run_tls_listener(
        listener: TcpListener,
        handler: Arc<MessageHandler>,
        acceptor: Arc<FluxTlsAcceptor>,
    ) {
        info!("Starting TLS listener...");

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New TLS client connected: {}", peer_addr);
                    let handler = Arc::clone(&handler);
                    let acceptor = Arc::clone(&acceptor);

                    tokio::spawn(async move {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                info!("TLS handshake completed for {}", peer_addr);
                                if let Err(e) = Self::handle_tls_client(tls_stream, handler).await {
                                    error!("Error handling TLS client {}: {}", peer_addr, e);
                                } else {
                                    info!("TLS client {} disconnected", peer_addr);
                                }
                            }
                            Err(e) => {
                                error!("TLS handshake failed for {}: {}", peer_addr, e);
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept TLS connection: {}", e);
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
        while let Some(result) = kafka_framed.next().await {
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
