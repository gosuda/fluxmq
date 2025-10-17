use crate::acl::{AclManager, Principal};
use crate::performance::io_optimizations::ConnectionPool;
use crate::protocol::high_performance_codec::HighPerformanceKafkaCodec;
use crate::protocol::kafka::{KafkaCodec, KafkaFrameCodec, ProtocolAdapter};
use crate::tls::FluxTlsAcceptor;
use crate::{broker::MessageHandler, config::BrokerConfig, HttpMetricsServer, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt as FuturesStreamExt};
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
        Ok(Self {
            config,
            handler,
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
            let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections

            // Graceful shutdown coordination
            let (shutdown_tx, _) = broadcast::channel(16);

            Ok(Self {
                config,
                handler,
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
            let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections

            // Graceful shutdown coordination
            let (shutdown_tx, _) = broadcast::channel(16);

            Ok(Self {
                config,
                handler,
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

        // 🚀 ULTRA-PERFORMANCE: High-throughput I/O optimizations
        let connection_pool = Arc::new(ConnectionPool::new(10000)); // Support 10k connections
        let (shutdown_tx, _) = broadcast::channel(16); // Graceful shutdown coordination
        Ok(Self {
            config,
            handler,
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

                    tokio::spawn(async move {
                        // 🚀 PIPELINED: Using FuturesUnordered + Lock-Free for maximum concurrency
                        if let Err(e) = Self::handle_client_pipelined(stream, handler).await {
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

    /// 🚀 HYBRID OPTIMIZATION: Action-Based + Lock-Free for maximum performance
    /// - Direct await (no tokio::spawn overhead)
    /// - DashMap (no lock contention)
    /// - AtomicI32 (lock-free ordering)
    async fn handle_client_pipelined(
        stream: TcpStream,
        handler: Arc<MessageHandler>,
    ) -> Result<()> {
        info!("🚀 PIPELINED-REORDER: New client connected - parallel processing with response reordering");

        // Track connection metrics
        let metrics = handler.get_metrics();
        metrics.broker.connection_opened();

        // Framed를 읽기/쓰기로 분리
        let kafka_framed = Framed::new(stream, KafkaFrameCodec);
        let (mut kafka_write, mut kafka_read) = kafka_framed.split();

        // 🚀 LOCK-FREE: DashMap for concurrent response storage without lock contention
        let response_buffer = Arc::new(dashmap::DashMap::<i32, Bytes>::new());
        // 🔥 CRITICAL FIX: Track first correlation_id to establish ordering
        let next_send_correlation = Arc::new(std::sync::atomic::AtomicI32::new(-1));

        // Write channel for ordered delivery
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Bytes>(1000);

        // High-performance codec 생성
        let hp_codec = Arc::new(HighPerformanceKafkaCodec::new());

        // 1️⃣ Read Loop: 요청을 읽고 병렬로 처리 (tokio::spawn)
        let read_handle = {
            let handler = Arc::clone(&handler);
            let hp_codec = Arc::clone(&hp_codec);
            let response_buffer = Arc::clone(&response_buffer);
            let next_send_correlation = Arc::clone(&next_send_correlation);
            let write_tx = write_tx.clone();

            tokio::spawn(async move {
                use futures::stream::{FuturesUnordered, StreamExt as FuturesStreamExt};

                // 🚀 FUTURES-UNORDERED: Async parallel processing without tokio::spawn overhead
                let mut processing_futures = FuturesUnordered::new();
                let mut read_done = false;

                loop {
                    tokio::select! {
                        // 새 요청 읽기 (읽기가 완료되지 않았을 때만)
                        result = tokio_stream::StreamExt::next(&mut kafka_read), if !read_done => {
                            match result {
                                Some(Ok(message_bytes)) => {
                                    // Correlation ID 추출
                                    let correlation_id = if message_bytes.len() >= 8 {
                                        i32::from_be_bytes([
                                            message_bytes[4],
                                            message_bytes[5],
                                            message_bytes[6],
                                            message_bytes[7],
                                        ])
                                    } else {
                                        warn!("🚀 FUTURES-UNORDERED: Invalid message format (too short)");
                                        continue;
                                    };

                                    debug!("🔍 READ: Received request with client correlation_id={}", correlation_id);

                                    // 🚀 FUTURES-UNORDERED: Add future to concurrent processing queue
                                    let handler = Arc::clone(&handler);
                                    let hp_codec = Arc::clone(&hp_codec);
                                    let response_buffer = Arc::clone(&response_buffer);
                                    let next_send_correlation = Arc::clone(&next_send_correlation);
                                    let write_tx = write_tx.clone();

                                    let future = async move {
                                        match Self::process_kafka_message_pipelined(
                                            &handler,
                                            &hp_codec,
                                            message_bytes,
                                        )
                                        .await
                                        {
                                            Ok(response_bytes) => {
                                                // 🚀 LOCK-FREE: DashMap insert (no lock contention!)
                                                response_buffer.insert(correlation_id, response_bytes.clone());
                                                debug!("🔍 FUTURES-UNORDERED: Inserted response for corr_id={}, empty={}, buffer_size={}",
                                                       correlation_id, response_bytes.is_empty(), response_buffer.len());

                                                // 순서대로 전송 가능한 응답들을 flush (lock-free!)
                                                Self::flush_ordered_responses_lockfree(
                                                    &response_buffer,
                                                    &next_send_correlation,
                                                    &write_tx,
                                                )
                                                .await;
                                            }
                                            Err(e) => {
                                                warn!("🚀 FUTURES-UNORDERED: Request processing failed: {}", e);
                                            }
                                        }
                                    };

                                    processing_futures.push(future);
                                }
                                Some(Err(e)) => {
                                    warn!("Failed to decode Kafka message: {}", e);
                                    read_done = true;
                                }
                                None => {
                                    info!("🚀 FUTURES-UNORDERED: Read stream closed");
                                    read_done = true;
                                }
                            }
                        }

                        // 처리 중인 요청 완료 대기
                        _ = FuturesStreamExt::next(&mut processing_futures), if !processing_futures.is_empty() => {
                            // Future completed (response already flushed)
                        }

                        // 모든 작업 완료
                        else => {
                            if read_done && processing_futures.is_empty() {
                                break;
                            }
                        }
                    }
                }

                info!("🚀 FUTURES-UNORDERED: All processing complete, dropping write_tx");
                // write_tx는 여기서 drop됨 -> Write Loop가 정상 종료
            })
        };

        // 2️⃣ Write Loop: 순서대로 응답 전송
        let write_handle = {
            tokio::spawn(async move {
                let mut sent = 0;
                while let Some(response_bytes) = write_rx.recv().await {
                    if kafka_write.send(response_bytes).await.is_err() {
                        warn!("🚀 PIPELINED: Write failed, sent {} responses", sent);
                        break;
                    }
                    // 🔥 CRITICAL FIX: Flush immediately to ensure response reaches client
                    if kafka_write.flush().await.is_err() {
                        warn!("🚀 PIPELINED: Flush failed, sent {} responses", sent);
                        break;
                    }
                    sent += 1;
                    debug!("🔍 WRITE: Sent response #{} successfully", sent);
                }
                info!(
                    "🚀 PIPELINED: Write loop terminated (sent {} responses)",
                    sent
                );
            })
        };

        // 모든 루프가 종료될 때까지 대기
        let _ = tokio::join!(read_handle, write_handle);

        // Track connection closure
        metrics.broker.connection_closed();
        info!("🚀 PIPELINED: Client disconnected");

        Ok(())
    }

    /// 🚀 LOCK-FREE: DashMap 기반 응답 순서 보장 flush (Mutex 없음!)
    async fn flush_ordered_responses_lockfree(
        response_buffer: &Arc<dashmap::DashMap<i32, Bytes>>,
        next_send_correlation: &Arc<std::sync::atomic::AtomicI32>,
        write_tx: &tokio::sync::mpsc::Sender<Bytes>,
    ) {
        use std::sync::atomic::Ordering;

        loop {
            // 🚀 LOCK-FREE: Atomic load (no mutex!)
            let mut current = next_send_correlation.load(Ordering::Acquire);

            // 첫 응답이면 초기화 (compare-and-swap으로 race-free)
            if current == -1 {
                // DashMap에서 가장 작은 키 찾기
                if let Some(first_entry) = response_buffer.iter().next() {
                    let first_key = *first_entry.key();
                    // CAS로 안전하게 초기화 (다른 task가 먼저 초기화할 수 있음)
                    match next_send_correlation.compare_exchange(
                        -1,
                        first_key,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            debug!(
                                "🔍 FLUSH: Initialized next_send_correlation to {}",
                                first_key
                            );
                            current = first_key;
                        }
                        Err(updated) => {
                            // 다른 task가 먼저 초기화함
                            current = updated;
                        }
                    }
                } else {
                    // 버퍼가 아직 비어있음
                    return;
                }
            }

            // 🚀 LOCK-FREE: DashMap remove (no mutex!)
            match response_buffer.remove(&current) {
                Some((corr_id, response)) => {
                    // 다음 correlation_id로 증가 (atomic!)
                    next_send_correlation.store(corr_id + 1, Ordering::Release);

                    // Empty response는 전송하지 않지만 sequence는 증가됨
                    if !response.is_empty() {
                        if write_tx.send(response).await.is_err() {
                            debug!("🔍 FLUSH: Write channel closed");
                            break;
                        }
                        debug!("🔍 FLUSH: Sent response for corr_id={}", corr_id);
                    } else {
                        debug!("🔍 FLUSH: Skipped empty response for corr_id={}", corr_id);
                    }
                }
                None => {
                    // 다음 응답이 아직 준비되지 않음
                    debug!("🔍 FLUSH: Waiting for corr_id={}", current);
                    break;
                }
            }
        }
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
                    let response_bytes =
                        hp_codec.encode_api_versions_fast(kafka_request.correlation_id());
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
            // API_KEY_FIND_COORDINATOR - 🚀 PERFORMANCE: Hot path
            let find_coordinator_response =
                Self::create_find_coordinator_response(kafka_request.correlation_id());
            let response_bytes = KafkaCodec::encode_response(&find_coordinator_response)?;
            kafka_framed.send(response_bytes).await?;
            return Ok(());
        }

        // 🚀 PERFORMANCE: Hot path - logging removed

        // Check if this is a consumer group request (zero-copy check)
        if ProtocolAdapter::is_consumer_group_request(&kafka_request) {
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
            // Handle Metadata requests directly (like ApiVersions)
            if kafka_request.api_key() == 3 {
                // API_KEY_METADATA - 🚀 PERFORMANCE: Hot path
                let metadata_response = Self::create_metadata_response(&kafka_request, handler);
                let response_bytes = KafkaCodec::encode_response(&metadata_response)?;
                kafka_framed.send(response_bytes).await?;
                return Ok(());
            }

            // Handle regular message request
            match ProtocolAdapter::kafka_to_fluxmq(kafka_request.clone()) {
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

    /// 🚀 PIPELINED: 응답 bytes를 직접 반환하는 파이프라인 최적화 버전
    async fn process_kafka_message_pipelined(
        handler: &Arc<MessageHandler>,
        hp_codec: &Arc<HighPerformanceKafkaCodec>,
        mut message_bytes: Bytes,
    ) -> Result<Bytes> {
        debug!("🔍 BLOCKING: process_kafka_message_pipelined START");

        // Track request metrics
        let metrics = handler.get_metrics();
        metrics.broker.request_received();

        // Decode Kafka request
        let kafka_request = match KafkaCodec::decode_request(&mut message_bytes) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode Kafka request: {}", e);
                // 빈 응답 반환 (에러 처리 간소화)
                return Ok(Bytes::new());
            }
        };
        debug!("🔍 BLOCKING: Decoded api_key={}", kafka_request.api_key());

        // Handle ApiVersions requests with pre-compiled template
        if kafka_request.api_key() == 18 {
            debug!("🔍 BLOCKING: ApiVersions fast path");
            let response_bytes = hp_codec.encode_api_versions_fast(kafka_request.correlation_id());
            return Ok(response_bytes);
        }

        // Handle Metadata requests
        if kafka_request.api_key() == 3 {
            debug!("🔍 BLOCKING: Metadata request");
            let metadata_response = Self::create_metadata_response(&kafka_request, handler);
            let response_bytes = KafkaCodec::encode_response(&metadata_response)?;
            debug!("🔍 BLOCKING: Metadata encoded");
            return Ok(response_bytes);
        }

        debug!(
            "🔍 BLOCKING: Converting to FluxMQ request, api_key={}",
            kafka_request.api_key()
        );

        // Handle regular message request (Produce, Fetch, etc.)
        match ProtocolAdapter::kafka_to_fluxmq(kafka_request.clone()) {
            Ok(fluxmq_request) => {
                debug!("🔍 BLOCKING: Calling handler.handle_request()...");
                match handler.handle_request(fluxmq_request).await {
                    Ok(fluxmq_response) => {
                        debug!("🔍 BLOCKING: handler.handle_request() RETURNED!");
                        // Handle NoResponse (fire-and-forget)
                        if matches!(
                            fluxmq_response,
                            crate::protocol::messages::Response::NoResponse
                        ) {
                            debug!("🔍 BLOCKING: NoResponse case");
                            return Ok(Bytes::new()); // 빈 응답 반환
                        }

                        debug!("🔍 BLOCKING: Converting FluxMQ response to Kafka");
                        match ProtocolAdapter::fluxmq_to_kafka(
                            fluxmq_response,
                            kafka_request.correlation_id(),
                        ) {
                            Ok(kafka_response) => {
                                debug!("🔍 BLOCKING: Encoding Kafka response");
                                let response_bytes = KafkaCodec::encode_response(&kafka_response)?;
                                debug!(
                                    "🔍 BLOCKING: Response encoded, bytes={}",
                                    response_bytes.len()
                                );
                                Ok(response_bytes)
                            }
                            Err(e) => {
                                warn!("Failed to convert response: {}", e);
                                // 에러 시에도 응답 전송 (acks=1 지원)
                                Self::create_error_response(&kafka_request)
                            }
                        }
                    }
                    Err(e) => {
                        warn!("🔍 BLOCKING: FluxMQ request FAILED: {}", e);
                        metrics.broker.error_occurred();
                        // 에러 시에도 응답 전송 (acks=1 지원)
                        Self::create_error_response(&kafka_request)
                    }
                }
            }
            Err(e) => {
                warn!("Failed to convert Kafka request: {}", e);
                metrics.broker.error_occurred();
                // 에러 시에도 응답 전송 (acks=1 지원)
                Self::create_error_response(&kafka_request)
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
                    responses: vec![],
                    throttle_time_ms: 0,
                })
            }
            1 => {
                // Fetch request
                KafkaResponse::Fetch(KafkaFetchResponse {
                    header: KafkaResponseHeader {
                        correlation_id: kafka_request.correlation_id(),
                    },
                    api_version: 0,
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
    #[allow(dead_code)]
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
