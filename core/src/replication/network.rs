use super::{BrokerId, ReplicationMessage};
use crate::Result;
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Network layer for replication communication between brokers
#[derive(Debug)]
pub struct ReplicationNetwork {
    broker_id: BrokerId,
    listen_addr: SocketAddr,
    /// Connections to other brokers (peer_id -> connection)
    peer_connections: Arc<RwLock<HashMap<BrokerId, Arc<PeerConnection>>>>,
    /// Message handlers for incoming replication messages
    message_handlers: Arc<RwLock<HashMap<BrokerId, mpsc::UnboundedSender<ReplicationMessage>>>>,
    /// Network configuration
    config: NetworkConfig,
}

/// Configuration for replication network
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Message timeout in milliseconds
    pub message_timeout_ms: u64,
    /// Maximum number of retries for failed connections
    pub max_retries: u32,
    /// Keep-alive interval in milliseconds
    pub keepalive_interval_ms: u64,
    /// Maximum message size in bytes
    pub max_message_size: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connection_timeout_ms: 5000,
            message_timeout_ms: 3000,
            max_retries: 3,
            keepalive_interval_ms: 1000,
            max_message_size: 1024 * 1024, // 1MB
        }
    }
}

/// Represents a connection to a peer broker
#[derive(Debug)]
pub struct PeerConnection {
    _broker_id: BrokerId,
    _addr: SocketAddr,
    sender: mpsc::UnboundedSender<ReplicationMessage>,
    _handle: tokio::task::JoinHandle<()>,
}

/// Network message format for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkMessage {
    from_broker: BrokerId,
    to_broker: BrokerId,
    sequence: u64,
    payload: ReplicationMessage,
}

impl ReplicationNetwork {
    pub fn new(broker_id: BrokerId, listen_addr: SocketAddr, config: NetworkConfig) -> Self {
        Self {
            broker_id,
            listen_addr,
            peer_connections: Arc::new(RwLock::new(HashMap::new())),
            message_handlers: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Start the replication network server
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        info!(
            "Replication network started on {} for broker {}",
            self.listen_addr, self.broker_id
        );

        let peer_connections = Arc::clone(&self.peer_connections);
        let message_handlers = Arc::clone(&self.message_handlers);
        let broker_id = self.broker_id;
        let config = self.config.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!("Accepted replication connection from {}", addr);
                        let peer_connections = Arc::clone(&peer_connections);
                        let message_handlers = Arc::clone(&message_handlers);
                        let config = config.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_incoming_connection(
                                stream,
                                addr,
                                broker_id,
                                peer_connections,
                                message_handlers,
                                config,
                            )
                            .await
                            {
                                error!("Failed to handle incoming replication connection: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept replication connection: {}", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Connect to a peer broker
    pub async fn connect_to_peer(&self, peer_id: BrokerId, peer_addr: SocketAddr) -> Result<()> {
        // Check if already connected
        {
            let connections = self.peer_connections.read().await;
            if connections.contains_key(&peer_id) {
                debug!("Already connected to broker {}", peer_id);
                return Ok(());
            }
        }

        info!("Connecting to peer broker {} at {}", peer_id, peer_addr);

        let stream = timeout(
            Duration::from_millis(self.config.connection_timeout_ms),
            TcpStream::connect(peer_addr),
        )
        .await
        .map_err(|_| crate::FluxmqError::Network("Connection timeout".to_string()))?
        .map_err(|e| crate::FluxmqError::Storage(e))?;

        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = self.spawn_connection_handler(stream, peer_id, sender.clone(), receiver);

        let connection = Arc::new(PeerConnection {
            _broker_id: peer_id,
            _addr: peer_addr,
            sender,
            _handle: handle,
        });

        {
            let mut connections = self.peer_connections.write().await;
            connections.insert(peer_id, connection);
        }

        info!("Successfully connected to peer broker {}", peer_id);
        Ok(())
    }

    /// Send a message to a peer broker
    pub async fn send_to_peer(&self, peer_id: BrokerId, message: ReplicationMessage) -> Result<()> {
        let connection = {
            let connections = self.peer_connections.read().await;
            connections.get(&peer_id).cloned()
        };

        if let Some(conn) = connection {
            conn.sender.send(message).map_err(|e| {
                crate::FluxmqError::Replication(format!(
                    "Failed to send message to peer {}: {}",
                    peer_id, e
                ))
            })?;
            Ok(())
        } else {
            Err(crate::FluxmqError::Replication(format!(
                "No connection to peer broker {}",
                peer_id
            )))
        }
    }

    /// Register a message handler for incoming replication messages
    pub async fn register_message_handler(
        &self,
        handler: mpsc::UnboundedSender<ReplicationMessage>,
    ) {
        let mut handlers = self.message_handlers.write().await;
        handlers.insert(self.broker_id, handler);
    }

    /// Disconnect from a peer broker
    pub async fn disconnect_from_peer(&self, peer_id: BrokerId) {
        let mut connections = self.peer_connections.write().await;
        if let Some(connection) = connections.remove(&peer_id) {
            debug!("Disconnected from peer broker {}", peer_id);
            // Connection will be dropped, which will stop the handler
            drop(connection);
        }
    }

    /// Get connected peer broker IDs
    pub async fn get_connected_peers(&self) -> Vec<BrokerId> {
        let connections = self.peer_connections.read().await;
        connections.keys().copied().collect()
    }

    /// Handle incoming connection from another broker
    async fn handle_incoming_connection(
        stream: TcpStream,
        _addr: SocketAddr,
        local_broker_id: BrokerId,
        _peer_connections: Arc<RwLock<HashMap<BrokerId, Arc<PeerConnection>>>>,
        message_handlers: Arc<RwLock<HashMap<BrokerId, mpsc::UnboundedSender<ReplicationMessage>>>>,
        config: NetworkConfig,
    ) -> Result<()> {
        let mut stream = stream;

        // Read handshake to identify the peer broker
        let mut handshake_buf = [0u8; 8];
        stream
            .read_exact(&mut handshake_buf)
            .await
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        let peer_broker_id = u32::from_be_bytes([
            handshake_buf[0],
            handshake_buf[1],
            handshake_buf[2],
            handshake_buf[3],
        ]);

        // Send handshake response
        let response = local_broker_id.to_be_bytes();
        stream
            .write_all(&response)
            .await
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        debug!(
            "Completed handshake with peer broker {} (incoming connection)",
            peer_broker_id
        );

        // Handle messages from this peer
        Self::handle_peer_messages(
            stream,
            peer_broker_id,
            local_broker_id,
            message_handlers,
            config,
        )
        .await
    }

    /// Spawn connection handler for outgoing connection
    fn spawn_connection_handler(
        &self,
        mut stream: TcpStream,
        peer_id: BrokerId,
        _sender: mpsc::UnboundedSender<ReplicationMessage>,
        mut receiver: mpsc::UnboundedReceiver<ReplicationMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let local_broker_id = self.broker_id;
        let message_handlers = Arc::clone(&self.message_handlers);
        let config = self.config.clone();

        tokio::spawn(async move {
            // Send handshake
            let handshake = local_broker_id.to_be_bytes();
            if let Err(e) = stream.write_all(&handshake).await {
                error!("Failed to send handshake to peer {}: {}", peer_id, e);
                return;
            }

            // Read handshake response
            let mut response_buf = [0u8; 4];
            if let Err(e) = stream.read_exact(&mut response_buf).await {
                error!(
                    "Failed to read handshake response from peer {}: {}",
                    peer_id, e
                );
                return;
            }

            let confirmed_peer_id = u32::from_be_bytes(response_buf);
            if confirmed_peer_id != peer_id {
                error!(
                    "Handshake mismatch: expected {}, got {}",
                    peer_id, confirmed_peer_id
                );
                return;
            }

            debug!("Completed handshake with peer broker {}", peer_id);

            // Split stream for bidirectional communication
            let (read_half, write_half) = stream.into_split();

            // Spawn message sender task
            let sender_handle = {
                let mut write_half = write_half;
                tokio::spawn(async move {
                    while let Some(message) = receiver.recv().await {
                        if let Err(e) =
                            Self::send_message(&mut write_half, &message, local_broker_id, peer_id)
                                .await
                        {
                            error!("Failed to send message to peer {}: {}", peer_id, e);
                            break;
                        }
                    }
                })
            };

            // Handle incoming messages
            let receiver_result = Self::handle_peer_messages(
                read_half,
                peer_id,
                local_broker_id,
                message_handlers,
                config,
            )
            .await;

            // Clean up
            sender_handle.abort();
            if let Err(e) = receiver_result {
                error!("Error handling messages from peer {}: {}", peer_id, e);
            }
        })
    }

    /// Handle messages from a peer broker
    async fn handle_peer_messages<T>(
        mut stream: T,
        peer_broker_id: BrokerId,
        local_broker_id: BrokerId,
        message_handlers: Arc<RwLock<HashMap<BrokerId, mpsc::UnboundedSender<ReplicationMessage>>>>,
        config: NetworkConfig,
    ) -> Result<()>
    where
        T: AsyncReadExt + Unpin,
    {
        let mut buffer = BytesMut::with_capacity(config.max_message_size);

        loop {
            // Read message length
            let mut len_buf = [0u8; 4];
            if let Err(e) = stream.read_exact(&mut len_buf).await {
                debug!("Peer {} disconnected: {}", peer_broker_id, e);
                break;
            }

            let message_len = u32::from_be_bytes(len_buf) as usize;
            if message_len > config.max_message_size {
                error!(
                    "Message from peer {} too large: {} bytes",
                    peer_broker_id, message_len
                );
                break;
            }

            // Read message data
            buffer.clear();
            buffer.resize(message_len, 0);
            if let Err(e) = stream.read_exact(&mut buffer).await {
                error!("Failed to read message from peer {}: {}", peer_broker_id, e);
                break;
            }

            // Deserialize message
            let network_message: NetworkMessage = match bincode::deserialize(&buffer) {
                Ok(msg) => msg,
                Err(e) => {
                    error!(
                        "Failed to deserialize message from peer {}: {}",
                        peer_broker_id, e
                    );
                    continue;
                }
            };

            // Verify message addressing
            if network_message.to_broker != local_broker_id {
                warn!(
                    "Received message for wrong broker: expected {}, got {}",
                    local_broker_id, network_message.to_broker
                );
                continue;
            }

            // Forward to appropriate handler
            let handlers = message_handlers.read().await;
            if let Some(handler) = handlers.get(&local_broker_id) {
                if let Err(e) = handler.send(network_message.payload) {
                    error!("Failed to forward message to handler: {}", e);
                    break;
                }
            } else {
                warn!("No handler registered for replication messages");
            }
        }

        Ok(())
    }

    /// Send a message to a peer
    async fn send_message<T>(
        stream: &mut T,
        message: &ReplicationMessage,
        from_broker: BrokerId,
        to_broker: BrokerId,
    ) -> Result<()>
    where
        T: AsyncWriteExt + Unpin,
    {
        let network_message = NetworkMessage {
            from_broker,
            to_broker,
            sequence: 0, // TODO: Implement sequence numbers
            payload: message.clone(),
        };

        let serialized = bincode::serialize(&network_message).map_err(|e| {
            crate::FluxmqError::Protocol(format!("Failed to serialize message: {}", e))
        })?;

        // Send message length followed by message data
        let len = serialized.len() as u32;
        stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        stream
            .write_all(&serialized)
            .await
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        stream
            .flush()
            .await
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        Ok(())
    }
}

/// Replication network manager that integrates with the broker
#[derive(Debug)]
pub struct ReplicationNetworkManager {
    network: Arc<ReplicationNetwork>,
    broker_registry: Arc<RwLock<HashMap<BrokerId, SocketAddr>>>,
}

impl ReplicationNetworkManager {
    pub fn new(broker_id: BrokerId, listen_addr: SocketAddr, config: NetworkConfig) -> Self {
        let network = Arc::new(ReplicationNetwork::new(broker_id, listen_addr, config));

        Self {
            network,
            broker_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the network manager
    pub async fn start(&self) -> Result<()> {
        self.network.start().await
    }

    /// Register a broker address
    pub async fn register_broker(&self, broker_id: BrokerId, addr: SocketAddr) {
        let mut registry = self.broker_registry.write().await;
        registry.insert(broker_id, addr);
        info!("Registered broker {} at address {}", broker_id, addr);
    }

    /// Connect to all registered brokers
    pub async fn connect_to_all_brokers(&self) -> Result<()> {
        let registry = self.broker_registry.read().await;
        for (&broker_id, &addr) in registry.iter() {
            if let Err(e) = self.network.connect_to_peer(broker_id, addr).await {
                warn!("Failed to connect to broker {}: {}", broker_id, e);
            }
        }
        Ok(())
    }

    /// Send a replication message
    pub async fn send_replication_message(
        &self,
        target_broker: BrokerId,
        message: ReplicationMessage,
    ) -> Result<()> {
        self.network.send_to_peer(target_broker, message).await
    }

    /// Register message handler
    pub async fn register_handler(&self, handler: mpsc::UnboundedSender<ReplicationMessage>) {
        self.network.register_message_handler(handler).await;
    }

    /// Get network statistics
    pub async fn get_stats(&self) -> NetworkStats {
        let connected_peers = self.network.get_connected_peers().await;
        let registered_brokers = {
            let registry = self.broker_registry.read().await;
            registry.len()
        };

        NetworkStats {
            connected_peers: connected_peers.len(),
            registered_brokers,
            active_connections: connected_peers,
        }
    }
}

/// Network statistics for monitoring
#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub connected_peers: usize,
    pub registered_brokers: usize,
    pub active_connections: Vec<BrokerId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_network_creation() {
        let broker_id = 1;
        let listen_addr = "127.0.0.1:0".parse().unwrap();
        let config = NetworkConfig::default();

        let network = ReplicationNetwork::new(broker_id, listen_addr, config);
        assert_eq!(network.broker_id, broker_id);
    }

    #[tokio::test]
    async fn test_network_manager_creation() {
        let broker_id = 1;
        let listen_addr = "127.0.0.1:0".parse().unwrap();
        let config = NetworkConfig::default();

        let manager = ReplicationNetworkManager::new(broker_id, listen_addr, config);

        // Test broker registration
        manager
            .register_broker(2, "127.0.0.1:9093".parse().unwrap())
            .await;

        let stats = manager.get_stats().await;
        assert_eq!(stats.registered_brokers, 1);
        assert_eq!(stats.connected_peers, 0); // Not connected yet
    }
}
