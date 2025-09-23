//! Connection management for FluxMQ client

use crate::error::FluxmqClientError;
use crate::protocol::{ClientCodec, Request, Response};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

/// Connection pool for managing broker connections
#[derive(Debug)]
pub struct ConnectionPool {
    connections: DashMap<String, Arc<Connection>>,
    brokers: Arc<RwLock<Vec<String>>>,
    connection_timeout: Duration,
    request_timeout: Duration,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(
        brokers: Vec<String>,
        connection_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        Self {
            connections: DashMap::new(),
            brokers: Arc::new(RwLock::new(brokers)),
            connection_timeout,
            request_timeout,
        }
    }

    /// Get a connection to any available broker
    pub async fn get_connection(&self) -> Result<Arc<Connection>, FluxmqClientError> {
        let brokers = self.brokers.read().clone();

        if brokers.is_empty() {
            return Err(FluxmqClientError::NoBrokersAvailable);
        }

        // Try existing connections first
        for broker in &brokers {
            if let Some(conn) = self.connections.get(broker) {
                if conn.is_healthy().await {
                    return Ok(conn.clone());
                } else {
                    // Remove unhealthy connection
                    self.connections.remove(broker);
                }
            }
        }

        // Create new connection to first available broker
        for broker in &brokers {
            match self.create_connection(broker.clone()).await {
                Ok(conn) => {
                    let conn_arc = Arc::new(conn);
                    self.connections.insert(broker.clone(), conn_arc.clone());
                    return Ok(conn_arc);
                }
                Err(e) => {
                    warn!("Failed to connect to broker {}: {}", broker, e);
                    continue;
                }
            }
        }

        Err(FluxmqClientError::NoBrokersAvailable)
    }

    /// Get a connection to a specific broker
    pub async fn get_connection_to(
        &self,
        broker: &str,
    ) -> Result<Arc<Connection>, FluxmqClientError> {
        // Check existing connection
        if let Some(conn) = self.connections.get(broker) {
            if conn.is_healthy().await {
                return Ok(conn.clone());
            } else {
                self.connections.remove(broker);
            }
        }

        // Create new connection
        let conn = self.create_connection(broker.to_string()).await?;
        let conn_arc = Arc::new(conn);
        self.connections
            .insert(broker.to_string(), conn_arc.clone());
        Ok(conn_arc)
    }

    async fn create_connection(&self, broker: String) -> Result<Connection, FluxmqClientError> {
        debug!("Creating connection to broker: {}", broker);

        let stream = timeout(self.connection_timeout, TcpStream::connect(&broker))
            .await
            .map_err(|_| FluxmqClientError::timeout(self.connection_timeout.as_millis() as u64))?
            .map_err(|e| {
                FluxmqClientError::connection(format!("Failed to connect to {}: {}", broker, e))
            })?;

        let connection = Connection::new(broker, stream, self.request_timeout).await?;
        info!("Connected to broker: {}", connection.broker);
        Ok(connection)
    }

    /// Update broker list
    pub fn update_brokers(&self, brokers: Vec<String>) {
        *self.brokers.write() = brokers;
    }

    /// Close all connections
    pub async fn close_all(&self) {
        for entry in self.connections.iter() {
            entry.value().close().await;
        }
        self.connections.clear();
    }
}

/// Individual connection to a broker
#[derive(Debug)]
pub struct Connection {
    pub broker: String,
    correlation_counter: AtomicI32,
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>,
    _handle: tokio::task::JoinHandle<()>,
}

impl Connection {
    /// Create a new connection
    pub async fn new(
        broker: String,
        stream: TcpStream,
        request_timeout: Duration,
    ) -> Result<Self, FluxmqClientError> {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let framed = Framed::new(stream, ClientCodec::new());

        let broker_clone = broker.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) =
                Self::connection_loop(broker_clone, framed, request_rx, request_timeout).await
            {
                error!("Connection loop error: {}", e);
            }
        });

        Ok(Self {
            broker,
            correlation_counter: AtomicI32::new(0),
            request_tx,
            _handle: handle,
        })
    }

    /// Send a request and wait for response
    pub async fn send_request(&self, mut request: Request) -> Result<Response, FluxmqClientError> {
        let correlation_id = self.correlation_counter.fetch_add(1, Ordering::SeqCst);

        // Set correlation ID
        match &mut request {
            Request::Produce(ref mut req) => req.correlation_id = correlation_id,
            Request::Fetch(ref mut req) => req.correlation_id = correlation_id,
            Request::Metadata(ref mut req) => req.correlation_id = correlation_id,
        }

        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send((request, response_tx))
            .map_err(|_| FluxmqClientError::connection("Connection closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| FluxmqClientError::connection("Response channel closed".to_string()))
    }

    /// Check if connection is healthy
    pub async fn is_healthy(&self) -> bool {
        !self.request_tx.is_closed()
    }

    /// Close the connection
    pub async fn close(&self) {
        // The connection loop will exit when the request_tx is dropped
        // and the handle will complete
    }

    async fn connection_loop(
        broker: String,
        mut framed: Framed<TcpStream, ClientCodec>,
        mut request_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>,
        request_timeout: Duration,
    ) -> Result<(), FluxmqClientError> {
        let mut pending_requests: VecDeque<oneshot::Sender<Response>> = VecDeque::new();

        loop {
            tokio::select! {
                // Handle incoming requests
                request = request_rx.recv() => {
                    match request {
                        Some((req, response_tx)) => {
                            debug!("Sending request to {}: {:?}", broker, req);

                            if let Err(e) = framed.send(req).await {
                                error!("Failed to send request to {}: {}", broker, e);
                                let _ = response_tx.send(Response::Produce(crate::protocol::ProduceResponse {
                                    correlation_id: -1,
                                    topic: String::new(),
                                    partition: 0,
                                    base_offset: 0,
                                    error_code: -1,
                                    error_message: Some(format!("Send error: {}", e)),
                                }));
                                break;
                            }

                            pending_requests.push_back(response_tx);
                        }
                        None => {
                            debug!("Request channel closed for {}", broker);
                            break;
                        }
                    }
                }

                // Handle incoming responses
                response = framed.next() => {
                    match response {
                        Some(Ok(resp)) => {
                            debug!("Received response from {}: {:?}", broker, resp);

                            if let Some(response_tx) = pending_requests.pop_front() {
                                let _ = response_tx.send(resp);
                            } else {
                                warn!("Received response with no pending request from {}", broker);
                            }
                        }
                        Some(Err(e)) => {
                            error!("Error receiving response from {}: {}", broker, e);

                            // Send errors to all pending requests
                            while let Some(response_tx) = pending_requests.pop_front() {
                                let _ = response_tx.send(Response::Produce(crate::protocol::ProduceResponse {
                                    correlation_id: -1,
                                    topic: String::new(),
                                    partition: 0,
                                    base_offset: 0,
                                    error_code: -1,
                                    error_message: Some(format!("Connection error: {}", e)),
                                }));
                            }
                            break;
                        }
                        None => {
                            debug!("Response stream closed for {}", broker);
                            break;
                        }
                    }
                }

                // Handle timeout for requests
                _ = tokio::time::sleep(request_timeout) => {
                    if !pending_requests.is_empty() {
                        warn!("Request timeout for {} pending requests to {}", pending_requests.len(), broker);

                        // Send timeout errors
                        while let Some(response_tx) = pending_requests.pop_front() {
                            let _ = response_tx.send(Response::Produce(crate::protocol::ProduceResponse {
                                correlation_id: -1,
                                topic: String::new(),
                                partition: 0,
                                base_offset: 0,
                                error_code: 7, // REQUEST_TIMED_OUT
                                error_message: Some("Request timed out".to_string()),
                            }));
                        }
                    }
                }
            }
        }

        info!("Connection to {} closed", broker);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_pool_creation() {
        let pool = ConnectionPool::new(
            vec!["localhost:9092".to_string()],
            Duration::from_secs(30),
            Duration::from_secs(30),
        );

        assert_eq!(pool.connections.len(), 0);
        assert_eq!(pool.brokers.read().len(), 1);
    }

    #[test]
    fn test_update_brokers() {
        let pool = ConnectionPool::new(
            vec!["localhost:9092".to_string()],
            Duration::from_secs(30),
            Duration::from_secs(30),
        );

        let new_brokers = vec!["broker1:9092".to_string(), "broker2:9092".to_string()];
        pool.update_brokers(new_brokers.clone());

        assert_eq!(*pool.brokers.read(), new_brokers);
    }
}
