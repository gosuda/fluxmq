//! HTTP server for metrics and admin endpoints
//!
//! This module provides HTTP endpoints for:
//! - Metrics export in JSON and Prometheus format
//! - Basic health check
//! - Admin operations

use crate::metrics::MetricsRegistry;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

pub struct HttpMetricsServer {
    metrics: Arc<MetricsRegistry>,
    port: u16,
}

impl HttpMetricsServer {
    pub fn new(metrics: Arc<MetricsRegistry>, port: u16) -> Self {
        Self { metrics, port }
    }

    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        info!("HTTP metrics server listening on port {}", self.port);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let metrics = Arc::clone(&self.metrics);
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, metrics).await {
                            warn!("HTTP connection error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept HTTP connection: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        mut stream: TcpStream,
        metrics: Arc<MetricsRegistry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = vec![0; 1024];
        let n = stream.read(&mut buffer).await?;
        let request = String::from_utf8_lossy(&buffer[..n]);

        let (response_body, content_type) = if request.starts_with("GET /metrics/json") {
            let snapshot = metrics.get_metrics_snapshot().await;
            match snapshot.to_json() {
                Ok(json) => (json, "application/json"),
                Err(e) => (format!("JSON Error: {}", e), "text/plain"),
            }
        } else if request.starts_with("GET /metrics") {
            let snapshot = metrics.get_metrics_snapshot().await;
            (
                snapshot.to_prometheus(),
                "text/plain; version=0.0.4; charset=utf-8",
            )
        } else if request.starts_with("GET /health") {
            (
                r#"{"status":"healthy","service":"fluxmq"}"#.to_string(),
                "application/json",
            )
        } else if request.starts_with("GET /") {
            (
                r#"
<html>
<head><title>FluxMQ Metrics</title></head>
<body>
    <h1>FluxMQ - Kafka-Compatible Message Broker</h1>
    <h2>Available Endpoints:</h2>
    <ul>
        <li><a href="/metrics">/metrics</a> - Prometheus format metrics</li>
        <li><a href="/metrics/json">/metrics/json</a> - JSON format metrics</li>
        <li><a href="/health">/health</a> - Health check</li>
    </ul>
</body>
</html>
            "#
                .to_string(),
                "text/html",
            )
        } else {
            ("404 Not Found".to_string(), "text/plain")
        };

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            content_type,
            response_body.len(),
            response_body
        );

        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;

        Ok(())
    }
}
