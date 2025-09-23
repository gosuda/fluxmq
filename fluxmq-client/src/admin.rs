//! Admin client for FluxMQ management operations

use crate::config::ClientConfig;
use crate::connection::ConnectionPool;
use crate::error::FluxmqClientError;
use crate::protocol::{Request, Response, TopicMetadata};
use std::sync::Arc;

/// Admin client for cluster management
pub struct AdminClient {
    connection_pool: Arc<ConnectionPool>,
}

impl AdminClient {
    /// Create a new admin client
    pub async fn new(config: ClientConfig) -> Result<Self, FluxmqClientError> {
        let connection_pool = Arc::new(ConnectionPool::new(
            config.brokers,
            config.connection_timeout,
            config.request_timeout,
        ));

        // Initialize connection
        connection_pool.get_connection().await?;

        Ok(Self { connection_pool })
    }

    /// List all topics
    pub async fn list_topics(&self) -> Result<Vec<String>, FluxmqClientError> {
        let metadata = self.get_metadata(Vec::new()).await?;
        Ok(metadata.into_iter().map(|tm| tm.name).collect())
    }

    /// Get metadata for specific topics
    pub async fn get_metadata(
        &self,
        topics: Vec<String>,
    ) -> Result<Vec<TopicMetadata>, FluxmqClientError> {
        let request = crate::protocol::MetadataRequest {
            correlation_id: 0,
            topics,
        };

        let response = self.send_request(Request::Metadata(request)).await?;

        match response {
            Response::Metadata(metadata_response) => Ok(metadata_response.topics),
            _ => Err(FluxmqClientError::protocol(
                "Unexpected response type for metadata request".to_string(),
            )),
        }
    }

    /// Close the admin client
    pub async fn close(&self) -> Result<(), FluxmqClientError> {
        self.connection_pool.close_all().await;
        Ok(())
    }

    async fn send_request(&self, request: Request) -> Result<Response, FluxmqClientError> {
        let connection = self.connection_pool.get_connection().await?;
        connection.send_request(request).await
    }
}
