/// Integration of sendfile for Fetch responses
///
/// This module provides efficient zero-copy transfer of messages from
/// storage files directly to network sockets for Fetch responses.
use crate::performance::sendfile_zero_copy::SendfileTransfer;
use crate::protocol::{Message, Offset};
use crate::Result;
use std::fs::File;
use std::path::Path;
use tokio::net::TcpStream;

/// High-performance Fetch response handler using sendfile
pub struct FetchSendfileHandler {
    transfer: SendfileTransfer,
}

impl FetchSendfileHandler {
    pub fn new() -> Self {
        Self {
            transfer: SendfileTransfer::new(),
        }
    }

    /// Send messages directly from storage file to socket using sendfile
    /// This avoids copying data to userspace, achieving kernel-level zero-copy
    pub async fn send_messages_sendfile(
        &mut self,
        segment_path: &Path,
        socket: &TcpStream,
        messages: &[(Offset, Message, usize, usize)], // (offset, message, file_offset, size)
    ) -> Result<usize> {
        // Open the segment file
        let file = File::open(segment_path).map_err(|e| crate::FluxmqError::Storage(e))?;

        let mut total_sent = 0;

        // Send each message directly using sendfile
        for (_msg_offset, _message, file_offset, size) in messages {
            let bytes_sent = self
                .transfer
                .sendfile_to_socket(&file, socket, *file_offset as u64, *size)
                .await?;

            total_sent += bytes_sent;

            if bytes_sent < *size {
                // Partial send, might need to handle this
                tracing::warn!("Partial sendfile: sent {} of {} bytes", bytes_sent, size);
                break;
            }
        }

        tracing::debug!(
            "Sendfile transferred {} bytes for {} messages",
            total_sent,
            messages.len()
        );

        Ok(total_sent)
    }

    /// Send a single large segment using sendfile
    pub async fn send_segment_range(
        &mut self,
        segment_path: &Path,
        socket: &TcpStream,
        offset: u64,
        length: usize,
    ) -> Result<usize> {
        let file = File::open(segment_path).map_err(|e| crate::FluxmqError::Storage(e))?;

        let bytes_sent = self
            .transfer
            .sendfile_to_socket(&file, socket, offset, length)
            .await?;

        tracing::debug!(
            "Sendfile segment range: offset={}, length={}, sent={}",
            offset,
            length,
            bytes_sent
        );

        Ok(bytes_sent)
    }

    /// Get sendfile statistics
    pub fn get_stats(&self) -> String {
        self.transfer.get_stats().report()
    }
}

/// Enhanced Fetch response with sendfile support
pub struct SendfileFetchResponse {
    pub correlation_id: i32,
    pub topic: String,
    pub partition: i32,

    // Option 1: In-memory messages (traditional)
    pub messages: Vec<(Offset, Message)>,

    // Option 2: Sendfile metadata for zero-copy
    pub sendfile_segments: Vec<SendfileSegment>,
}

/// Metadata for sendfile-based message transfer
#[derive(Debug, Clone)]
pub struct SendfileSegment {
    pub segment_path: String,
    pub file_offset: u64,
    pub length: usize,
    pub message_count: usize,
}

impl SendfileFetchResponse {
    /// Check if this response can use sendfile optimization
    pub fn can_use_sendfile(&self) -> bool {
        !self.sendfile_segments.is_empty()
    }

    /// Calculate total bytes to be sent
    pub fn total_bytes(&self) -> usize {
        if self.can_use_sendfile() {
            self.sendfile_segments.iter().map(|s| s.length).sum()
        } else {
            self.messages
                .iter()
                .map(|(_, msg)| {
                    msg.value.len() + msg.key.as_ref().map(|k| k.len()).unwrap_or(0) + 8 + 8 + 4
                    // offset + timestamp + header size
                })
                .sum()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_fetch_sendfile_handler() {
        // Create test directory and file
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_0.log");

        // Write test data
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&segment_path)
            .unwrap();

        let test_data = b"Test message data for sendfile";
        file.write_all(test_data).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Create handler
        let handler = FetchSendfileHandler::new();

        // Test segment range send
        // Note: This would need a real socket in production
        // For testing, we're just validating the API

        assert!(segment_path.exists());

        // Verify stats reporting
        let stats = handler.get_stats();
        assert!(stats.contains("Sendfile Stats"));
    }
}
