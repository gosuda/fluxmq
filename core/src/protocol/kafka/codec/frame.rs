//! Kafka frame codec for TCP message framing (length-prefixed messages).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{KafkaCodecError, Result};

use std::io::Cursor;
#[cfg(debug_assertions)]
use tracing::debug;

/// Frame codec that handles Kafka message framing (length-prefixed messages)
pub struct KafkaFrameCodec;

impl Decoder for KafkaFrameCodec {
    type Item = Bytes;
    type Error = KafkaCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < 4 {
            // Need at least 4 bytes for message length
            return Ok(None);
        }

        // Peek at message length without consuming bytes
        let message_length = {
            let mut cursor = Cursor::new(src.as_ref());
            cursor.get_i32()
        };

        // Kafka default max.request.size is ~1MB; 16MB is a generous upper bound
        const MAX_FRAME_SIZE: i32 = 16 * 1024 * 1024; // 16MB
        if message_length < 0 || message_length > MAX_FRAME_SIZE {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Invalid message length: {}",
                message_length
            )));
        }

        let total_length = 4 + message_length as usize;

        if src.len() < total_length {
            // Don't have the full message yet
            return Ok(None);
        }

        // Extract the complete message including length prefix
        let mut full_message = src.split_to(total_length);
        // Skip the 4-byte length prefix to get just the Kafka message
        full_message.advance(4); // Remove first 4 bytes (length prefix)
        let message = full_message.freeze();
        #[cfg(debug_assertions)]
        debug!("KafkaFrameCodec: Decoded request without length prefix: {} bytes", message.len());
        Ok(Some(message))
    }
}

impl Encoder<Bytes> for KafkaFrameCodec {
    type Error = KafkaCodecError;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        // CRITICAL FIX: Kafka protocol requires 4-byte length prefix before response body
        // Java client expects: [LENGTH][RESPONSE_BODY] format
        // Without this prefix, Java misreads the response structure
        let message_len = item.len() as u32;
        dst.put_u32(message_len);  // Big-endian 4-byte length prefix
        dst.extend_from_slice(&item);
        #[cfg(debug_assertions)]
        debug!("KafkaFrameCodec: Encoded response with length prefix: {} bytes total", message_len + 4);

        // DEBUG: Show exact hex bytes after framing (first 12 bytes)
        if dst.len() >= 12 {
            let first_12_bytes = &dst[0..12];
            #[cfg(debug_assertions)]
            debug!("  - FRAMED RESPONSE BYTES: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
                  first_12_bytes[0], first_12_bytes[1], first_12_bytes[2], first_12_bytes[3],
                  first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7],
                  first_12_bytes[8], first_12_bytes[9], first_12_bytes[10], first_12_bytes[11]);
            #[cfg(debug_assertions)]
            debug!("  - LENGTH_PREFIX: {} = 0x{:08x}", message_len, message_len);
            #[cfg(debug_assertions)]
            debug!("  - ACTUAL_CORRELATION_ID_AT_BYTES_4-7: {} (this should be correlation_id=1)",
                  i32::from_be_bytes([first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7]]));
        }
        Ok(())
    }
}
