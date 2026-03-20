//! Kafka wire protocol primitive encoding and decoding functions.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
use tracing::warn;

use super::{KafkaCodecError, Result};

#[cfg(debug_assertions)]
use tracing::debug;

use crate::protocol::kafka::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_DELETE_TOPICS,
    API_KEY_DESCRIBE_GROUPS, API_KEY_FETCH, API_KEY_FIND_COORDINATOR,
    API_KEY_HEARTBEAT, API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS,
    API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_FETCH,
    API_KEY_PRODUCE, API_KEY_SYNC_GROUP,
    API_KEY_INIT_PRODUCER_ID, API_KEY_ADD_PARTITIONS_TO_TXN, API_KEY_ADD_OFFSETS_TO_TXN,
    API_KEY_END_TXN, API_KEY_TXN_OFFSET_COMMIT,
};

impl super::KafkaCodec {
    // ========================================================================
    // BASIC ENCODING/DECODING PRIMITIVES
    // ========================================================================

    pub(crate) fn decode_array_count(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<usize> {
        if cursor.remaining() < 4 {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "{}: not enough bytes for array count",
                context
            )));
        }
        let raw = cursor.get_i32();
        if raw < 0 {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "{}: negative array count {}",
                context, raw
            )));
        }
        // Cap at 100_000 elements — no legitimate Kafka request has more.
        let count = raw as usize;
        if count > 100_000 {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "{}: array count {} exceeds maximum 100000",
                context, count
            )));
        }
        Ok(count)
    }

    pub(crate) fn decode_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
        if cursor.remaining() < 2 {
            return Err(KafkaCodecError::BufferUnderrun {
                needed: 2,
                available: cursor.remaining(),
            });
        }
        let len = cursor.get_i16();
        #[cfg(debug_assertions)]
        debug!("decode_string: length = {}", len);
        if len == -1 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null string".to_string(),
            ));
        }

        if len == 0 {
            #[cfg(debug_assertions)]
            debug!("decode_string: empty string");
            return Ok(String::new());
        }

        // Note: len is i16, so already bounded to -32768..32767
        // No need for upper bound check since i16 max is 32767

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len().saturating_sub(cursor.position() as usize);
        if len as usize > remaining {
            // DEFENSIVE: Log detailed information for debugging framing issues
            warn!("Buffer underrun in decode_string: requested {} bytes, only {} available at position {}. Buffer total length: {}",
                  len, remaining, cursor.position(), cursor.get_ref().len());
            return Err(KafkaCodecError::InvalidFormat(format!(
                "String length {} exceeds remaining buffer size {}. This may indicate a framing error or corrupt message.",
                len, remaining
            )));
        }

        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        let result = String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;
        #[cfg(debug_assertions)]
        debug!("decode_string: result = '{}'", result);
        Ok(result)
    }

    pub(crate) fn decode_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>> {
        if cursor.remaining() < 2 {
            return Err(KafkaCodecError::BufferUnderrun {
                needed: 2,
                available: cursor.remaining(),
            });
        }
        let len = cursor.get_i16();
        if len == -1 {
            return Ok(None);
        }

        if len == 0 {
            return Ok(Some(String::new()));
        }

        // Note: len is i16, so already bounded to -32768..32767
        // No need for upper bound check since i16 max is 32767

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len().saturating_sub(cursor.position() as usize);
        if len as usize > remaining {
            // DEFENSIVE: If buffer underrun, return a graceful error instead of panic
            warn!("Buffer underrun in decode_nullable_string: requested {} bytes, only {} available at position {}. Buffer total length: {}",
                  len, remaining, cursor.position(), cursor.get_ref().len());
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Nullable string length {} exceeds remaining buffer size {}. This may indicate a framing error or corrupt message.",
                len, remaining
            )));
        }

        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        let s = String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;
        Ok(Some(s))
    }


    pub(crate) fn decode_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Bytes> {
        let len = cursor.get_i32();
        if len == -1 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null bytes".to_string(),
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len().saturating_sub(cursor.position() as usize);
        if len as usize > remaining {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Bytes length {} exceeds remaining buffer size {}", len, remaining
            )));
        }

        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        Ok(Bytes::from(buf))
    }

    pub(crate) fn decode_nullable_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Option<Bytes>> {
        let len = cursor.get_i32();
        if len == -1 {
            return Ok(None);
        }

        if len == 0 {
            return Ok(Some(Bytes::new()));
        }

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len().saturating_sub(cursor.position() as usize);
        if len as usize > remaining {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Nullable bytes length {} exceeds remaining buffer size {}", len, remaining
            )));
        }

        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        Ok(Some(Bytes::from(buf)))
    }

    pub(crate) fn encode_string(s: &str, buf: &mut BytesMut) {
        buf.put_i16(s.len() as i16);
        buf.put_slice(s.as_bytes());
    }

    pub(crate) fn encode_nullable_string(s: &Option<String>, buf: &mut BytesMut) {
        match s {
            Some(s) => {
                buf.put_i16(s.len() as i16);
                buf.put_slice(s.as_bytes());
            }
            None => buf.put_i16(-1),
        }
    }

    pub(crate) fn encode_bytes(bytes: &Bytes, buf: &mut BytesMut) {
        buf.put_i32(bytes.len() as i32);
        buf.put_slice(bytes);
    }

    pub(crate) fn encode_nullable_bytes(bytes: &Option<Bytes>, buf: &mut BytesMut) {
        match bytes {
            Some(bytes) => {
                buf.put_i32(bytes.len() as i32);
                buf.put_slice(bytes);
            }
            None => buf.put_i32(-1),
        }
    }

    // ========================================================================
    // VARINT / FLEXIBLE FORMAT ENCODING/DECODING
    // ========================================================================

    pub(crate) fn encode_varint(buf: &mut BytesMut, value: u64) {
        let mut val = value;
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val != 0 {
                byte |= 0x80;
            }
            buf.put_u8(byte);
            if val == 0 {
                break;
            }
        }
    }

    /// Decodes a varint (variable-length integer) according to LEB128 encoding
    pub(crate) fn decode_varint(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;

        loop {
            if shift >= 64 {
                return Err(KafkaCodecError::InvalidFormat("Varint too large".to_string()));
            }

            if !cursor.has_remaining() {
                return Err(KafkaCodecError::InvalidFormat("Incomplete varint".to_string()));
            }

            let byte = cursor.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;

            if (byte & 0x80) == 0 {
                break;
            }

            shift += 7;
        }

        Ok(result)
    }


    /// Decodes a compact string (nullable string with varint length) for flexible versions
    pub(crate) fn decode_compact_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            // Null string
            return Ok(None);
        }

        if len == 1 {
            // Empty string
            return Ok(Some(String::new()));
        }

        // Actual length is len - 1
        let actual_len = (len - 1) as usize;
        // Guard against excessively large varint-decoded string lengths (max 10MB)
        const MAX_COMPACT_STRING_LEN: usize = 10 * 1024 * 1024;
        if actual_len > MAX_COMPACT_STRING_LEN {
            return Err(KafkaCodecError::InvalidFormat(
                format!("Compact string length {} exceeds limit {}", actual_len, MAX_COMPACT_STRING_LEN)
            ));
        }
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(
                "Not enough bytes for compact string".to_string()
            ));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        let result = String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;

        Ok(Some(result))
    }

    /// Encodes an empty tagged field array (just a varint 0)
    #[allow(dead_code)]
    pub(crate) fn encode_empty_tagged_fields(buf: &mut BytesMut) {
        // Empty tagged field array = varint 0
        buf.put_u8(0);
    }

    /// Decodes tagged fields (for now, just skip them) - with improved error handling
    #[allow(dead_code)]
    pub(crate) fn decode_tagged_fields(cursor: &mut Cursor<&[u8]>) -> Result<()> {
        // If no remaining bytes, assume empty tagged fields
        if cursor.remaining() == 0 {
            #[cfg(debug_assertions)]
            debug!("No remaining bytes for tagged fields, treating as empty");
            return Ok(());
        }

        let num_fields = match Self::decode_varint(cursor) {
            Ok(n) => n,
            Err(e) => {
                #[cfg(debug_assertions)]
                debug!("Failed to decode tagged field count, treating as empty: {}", e);
                // If we can't even read the count, treat as empty tagged fields
                return Ok(());
            }
        };

        // Guard against excessively large tagged field count from untrusted varint
        const MAX_TAGGED_FIELDS: u64 = 1_000;
        if num_fields > MAX_TAGGED_FIELDS {
            return Err(KafkaCodecError::InvalidFormat(
                format!("Tagged field count {} exceeds limit {}", num_fields, MAX_TAGGED_FIELDS)
            ));
        }

        #[cfg(debug_assertions)]
        debug!("Decoding {} tagged fields", num_fields);

        // For each tagged field, skip tag and data
        for i in 0..num_fields {
            #[cfg(debug_assertions)]
            debug!("Processing tagged field {}/{}", i + 1, num_fields);

            // Skip tag
            let tag = match Self::decode_varint(cursor) {
                Ok(tag) => tag,
                Err(e) => {
                    #[cfg(debug_assertions)]
                    debug!("Failed to decode tag {}: {}, stopping tagged field parsing", i, e);
                    break;
                }
            };

            // Skip data length
            let data_len = match Self::decode_varint(cursor) {
                Ok(len) => len,
                Err(e) => {
                    #[cfg(debug_assertions)]
                    debug!("Failed to decode data length for tag {}: {}, stopping tagged field parsing", tag, e);
                    break;
                }
            };

            #[cfg(debug_assertions)]
            debug!("Tagged field: tag={}, data_len={}", tag, data_len);

            // Skip data bytes with bounds checking
            if cursor.remaining() < data_len as usize {
                #[cfg(debug_assertions)]
                debug!(
                    "Not enough bytes for tagged field data: needed={}, available={}, treating as end of fields",
                    data_len, cursor.remaining()
                );
                break;
            }
            cursor.advance(data_len as usize);
            #[cfg(debug_assertions)]
            debug!("Successfully skipped tagged field data ({} bytes)", data_len);
        }

        #[cfg(debug_assertions)]
        debug!("Completed tagged fields parsing");
        Ok(())
    }

    /// Encodes a compact string (string with varint length) for flexible versions
    #[allow(dead_code)]
    pub(crate) fn encode_compact_string(s: &str, buf: &mut BytesMut) {
        // CRITICAL FIX: Compact string encoding MUST use varint consistently
        // Java Kafka expects: readUnsignedVarint() - 1 = actual_length
        let len = s.len();
        let varint_len = len as u64 + 1;
        Self::encode_varint(buf, varint_len);
        #[cfg(debug_assertions)]
        debug!("  - FIXED compact string '{}': length={}, encoded as varint {} (Java will read {})",
              s, len, varint_len, len);
        buf.put_slice(s.as_bytes());
    }

    /// Encodes a compact nullable string (with varint length) for flexible versions
    #[allow(dead_code)]
    pub(crate) fn encode_compact_nullable_string(s: &Option<String>, buf: &mut BytesMut) {
        match s {
            Some(s) => {
                // CRITICAL FIX: Compact nullable string MUST use varint consistently
                // Java Kafka expects: readUnsignedVarint() - 1 = actual_length
                let len = s.len();
                let varint_len = len as u64 + 1;
                Self::encode_varint(buf, varint_len);
                #[cfg(debug_assertions)]
                debug!("  - FIXED compact nullable string '{}': length={}, encoded as varint {} (Java will read {})",
                      s, len, varint_len, len);
                buf.put_slice(s.as_bytes());
            }
            None => {
                // CRITICAL FIX: Null string MUST be encoded as single byte 0, not varint
                buf.put_u8(0);
                #[cfg(debug_assertions)]
                debug!("  - FIXED compact nullable string: null, encoded as 0x00");
            }
        }
    }


    /// Encodes a compact array (array with varint length) for flexible versions
    #[allow(dead_code)]
    pub(crate) fn encode_compact_array_len(len: usize, buf: &mut BytesMut) {
        // Compact arrays use length + 1 encoding (0 = null, 1 = empty, 2+ = length + 1)
        Self::encode_varint(buf, len as u64 + 1);
    }

    /// Decodes a compact array length (returns actual count, not encoded value)
    /// Returns None if array is null (encoded as 0)
    #[allow(dead_code)]
    pub(crate) fn decode_compact_array_len(cursor: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let encoded = Self::decode_varint(cursor)?;
        if encoded == 0 {
            // Null array
            Ok(None)
        } else {
            // Actual length is encoded - 1
            Ok(Some((encoded - 1) as usize))
        }
    }

    /// Decodes a compact string (non-nullable) for flexible versions
    /// Unlike decode_compact_string which returns Option<String>, this requires non-null
    #[allow(dead_code)]
    pub(crate) fn decode_compact_non_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            // Null string - should not happen for non-nullable
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null compact string".to_string(),
            ));
        }

        if len == 1 {
            // Empty string
            return Ok(String::new());
        }

        // Actual length is len - 1
        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Compact string length {} exceeds remaining buffer {}",
                actual_len, cursor.remaining()
            )));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))
    }

    /// Encodes compact bytes (bytes with varint length) for flexible versions
    #[allow(dead_code)]
    pub(crate) fn encode_compact_bytes(bytes: &Bytes, buf: &mut BytesMut) {
        // Compact bytes use length + 1 encoding
        let len = bytes.len();
        Self::encode_varint(buf, len as u64 + 1);
        buf.put_slice(bytes);
    }

    /// Encodes compact nullable bytes for flexible versions
    #[allow(dead_code)]
    pub(crate) fn encode_compact_nullable_bytes(bytes: &Option<Bytes>, buf: &mut BytesMut) {
        match bytes {
            Some(b) => {
                let len = b.len();
                Self::encode_varint(buf, len as u64 + 1);
                buf.put_slice(b);
            }
            None => {
                // Null bytes = 0
                buf.put_u8(0);
            }
        }
    }

    /// Decodes compact bytes for flexible versions
    #[allow(dead_code)]
    pub(crate) fn decode_compact_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Bytes> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null compact bytes".to_string(),
            ));
        }

        if len == 1 {
            return Ok(Bytes::new());
        }

        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Compact bytes length {} exceeds remaining buffer {}",
                actual_len, cursor.remaining()
            )));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        Ok(Bytes::from(buf))
    }

    /// Decodes compact nullable bytes for flexible versions
    #[allow(dead_code)]
    pub(crate) fn decode_compact_nullable_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Option<Bytes>> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            return Ok(None);
        }

        if len == 1 {
            return Ok(Some(Bytes::new()));
        }

        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Compact nullable bytes length {} exceeds remaining buffer {}",
                actual_len, cursor.remaining()
            )));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        Ok(Some(Bytes::from(buf)))
    }

    /// Helper to determine if an API version uses flexible encoding
    /// Based on Kafka protocol specifications
    #[allow(dead_code)]
    pub(crate) fn is_flexible_version(api_key: i16, api_version: i16) -> bool {
        match api_key {
            API_KEY_PRODUCE => api_version >= 9,
            API_KEY_FETCH => api_version >= 12,
            API_KEY_LIST_OFFSETS => api_version >= 6,
            API_KEY_METADATA => api_version >= 9,
            API_KEY_OFFSET_COMMIT => api_version >= 8,
            API_KEY_OFFSET_FETCH => api_version >= 6,
            API_KEY_FIND_COORDINATOR => api_version >= 3,
            API_KEY_JOIN_GROUP => api_version >= 6,
            API_KEY_HEARTBEAT => api_version >= 4,
            API_KEY_LEAVE_GROUP => api_version >= 4,
            API_KEY_SYNC_GROUP => api_version >= 4,
            API_KEY_DESCRIBE_GROUPS => api_version >= 5,
            API_KEY_LIST_GROUPS => api_version >= 3,
            API_KEY_API_VERSIONS => api_version >= 3,
            API_KEY_CREATE_TOPICS => api_version >= 5,
            API_KEY_DELETE_TOPICS => api_version >= 4,
            API_KEY_INIT_PRODUCER_ID => api_version >= 2,
            API_KEY_ADD_PARTITIONS_TO_TXN => api_version >= 2,
            API_KEY_ADD_OFFSETS_TO_TXN => api_version >= 2,
            API_KEY_END_TXN => api_version >= 2,
            API_KEY_TXN_OFFSET_COMMIT => api_version >= 2,
            32 => api_version >= 4, // DescribeConfigs
            33 => api_version >= 2, // AlterConfigs
            _ => false,
        }
    }

    /// Skip tagged fields (for decoding requests where we don't need the field values)
    #[allow(dead_code)]
    pub(crate) fn skip_tagged_fields(cursor: &mut Cursor<&[u8]>) -> Result<()> {
        if cursor.remaining() == 0 {
            return Ok(());
        }

        // Read the number of tagged fields (varint)
        let num_fields = match Self::decode_varint(cursor) {
            Ok(n) => n,
            Err(_) => return Ok(()), // If we can't read varint, just assume 0 fields
        };

        // If num_fields is 0, we're done
        if num_fields == 0 {
            return Ok(());
        }

        for _ in 0..num_fields {
            // Check if there's enough data for at least tag and length varints
            if cursor.remaining() < 2 {
                // Not enough data, just return OK (might be trailing garbage)
                return Ok(());
            }

            // Skip tag
            if Self::decode_varint(cursor).is_err() {
                return Ok(()); // Can't read tag, just return
            }

            // Get data length
            let data_len = match Self::decode_varint(cursor) {
                Ok(len) => len as usize,
                Err(_) => return Ok(()), // Can't read length, just return
            };

            if cursor.remaining() < data_len {
                // Not enough data for this field, just skip what we can
                let skip = cursor.remaining();
                cursor.advance(skip);
                return Ok(());
            }
            cursor.advance(data_len);
        }

        Ok(())
    }
}

// ========================================================================
// UNIT TESTS
// ========================================================================

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::io::Cursor;

    #[test]
    fn test_string_encoding_decoding() {
        let mut buf = BytesMut::new();
        let test_string = "hello world";

        KafkaCodec::encode_string(test_string, &mut buf);

        let mut cursor = Cursor::new(buf.as_ref());
        let decoded = KafkaCodec::decode_string(&mut cursor).unwrap();

        assert_eq!(decoded, test_string);
    }

    #[test]
    fn test_nullable_string_encoding_decoding() {
        let mut buf = BytesMut::new();

        // Test non-null string
        KafkaCodec::encode_nullable_string(&Some("test".to_string()), &mut buf);
        // Test null string
        KafkaCodec::encode_nullable_string(&None, &mut buf);

        let mut cursor = Cursor::new(buf.as_ref());

        let decoded1 = KafkaCodec::decode_nullable_string(&mut cursor).unwrap();
        let decoded2 = KafkaCodec::decode_nullable_string(&mut cursor).unwrap();

        assert_eq!(decoded1, Some("test".to_string()));
        assert_eq!(decoded2, None);
    }

    #[test]
    fn test_bytes_encoding_decoding() {
        let mut buf = BytesMut::new();
        let test_bytes = Bytes::from("test data");

        KafkaCodec::encode_bytes(&test_bytes, &mut buf);

        let mut cursor = Cursor::new(buf.as_ref());
        let decoded = KafkaCodec::decode_bytes(&mut cursor).unwrap();

        assert_eq!(decoded, test_bytes);
    }
}
