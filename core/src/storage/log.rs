use crate::protocol::{Message, Offset};
use crate::{FluxmqError, Result};
use bytes::Bytes;
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Log record format:
/// [length: 4 bytes][crc: 4 bytes][timestamp: 8 bytes][key_len: 4 bytes][key: key_len bytes][value: remaining bytes]
const RECORD_HEADER_SIZE: usize = 20; // 4 + 4 + 8 + 4

#[derive(Debug)]
pub struct LogEntry {
    pub offset: Offset,
    pub timestamp: u64,
    pub key: Option<Bytes>,
    pub value: Bytes,
}

impl LogEntry {
    pub fn from_message(offset: Offset, message: &Message) -> Self {
        Self {
            offset,
            timestamp: message.timestamp,
            key: message.key.clone(),
            value: message.value.clone(),
        }
    }

    pub fn to_message(&self) -> Message {
        Message {
            key: self.key.clone(),
            value: self.value.clone(),
            timestamp: self.timestamp,
            headers: std::collections::HashMap::new(),
        }
    }

    /// Calculate the serialized size of this entry
    pub fn serialized_size(&self) -> usize {
        let key_len = self.key.as_ref().map_or(0, |k| k.len());
        RECORD_HEADER_SIZE + key_len + self.value.len()
    }

    /// Serialize this entry to bytes
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let key_len = self.key.as_ref().map_or(0, |k| k.len()) as u32;
        let value_len = self.value.len() as u32;
        // Total length is payload size (excluding the 4-byte length prefix itself)
        let payload_len = 16 + key_len + value_len; // 4(crc) + 8(timestamp) + 4(key_len) + key + value

        let mut buf = Vec::with_capacity(payload_len as usize + 4); // +4 for length prefix

        // Write length prefix (payload size, not including this 4-byte prefix)
        buf.extend_from_slice(&payload_len.to_be_bytes());

        // Calculate CRC32 of the payload (everything after length and crc fields)
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.timestamp.to_be_bytes());
        payload.extend_from_slice(&key_len.to_be_bytes());
        if let Some(key) = &self.key {
            payload.extend_from_slice(key);
        }
        payload.extend_from_slice(&self.value);

        let crc = crc32fast::hash(&payload);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&payload);

        Ok(buf)
    }

    /// Deserialize an entry from bytes
    pub fn deserialize(data: &[u8], offset: Offset) -> Result<Self> {
        if data.len() < 8 {
            // minimum: 4 bytes length + 4 bytes CRC
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Insufficient data for log entry header",
            )));
        }

        let total_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + total_len {
            // 4 bytes length prefix + actual data
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Insufficient data for log entry",
            )));
        }

        let crc = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let payload = &data[8..4 + total_len]; // Skip length prefix and CRC

        // Verify CRC
        let computed_crc = crc32fast::hash(payload);
        if crc != computed_crc {
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::InvalidData,
                "CRC mismatch in log entry",
            )));
        }

        if payload.len() < 12 {
            // timestamp (8) + key_len (4)
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Insufficient payload data",
            )));
        }

        let timestamp = u64::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);

        let key_len =
            u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]) as usize;

        if payload.len() < 12 + key_len {
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Insufficient data for key",
            )));
        }

        let key = if key_len > 0 {
            Some(Bytes::copy_from_slice(&payload[12..12 + key_len]))
        } else {
            None
        };

        let value = Bytes::copy_from_slice(&payload[12 + key_len..]);

        Ok(LogEntry {
            offset,
            timestamp,
            key,
            value,
        })
    }
}

/// Represents a log file for a single partition
#[derive(Debug)]
pub struct Log {
    pub path: PathBuf,
    pub base_offset: Offset,
    file: File,
    next_offset: Offset,
}

impl Log {
    /// Create a new log file
    pub fn create<P: AsRef<Path>>(path: P, base_offset: Offset) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)?;

        Ok(Log {
            path,
            base_offset,
            file,
            next_offset: base_offset,
        })
    }

    /// Open an existing log file
    pub fn open<P: AsRef<Path>>(path: P, base_offset: Offset) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().write(true).read(true).open(&path)?;

        let mut log = Log {
            path,
            base_offset,
            file,
            next_offset: base_offset,
        };

        // Scan the file to find the next offset
        log.scan_and_recover()?;

        Ok(log)
    }

    /// Append messages to the log
    pub fn append(&mut self, messages: &[Message]) -> Result<Offset> {
        let base_offset = self.next_offset;
        let mut writer = BufWriter::new(&mut self.file);

        // Seek to end of file
        writer.seek(SeekFrom::End(0))?;

        for message in messages {
            let entry = LogEntry::from_message(self.next_offset, message);
            let serialized = entry.serialize()?;
            writer.write_all(&serialized)?;
            self.next_offset += 1;
        }

        writer.flush()?;
        drop(writer); // Explicitly drop the writer to release the mutable borrow
        self.file.sync_all()?;

        Ok(base_offset)
    }

    /// Read messages from the log starting at the given offset
    pub fn read(&self, offset: Offset, max_bytes: usize) -> Result<Vec<LogEntry>> {
        if offset < self.base_offset {
            return Ok(vec![]);
        }

        let file_size = self.file.metadata()?.len() as usize;
        if file_size == 0 {
            return Ok(vec![]);
        }

        // Memory map the file for efficient reading
        let mmap = unsafe { MmapOptions::new().map(&self.file)? };

        let mut entries = Vec::new();
        let mut pos = 0;
        let mut current_offset = self.base_offset;
        let mut bytes_read = 0;

        // Scan to find the starting position
        while pos < mmap.len() && current_offset < offset {
            if pos + 4 > mmap.len() {
                break;
            }

            let record_len =
                u32::from_be_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]])
                    as usize;

            pos += 4 + record_len;
            current_offset += 1;
        }

        // Read entries from the target offset
        while pos < mmap.len() && bytes_read < max_bytes {
            if pos + 4 > mmap.len() {
                break;
            }

            let record_len =
                u32::from_be_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]])
                    as usize;

            if pos + 4 + record_len > mmap.len() {
                break;
            }

            match LogEntry::deserialize(&mmap[pos..pos + 4 + record_len], current_offset) {
                Ok(entry) => {
                    bytes_read += entry.serialized_size();
                    entries.push(entry);
                }
                Err(e) => {
                    // Log corruption, stop reading
                    eprintln!(
                        "Warning: Log corruption detected at offset {}: {}",
                        current_offset, e
                    );
                    break;
                }
            }

            pos += 4 + record_len;
            current_offset += 1;
        }

        Ok(entries)
    }

    /// Get the next offset that will be written
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    /// Get the base offset of this log
    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Scan the log file and recover the next offset
    fn scan_and_recover(&mut self) -> Result<()> {
        let file_size = self.file.metadata()?.len() as usize;
        if file_size == 0 {
            return Ok(());
        }

        let mmap = unsafe { MmapOptions::new().map(&self.file)? };
        let mut pos = 0;
        let mut current_offset = self.base_offset;

        while pos < mmap.len() {
            if pos + 4 > mmap.len() {
                // Incomplete record at end of file, truncate
                self.file.set_len(pos as u64)?;
                break;
            }

            let record_len =
                u32::from_be_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]])
                    as usize;

            if pos + 4 + record_len > mmap.len() {
                // Incomplete record, truncate
                self.file.set_len(pos as u64)?;
                break;
            }

            // Validate the record
            match LogEntry::deserialize(&mmap[pos..pos + 4 + record_len], current_offset) {
                Ok(_) => {
                    pos += 4 + record_len;
                    current_offset += 1;
                }
                Err(_) => {
                    // Corrupted record, truncate from here
                    self.file.set_len(pos as u64)?;
                    break;
                }
            }
        }

        self.next_offset = current_offset;
        Ok(())
    }

    /// Get the file size
    pub fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Flush all data to disk
    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry {
            offset: 42,
            timestamp: 1234567890,
            key: Some(Bytes::from("test-key")),
            value: Bytes::from("test-value"),
        };

        let serialized = entry.serialize().unwrap();
        let deserialized = LogEntry::deserialize(&serialized, 42).unwrap();

        assert_eq!(deserialized.offset, 42);
        assert_eq!(deserialized.timestamp, 1234567890);
        assert_eq!(deserialized.key.as_ref().unwrap(), "test-key");
        assert_eq!(deserialized.value, Bytes::from("test-value"));
    }

    #[test]
    fn test_log_append_and_read() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let mut log = Log::create(&log_path, 0).unwrap();

        // Append some messages
        let messages = vec![
            Message::new("message 1").with_key("key1"),
            Message::new("message 2").with_key("key2"),
            Message::new("message 3"),
        ];

        let base_offset = log.append(&messages).unwrap();
        assert_eq!(base_offset, 0);
        assert_eq!(log.next_offset(), 3);

        // Read messages back
        let entries = log.read(0, 1024).unwrap();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[0].value, Bytes::from("message 1"));
        assert_eq!(entries[0].key.as_ref().unwrap(), "key1");

        assert_eq!(entries[1].offset, 1);
        assert_eq!(entries[1].value, Bytes::from("message 2"));
        assert_eq!(entries[1].key.as_ref().unwrap(), "key2");

        assert_eq!(entries[2].offset, 2);
        assert_eq!(entries[2].value, Bytes::from("message 3"));
        assert!(entries[2].key.is_none());
    }

    #[test]
    fn test_log_recovery() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test.log");

        {
            let mut log = Log::create(&log_path, 0).unwrap();
            let messages = vec![Message::new("message 1"), Message::new("message 2")];
            log.append(&messages).unwrap();
        }

        // Reopen the log
        let log = Log::open(&log_path, 0).unwrap();
        assert_eq!(log.next_offset(), 2);

        let entries = log.read(0, 1024).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].value, Bytes::from("message 1"));
        assert_eq!(entries[1].value, Bytes::from("message 2"));
    }

    #[test]
    fn test_log_read_from_offset() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let mut log = Log::create(&log_path, 0).unwrap();

        let messages = vec![
            Message::new("message 0"),
            Message::new("message 1"),
            Message::new("message 2"),
            Message::new("message 3"),
        ];

        log.append(&messages).unwrap();

        // Read from offset 2
        let entries = log.read(2, 1024).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].offset, 2);
        assert_eq!(entries[0].value, Bytes::from("message 2"));
        assert_eq!(entries[1].offset, 3);
        assert_eq!(entries[1].value, Bytes::from("message 3"));
    }
}
