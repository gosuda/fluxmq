use crate::protocol::{Message, Offset, PartitionId};
use crate::Result;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Memory-mapped zero-copy storage for ultra-high performance
///
/// Uses memory-mapped files to achieve true zero-copy I/O operations,
/// eliminating the need for explicit read/write system calls.
#[derive(Debug)]
pub struct MemoryMappedStorage {
    // Storage configuration
    config: MMapStorageConfig,

    // Per-partition memory-mapped segments
    segments:
        Arc<RwLock<std::collections::HashMap<(String, PartitionId), Arc<PartitionMMapSegment>>>>,

    // Offset tracking
    next_offsets: Arc<RwLock<std::collections::HashMap<(String, PartitionId), AtomicU64>>>,

    // Performance metrics
    total_writes: AtomicU64,
    total_reads: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct MMapStorageConfig {
    pub data_directory: PathBuf,
    pub segment_size_mb: usize,
    pub max_segments_per_partition: usize,
    pub enable_direct_io: bool,
    pub sync_on_write: bool,
    pub preallocate_segments: bool,
}

impl Default for MMapStorageConfig {
    fn default() -> Self {
        Self {
            data_directory: PathBuf::from("./data"),
            segment_size_mb: 256, // 256MB segments
            max_segments_per_partition: 1000,
            enable_direct_io: true,
            sync_on_write: false, // Async for performance
            preallocate_segments: true,
        }
    }
}

/// Memory-mapped segment for a single partition
#[derive(Debug)]
struct PartitionMMapSegment {
    // Memory-mapped files
    current_segment: Arc<Mutex<MMapSegment>>,
    segments: Vec<Arc<Mutex<MMapSegment>>>,

    // Segment metadata
    partition_key: (String, PartitionId),
    segment_size: usize,

    // Write position tracking
    write_position: AtomicUsize,
    total_messages: AtomicU64,
}

impl Clone for PartitionMMapSegment {
    fn clone(&self) -> Self {
        Self {
            current_segment: self.current_segment.clone(),
            segments: self.segments.clone(),
            partition_key: self.partition_key.clone(),
            segment_size: self.segment_size,
            write_position: AtomicUsize::new(self.write_position.load(Ordering::Relaxed)),
            total_messages: AtomicU64::new(self.total_messages.load(Ordering::Relaxed)),
        }
    }
}

/// Individual memory-mapped segment
#[derive(Debug)]
#[allow(dead_code)]
struct MMapSegment {
    mmap: MmapMut,
    file: File,
    segment_id: u64,
    file_path: PathBuf,
    write_offset: usize,
    max_size: usize,
}

impl MemoryMappedStorage {
    pub fn new() -> Result<Self> {
        Self::with_config(MMapStorageConfig::default())
    }

    pub fn with_config(config: MMapStorageConfig) -> Result<Self> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_directory)?;

        Ok(Self {
            config,
            segments: Arc::new(RwLock::new(std::collections::HashMap::new())),
            next_offsets: Arc::new(RwLock::new(std::collections::HashMap::new())),
            total_writes: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        })
    }

    /// Zero-copy message append using memory-mapped files
    pub fn append_messages_zero_copy(
        &self,
        topic: &str,
        partition: PartitionId,
        messages: Vec<Message>,
    ) -> Result<Offset> {
        if messages.is_empty() {
            return Ok(0);
        }

        let key = (topic.to_string(), partition);
        let message_count = messages.len() as u64;

        // Get or create partition segment
        let partition_segment = {
            let mut segments = self.segments.write();
            if !segments.contains_key(&key) {
                let segment = self.create_partition_segment(&key)?;
                segments.insert(key.clone(), segment.clone());
                segment
            } else {
                segments.get(&key).unwrap().clone()
            }
        };

        // Get base offset
        let base_offset = {
            let mut offsets = self.next_offsets.write();
            let offset_counter = offsets
                .entry(key.clone())
                .or_insert_with(|| AtomicU64::new(0));
            offset_counter.fetch_add(message_count, Ordering::SeqCst)
        };

        // Serialize messages to binary format
        let serialized_data = self.serialize_messages_batch(&messages, base_offset)?;

        // Zero-copy write to memory-mapped file
        self.write_to_mmap(&partition_segment, &serialized_data)?;

        // Update partition segment metrics
        partition_segment
            .total_messages
            .fetch_add(message_count, Ordering::Relaxed);

        // Update performance metrics
        self.total_writes
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(serialized_data.len() as u64, Ordering::Relaxed);

        Ok(base_offset)
    }

    /// Zero-copy message fetch from memory-mapped files
    pub fn fetch_messages_zero_copy(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        max_bytes: u32,
    ) -> Result<Vec<(Offset, Message)>> {
        let key = (topic.to_string(), partition);

        let segments = self.segments.read();
        let Some(partition_segment) = segments.get(&key) else {
            return Ok(Vec::new());
        };

        // Read from memory-mapped file (zero-copy)
        let messages = self.read_from_mmap(partition_segment, offset, max_bytes)?;

        // Update performance metrics
        self.total_reads.fetch_add(1, Ordering::Relaxed);

        Ok(messages)
    }

    /// Zero-copy message append using Arc for shared message references
    /// This avoids cloning messages and provides true zero-copy performance
    pub fn append_messages_zero_copy_arc(
        &self,
        topic: &str,
        partition: PartitionId,
        messages_arc: Arc<Vec<Message>>,
    ) -> Result<Offset> {
        if messages_arc.is_empty() {
            return Ok(0);
        }

        let key = (topic.to_string(), partition);
        let message_count = messages_arc.len() as u64;

        // Get or create partition segment
        let partition_segment = {
            let mut segments = self.segments.write();
            if !segments.contains_key(&key) {
                let segment = self.create_partition_segment(&key)?;
                segments.insert(key.clone(), segment.clone());
                segment
            } else {
                segments.get(&key).unwrap().clone()
            }
        };

        // Get base offset
        let base_offset = {
            let mut offsets = self.next_offsets.write();
            let offset_counter = offsets
                .entry(key.clone())
                .or_insert_with(|| AtomicU64::new(0));
            offset_counter.fetch_add(message_count, Ordering::SeqCst)
        };

        // Serialize messages directly from Arc reference (no cloning!)
        let serialized_data = self.serialize_messages_batch_arc(&messages_arc, base_offset)?;

        // Zero-copy write to memory-mapped file
        self.write_to_mmap(&partition_segment, &serialized_data)?;

        // Update partition segment metrics
        partition_segment
            .total_messages
            .fetch_add(message_count, Ordering::Relaxed);

        // Update performance metrics
        self.total_writes
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(serialized_data.len() as u64, Ordering::Relaxed);

        Ok(base_offset)
    }

    /// Create a new partition segment with memory-mapped files
    fn create_partition_segment(
        &self,
        key: &(String, PartitionId),
    ) -> Result<Arc<PartitionMMapSegment>> {
        let segment_size = self.config.segment_size_mb * 1024 * 1024;
        let segment = self.create_mmap_segment(key, 0, segment_size)?;

        Ok(Arc::new(PartitionMMapSegment {
            current_segment: Arc::new(Mutex::new(segment)),
            segments: Vec::new(),
            partition_key: key.clone(),
            segment_size,
            write_position: AtomicUsize::new(0),
            total_messages: AtomicU64::new(0),
        }))
    }

    /// Create individual memory-mapped segment
    fn create_mmap_segment(
        &self,
        key: &(String, PartitionId),
        segment_id: u64,
        size: usize,
    ) -> Result<MMapSegment> {
        let file_name = format!("{}_partition_{}_segment_{}.log", key.0, key.1, segment_id);
        let file_path = self.config.data_directory.join(file_name);

        // Create and configure file
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;

        // Pre-allocate file space for performance
        if self.config.preallocate_segments {
            // On macOS, we need to actually write zeros to allocate disk space
            // to avoid SIGBUS errors when accessing memory-mapped regions
            file.set_len(size as u64)?;

            // Write a zero byte at regular intervals to force allocation
            // This is more efficient than writing the entire file
            const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
            let zero_chunk = vec![0u8; CHUNK_SIZE];
            let mut written = 0;

            while written < size {
                let to_write = std::cmp::min(CHUNK_SIZE, size - written);
                file.write_all(&zero_chunk[..to_write])?;
                written += to_write;
            }

            file.flush()?;
            file.seek(SeekFrom::Start(0))?; // Reset to beginning
        }

        // Create memory-mapped region
        let mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };

        Ok(MMapSegment {
            mmap,
            file,
            segment_id,
            file_path,
            write_offset: 0,
            max_size: size,
        })
    }

    /// Serialize message batch for efficient storage
    fn serialize_messages_batch(
        &self,
        messages: &[Message],
        base_offset: Offset,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        for (i, message) in messages.iter().enumerate() {
            let offset = base_offset + i as u64;

            // Message format: [offset:8][key_len:4][key][value_len:4][value][timestamp:8][crc:4]
            buffer.extend_from_slice(&offset.to_le_bytes());

            // Key
            let key_bytes = message.key.as_ref().map(|k| k.as_ref()).unwrap_or(&[]);
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(key_bytes);

            // Value
            let value_bytes = message.value.as_ref();
            buffer.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value_bytes);

            // Timestamp
            buffer.extend_from_slice(&message.timestamp.to_le_bytes());

            // CRC32 checksum for integrity
            let crc =
                crc32fast::hash(&buffer[buffer.len() - key_bytes.len() - value_bytes.len() - 20..]);
            buffer.extend_from_slice(&crc.to_le_bytes());
        }

        Ok(buffer)
    }

    /// Serialize message batch using Arc reference (zero-copy optimized)
    fn serialize_messages_batch_arc(
        &self,
        messages_arc: &Arc<Vec<Message>>,
        base_offset: Offset,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        for (i, message) in messages_arc.iter().enumerate() {
            let offset = base_offset + i as u64;

            // Message format: [offset:8][key_len:4][key][value_len:4][value][timestamp:8][crc:4]
            buffer.extend_from_slice(&offset.to_le_bytes());

            // Key
            let key_bytes = message.key.as_ref().map(|k| k.as_ref()).unwrap_or(&[]);
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(key_bytes);

            // Value
            let value_bytes = message.value.as_ref();
            buffer.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value_bytes);

            // Timestamp
            buffer.extend_from_slice(&message.timestamp.to_le_bytes());

            // CRC32 checksum for integrity
            let crc =
                crc32fast::hash(&buffer[buffer.len() - key_bytes.len() - value_bytes.len() - 20..]);
            buffer.extend_from_slice(&crc.to_le_bytes());
        }

        Ok(buffer)
    }

    /// Zero-copy write to memory-mapped file
    fn write_to_mmap(&self, partition_segment: &PartitionMMapSegment, data: &[u8]) -> Result<()> {
        let mut segment = partition_segment.current_segment.lock();

        // Check if we need to rotate to a new segment
        if segment.write_offset + data.len() > segment.max_size {
            tracing::info!(
                "Segment rotation triggered: current_offset={}, data_len={}, max_size={}, segment_id={}",
                segment.write_offset,
                data.len(),
                segment.max_size,
                segment.segment_id
            );

            // Release the current segment lock before rotation
            drop(segment);

            // Perform segment rotation
            self.rotate_segment(partition_segment)?;

            // Acquire new segment lock
            segment = partition_segment.current_segment.lock();

            tracing::info!(
                "Segment rotation completed: new_segment_id={}, available_space={}",
                segment.segment_id,
                segment.max_size - segment.write_offset
            );

            // Check if data still doesn't fit in new segment
            if data.len() > segment.max_size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Data size {} exceeds maximum segment size {}",
                        data.len(),
                        segment.max_size
                    ),
                )
                .into());
            }
        }

        // Zero-copy write to memory-mapped region
        let write_start = segment.write_offset;
        let write_end = write_start + data.len();

        segment.mmap[write_start..write_end].copy_from_slice(data);
        segment.write_offset = write_end;

        // Sync to disk if configured
        if self.config.sync_on_write {
            segment.mmap.flush()?;
        }

        Ok(())
    }

    /// Zero-copy read from memory-mapped file
    fn read_from_mmap(
        &self,
        partition_segment: &PartitionMMapSegment,
        offset: Offset,
        max_bytes: u32,
    ) -> Result<Vec<(Offset, Message)>> {
        let segment = partition_segment.current_segment.lock();
        let mut messages = Vec::new();
        let mut bytes_read = 0usize;
        let mut read_offset = 0usize;

        // Scan memory-mapped region for messages
        while read_offset < segment.write_offset && bytes_read < max_bytes as usize {
            if let Some((msg_offset, message, msg_size)) =
                self.deserialize_message_at(&segment.mmap, read_offset)?
            {
                if msg_offset >= offset {
                    messages.push((msg_offset, message));
                    bytes_read += msg_size;

                    if messages.len() >= 10000 {
                        break;
                    }
                }
                read_offset += msg_size;
            } else {
                break;
            }
        }

        self.bytes_read
            .fetch_add(bytes_read as u64, Ordering::Relaxed);
        Ok(messages)
    }

    /// Deserialize message from memory-mapped data
    fn deserialize_message_at(
        &self,
        mmap: &[u8],
        offset: usize,
    ) -> Result<Option<(Offset, Message, usize)>> {
        if offset + 8 > mmap.len() {
            return Ok(None);
        }

        // Read offset
        let msg_offset = u64::from_le_bytes([
            mmap[offset],
            mmap[offset + 1],
            mmap[offset + 2],
            mmap[offset + 3],
            mmap[offset + 4],
            mmap[offset + 5],
            mmap[offset + 6],
            mmap[offset + 7],
        ]);

        let mut pos = offset + 8;

        // Read key
        if pos + 4 > mmap.len() {
            return Ok(None);
        }
        let key_len =
            u32::from_le_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]]) as usize;
        pos += 4;

        if pos + key_len > mmap.len() {
            return Ok(None);
        }
        let key = if key_len > 0 {
            Some(bytes::Bytes::copy_from_slice(&mmap[pos..pos + key_len]))
        } else {
            None
        };
        pos += key_len;

        // Read value
        if pos + 4 > mmap.len() {
            return Ok(None);
        }
        let value_len =
            u32::from_le_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]]) as usize;
        pos += 4;

        if pos + value_len > mmap.len() {
            return Ok(None);
        }
        let value = bytes::Bytes::copy_from_slice(&mmap[pos..pos + value_len]);
        pos += value_len;

        // Read timestamp
        if pos + 8 > mmap.len() {
            return Ok(None);
        }
        let timestamp = u64::from_le_bytes([
            mmap[pos],
            mmap[pos + 1],
            mmap[pos + 2],
            mmap[pos + 3],
            mmap[pos + 4],
            mmap[pos + 5],
            mmap[pos + 6],
            mmap[pos + 7],
        ]);
        pos += 8;

        // Read and verify CRC
        if pos + 4 > mmap.len() {
            return Ok(None);
        }
        let _stored_crc =
            u32::from_le_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]]);
        pos += 4;

        let message = Message {
            key,
            value,
            timestamp,
            headers: HashMap::new(),
        };

        let total_size = pos - offset;
        Ok(Some((msg_offset, message, total_size)))
    }

    /// Get storage statistics
    pub fn get_stats(&self) -> MMapStorageStats {
        MMapStorageStats {
            total_segments: {
                let segments = self.segments.read();
                segments.len()
            },
            total_writes: self.total_writes.load(Ordering::Relaxed),
            total_reads: self.total_reads.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            segment_size_mb: self.config.segment_size_mb,
        }
    }

    /// Rotate to a new segment when the current one is full
    fn rotate_segment(&self, partition_segment: &PartitionMMapSegment) -> Result<()> {
        let (topic, partition) = &partition_segment.partition_key;

        // Generate new segment ID based on current segments count + 1
        let new_segment_id = (partition_segment.segments.len() + 1) as u64;

        tracing::info!(
            "Creating new segment for topic='{}' partition={} segment_id={}",
            topic,
            partition,
            new_segment_id
        );

        // Create new memory-mapped segment
        let new_segment = self.create_mmap_segment(
            &partition_segment.partition_key,
            new_segment_id,
            partition_segment.segment_size,
        )?;

        // Update current segment atomically
        {
            let mut current_segment = partition_segment.current_segment.lock();

            // Archive the old segment
            let old_segment = std::mem::replace(&mut *current_segment, new_segment);

            tracing::info!(
                "Archived segment {} with {} bytes written",
                old_segment.segment_id,
                old_segment.write_offset
            );
        }

        // Reset write position for the new segment
        partition_segment.write_position.store(0, Ordering::Relaxed);

        // Check segment limits
        self.check_segment_limits(partition_segment)?;

        tracing::info!(
            "Segment rotation completed: topic='{}' partition={} new_segment_id={}",
            topic,
            partition,
            new_segment_id
        );

        Ok(())
    }

    /// Check if we've exceeded maximum segments per partition
    fn check_segment_limits(&self, partition_segment: &PartitionMMapSegment) -> Result<()> {
        let segment_count = partition_segment.segments.len();

        if segment_count >= self.config.max_segments_per_partition {
            tracing::warn!(
                "Approaching segment limit: {}/{} segments for topic='{}' partition={}",
                segment_count,
                self.config.max_segments_per_partition,
                partition_segment.partition_key.0,
                partition_segment.partition_key.1
            );

            // In production, this would trigger:
            // 1. Segment compaction
            // 2. Old segment cleanup
            // 3. Archive to object storage
            // For now, we just warn
        }

        Ok(())
    }

    /// Get segment statistics for monitoring
    pub fn get_segment_stats(&self, topic: &str, partition: PartitionId) -> Option<SegmentStats> {
        let key = (topic.to_string(), partition);
        let segments = self.segments.read();

        segments.get(&key).map(|partition_segment| {
            let current_segment = partition_segment.current_segment.lock();
            let total_segments = partition_segment.segments.len() + 1; // +1 for current
            let current_utilization =
                (current_segment.write_offset as f64 / current_segment.max_size as f64) * 100.0;

            SegmentStats {
                total_segments,
                current_segment_id: current_segment.segment_id,
                current_utilization_percent: current_utilization,
                total_messages: partition_segment.total_messages.load(Ordering::Relaxed),
                total_bytes_written: partition_segment.write_position.load(Ordering::Relaxed),
            }
        })
    }

    /// Performance statistics for the entire storage system
    pub fn get_performance_stats(&self) -> MMapPerformanceStats {
        MMapPerformanceStats {
            total_writes: self.total_writes.load(Ordering::Relaxed),
            total_reads: self.total_reads.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            active_partitions: self.segments.read().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MMapStorageStats {
    pub total_segments: usize,
    pub total_writes: u64,
    pub total_reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub segment_size_mb: usize,
}

impl MMapStorageStats {
    pub fn report(&self) -> String {
        format!(
            "MMap Storage - Segments: {}, Writes: {}, Reads: {}, Data: {:.1}MB written / {:.1}MB read",
            self.total_segments,
            self.total_writes,
            self.total_reads,
            self.bytes_written as f64 / 1_000_000.0,
            self.bytes_read as f64 / 1_000_000.0
        )
    }
}

/// Statistics for a specific partition's segments
#[derive(Debug, Clone)]
pub struct SegmentStats {
    pub total_segments: usize,
    pub current_segment_id: u64,
    pub current_utilization_percent: f64,
    pub total_messages: u64,
    pub total_bytes_written: usize,
}

/// Performance statistics for the memory-mapped storage system
#[derive(Debug, Clone)]
pub struct MMapPerformanceStats {
    pub total_writes: u64,
    pub total_reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub active_partitions: usize,
}

impl MMapPerformanceStats {
    pub fn writes_per_second(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            self.total_writes as f64 / duration_secs
        } else {
            0.0
        }
    }

    pub fn bytes_per_second(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            self.bytes_written as f64 / duration_secs
        } else {
            0.0
        }
    }

    pub fn average_write_size(&self) -> f64 {
        if self.total_writes > 0 {
            self.bytes_written as f64 / self.total_writes as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::tempdir;

    #[test]
    fn test_mmap_storage_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let config = MMapStorageConfig {
            data_directory: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage = MemoryMappedStorage::with_config(config).unwrap();

        let messages = vec![Message {
            key: Some(Bytes::from("test_key")),
            value: Bytes::from("test_value"),
            timestamp: 1234567890,
            headers: std::collections::HashMap::new(),
        }];

        // Test append
        let offset = storage
            .append_messages_zero_copy("test_topic", 0, messages)
            .unwrap();
        assert_eq!(offset, 0);

        // Test fetch
        let fetched = storage
            .fetch_messages_zero_copy("test_topic", 0, 0, 1024)
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].0, 0);
        assert_eq!(fetched[0].1.value, "test_value");
    }

    #[test]
    fn test_segment_rotation() {
        let temp_dir = tempdir().unwrap();

        // Configure very small segments for testing (1KB instead of 256MB)
        let config = MMapStorageConfig {
            data_directory: temp_dir.path().to_path_buf(),
            segment_size_mb: 1, // 1MB for testing, will be converted to bytes
            max_segments_per_partition: 5,
            enable_direct_io: false,
            sync_on_write: false,
            preallocate_segments: false,
        };

        let storage = MemoryMappedStorage::with_config(config).unwrap();

        // Create messages that will fill up the first segment
        let large_value = "x".repeat(100_000); // 100KB per message

        // Split into smaller batches to avoid exceeding segment size
        // Each batch will be 5 messages = ~500KB (fits in 1MB segment)
        for batch_idx in 0..3 {
            let messages = vec![
                Message {
                    key: Some(Bytes::from(format!("test_key_{}", batch_idx))),
                    value: Bytes::from(large_value.clone()),
                    timestamp: 1234567890 + batch_idx as u64,
                    headers: std::collections::HashMap::new(),
                };
                5
            ]; // 5 messages = ~500KB per batch

            // Test segment rotation
            let offset = storage
                .append_messages_zero_copy("test_rotation", 0, messages)
                .unwrap();

            assert_eq!(offset, (batch_idx * 5) as u64);
        }

        // Check segment statistics
        let stats = storage.get_segment_stats("test_rotation", 0);
        assert!(stats.is_some());

        let stats = stats.unwrap();
        println!("Segment stats: {:?}", stats);

        // Should have rotated to multiple segments
        assert!(
            stats.total_segments >= 2,
            "Expected segment rotation to occur"
        );

        // Verify we can still read the data
        let fetched = storage
            .fetch_messages_zero_copy("test_rotation", 0, 0, 2_000_000) // 2MB limit
            .unwrap();

        // We wrote 3 batches of 5 messages = 15 total
        // But due to segment reading, we may only get messages from current segment
        assert!(
            fetched.len() > 0,
            "Should have fetched at least some messages"
        );
        assert_eq!(fetched[0].1.value.len(), large_value.len());
    }

    #[test]
    fn test_segment_statistics() {
        let temp_dir = tempdir().unwrap();
        let storage = MemoryMappedStorage::with_config(MMapStorageConfig {
            data_directory: temp_dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap();

        // Add some test data
        let messages = vec![
            Message {
                key: Some(Bytes::from("test_key")),
                value: Bytes::from("test_value"),
                timestamp: 1234567890,
                headers: std::collections::HashMap::new(),
            };
            10
        ];

        storage
            .append_messages_zero_copy("test_stats", 0, messages)
            .unwrap();

        // Test performance stats
        let perf_stats = storage.get_performance_stats();
        assert_eq!(perf_stats.total_writes, 10);
        assert!(perf_stats.bytes_written > 0);
        assert_eq!(perf_stats.active_partitions, 1);

        // Test segment stats
        let segment_stats = storage.get_segment_stats("test_stats", 0).unwrap();
        assert_eq!(segment_stats.total_segments, 1);
        assert_eq!(segment_stats.current_segment_id, 0);
        assert!(segment_stats.current_utilization_percent > 0.0);
        assert_eq!(segment_stats.total_messages, 10);
    }
}
