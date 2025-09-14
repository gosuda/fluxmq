#![allow(dead_code)]
/// Zero-copy storage with memory-mapped files for ultimate performance
///
/// This module implements memory-mapped file I/O for zero-copy operations,
/// targeting 400k+ msg/sec through elimination of buffer copies.
use crate::protocol::{Message, Offset, PartitionId};
use crate::Result;
use bytes::Bytes;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Memory-mapped storage segment for zero-copy operations
pub struct MappedStorageSegment {
    // Memory-mapped file
    mmap: MmapMut,

    // Segment metadata
    file_path: String,
    segment_id: u64,
    capacity: usize,

    // Write/read positions
    write_position: AtomicUsize,
    committed_position: AtomicUsize,

    // Performance counters
    writes: AtomicU64,
    reads: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

impl MappedStorageSegment {
    /// Create new memory-mapped segment
    pub fn create(file_path: String, segment_id: u64, capacity: usize) -> Result<Self> {
        // Create/open file
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        // Set file size
        file.set_len(capacity as u64)
            .map_err(|e| crate::FluxmqError::Storage(e))?;

        // Create memory mapping
        let mmap = unsafe {
            MmapOptions::new()
                .len(capacity)
                .map_mut(&file)
                .map_err(|e| {
                    crate::FluxmqError::Storage(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?
        };

        Ok(Self {
            mmap,
            file_path,
            segment_id,
            capacity,
            write_position: AtomicUsize::new(0),
            committed_position: AtomicUsize::new(0),
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        })
    }

    /// Zero-copy write to memory-mapped region
    pub fn write_zero_copy(&mut self, data: &[u8]) -> Result<usize> {
        let write_pos = self.write_position.load(Ordering::Relaxed);

        if write_pos + data.len() > self.capacity {
            return Err(crate::FluxmqError::Storage(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Segment full",
            )));
        }

        // Direct memory copy to mapped region
        unsafe {
            let dst = self.mmap.as_mut_ptr().add(write_pos);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }

        // Update write position
        let new_pos = self.write_position.fetch_add(data.len(), Ordering::Relaxed) + data.len();

        // Update statistics
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        Ok(new_pos - data.len())
    }

    /// Zero-copy read from memory-mapped region
    pub fn read_zero_copy(&self, offset: usize, length: usize) -> Result<&[u8]> {
        let committed_pos = self.committed_position.load(Ordering::Relaxed);

        if offset + length > committed_pos {
            return Err(crate::FluxmqError::Storage(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Read beyond committed data",
            )));
        }

        // Zero-copy slice into mapped memory
        let data = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr().add(offset), length) };

        // Update statistics
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(length as u64, Ordering::Relaxed);

        Ok(data)
    }

    /// Commit written data (make it available for reads)
    pub fn commit_writes(&self) -> Result<usize> {
        let write_pos = self.write_position.load(Ordering::Relaxed);
        self.committed_position.store(write_pos, Ordering::Relaxed);

        // Sync to disk
        self.mmap.flush().map_err(|e| {
            crate::FluxmqError::Storage(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        Ok(write_pos)
    }

    /// Get segment statistics
    pub fn get_stats(&self) -> MappedSegmentStats {
        MappedSegmentStats {
            segment_id: self.segment_id,
            file_path: self.file_path.clone(),
            capacity: self.capacity,
            write_position: self.write_position.load(Ordering::Relaxed),
            committed_position: self.committed_position.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            reads: self.reads.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MappedSegmentStats {
    pub segment_id: u64,
    pub file_path: String,
    pub capacity: usize,
    pub write_position: usize,
    pub committed_position: usize,
    pub writes: u64,
    pub reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

impl MappedSegmentStats {
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.write_position as f64 / self.capacity as f64
        }
    }
}

/// Zero-copy message storage manager
pub struct ZeroCopyMessageStorage {
    // Segment management
    segments: Arc<RwLock<HashMap<(String, PartitionId), Vec<Arc<MappedStorageSegment>>>>>,

    // Configuration
    segment_size: usize,
    max_segments_per_partition: usize,

    // Message indexing for fast lookups
    message_index: Arc<RwLock<HashMap<(String, PartitionId, Offset), (u64, usize, usize)>>>, // (segment_id, offset, length)

    // Performance counters
    total_messages: AtomicU64,
    total_segments: AtomicU64,
    zero_copy_reads: AtomicU64,
    zero_copy_writes: AtomicU64,

    next_segment_id: AtomicU64,
}

impl ZeroCopyMessageStorage {
    pub fn new() -> Self {
        Self {
            segments: Arc::new(RwLock::new(HashMap::new())),
            segment_size: 64 * 1024 * 1024, // 64MB segments
            max_segments_per_partition: 1000,
            message_index: Arc::new(RwLock::new(HashMap::new())),
            total_messages: AtomicU64::new(0),
            total_segments: AtomicU64::new(0),
            zero_copy_reads: AtomicU64::new(0),
            zero_copy_writes: AtomicU64::new(0),
            next_segment_id: AtomicU64::new(1),
        }
    }

    /// Append message using zero-copy storage
    pub fn append_message_zero_copy(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        message: &Message,
    ) -> Result<()> {
        // Serialize message
        let serialized = self.serialize_message(message)?;

        // Get or create segment
        let segment = self.get_or_create_segment(topic, partition)?;

        // TODO: Implement full zero-copy operations
        // For now, just track in simple index for proof of concept
        {
            let mut index = self.message_index.write();
            index.insert(
                (topic.to_string(), partition, offset),
                (segment.segment_id, 0, serialized.len()), // Use 0 as placeholder offset
            );
        }

        // TODO: Implement segment commit when mutable access resolved

        self.total_messages.fetch_add(1, Ordering::Relaxed);
        self.zero_copy_writes.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Read message using zero-copy
    pub fn read_message_zero_copy(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
    ) -> Result<Message> {
        // Look up message in index
        let (segment_id, segment_offset, length) = {
            let index = self.message_index.read();
            index
                .get(&(topic.to_string(), partition, offset))
                .cloned()
                .ok_or_else(|| {
                    crate::FluxmqError::Storage(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Message not found",
                    ))
                })?
        };

        // Find segment
        let segment = self.find_segment(topic, partition, segment_id)?;

        // Zero-copy read
        let data = segment.read_zero_copy(segment_offset, length)?;

        // Deserialize message
        let message = self.deserialize_message(data)?;

        self.zero_copy_reads.fetch_add(1, Ordering::Relaxed);

        Ok(message)
    }

    /// Read message range using zero-copy (for fetch operations)
    pub fn read_message_range_zero_copy(
        &self,
        topic: &str,
        partition: PartitionId,
        start_offset: Offset,
        max_messages: usize,
    ) -> Result<Vec<(Offset, Message)>> {
        let mut messages = Vec::with_capacity(max_messages);
        let mut current_offset = start_offset;

        for _ in 0..max_messages {
            match self.read_message_zero_copy(topic, partition, current_offset) {
                Ok(message) => {
                    messages.push((current_offset, message));
                    current_offset += 1;
                }
                Err(_) => break, // No more messages
            }
        }

        Ok(messages)
    }

    /// Get or create segment for partition
    fn get_or_create_segment(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Arc<MappedStorageSegment>> {
        let key = (topic.to_string(), partition);

        {
            let segments = self.segments.read();
            if let Some(partition_segments) = segments.get(&key) {
                if let Some(segment) = partition_segments.last() {
                    // Check if current segment has space
                    if segment.write_position.load(Ordering::Relaxed) + 1024 < segment.capacity {
                        return Ok(segment.clone());
                    }
                }
            }
        }

        // Create new segment
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let file_path = format!(
            "./data/{}/partition-{}/segment-{:010}.log",
            topic, partition, segment_id
        );

        // Ensure directory exists
        if let Some(parent) = std::path::Path::new(&file_path).parent() {
            std::fs::create_dir_all(parent).map_err(|e| crate::FluxmqError::Storage(e))?;
        }

        let segment = Arc::new(MappedStorageSegment::create(
            file_path,
            segment_id,
            self.segment_size,
        )?);

        // Add to segments
        {
            let mut segments = self.segments.write();
            segments
                .entry(key)
                .or_insert_with(Vec::new)
                .push(segment.clone());
        }

        self.total_segments.fetch_add(1, Ordering::Relaxed);

        Ok(segment)
    }

    /// Find segment by ID
    fn find_segment(
        &self,
        topic: &str,
        partition: PartitionId,
        segment_id: u64,
    ) -> Result<Arc<MappedStorageSegment>> {
        let key = (topic.to_string(), partition);
        let segments = self.segments.read();

        if let Some(partition_segments) = segments.get(&key) {
            for segment in partition_segments {
                if segment.segment_id == segment_id {
                    return Ok(segment.clone());
                }
            }
        }

        Err(crate::FluxmqError::Storage(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Segment not found",
        )))
    }

    /// Serialize message to bytes (simplified)
    fn serialize_message(&self, message: &Message) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Message format: [key_len:4][key][value_len:4][value][timestamp:8]
        let key_bytes = message.key.as_ref().map(|k| k.as_ref()).unwrap_or(&[]);
        let value_bytes = message.value.as_ref();

        buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(value_bytes);
        buf.extend_from_slice(&message.timestamp.to_be_bytes());

        Ok(buf)
    }

    /// Deserialize message from bytes (simplified)
    fn deserialize_message(&self, data: &[u8]) -> Result<Message> {
        if data.len() < 16 {
            // Minimum size check
            return Err(crate::FluxmqError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid message data",
            )));
        }

        let mut offset = 0;

        // Read key
        let key_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        let key = if key_len > 0 {
            Some(Bytes::copy_from_slice(&data[offset..offset + key_len]))
        } else {
            None
        };
        offset += key_len;

        // Read value
        let value_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        let value = Bytes::copy_from_slice(&data[offset..offset + value_len]);
        offset += value_len;

        // Read timestamp
        let timestamp = u64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);

        Ok(Message {
            key,
            value,
            timestamp,
            headers: std::collections::HashMap::new(),
        })
    }

    /// Commit all pending writes
    pub fn commit_all_writes(&self) -> Result<()> {
        let segments = self.segments.read();

        for partition_segments in segments.values() {
            for segment in partition_segments {
                segment.commit_writes()?;
            }
        }

        Ok(())
    }

    /// Get comprehensive storage statistics
    pub fn get_zero_copy_stats(&self) -> ZeroCopyStorageStats {
        let segments_guard = self.segments.read();
        let mut segment_stats = Vec::new();

        for partition_segments in segments_guard.values() {
            for segment in partition_segments {
                segment_stats.push(segment.get_stats());
            }
        }

        ZeroCopyStorageStats {
            total_messages: self.total_messages.load(Ordering::Relaxed),
            total_segments: self.total_segments.load(Ordering::Relaxed),
            zero_copy_reads: self.zero_copy_reads.load(Ordering::Relaxed),
            zero_copy_writes: self.zero_copy_writes.load(Ordering::Relaxed),
            segment_stats,
            index_size: self.message_index.read().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZeroCopyStorageStats {
    pub total_messages: u64,
    pub total_segments: u64,
    pub zero_copy_reads: u64,
    pub zero_copy_writes: u64,
    pub segment_stats: Vec<MappedSegmentStats>,
    pub index_size: usize,
}

impl ZeroCopyStorageStats {
    pub fn report(&self) -> String {
        let total_capacity: usize = self.segment_stats.iter().map(|s| s.capacity).sum();
        let total_used: usize = self.segment_stats.iter().map(|s| s.write_position).sum();
        let avg_utilization = if total_capacity > 0 {
            total_used as f64 / total_capacity as f64 * 100.0
        } else {
            0.0
        };

        format!(
            "Zero-Copy Storage Stats:\n\
             Messages: {} total\n\
             Segments: {} (avg {:.1}% utilization)\n\
             Zero-Copy Ops: {} reads, {} writes\n\
             Index Size: {} entries\n\
             Storage: {:.1} MB used / {:.1} MB allocated",
            self.total_messages,
            self.total_segments,
            avg_utilization,
            self.zero_copy_reads,
            self.zero_copy_writes,
            self.index_size,
            total_used as f64 / 1_000_000.0,
            total_capacity as f64 / 1_000_000.0
        )
    }
}
