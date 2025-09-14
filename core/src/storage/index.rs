use crate::protocol::Offset;
use crate::{FluxmqError, Result};
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Index entry size: 8 bytes offset + 4 bytes position = 12 bytes
const INDEX_ENTRY_SIZE: usize = 12;

/// Index entry representing offset -> file position mapping
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    /// The message offset
    pub offset: Offset,
    /// File position where this offset starts
    pub position: u32,
}

impl IndexEntry {
    pub fn new(offset: Offset, position: u32) -> Self {
        Self { offset, position }
    }

    /// Serialize index entry to bytes
    pub fn to_bytes(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut bytes = [0u8; INDEX_ENTRY_SIZE];
        bytes[0..8].copy_from_slice(&self.offset.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.position.to_be_bytes());
        bytes
    }

    /// Deserialize index entry from bytes
    pub fn from_bytes(bytes: &[u8; INDEX_ENTRY_SIZE]) -> Self {
        let offset = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let position = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        Self { offset, position }
    }
}

/// Sparse index for efficient offset lookups in log files
///
/// The index stores periodic offset -> file position mappings to enable
/// efficient binary search for any offset without scanning the entire log.
pub struct OffsetIndex {
    #[allow(dead_code)] // Used for index file management
    path: PathBuf,
    file: File,
    entries: Vec<IndexEntry>,
    /// Interval between indexed offsets (e.g., index every 4096 bytes)
    index_interval: u32,
    /// Last indexed position in the log file
    last_indexed_position: u32,
}

impl OffsetIndex {
    /// Create a new index file
    pub fn create<P: AsRef<Path>>(path: P, index_interval: u32) -> Result<Self> {
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

        Ok(OffsetIndex {
            path,
            file,
            entries: Vec::new(),
            index_interval,
            last_indexed_position: 0,
        })
    }

    /// Open an existing index file
    pub fn open<P: AsRef<Path>>(path: P, index_interval: u32) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().write(true).read(true).open(&path)?;

        let mut index = OffsetIndex {
            path,
            file,
            entries: Vec::new(),
            index_interval,
            last_indexed_position: 0,
        };

        index.load_entries()?;
        Ok(index)
    }

    /// Add an index entry if the position interval has been reached
    pub fn maybe_append(&mut self, offset: Offset, position: u32) -> Result<bool> {
        if self.entries.is_empty() || position >= self.last_indexed_position + self.index_interval {
            self.append(offset, position)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Add an index entry
    pub fn append(&mut self, offset: Offset, position: u32) -> Result<()> {
        let entry = IndexEntry::new(offset, position);

        // Append to file
        let bytes = entry.to_bytes();
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;

        // Add to in-memory entries
        self.entries.push(entry);
        self.last_indexed_position = position;

        Ok(())
    }

    /// Find the largest index entry with offset <= target_offset
    /// Returns (offset, file_position) to start scanning from
    pub fn lookup(&self, target_offset: Offset) -> Option<(Offset, u32)> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search for the largest offset <= target
        let mut left = 0;
        let mut right = self.entries.len();
        let mut result = None;

        while left < right {
            let mid = left + (right - left) / 2;
            let entry = &self.entries[mid];

            if entry.offset <= target_offset {
                result = Some((entry.offset, entry.position));
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        result
    }

    /// Get all index entries
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Get the number of index entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the file size
    pub fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Flush index to disk
    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Load index entries from file
    fn load_entries(&mut self) -> Result<()> {
        let file_size = self.file.metadata()?.len() as usize;
        if file_size == 0 {
            return Ok(());
        }

        if file_size % INDEX_ENTRY_SIZE != 0 {
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::InvalidData,
                "Index file size is not a multiple of entry size",
            )));
        }

        let mmap = unsafe { MmapOptions::new().map(&self.file)? };
        let entry_count = file_size / INDEX_ENTRY_SIZE;
        self.entries.reserve(entry_count);

        for i in 0..entry_count {
            let start = i * INDEX_ENTRY_SIZE;
            let end = start + INDEX_ENTRY_SIZE;
            let entry_bytes: &[u8; INDEX_ENTRY_SIZE] =
                mmap[start..end].try_into().map_err(|_| {
                    FluxmqError::Storage(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Failed to read index entry",
                    ))
                })?;

            let entry = IndexEntry::from_bytes(entry_bytes);
            self.last_indexed_position = self.last_indexed_position.max(entry.position);
            self.entries.push(entry);
        }

        // Entries should be sorted by offset, but verify
        if !self.entries.windows(2).all(|w| w[0].offset <= w[1].offset) {
            return Err(FluxmqError::Storage(io::Error::new(
                io::ErrorKind::InvalidData,
                "Index entries are not sorted by offset",
            )));
        }

        Ok(())
    }
}

/// Combined index and log manager for efficient offset-based operations
pub struct IndexedLog {
    #[allow(dead_code)] // Used for log file management
    log_path: PathBuf,
    #[allow(dead_code)] // Used for index file management
    index_path: PathBuf,
    index: OffsetIndex,
    /// Interval in bytes between index entries
    index_interval: u32,
}

impl IndexedLog {
    /// Create a new indexed log
    pub fn create<P: AsRef<Path>>(log_path: P, index_interval: u32) -> Result<Self> {
        let log_path = log_path.as_ref().to_path_buf();
        let index_path = log_path.with_extension("index");

        let index = OffsetIndex::create(&index_path, index_interval)?;

        Ok(IndexedLog {
            log_path,
            index_path,
            index,
            index_interval,
        })
    }

    /// Open an existing indexed log
    pub fn open<P: AsRef<Path>>(log_path: P, index_interval: u32) -> Result<Self> {
        let log_path = log_path.as_ref().to_path_buf();
        let index_path = log_path.with_extension("index");

        let index = if index_path.exists() {
            OffsetIndex::open(&index_path, index_interval)?
        } else {
            OffsetIndex::create(&index_path, index_interval)?
        };

        Ok(IndexedLog {
            log_path,
            index_path,
            index,
            index_interval,
        })
    }

    /// Update the index with a new log entry
    pub fn index_entry(&mut self, offset: Offset, file_position: u32) -> Result<()> {
        self.index.maybe_append(offset, file_position)?;
        Ok(())
    }

    /// Find the best starting position for reading from a given offset
    /// Returns None if the offset is before the first indexed offset
    pub fn find_position(&self, target_offset: Offset) -> Option<u32> {
        self.index.lookup(target_offset).map(|(_, pos)| pos)
    }

    /// Get index statistics
    pub fn index_stats(&self) -> IndexStats {
        IndexStats {
            entry_count: self.index.len(),
            file_size: self.index.size().unwrap_or(0),
            index_interval: self.index_interval,
        }
    }

    /// Flush the index to disk
    pub fn flush(&mut self) -> Result<()> {
        self.index.flush()
    }
}

/// Statistics about an index
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub entry_count: usize,
    pub file_size: u64,
    pub index_interval: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_index_entry_serialization() {
        let entry = IndexEntry::new(12345, 67890);
        let bytes = entry.to_bytes();
        let deserialized = IndexEntry::from_bytes(&bytes);

        assert_eq!(entry, deserialized);
        assert_eq!(entry.offset, 12345);
        assert_eq!(entry.position, 67890);
    }

    #[test]
    fn test_offset_index_creation() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test.index");

        let index = OffsetIndex::create(&index_path, 4096).unwrap();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_offset_index_append_and_lookup() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path, 1000).unwrap();

        // Add some entries
        index.append(0, 0).unwrap();
        index.append(100, 5000).unwrap();
        index.append(200, 10000).unwrap();
        index.append(300, 15000).unwrap();

        assert_eq!(index.len(), 4);

        // Test lookups
        assert_eq!(index.lookup(0), Some((0, 0)));
        assert_eq!(index.lookup(50), Some((0, 0)));
        assert_eq!(index.lookup(100), Some((100, 5000)));
        assert_eq!(index.lookup(150), Some((100, 5000)));
        assert_eq!(index.lookup(200), Some((200, 10000)));
        assert_eq!(index.lookup(250), Some((200, 10000)));
        assert_eq!(index.lookup(300), Some((300, 15000)));
        assert_eq!(index.lookup(400), Some((300, 15000)));
    }

    #[test]
    fn test_offset_index_maybe_append() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path, 1000).unwrap();

        // First entry should always be added
        assert!(index.maybe_append(0, 0).unwrap());
        assert_eq!(index.len(), 1);

        // Entry within interval should not be added
        assert!(!index.maybe_append(10, 500).unwrap());
        assert_eq!(index.len(), 1);

        // Entry beyond interval should be added
        assert!(index.maybe_append(50, 1000).unwrap());
        assert_eq!(index.len(), 2);

        // Another entry within new interval should not be added
        assert!(!index.maybe_append(60, 1500).unwrap());
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_offset_index_persistence() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test.index");

        // Create and populate index
        {
            let mut index = OffsetIndex::create(&index_path, 1000).unwrap();
            index.append(0, 0).unwrap();
            index.append(100, 5000).unwrap();
            index.append(200, 10000).unwrap();
        }

        // Reopen and verify
        let index = OffsetIndex::open(&index_path, 1000).unwrap();
        assert_eq!(index.len(), 3);
        assert_eq!(index.lookup(150), Some((100, 5000)));
    }

    #[test]
    fn test_indexed_log() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let mut indexed_log = IndexedLog::create(&log_path, 1000).unwrap();

        // Simulate indexing log entries
        indexed_log.index_entry(0, 0).unwrap();
        indexed_log.index_entry(10, 500).unwrap(); // Should not be indexed due to interval
        indexed_log.index_entry(50, 1000).unwrap(); // Should be indexed
        indexed_log.index_entry(100, 2000).unwrap(); // Should be indexed

        // Test position finding
        assert_eq!(indexed_log.find_position(0), Some(0));
        assert_eq!(indexed_log.find_position(25), Some(0));
        assert_eq!(indexed_log.find_position(50), Some(1000));
        assert_eq!(indexed_log.find_position(75), Some(1000));
        assert_eq!(indexed_log.find_position(100), Some(2000));
        assert_eq!(indexed_log.find_position(200), Some(2000));

        let stats = indexed_log.index_stats();
        assert_eq!(stats.entry_count, 3); // 0, 50, 100 should be indexed
        assert_eq!(stats.index_interval, 1000);
    }

    #[test]
    fn test_binary_search_edge_cases() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path, 1000).unwrap();

        // Single entry
        index.append(100, 5000).unwrap();
        assert_eq!(index.lookup(50), None); // Before first entry
        assert_eq!(index.lookup(100), Some((100, 5000)));
        assert_eq!(index.lookup(150), Some((100, 5000)));

        // Multiple entries with gaps
        index.append(300, 15000).unwrap();
        index.append(500, 25000).unwrap();

        assert_eq!(index.lookup(200), Some((100, 5000)));
        assert_eq!(index.lookup(300), Some((300, 15000)));
        assert_eq!(index.lookup(400), Some((300, 15000)));
        assert_eq!(index.lookup(500), Some((500, 25000)));
        assert_eq!(index.lookup(600), Some((500, 25000)));
    }
}
