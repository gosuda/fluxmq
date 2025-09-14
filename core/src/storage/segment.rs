use crate::protocol::{Message, Offset};
use crate::storage::log::{Log, LogEntry};
use crate::{FluxmqError, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Configuration for segment management
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Maximum size of a segment file in bytes
    pub max_segment_size: u64,
    /// Base directory for storing segments
    pub base_dir: PathBuf,
    /// Segment file name format: {base_offset:020}.log
    pub segment_prefix: String,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_size: 1024 * 1024 * 1024, // 1GB default
            base_dir: PathBuf::from("data"),
            segment_prefix: "segment".to_string(),
        }
    }
}

/// Manages multiple log segments for a topic-partition
#[derive(Debug)]
pub struct SegmentManager {
    config: SegmentConfig,
    segments: BTreeMap<Offset, Log>,
    active_segment_offset: Offset,
    next_offset: Offset,
}

impl SegmentManager {
    /// Create a new segment manager
    pub fn new(config: SegmentConfig) -> Result<Self> {
        // Create base directory if it doesn't exist
        std::fs::create_dir_all(&config.base_dir)?;

        let mut manager = Self {
            config,
            segments: BTreeMap::new(),
            active_segment_offset: 0,
            next_offset: 0,
        };

        // Load existing segments or create the first one
        manager.load_existing_segments()?;

        if manager.segments.is_empty() {
            manager.create_new_segment(0)?;
        }

        Ok(manager)
    }

    /// Create segment manager from existing directory
    pub fn from_directory<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let config = SegmentConfig {
            base_dir: base_dir.as_ref().to_path_buf(),
            ..Default::default()
        };
        Self::new(config)
    }

    /// Append messages to the active segment
    pub fn append(&mut self, messages: &[Message]) -> Result<Offset> {
        let base_offset = self.next_offset;

        // Check if we need to roll to a new segment
        if self.should_roll_segment(messages)? {
            self.roll_segment()?;
        }

        // Get the active segment
        let active_segment = self
            .segments
            .get_mut(&self.active_segment_offset)
            .ok_or_else(|| {
                FluxmqError::Storage(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Active segment not found",
                ))
            })?;

        // Append to the active segment
        let _segment_base_offset = active_segment.append(messages)?;
        self.next_offset = active_segment.next_offset();

        Ok(base_offset)
    }

    /// Read messages starting from the given offset
    pub fn read(&self, offset: Offset, max_bytes: usize) -> Result<Vec<LogEntry>> {
        // Find the segment that contains the requested offset
        let segment_offset = self.find_segment_for_offset(offset);

        let mut entries = Vec::new();
        let mut remaining_bytes = max_bytes;

        // Start reading from the found segment and potentially continue to subsequent segments
        for (&seg_offset, segment) in self.segments.range(segment_offset..) {
            if remaining_bytes == 0 {
                break;
            }

            let segment_entries = segment.read(offset.max(seg_offset), remaining_bytes)?;

            for entry in segment_entries {
                if entry.offset >= offset {
                    let entry_size = entry.serialized_size();
                    if entry_size <= remaining_bytes {
                        remaining_bytes -= entry_size;
                        entries.push(entry);
                    } else {
                        // Entry is too large for remaining bytes
                        break;
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Get the next offset that will be written
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    /// Get all segment base offsets
    pub fn segment_offsets(&self) -> Vec<Offset> {
        self.segments.keys().cloned().collect()
    }

    /// Get segment count
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Get total size of all segments
    pub fn total_size(&self) -> Result<u64> {
        let mut total = 0;
        for segment in self.segments.values() {
            total += segment.size()?;
        }
        Ok(total)
    }

    /// Flush all segments to disk
    pub fn flush(&mut self) -> Result<()> {
        for segment in self.segments.values_mut() {
            segment.flush()?;
        }
        Ok(())
    }

    /// Remove old segments based on retention policy
    pub fn cleanup(&mut self, retain_segments: usize) -> Result<Vec<Offset>> {
        if self.segments.len() <= retain_segments {
            return Ok(vec![]);
        }

        let segments_to_remove = self.segments.len() - retain_segments;
        let mut removed_offsets = Vec::new();

        // Remove oldest segments
        let offsets_to_remove: Vec<Offset> = self
            .segments
            .keys()
            .take(segments_to_remove)
            .cloned()
            .collect();

        for offset in offsets_to_remove {
            if let Some(segment) = self.segments.remove(&offset) {
                // Remove the file
                std::fs::remove_file(&segment.path)?;
                removed_offsets.push(offset);
            }
        }

        Ok(removed_offsets)
    }

    /// Load existing segments from disk
    fn load_existing_segments(&mut self) -> Result<()> {
        let entries = std::fs::read_dir(&self.config.base_dir)?;
        let mut segment_files = Vec::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(base_offset) = filename.parse::<Offset>() {
                        segment_files.push((base_offset, path));
                    }
                }
            }
        }

        // Sort by base offset
        segment_files.sort_by_key(|(offset, _)| *offset);

        // Load segments
        for (base_offset, path) in segment_files {
            match Log::open(&path, base_offset) {
                Ok(segment) => {
                    self.next_offset = self.next_offset.max(segment.next_offset());
                    self.segments.insert(base_offset, segment);
                    self.active_segment_offset = base_offset;
                }
                Err(e) => {
                    eprintln!("Warning: Failed to load segment {}: {}", path.display(), e);
                    // Optionally move corrupted segment to backup location
                }
            }
        }

        Ok(())
    }

    /// Create a new segment
    fn create_new_segment(&mut self, base_offset: Offset) -> Result<()> {
        let filename = format!("{:020}.log", base_offset);
        let path = self.config.base_dir.join(filename);

        let segment = Log::create(&path, base_offset)?;
        self.segments.insert(base_offset, segment);
        self.active_segment_offset = base_offset;

        Ok(())
    }

    /// Check if we should roll to a new segment
    fn should_roll_segment(&self, messages: &[Message]) -> Result<bool> {
        let active_segment = self.segments.get(&self.active_segment_offset);

        if let Some(segment) = active_segment {
            let current_size = segment.size()?;

            // Estimate size of new messages
            let estimated_new_size: usize = messages
                .iter()
                .map(|m| {
                    let key_len = m.key.as_ref().map_or(0, |k| k.len());
                    20 + key_len + m.value.len() + 4 // Record header + key + value + length prefix
                })
                .sum();

            Ok(current_size + estimated_new_size as u64 > self.config.max_segment_size)
        } else {
            Ok(true) // No active segment, should create one
        }
    }

    /// Roll to a new segment
    fn roll_segment(&mut self) -> Result<()> {
        let new_base_offset = self.next_offset;
        self.create_new_segment(new_base_offset)?;
        Ok(())
    }

    /// Find the segment that should contain the given offset
    fn find_segment_for_offset(&self, offset: Offset) -> Offset {
        // Find the largest segment base offset that is <= the target offset
        self.segments
            .keys()
            .rev()
            .find(|&&seg_offset| seg_offset <= offset)
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::tempdir;

    #[test]
    fn test_segment_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024, // Small size for testing
            ..Default::default()
        };

        let manager = SegmentManager::new(config).unwrap();
        assert_eq!(manager.segment_count(), 1);
        assert_eq!(manager.next_offset(), 0);
    }

    #[test]
    fn test_segment_append_and_read() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024,
            ..Default::default()
        };

        let mut manager = SegmentManager::new(config).unwrap();

        // Append messages
        let messages = vec![
            Message::new("message 1").with_key("key1"),
            Message::new("message 2").with_key("key2"),
        ];

        let base_offset = manager.append(&messages).unwrap();
        assert_eq!(base_offset, 0);
        assert_eq!(manager.next_offset(), 2);

        // Read messages back
        let entries = manager.read(0, 1024).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[0].value.as_ref(), b"message 1");
        assert_eq!(entries[1].offset, 1);
        assert_eq!(entries[1].value.as_ref(), b"message 2");
    }

    #[test]
    fn test_segment_rolling() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 100, // Very small to force rolling
            ..Default::default()
        };

        let mut manager = SegmentManager::new(config).unwrap();
        assert_eq!(manager.segment_count(), 1);

        // Add messages to trigger segment rolling
        for i in 0..10 {
            let message = Message::new(format!(
                "This is a longer message {} that should trigger segment rolling",
                i
            ));
            let messages = vec![message];
            manager.append(&messages).unwrap();
        }

        // Should have created multiple segments
        assert!(manager.segment_count() > 1);

        // Verify we can read all messages
        let entries = manager.read(0, 4096).unwrap();
        assert_eq!(entries.len(), 10);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }
    }

    #[test]
    fn test_segment_recovery() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 200,
            ..Default::default()
        };

        // Create some segments
        {
            let mut manager = SegmentManager::new(config.clone()).unwrap();
            for i in 0..6 {
                let message = Message::new(format!("message {}", i));
                manager.append(&[message]).unwrap();
            }
        }

        // Recreate manager from existing directory
        let manager = SegmentManager::new(config).unwrap();
        assert_eq!(manager.next_offset(), 6);

        // Verify we can read all messages
        let entries = manager.read(0, 1024).unwrap();
        assert_eq!(entries.len(), 6);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
            assert_eq!(entry.value, Bytes::from(format!("message {}", i)));
        }
    }

    #[test]
    fn test_segment_cleanup() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 50, // Small to create many segments
            ..Default::default()
        };

        let mut manager = SegmentManager::new(config).unwrap();

        // Create multiple segments
        for i in 0..20 {
            let message = Message::new(format!("message {}", i));
            manager.append(&[message]).unwrap();
        }

        let initial_count = manager.segment_count();
        assert!(initial_count > 3);

        // Cleanup old segments, keep only 3
        let removed = manager.cleanup(3).unwrap();
        assert_eq!(manager.segment_count(), 3);
        assert_eq!(removed.len(), initial_count - 3);

        // Verify we can still read recent messages
        let entries = manager.read(15, 1024).unwrap();
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_segment_read_from_offset() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 100,
            ..Default::default()
        };

        let mut manager = SegmentManager::new(config).unwrap();

        // Add messages across multiple segments
        for i in 0..10 {
            let message = Message::new(format!("message {}", i));
            manager.append(&[message]).unwrap();
        }

        // Read from offset 5
        let entries = manager.read(5, 1024).unwrap();
        assert_eq!(entries[0].offset, 5);
        assert_eq!(entries.len(), 5); // messages 5-9

        // Read from offset 8 with limited bytes
        let entries = manager.read(8, 60).unwrap();
        assert!(entries.len() >= 1);
        assert_eq!(entries[0].offset, 8);
    }
}
