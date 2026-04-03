use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::util::time::{optional_timestamps_match, timestamps_match};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileTransferState {
    pub file_path: String,
    pub total_chunks: usize,
    pub completed_chunks: HashSet<usize>,
    pub progress: u8,
    #[serde(default)]
    pub source_size: Option<u64>,
    #[serde(default)]
    pub source_modified: Option<i64>,
    #[serde(default)]
    pub source_change_time: Option<i64>,
    #[serde(default)]
    pub source_inode: Option<u64>,
    #[serde(default)]
    pub dest_size: Option<u64>,
    #[serde(default)]
    pub dest_modified: Option<i64>,
    #[serde(default)]
    pub dest_change_time: Option<i64>,
    #[serde(default)]
    pub dest_inode: Option<u64>,
    #[serde(default)]
    pub cache_only: bool,
}

impl FileTransferState {
    pub fn new(file_path: String, total_chunks: usize) -> Self {
        Self {
            file_path,
            total_chunks,
            completed_chunks: HashSet::new(),
            progress: 0,
            source_size: None,
            source_modified: None,
            source_change_time: None,
            source_inode: None,
            dest_size: None,
            dest_modified: None,
            dest_change_time: None,
            dest_inode: None,
            cache_only: false,
        }
    }

    pub fn with_source_version(
        mut self,
        source_size: u64,
        source_modified: i64,
        source_change_time: Option<i64>,
        source_inode: Option<u64>,
    ) -> Self {
        self.source_size = Some(source_size);
        self.source_modified = Some(source_modified);
        self.source_change_time = source_change_time;
        self.source_inode = source_inode;
        self
    }

    pub fn with_destination_version(
        mut self,
        dest_size: u64,
        dest_modified: i64,
        dest_change_time: Option<i64>,
        dest_inode: Option<u64>,
    ) -> Self {
        self.dest_size = Some(dest_size);
        self.dest_modified = Some(dest_modified);
        self.dest_change_time = dest_change_time;
        self.dest_inode = dest_inode;
        self
    }

    pub fn mark_cache_only(mut self) -> Self {
        self.cache_only = true;
        self
    }

    pub fn mark_chunk_completed(&mut self, chunk_index: usize) {
        self.completed_chunks.insert(chunk_index);
        self.update_progress();
    }

    pub fn is_chunk_completed(&self, chunk_index: usize) -> bool {
        self.completed_chunks.contains(&chunk_index)
    }

    pub fn is_completed(&self) -> bool {
        self.completed_chunks.len() == self.total_chunks
    }

    pub fn set_destination_version(
        &mut self,
        dest_size: u64,
        dest_modified: i64,
        dest_change_time: Option<i64>,
        dest_inode: Option<u64>,
    ) {
        self.dest_size = Some(dest_size);
        self.dest_modified = Some(dest_modified);
        self.dest_change_time = dest_change_time;
        self.dest_inode = dest_inode;
    }

    pub fn clear_destination_version(&mut self) {
        self.dest_size = None;
        self.dest_modified = None;
        self.dest_change_time = None;
        self.dest_inode = None;
    }

    pub fn matches_source_version(
        &self,
        source_size: u64,
        source_modified: i64,
        source_change_time: Option<i64>,
        source_inode: Option<u64>,
        total_chunks: usize,
    ) -> bool {
        let metadata_compatible = self.total_chunks == total_chunks
            && self
                .completed_chunks
                .iter()
                .all(|chunk_index| *chunk_index < total_chunks)
            && self.source_size == Some(source_size)
            && self
                .source_modified
                .map(|saved| timestamps_match(saved, source_modified))
                .unwrap_or(false);
        if !metadata_compatible {
            return false;
        }

        if source_change_time.is_some()
            && !optional_timestamps_match(self.source_change_time, source_change_time)
        {
            return false;
        }
        if self.source_change_time.is_some() && source_change_time.is_none() {
            return false;
        }

        if source_inode.is_some() && self.source_inode != source_inode {
            return false;
        }
        if self.source_inode.is_some() && source_inode.is_none() {
            return false;
        }

        true
    }

    pub fn matches_destination_version(
        &self,
        dest_size: Option<u64>,
        dest_modified: Option<i64>,
        dest_change_time: Option<i64>,
        dest_inode: Option<u64>,
    ) -> bool {
        if self.completed_chunks.is_empty() {
            return true;
        }

        let metadata_compatible = self.dest_size.is_some()
            && self.dest_modified.is_some()
            && self.dest_size == dest_size
            && optional_timestamps_match(self.dest_modified, dest_modified);
        if !metadata_compatible {
            return false;
        }

        if dest_change_time.is_some()
            && !optional_timestamps_match(self.dest_change_time, dest_change_time)
        {
            return false;
        }
        if self.dest_change_time.is_some() && dest_change_time.is_none() {
            return false;
        }

        if dest_inode.is_some() && self.dest_inode != dest_inode {
            return false;
        }
        if self.dest_inode.is_some() && dest_inode.is_none() {
            return false;
        }

        true
    }

    fn update_progress(&mut self) {
        if self.total_chunks > 0 {
            self.progress =
                ((self.completed_chunks.len() as f64 / self.total_chunks as f64) * 100.0) as u8;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FileTransferState;

    fn state_with_source_identity(
        change_time: Option<i64>,
        inode: Option<u64>,
    ) -> FileTransferState {
        FileTransferState::new("test.bin".to_string(), 4).with_source_version(
            1024,
            100,
            change_time,
            inode,
        )
    }

    fn state_with_dest_identity(change_time: Option<i64>, inode: Option<u64>) -> FileTransferState {
        let mut state = FileTransferState::new("test.bin".to_string(), 4)
            .with_source_version(1024, 100, None, None)
            .with_destination_version(1024, 200, change_time, inode);
        state.mark_chunk_completed(0);
        state
    }

    // --- matches_source_version ---

    #[test]
    fn source_both_none_matches() {
        let state = state_with_source_identity(None, None);
        assert!(state.matches_source_version(1024, 100, None, None, 4));
    }

    #[test]
    fn source_both_same_matches() {
        let state = state_with_source_identity(Some(50), Some(99));
        assert!(state.matches_source_version(1024, 100, Some(50), Some(99), 4));
    }

    #[test]
    fn source_both_different_rejects() {
        let state = state_with_source_identity(Some(50), Some(99));
        assert!(!state.matches_source_version(1024, 100, Some(60), Some(99), 4));
        assert!(!state.matches_source_version(1024, 100, Some(50), Some(88), 4));
    }

    #[test]
    fn source_checkpoint_has_identity_scan_returns_none_rejects() {
        let state = state_with_source_identity(Some(50), Some(99));
        assert!(!state.matches_source_version(1024, 100, None, None, 4));
    }

    #[test]
    fn source_checkpoint_none_scan_has_identity_rejects() {
        let state = state_with_source_identity(None, None);
        assert!(!state.matches_source_version(1024, 100, Some(50), Some(99), 4));
    }

    #[test]
    fn source_legacy_second_precision_matches_nanos_scan() {
        let state = FileTransferState::new("test.bin".to_string(), 4).with_source_version(
            1024,
            1_710_000_000,
            Some(1_710_000_000),
            Some(99),
        );
        assert!(state.matches_source_version(
            1024,
            1_710_000_000_123_456_789,
            Some(1_710_000_000_987_654_321),
            Some(99),
            4
        ));
    }

    // --- matches_destination_version ---

    #[test]
    fn dest_both_none_matches() {
        let state = state_with_dest_identity(None, None);
        assert!(state.matches_destination_version(Some(1024), Some(200), None, None));
    }

    #[test]
    fn dest_both_same_matches() {
        let state = state_with_dest_identity(Some(50), Some(99));
        assert!(state.matches_destination_version(Some(1024), Some(200), Some(50), Some(99)));
    }

    #[test]
    fn dest_both_different_rejects() {
        let state = state_with_dest_identity(Some(50), Some(99));
        assert!(!state.matches_destination_version(Some(1024), Some(200), Some(60), Some(99)));
    }

    #[test]
    fn dest_checkpoint_has_identity_scan_returns_none_rejects() {
        let state = state_with_dest_identity(Some(50), Some(99));
        assert!(!state.matches_destination_version(Some(1024), Some(200), None, None));
    }

    #[test]
    fn dest_checkpoint_none_scan_has_identity_rejects() {
        let state = state_with_dest_identity(None, None);
        assert!(!state.matches_destination_version(Some(1024), Some(200), Some(50), Some(99)));
    }

    #[test]
    fn dest_legacy_second_precision_matches_nanos_scan() {
        let mut state = FileTransferState::new("test.bin".to_string(), 4)
            .with_source_version(1024, 100, None, None)
            .with_destination_version(1024, 1_710_000_200, Some(1_710_000_200), Some(99));
        state.mark_chunk_completed(0);
        assert!(state.matches_destination_version(
            Some(1024),
            Some(1_710_000_200_999_999_999),
            Some(1_710_000_200_111_111_111),
            Some(99)
        ));
    }

    #[test]
    fn dest_empty_chunks_always_matches() {
        let state = FileTransferState::new("test.bin".to_string(), 4)
            .with_source_version(1024, 100, None, None)
            .with_destination_version(1024, 200, Some(50), Some(99));
        assert!(state.matches_destination_version(Some(9999), Some(9999), None, None));
    }
}
