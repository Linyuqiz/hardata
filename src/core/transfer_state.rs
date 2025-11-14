use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTransferState {
    pub file_path: String,
    pub total_chunks: usize,
    pub completed_chunks: HashSet<usize>,
    pub progress: u8,
}

impl FileTransferState {
    pub fn new(file_path: String, total_chunks: usize) -> Self {
        Self {
            file_path,
            total_chunks,
            completed_chunks: HashSet::new(),
            progress: 0,
        }
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

    fn update_progress(&mut self) {
        if self.total_chunks > 0 {
            self.progress =
                ((self.completed_chunks.len() as f64 / self.total_chunks as f64) * 100.0) as u8;
        }
    }
}
