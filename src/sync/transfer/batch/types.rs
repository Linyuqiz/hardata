#[derive(Debug, Clone)]
pub struct BatchTransferItem {
    pub source_path: String,
    pub dest_path: String,
    pub source_offset: u64,
    pub dest_offset: u64,
    pub length: u64,
    pub weak_hash: u64,
}

impl BatchTransferItem {
    pub fn new(
        source_path: String,
        dest_path: String,
        source_offset: u64,
        dest_offset: u64,
        length: u64,
        weak_hash: u64,
    ) -> Self {
        Self {
            source_path,
            dest_path,
            source_offset,
            dest_offset,
            length,
            weak_hash,
        }
    }
}

#[derive(Debug)]
pub struct BatchTransferResult {
    pub succeeded: usize,
    pub failed: usize,
    pub total_bytes: u64,
}

pub type ProgressCallback = std::sync::Arc<dyn Fn(u64) + Send + Sync>;
