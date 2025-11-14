use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum JobType {
    #[default]
    Once,
    Full,
    Sync,
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Once => "once",
            Self::Full => "full",
            Self::Sync => "sync",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "full" => Self::Full,
            "sync" => Self::Sync,
            _ => Self::Once,
        }
    }

    pub fn is_sync(&self) -> bool {
        matches!(self, Self::Sync)
    }

    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Syncing,
    Paused,
    Cancelled,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Syncing => "syncing",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub compression: String,
    pub workers: u32,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
}

fn default_chunk_size() -> u64 {
    4 * 1024 * 1024
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            workers: 10,
            chunk_size: default_chunk_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPath {
    pub path: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub job_id: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub source: JobPath,
    pub dest: JobPath,
    pub status: JobStatus,
    #[serde(default)]
    pub job_type: JobType,
    #[serde(default)]
    pub exclude_regex: Vec<String>,
    #[serde(default)]
    pub include_regex: Vec<String>,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default)]
    pub options: JobConfig,
    #[serde(default)]
    pub progress: u8,
    #[serde(default)]
    pub current_size: u64,
    #[serde(default)]
    pub total_size: u64,
    #[serde(default = "chrono::Utc::now")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[serde(default = "chrono::Utc::now")]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

fn default_region() -> String {
    "default".to_string()
}

fn default_priority() -> i32 {
    100
}

impl Job {
    pub fn new(job_id: String, source: JobPath, dest: JobPath) -> Self {
        let now = chrono::Utc::now();
        Self {
            job_id,
            region: default_region(),
            source,
            dest,
            status: JobStatus::Pending,
            job_type: JobType::Once,
            exclude_regex: Vec::new(),
            include_regex: Vec::new(),
            priority: default_priority(),
            options: JobConfig::default(),
            progress: 0,
            current_size: 0,
            total_size: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_job_type(mut self, job_type: JobType) -> Self {
        self.job_type = job_type;
        self
    }

    pub fn update_progress(&mut self, current: u64, total: u64) {
        self.current_size = current;
        self.total_size = total;
        if total > 0 {
            self.progress = ((current as f64 / total as f64) * 100.0).min(100.0) as u8;
        }
        self.updated_at = chrono::Utc::now();
    }
}
