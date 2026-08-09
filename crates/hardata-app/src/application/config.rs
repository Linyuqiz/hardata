use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegionConfig {
    pub name: String,
    pub quic_bind: String,
    pub tcp_bind: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SyncConfig {
    pub http_bind: String,
    #[serde(default = "default_sync_data_dir")]
    pub data_dir: String,
    #[serde(default)]
    pub allow_external_destinations: bool,
    #[serde(default = "default_metadata_dir")]
    pub metadata: String,
    #[serde(default)]
    pub web_ui: bool,
    #[serde(default)]
    pub api_token: Option<String>,
    #[serde(default)]
    pub regions: Vec<RegionConfig>,
    #[serde(default = "default_stability_threshold_secs")]
    pub stability_threshold_secs: u64,
    #[serde(default)]
    pub replicate_mode: crate::application::sync::engine::scheduler::ReplicateMode,
}

impl SyncConfig {
    pub fn db_path(&self) -> String {
        format!("{}/data.db", self.metadata)
    }

    pub fn chunk_cache_path(&self) -> String {
        format!("{}/.cache", self.metadata)
    }
}

fn default_sync_data_dir() -> String {
    ".hardata/sync".to_string()
}

fn default_metadata_dir() -> String {
    ".hardata".to_string()
}

fn default_stability_threshold_secs() -> u64 {
    20
}
