use super::SyncScheduler;
use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::scheduler::core::FileSyncState;
use crate::application::sync::engine::scheduler::SchedulerConfig;
use crate::domain::{Job, JobPath, JobStatus, JobType};
use dashmap::DashMap;
use sqlx::sqlite::SqlitePool;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;

static GLOBAL_TLS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ScopedFileOverride {
    path: std::path::PathBuf,
    original: Option<Vec<u8>>,
    created_parent_dirs: Vec<std::path::PathBuf>,
}

impl ScopedFileOverride {
    fn replace(path: std::path::PathBuf, contents: &[u8]) -> Self {
        let original = fs::read(&path).ok();
        let created_parent_dirs = create_missing_parent_dirs(&path);
        fs::write(&path, contents).unwrap();
        Self {
            path,
            original,
            created_parent_dirs,
        }
    }

    fn remove(path: std::path::PathBuf) -> Self {
        let original = fs::read(&path).ok();
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }
        Self {
            path,
            original,
            created_parent_dirs: Vec::new(),
        }
    }
}

impl Drop for ScopedFileOverride {
    fn drop(&mut self) {
        if let Some(original) = &self.original {
            let _ = fs::write(&self.path, original);
        } else if self.path.exists() {
            let _ = fs::remove_file(&self.path);
        }

        for parent in &self.created_parent_dirs {
            if fs::remove_dir(parent).is_err() {
                break;
            }
        }
    }
}

fn create_missing_parent_dirs(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Some(parent) = path.parent() else {
        return Vec::new();
    };

    let mut missing = Vec::new();
    let mut cursor = parent.to_path_buf();
    while !cursor.exists() {
        missing.push(cursor.clone());
        let Some(next) = cursor.parent() else {
            break;
        };
        cursor = next.to_path_buf();
    }

    fs::create_dir_all(parent).unwrap();
    missing
}

fn create_temp_dir(label: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hardata-scheduler-core-{label}-{unique}"));
    fs::create_dir_all(&path).unwrap();
    path
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn write_quic_ca_cert(temp_dir: &std::path::Path) -> std::path::PathBuf {
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let cert_der: rustls::pki_types::CertificateDer<'static> = certified.cert.into();
    let cert_path = temp_dir.join("quic-ca.der");
    fs::write(&cert_path, cert_der.as_ref()).unwrap();
    cert_path
}

include!("part_01.rs");
