use crate::core::JobStatus;
use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::net::bandwidth::BandwidthProbe;
use crate::sync::net::quic::QuicClient;
use crate::sync::net::tcp::TcpClient;
use crate::sync::net::transport::ProtocolSelector;
use crate::sync::storage::db::Database;
use crate::util::error::Result;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::infrastructure::config::{
    ConnectionPool, JobRuntimeStatus, JobStatusCallback, SchedulerConfig,
};
use super::infrastructure::connection;
use super::{
    AdaptiveConcurrencyController, DelayedQueue, NetworkAdaptiveController, PrefetchManager,
    PriorityQueue, SizeFreezer, SmartRetryPolicy,
};

fn build_region_tcp_client(
    region: &crate::sync::RegionConfig,
    pool_size: usize,
    quic_available: bool,
) -> Result<Option<TcpClient>> {
    match TcpClient::with_pool(region.tcp_bind.clone(), Some(pool_size)) {
        Ok(client) => Ok(Some(client)),
        Err(pool_error) => {
            warn!(
                "Failed to create pooled TCP client for region '{}': {}, falling back to direct TCP client",
                region.name, pool_error
            );

            match TcpClient::new(region.tcp_bind.clone()) {
                Ok(client) => Ok(Some(client)),
                Err(direct_error) if quic_available => {
                    warn!(
                        "Region '{}' will continue with QUIC only because TCP client initialization failed (pool: {}; direct: {})",
                        region.name, pool_error, direct_error
                    );
                    Ok(None)
                }
                Err(direct_error) => Err(crate::util::error::HarDataError::InvalidConfig(format!(
                    "Region '{}' has no usable transport client (tcp pool: {}; tcp direct: {})",
                    region.name, pool_error, direct_error
                ))),
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileSyncState {
    pub size: u64,
    pub mtime: i64,
    pub change_time: Option<i64>,
    pub inode: Option<u64>,
    pub dest_mtime: Option<i64>,
    pub dest_change_time: Option<i64>,
    pub dest_inode: Option<u64>,
    pub updated_at: i64,
}

#[derive(Clone)]
pub struct SyncScheduler {
    pub(super) job_queue: Arc<PriorityQueue<SyncJob>>,
    pub(super) job_notify: Arc<Notify>,
    pub(super) job_status_cache: Arc<DashMap<String, JobRuntimeStatus>>,
    pub(super) job_cache: Arc<DashMap<String, SyncJob>>,
    pub(super) cancelled_jobs: Arc<DashMap<String, ()>>,
    pub(super) running_jobs: Arc<DashMap<String, ()>>,
    pub(super) config: Arc<SchedulerConfig>,
    pub(super) transfer_manager_pool: Arc<TransferManagerPool>,
    pub(super) connection_pools: Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    pub(super) semaphore: Arc<Semaphore>,
    pub(super) shutdown: Arc<AtomicBool>,
    pub(super) shutdown_signal: watch::Sender<bool>,
    pub(super) queue_update_lock: Arc<Mutex<()>>,
    pub(super) workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    pub(super) retry_scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) delayed_scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) delayed_queue: Arc<DelayedQueue<SyncJob>>,
    pub(super) cache_cleaner: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) cache_builder: Arc<Mutex<Option<Arc<super::CacheBuilder>>>>,
    pub(super) status_callback: Arc<Mutex<Option<Arc<dyn JobStatusCallback>>>>,
    pub(super) db: Arc<Database>,
    pub(super) chunk_index: Arc<crate::sync::engine::CDCResultCache>,
    pub(super) adaptive_controller: Arc<NetworkAdaptiveController>,
    pub(super) size_freezers: Arc<DashMap<String, Arc<SizeFreezer>>>,
    pub(super) concurrency_controllers: Arc<DashMap<String, Arc<AdaptiveConcurrencyController>>>,
    pub(super) prefetch_manager: Arc<PrefetchManager>,
    pub(super) retry_policy: Arc<SmartRetryPolicy>,
    pub(super) protocol_selector: Arc<ProtocolSelector>,
    pub(super) synced_files_cache: Arc<DashMap<String, DashMap<String, FileSyncState>>>,
}

impl SyncScheduler {
    pub async fn new(
        config: SchedulerConfig,
        sync_db: Arc<crate::sync::storage::db::Database>,
    ) -> Result<Arc<Self>> {
        let connection_pools = Arc::new(DashMap::new());
        let concurrency_controllers: Arc<DashMap<String, Arc<AdaptiveConcurrencyController>>> =
            Arc::new(DashMap::new());

        use crate::core::constants::{
            SCHEDULER_MAX_CONCURRENCY, SCHEDULER_MIN_CONCURRENCY, SCHEDULER_POOL_SIZE,
        };

        for region in &config.regions {
            let quic_host = region.quic_bind.split(':').next().unwrap_or("").to_string();
            let quic_client = if quic_host.is_empty() || quic_host == "0.0.0.0" {
                None
            } else {
                let server_name = quic_host.clone();
                let ca_cert_path = {
                    let safe_name = server_name.replace(':', "-");
                    format!(".hardata/tls/agent-cert-{}.der", safe_name)
                };
                if std::path::Path::new(&ca_cert_path).exists() {
                    match QuicClient::new(
                        region.quic_bind.clone(),
                        server_name.clone(),
                        ca_cert_path.clone(),
                    ) {
                        Ok(client) => Some(client),
                        Err(err) => {
                            warn!(
                                "Region '{}' QUIC init failed (cert={}): {}, falling back to TCP only",
                                region.name, ca_cert_path, err
                            );
                            None
                        }
                    }
                } else {
                    info!(
                        "Region '{}' QUIC cert not found at {}, using TCP only",
                        region.name, ca_cert_path
                    );
                    None
                }
            };
            let has_quic = quic_client.is_some();
            let tcp_client =
                build_region_tcp_client(region, SCHEDULER_POOL_SIZE, has_quic)?;
            let pool = Arc::new(Mutex::new(ConnectionPool::new(quic_client, tcp_client)));
            connection_pools.insert(region.name.clone(), pool);

            let controller = AdaptiveConcurrencyController::new(
                SCHEDULER_MIN_CONCURRENCY,
                SCHEDULER_MAX_CONCURRENCY,
            );
            concurrency_controllers.insert(region.name.clone(), controller);

            info!(
                "Created connection pool and AIMD controller for region '{}': quic={}, tcp={} (pool_size={}), concurrency={}-{}",
                region.name,
                if has_quic { quic_host.as_str() } else { "disabled" },
                region.tcp_bind,
                SCHEDULER_POOL_SIZE,
                SCHEDULER_MIN_CONCURRENCY,
                SCHEDULER_MAX_CONCURRENCY
            );
        }

        let bandwidth_probe = Arc::new(BandwidthProbe::new());
        let (shutdown_signal, _) = watch::channel(false);

        let adaptive_controller = NetworkAdaptiveController::new(bandwidth_probe);
        info!("NetworkAdaptiveController initialized");

        let size_freezers = Arc::new(DashMap::new());
        info!(
            "SizeFreezer registry initialized with {:?} threshold",
            config.stability_threshold
        );

        let prefetch_manager = PrefetchManager::new();
        info!("PrefetchManager initialized (100MB cache, 4 prefetch ahead)");

        let retry_policy = Arc::new(SmartRetryPolicy);
        info!("SmartRetryPolicy initialized");

        let protocol_selector = ProtocolSelector::new();
        info!("ProtocolSelector initialized");

        let chunk_cache_path = std::path::Path::new(&config.chunk_cache_path);
        let chunk_index = Arc::new(crate::sync::engine::CDCResultCache::new(chunk_cache_path)?);
        info!("CDCResultCache initialized at: {:?}", chunk_cache_path);

        let scheduler = Arc::new(Self {
            job_queue: PriorityQueue::new(),
            job_notify: Arc::new(Notify::new()),
            job_status_cache: Arc::new(DashMap::new()),
            job_cache: Arc::new(DashMap::new()),
            cancelled_jobs: Arc::new(DashMap::new()),
            running_jobs: Arc::new(DashMap::new()),
            config: Arc::new(config.clone()),
            transfer_manager_pool: Arc::new(TransferManagerPool::new(sync_db.clone())),
            connection_pools,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_jobs)),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_signal,
            queue_update_lock: Arc::new(Mutex::new(())),
            workers: Arc::new(Mutex::new(Vec::new())),
            retry_scheduler: Arc::new(Mutex::new(None)),
            delayed_scheduler: Arc::new(Mutex::new(None)),
            delayed_queue: Arc::new(DelayedQueue::new()),
            cache_cleaner: Arc::new(Mutex::new(None)),
            cache_builder: Arc::new(Mutex::new(None)),
            status_callback: Arc::new(Mutex::new(None)),
            db: sync_db,
            chunk_index,
            adaptive_controller,
            size_freezers,
            concurrency_controllers,
            prefetch_manager,
            retry_policy,
            protocol_selector,
            synced_files_cache: Arc::new(DashMap::new()),
        });

        info!(
            "SyncScheduler created: max_concurrent_jobs={}, regions={}, components=6",
            config.max_concurrent_jobs,
            config.regions.len()
        );

        Ok(scheduler)
    }

    pub(super) fn get_concurrency_controller(
        &self,
        region: &str,
    ) -> Option<Arc<AdaptiveConcurrencyController>> {
        self.concurrency_controllers
            .get(region)
            .map(|controller| Arc::clone(controller.value()))
    }

    pub(in crate::sync::engine::scheduler) fn size_freezer_for_job(
        &self,
        job_id: &str,
    ) -> Arc<SizeFreezer> {
        let freeze_threshold = self.config.stability_threshold;
        let entry = self
            .size_freezers
            .entry(job_id.to_string())
            .or_insert_with(|| Arc::new(SizeFreezer::with_threshold(freeze_threshold)));
        Arc::clone(entry.value())
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        info!("Starting SyncScheduler...");

        if let Err(e) = connection::establish_all_connections(
            &self.config,
            &self.connection_pools,
            &self.shutdown,
        )
        .await
        {
            warn!(
                "Initial region connection setup failed, continuing with lazy reconnects: {}",
                e
            );
        }

        self.cleanup_old_jobs(7).await;
        self.cleanup_terminal_job_artifacts().await;

        self.recover_pending_jobs().await?;

        if self.config.enable_cache_preheat {
            let cache_builder = Arc::new(super::CacheBuilder::new(
                self.config.clone(),
                self.config.global_index.clone(),
                Arc::clone(&self.chunk_index),
                Arc::clone(&self.db),
                self.transfer_manager_pool.tmp_write_paths(),
            ));

            cache_builder.start().await;
            *self.cache_builder.lock().await = Some(cache_builder);
            info!("CacheBuilder started for background CDC and GlobalIndex building");
        }

        let worker_count = self.config.max_concurrent_jobs;
        info!("Starting {} workers...", worker_count);

        let mut workers = self.workers.lock().await;
        for worker_id in 0..worker_count {
            let scheduler = Arc::clone(self);
            let handle = tokio::spawn(async move {
                scheduler.worker_loop(worker_id).await;
            });
            workers.push(handle);
        }

        let scheduler = Arc::clone(self);
        let retry_handle = tokio::spawn(async move {
            scheduler.retry_scheduler_loop().await;
        });
        *self.retry_scheduler.lock().await = Some(retry_handle);
        info!("Retry scheduler started");

        let scheduler = Arc::clone(self);
        let delayed_handle = tokio::spawn(async move {
            scheduler.delayed_scheduler_loop().await;
        });
        *self.delayed_scheduler.lock().await = Some(delayed_handle);
        info!("Delayed scheduler started for sync jobs");

        use crate::core::constants::{FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS};
        let scheduler = Arc::clone(self);
        let cache_handle = tokio::spawn(async move {
            scheduler.cache_cleaner_loop().await;
        });
        *self.cache_cleaner.lock().await = Some(cache_handle);
        info!(
            "Cache cleaner started (max_entries={}, ttl={}s)",
            FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS
        );

        info!("SyncScheduler started successfully");
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down SyncScheduler...");

        self.shutdown.store(true, Ordering::Relaxed);
        self.shutdown_signal.send_replace(true);
        self.job_notify.notify_waiters();

        let mut workers = self.workers.lock().await;
        while let Some(worker) = workers.pop() {
            if let Err(e) = worker.await {
                warn!("Worker join error: {}", e);
            }
        }

        let mut retry_scheduler = self.retry_scheduler.lock().await;
        if let Some(handle) = retry_scheduler.take() {
            if let Err(e) = handle.await {
                warn!("Retry scheduler join error: {}", e);
            }
        }

        let mut delayed_scheduler = self.delayed_scheduler.lock().await;
        if let Some(handle) = delayed_scheduler.take() {
            if let Err(e) = handle.await {
                warn!("Delayed scheduler join error: {}", e);
            }
        }

        let mut cache_cleaner = self.cache_cleaner.lock().await;
        if let Some(handle) = cache_cleaner.take() {
            if let Err(e) = handle.await {
                warn!("Cache cleaner join error: {}", e);
            }
        }

        let mut cache_builder = self.cache_builder.lock().await;
        if let Some(builder) = cache_builder.take() {
            builder.shutdown().await;
        }

        self.transfer_manager_pool.shutdown().await;

        info!("SyncScheduler shut down successfully");
        Ok(())
    }

    pub(super) async fn delayed_scheduler_loop(&self) {
        info!("Delayed scheduler loop started");
        let check_interval = std::time::Duration::from_millis(100);
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        loop {
            if self.shutdown.load(Ordering::Relaxed) || *shutdown_rx.borrow() {
                info!("Delayed scheduler received shutdown signal");
                break;
            }

            let ready_jobs = self.delayed_queue.pop_ready().await;
            for mut job in ready_jobs {
                job.start_new_round();
                info!(
                    "Delayed job {} ready for round {} (type=sync)",
                    job.job_id, job.round_id
                );
                self.enqueue_job_replacing_queued(job).await;
                self.job_notify.notify_one();
            }

            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        info!("Delayed scheduler wake-up received shutdown signal");
                        break;
                    }
                }
            }
        }

        info!("Delayed scheduler loop stopped");
    }

    pub(in crate::sync::engine::scheduler) async fn enqueue_job_replacing_queued(
        &self,
        job: SyncJob,
    ) {
        let _queue_guard = self.queue_update_lock.lock().await;
        if self.should_skip_terminal_queue_enqueue(&job.job_id).await {
            return;
        }
        self.job_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.job_queue.enqueue(job.priority, job).await;
    }

    pub(in crate::sync::engine::scheduler) async fn schedule_delayed_job(
        &self,
        run_at: std::time::Instant,
        job: SyncJob,
    ) {
        let _queue_guard = self.queue_update_lock.lock().await;
        if self.should_skip_terminal_queue_enqueue(&job.job_id).await {
            return;
        }
        self.job_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue.insert(run_at, job).await;
    }

    pub(in crate::sync::engine::scheduler) async fn remove_queued_jobs(&self, job_id: &str) {
        let _queue_guard = self.queue_update_lock.lock().await;
        self.job_queue
            .retain(|queued| queued.job_id != job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job_id)
            .await;
    }

    async fn should_skip_terminal_queue_enqueue(&self, job_id: &str) -> bool {
        match match self.try_resolve_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => {
                warn!(
                    "Skipping queue enqueue for job {} because status resolution failed: {}",
                    job_id, e
                );
                return true;
            }
        } {
            Some(JobStatus::Paused) => {
                info!("Skipping queue enqueue for paused job {}", job_id);
                true
            }
            Some(JobStatus::Cancelled) | Some(JobStatus::Completed) => {
                info!("Skipping queue enqueue for terminal job {}", job_id);
                true
            }
            Some(JobStatus::Failed) => {
                let has_pending_retry = match self.db.get_retry(job_id).await {
                    Ok(Some(retry)) => retry.retry_count < retry.max_retries,
                    Ok(None) => false,
                    Err(e) => {
                        warn!(
                            "Skipping queue enqueue for failed job {} because retry lookup failed: {}",
                            job_id, e
                        );
                        return true;
                    }
                };
                if has_pending_retry {
                    false
                } else {
                    info!(
                        "Skipping queue enqueue for failed job {} without pending retry",
                        job_id
                    );
                    true
                }
            }
            _ => false,
        }
    }

    pub(super) async fn cache_cleaner_loop(&self) {
        use crate::core::constants::{
            FILE_CACHE_CLEANUP_INTERVAL_SECS, FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS,
        };

        let check_interval = std::time::Duration::from_secs(FILE_CACHE_CLEANUP_INTERVAL_SECS);
        let ttl_secs = FILE_CACHE_TTL_SECS as i64;
        let max_entries = FILE_CACHE_MAX_ENTRIES;
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        info!(
            "Cache cleaner loop started: interval={}s, ttl={}s, max_entries={}",
            FILE_CACHE_CLEANUP_INTERVAL_SECS, ttl_secs, max_entries
        );

        loop {
            if self.shutdown.load(Ordering::Relaxed) || *shutdown_rx.borrow() {
                info!("Cache cleaner received shutdown signal");
                break;
            }

            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        info!("Cache cleaner wake-up received shutdown signal");
                        break;
                    }
                }
            }

            self.run_cache_cleanup_cycle(chrono::Utc::now().timestamp(), ttl_secs, max_entries)
                .await;
        }

        info!("Cache cleaner loop stopped");
    }

    async fn run_cache_cleanup_cycle(&self, now: i64, ttl_secs: i64, max_entries: usize) {
        let mut total_entries = 0usize;
        let mut expired_count = 0usize;
        let mut jobs_to_remove = Vec::new();

        for job_entry in self.synced_files_cache.iter() {
            let job_id = job_entry.key().clone();
            let job_cache = job_entry.value();
            let mut expired_files = Vec::new();

            for file_entry in job_cache.iter() {
                total_entries += 1;
                let state = file_entry.value();
                if now - state.updated_at > ttl_secs {
                    expired_files.push(file_entry.key().clone());
                }
            }

            for file_path in expired_files {
                job_cache.remove(&file_path);
                expired_count += 1;
            }

            if job_cache.is_empty() {
                jobs_to_remove.push(job_id);
            }
        }

        for job_id in jobs_to_remove {
            self.synced_files_cache.remove(&job_id);
        }

        let mut remaining_entries = total_entries.saturating_sub(expired_count);

        if remaining_entries > max_entries {
            let mut all_entries: Vec<(String, String, i64)> = Vec::new();

            for job_entry in self.synced_files_cache.iter() {
                let job_id = job_entry.key().clone();
                for file_entry in job_entry.value().iter() {
                    all_entries.push((
                        job_id.clone(),
                        file_entry.key().clone(),
                        file_entry.value().updated_at,
                    ));
                }
            }

            all_entries.sort_by_key(|(_, _, updated_at)| *updated_at);

            let to_remove = all_entries.len().saturating_sub(max_entries);
            let mut removed_by_limit = 0usize;

            for (job_id, file_path, _) in all_entries.into_iter().take(to_remove) {
                if let Some(job_cache) = self.synced_files_cache.get(&job_id) {
                    job_cache.remove(&file_path);
                    removed_by_limit += 1;
                }
            }

            if removed_by_limit > 0 {
                info!(
                    "Cache cleaner: removed {} entries by limit (max={})",
                    removed_by_limit, max_entries
                );
            }

            let empty_jobs: Vec<String> = self
                .synced_files_cache
                .iter()
                .filter_map(|entry| entry.value().is_empty().then(|| entry.key().clone()))
                .collect();
            for job_id in empty_jobs {
                self.synced_files_cache.remove(&job_id);
            }

            remaining_entries = self
                .synced_files_cache
                .iter()
                .map(|entry| entry.value().len())
                .sum();
        }

        if expired_count > 0 {
            info!(
                "Cache cleaner: removed {} expired entries (ttl={}s), {} entries remaining",
                expired_count, ttl_secs, remaining_entries
            );
        }

        self.cleanup_old_jobs(7).await;
    }
}

#[cfg(test)]
mod tests {
    use super::SyncScheduler;
    use crate::core::{Job, JobPath, JobStatus, JobType};
    use crate::sync::engine::scheduler::core::FileSyncState;
    use crate::sync::engine::scheduler::SchedulerConfig;
    use crate::sync::storage::db::Database;
    use dashmap::DashMap;
    use sqlx::sqlite::SqlitePool;
    use std::fs;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::net::UdpSocket;

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
        let certified = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        let cert_der: rustls::pki_types::CertificateDer<'static> = certified.cert.into();
        let cert_path = temp_dir.join("quic-ca.der");
        fs::write(&cert_path, cert_der.as_ref()).unwrap();
        cert_path
    }

    #[tokio::test]
    async fn start_succeeds_even_when_initial_region_connection_is_unavailable() {
        let temp_dir = create_temp_dir("offline-start");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let missing_tcp = free_port();
        let missing_quic = free_port();

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::sync::RegionConfig {
                name: "offline".to_string(),
                quic_bind: format!("127.0.0.1:{}", missing_quic),
                tcp_bind: format!("127.0.0.1:{}", missing_tcp),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        scheduler.start().await.unwrap();
        let shutdown_started = Instant::now();
        scheduler.shutdown().await.unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(5));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn new_rejects_region_with_invalid_tcp_bind_even_without_quic() {
        let temp_dir = create_temp_dir("invalid-tcp-config");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::sync::RegionConfig {
                name: "broken".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: "".to_string(),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let err = match SyncScheduler::new(config, db).await {
            Ok(_) => panic!("scheduler creation should fail for invalid tcp bind"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("no usable transport client"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn start_times_out_hanging_quic_handshakes_during_initial_connection() {
        let temp_dir = create_temp_dir("offline-start-hanging-quic");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let missing_tcp = free_port();
        let blackhole = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let quic_port = blackhole.local_addr().unwrap().port();
        let quic_ca_cert_path = write_quic_ca_cert(&temp_dir);
        let auto_tls_dir = std::path::PathBuf::from(".hardata/tls");
        std::fs::create_dir_all(&auto_tls_dir).unwrap();
        std::fs::copy(&quic_ca_cert_path, auto_tls_dir.join("agent-cert-127.0.0.1.der")).unwrap();

        let blackhole_task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                if blackhole.recv_from(&mut buf).await.is_err() {
                    break;
                }
            }
        });

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::sync::RegionConfig {
                name: "offline".to_string(),
                quic_bind: format!("127.0.0.1:{}", quic_port),
                tcp_bind: format!("127.0.0.1:{}", missing_tcp),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let start_started = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), scheduler.start())
            .await
            .expect("scheduler start should not hang")
            .unwrap();
        assert!(start_started.elapsed() < Duration::from_secs(8));

        let shutdown_started = Instant::now();
        scheduler.shutdown().await.unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(5));

        blackhole_task.abort();
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn new_allows_missing_quic_ca_cert_when_tcp_is_available() {
        let temp_dir = create_temp_dir("missing-quic-cert");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let tcp_port = free_port();

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::sync::RegionConfig {
                name: "mixed".to_string(),
                quic_bind: format!("127.0.0.1:{}", free_port()),
                tcp_bind: format!("127.0.0.1:{}", tcp_port),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let pool = scheduler.connection_pools.get("mixed").unwrap();
        let pool = pool.lock().await;
        assert!(pool.quic_client.is_none());
        assert!(pool.tcp_client.is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn size_freezer_is_isolated_per_job_and_cleanup_releases_it() {
        let temp_dir = create_temp_dir("size-freezer-scope");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let freezer_a = scheduler.size_freezer_for_job("job-a");
        let freezer_b = scheduler.size_freezer_for_job("job-b");
        assert!(!Arc::ptr_eq(&freezer_a, &freezer_b));

        scheduler.cleanup_runtime_job("job-a");
        let freezer_a_recreated = scheduler.size_freezer_for_job("job-a");
        assert!(!Arc::ptr_eq(&freezer_a, &freezer_a_recreated));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn enqueue_and_schedule_skip_cancelled_jobs() {
        let temp_dir = create_temp_dir("skip-terminal-queue-enqueue");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();
        let mut job = Job::new(
            "job-skip-terminal-queue".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Cancelled;
        db.save_job(&job).await.unwrap();

        let sync_job = crate::sync::engine::job::SyncJob::new(
            job.job_id.clone(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(job.job_type);

        scheduler
            .enqueue_job_replacing_queued(sync_job.clone())
            .await;
        scheduler
            .schedule_delayed_job(Instant::now() + Duration::from_secs(60), sync_job)
            .await;

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn enqueue_and_schedule_skip_paused_jobs() {
        let temp_dir = create_temp_dir("skip-paused-queue-enqueue");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();
        let mut job = Job::new(
            "job-skip-paused-queue".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Paused;
        db.save_job(&job).await.unwrap();

        let sync_job = crate::sync::engine::job::SyncJob::new(
            job.job_id.clone(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(job.job_type);

        scheduler
            .enqueue_job_replacing_queued(sync_job.clone())
            .await;
        scheduler
            .schedule_delayed_job(Instant::now() + Duration::from_secs(60), sync_job)
            .await;

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn enqueue_and_schedule_skip_when_status_lookup_fails() {
        let temp_dir = create_temp_dir("skip-queue-enqueue-on-status-error");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let sync_job = crate::sync::engine::job::SyncJob::new(
            "job-skip-queue-status-error".to_string(),
            std::path::PathBuf::from("/tmp/source.bin"),
            "dest.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("DROP TABLE jobs").execute(&pool).await.unwrap();

        scheduler
            .enqueue_job_replacing_queued(sync_job.clone())
            .await;
        scheduler
            .schedule_delayed_job(Instant::now() + Duration::from_secs(60), sync_job)
            .await;

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cache_cleanup_cycle_also_runs_old_job_retention() {
        let temp_dir = create_temp_dir("cache-cycle-cleans-old-jobs");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();
        let mut job = Job::new(
            "job-old-completed".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = JobStatus::Completed;
        db.save_job(&job).await.unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE jobs SET updated_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler
            .run_cache_cleanup_cycle(chrono::Utc::now().timestamp(), 60, 10)
            .await;

        assert!(db.load_job(&job.job_id).await.unwrap().is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cache_cleanup_cycle_removes_empty_job_caches_after_limit_pruning() {
        let temp_dir = create_temp_dir("cache-cycle-removes-empty-job-caches");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let old_cache = DashMap::new();
        old_cache.insert(
            "/remote/old.bin".to_string(),
            FileSyncState {
                size: 1,
                mtime: 1,
                change_time: None,
                inode: None,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
                updated_at: 10,
            },
        );
        scheduler
            .synced_files_cache
            .insert("job-old".to_string(), old_cache);

        let new_cache = DashMap::new();
        new_cache.insert(
            "/remote/new.bin".to_string(),
            FileSyncState {
                size: 1,
                mtime: 1,
                change_time: None,
                inode: None,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
                updated_at: 20,
            },
        );
        scheduler
            .synced_files_cache
            .insert("job-new".to_string(), new_cache);

        scheduler.run_cache_cleanup_cycle(20, 60, 1).await;

        assert!(scheduler.synced_files_cache.get("job-old").is_none());
        let remaining = scheduler
            .synced_files_cache
            .get("job-new")
            .expect("newest cache should be retained");
        assert_eq!(remaining.len(), 1);

        let _ = fs::remove_dir_all(temp_dir);
    }
}
