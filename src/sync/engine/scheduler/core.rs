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
use tokio::sync::{Mutex, Notify, Semaphore};
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

#[derive(Clone, Debug)]
pub struct FileSyncState {
    pub size: u64,
    pub mtime: i64,
    pub updated_at: i64,
}

#[derive(Clone)]
pub struct SyncScheduler {
    pub(super) job_queue: Arc<PriorityQueue<SyncJob>>,
    pub(super) job_notify: Arc<Notify>,
    pub(super) job_status_cache: Arc<DashMap<String, JobRuntimeStatus>>,
    pub(super) job_cache: Arc<DashMap<String, SyncJob>>,
    pub(super) config: Arc<SchedulerConfig>,
    pub(super) transfer_manager_pool: Arc<TransferManagerPool>,
    pub(super) connection_pools: Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    pub(super) semaphore: Arc<Semaphore>,
    pub(super) shutdown: Arc<AtomicBool>,
    pub(super) workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    pub(super) retry_scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) delayed_scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) delayed_queue: Arc<DelayedQueue<SyncJob>>,
    pub(super) cache_cleaner: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) status_callback: Arc<Mutex<Option<Arc<dyn JobStatusCallback>>>>,
    pub(super) db: Arc<Database>,
    pub(super) chunk_index: Arc<crate::sync::engine::CDCResultCache>,
    pub(super) adaptive_controller: Arc<NetworkAdaptiveController>,
    pub(super) size_freezer: Arc<SizeFreezer>,
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
            let quic_client = QuicClient::new(region.quic_bind.clone()).ok();
            let tcp_client =
                TcpClient::with_pool(region.tcp_bind.clone(), Some(SCHEDULER_POOL_SIZE)).ok();
            let pool = Arc::new(Mutex::new(ConnectionPool::new(quic_client, tcp_client)));
            connection_pools.insert(region.name.clone(), pool);

            let controller = AdaptiveConcurrencyController::new(
                SCHEDULER_MIN_CONCURRENCY,
                SCHEDULER_MAX_CONCURRENCY,
            );
            concurrency_controllers.insert(region.name.clone(), controller);

            info!(
                "Created connection pool and AIMD controller for region '{}': quic={}, tcp={} (pool_size={}), concurrency={}-{}",
                region.name, region.quic_bind, region.tcp_bind, SCHEDULER_POOL_SIZE, SCHEDULER_MIN_CONCURRENCY, SCHEDULER_MAX_CONCURRENCY
            );
        }

        let bandwidth_probe = Arc::new(BandwidthProbe::new());

        let adaptive_controller = NetworkAdaptiveController::new(bandwidth_probe);
        info!("NetworkAdaptiveController initialized");

        let size_freezer = Arc::new(SizeFreezer::new("global"));
        info!("SizeFreezer initialized with 20s threshold");

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
            config: Arc::new(config.clone()),
            transfer_manager_pool: Arc::new(TransferManagerPool::new(sync_db.clone())),
            connection_pools,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_jobs)),
            shutdown: Arc::new(AtomicBool::new(false)),
            workers: Arc::new(Mutex::new(Vec::new())),
            retry_scheduler: Arc::new(Mutex::new(None)),
            delayed_scheduler: Arc::new(Mutex::new(None)),
            delayed_queue: Arc::new(DelayedQueue::new()),
            cache_cleaner: Arc::new(Mutex::new(None)),
            status_callback: Arc::new(Mutex::new(None)),
            db: sync_db,
            chunk_index,
            adaptive_controller,
            size_freezer,
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

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        info!("Starting SyncScheduler...");

        connection::establish_all_connections(&self.config, &self.connection_pools, &self.shutdown)
            .await?;

        self.cleanup_old_jobs(7).await;

        self.recover_pending_jobs().await?;

        if self.config.enable_cache_preheat {
            let cache_builder = Arc::new(super::CacheBuilder::new(
                self.config.clone(),
                self.config.global_index.clone(),
                Arc::clone(&self.chunk_index),
                Arc::clone(&self.db),
            ));

            cache_builder.start();
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

        info!("SyncScheduler shut down successfully");
        Ok(())
    }

    pub(super) async fn delayed_scheduler_loop(&self) {
        info!("Delayed scheduler loop started");
        let check_interval = std::time::Duration::from_millis(100);

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
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
                self.job_queue.enqueue(job.priority, job).await;
                self.job_notify.notify_one();
            }

            tokio::time::sleep(check_interval).await;
        }

        info!("Delayed scheduler loop stopped");
    }

    pub(super) async fn cache_cleaner_loop(&self) {
        use crate::core::constants::{
            FILE_CACHE_CLEANUP_INTERVAL_SECS, FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS,
        };

        let check_interval = std::time::Duration::from_secs(FILE_CACHE_CLEANUP_INTERVAL_SECS);
        let ttl_secs = FILE_CACHE_TTL_SECS as i64;
        let max_entries = FILE_CACHE_MAX_ENTRIES;

        info!(
            "Cache cleaner loop started: interval={}s, ttl={}s, max_entries={}",
            FILE_CACHE_CLEANUP_INTERVAL_SECS, ttl_secs, max_entries
        );

        loop {
            tokio::time::sleep(check_interval).await;

            if self.shutdown.load(Ordering::Relaxed) {
                info!("Cache cleaner received shutdown signal");
                break;
            }

            let now = chrono::Utc::now().timestamp();
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

            if total_entries - expired_count > max_entries {
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
            }

            if expired_count > 0 {
                info!(
                    "Cache cleaner: removed {} expired entries (ttl={}s), {} entries remaining",
                    expired_count,
                    ttl_secs,
                    total_entries - expired_count
                );
            }
        }

        info!("Cache cleaner loop stopped");
    }
}
