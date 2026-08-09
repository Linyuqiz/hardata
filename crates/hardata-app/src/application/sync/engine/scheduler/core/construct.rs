impl SyncScheduler {
    pub async fn new(
        config: SchedulerConfig,
        sync_db: Arc<crate::adapters::outbound::persistence::db::Database>,
    ) -> Result<Arc<Self>> {
        let connection_pools = Arc::new(DashMap::new());
        let concurrency_controllers: Arc<DashMap<String, Arc<AdaptiveConcurrencyController>>> =
            Arc::new(DashMap::new());

        use crate::shared::constants::{
            SCHEDULER_MAX_CONCURRENCY, SCHEDULER_MIN_CONCURRENCY, SCHEDULER_POOL_SIZE,
        };

        for region in &config.regions {
            let quic_host = connection::quic_server_name(&region.quic_bind);
            let quic_client = if let Some(quic_host) = quic_host.as_deref() {
                let server_name = quic_host.to_string();
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
                                operation = "scheduler.quic_init_failed",
                                region = %region.name,
                                certificate = %ca_cert_path,
                                error = %err,
                                fallback = "tcp_only",
                                "QUIC unavailable; falling back to TCP"
                            );
                            None
                        }
                    }
                } else {
                    info!(
                        operation = "scheduler.quic_disabled",
                        region = %region.name,
                        certificate = %ca_cert_path,
                        reason = "certificate_missing",
                        "QUIC disabled; using TCP only"
                    );
                    None
                }
            } else {
                None
            };
            let has_quic = quic_client.is_some();
            let tcp_client = build_region_tcp_client(region, SCHEDULER_POOL_SIZE, has_quic)?;
            let pool = Arc::new(Mutex::new(ConnectionPool::new(quic_client, tcp_client)));
            connection_pools.insert(region.name.clone(), pool);

            let controller = AdaptiveConcurrencyController::new(
                SCHEDULER_MIN_CONCURRENCY,
                SCHEDULER_MAX_CONCURRENCY,
            );
            concurrency_controllers.insert(region.name.clone(), controller);

            info!(
                operation = "scheduler.region_configured",
                region = %region.name,
                quic = has_quic,
                tcp_bind = %region.tcp_bind,
                pool_size = SCHEDULER_POOL_SIZE,
                min_concurrency = SCHEDULER_MIN_CONCURRENCY,
                max_concurrency = SCHEDULER_MAX_CONCURRENCY,
                "scheduler region configured"
            );
        }

        let bandwidth_probe = Arc::new(BandwidthProbe::new());
        let (shutdown_signal, _) = watch::channel(false);

        let adaptive_controller = NetworkAdaptiveController::new(bandwidth_probe);
        let size_freezers = Arc::new(DashMap::new());
        let prefetch_manager = PrefetchManager::new();
        let retry_policy = Arc::new(SmartRetryPolicy);
        let protocol_selector = ProtocolSelector::new();

        let chunk_cache_path = std::path::Path::new(&config.chunk_cache_path);
        let chunk_index = Arc::new(crate::application::sync::engine::CDCResultCache::new(
            chunk_cache_path,
        )?);

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
            operation = "scheduler.created",
            max_concurrent_jobs = config.max_concurrent_jobs,
            region_count = config.regions.len(),
            stability_threshold = ?config.stability_threshold,
            chunk_cache = %chunk_cache_path.display(),
            "sync scheduler created"
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

    pub(in crate::application::sync::engine::scheduler) fn size_freezer_for_job(
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

}
