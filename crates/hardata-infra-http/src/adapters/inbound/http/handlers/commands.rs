pub async fn create_job(
    State(state): State<SyncApiState>,
    headers: HeaderMap,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let idempotency_key = extract_create_job_idempotency_key(&headers, req.request_id.as_deref())?;
    if req.source_path.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "source_path is required".to_string(),
        ));
    }
    if req.dest_path.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "dest_path is required".to_string()));
    }
    if req.region.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "region is required".to_string()));
    }
    validate_job_path(&req.source_path, "source_path")?;
    validate_job_path(&req.dest_path, "dest_path")?;
    validate_destination_scope(
        &req.dest_path,
        &state.data_dir,
        state.allow_external_destinations,
    )?;
    validate_regex_patterns(&req.exclude_regex, "exclude_regex")?;
    validate_regex_patterns(&req.include_regex, "include_regex")?;

    if !state.regions.iter().any(|r| r.name == req.region) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Invalid region '{}'", req.region),
        ));
    }

    let job_type_lower = req.job_type.to_lowercase();
    if job_type_lower != "once" && job_type_lower != "sync" && job_type_lower != "full" {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid job_type '{}', must be 'once', 'full' or 'sync'",
                req.job_type
            ),
        ));
    }

    let job_type = JobType::parse(&req.job_type);
    let request_fingerprint = match idempotency_key.as_deref() {
        Some(_) => Some(create_job_request_fingerprint(&req, job_type)?),
        None => None,
    };
    let mut reused_existing_job = false;
    let job_id = if let (Some(idempotency_key), Some(request_fingerprint)) =
        (idempotency_key.as_deref(), request_fingerprint.as_deref())
    {
        let candidate_job_id = uuid::Uuid::new_v4().to_string();
        let record = state
            .scheduler
            .reserve_create_job_idempotency_key(
                idempotency_key,
                request_fingerprint,
                &candidate_job_id,
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to reserve idempotency key: {}", e),
                )
            })?;
        if record.request_fingerprint != request_fingerprint {
            return Err((
                StatusCode::CONFLICT,
                format!(
                    "Idempotency key '{}' is already used for a different create_job request",
                    idempotency_key
                ),
            ));
        }

        reused_existing_job = state
            .scheduler
            .load_job_snapshot(&record.job_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to load idempotent job: {}", e),
                )
            })?
            .is_some();
        record.job_id
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    info!(
        operation = "http.job_create_received",
        job_id = %job_id,
        source = %req.source_path,
        destination = %req.dest_path,
        region = %req.region,
        job_type = job_type.as_str(),
        idempotency_key_present = idempotency_key.is_some(),
        "job create request received"
    );

    if reused_existing_job {
        info!(
            operation = "http.job_create_idempotent",
            job_id = %job_id,
            "existing job returned for idempotent create request"
        );
        return Ok(Json(CreateJobResponse { job_id }));
    }

    let sync_job = hardata_app::application::sync::engine::job::SyncJob::new(
        job_id.clone(),
        std::path::PathBuf::from(&req.source_path),
        req.dest_path.clone(),
        req.region,
    )
    .with_priority(req.priority)
    .with_filters(req.exclude_regex, req.include_regex)
    .with_job_type(job_type);

    match state.scheduler.submit_job(sync_job).await {
        Ok(_) => {
            info!(
                operation = "http.job_create_succeeded",
                job_id = %job_id,
                "job create request succeeded"
            );
            Ok(Json(CreateJobResponse { job_id }))
        }
        Err(e) => {
            if idempotency_key.is_some()
                && state
                    .scheduler
                    .load_job_snapshot(&job_id)
                    .await
                    .map_err(|load_error| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load idempotent job after submit: {}", load_error),
                        )
                    })?
                    .is_some()
            {
                info!(
                    operation = "http.job_create_idempotent",
                    job_id = %job_id,
                    reason = "concurrent_submission",
                    "concurrently created job returned"
                );
                return Ok(Json(CreateJobResponse { job_id }));
            }
            let status = map_create_job_error_status(&e);
            error!(
                operation = "http.job_create_failed",
                job_id = %job_id,
                status = status.as_u16(),
                error = %e,
                "job create request failed"
            );
            Err((status, format!("Failed to submit job: {}", e)))
        }
    }
}

pub async fn get_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<JobStatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let public_job_id = resolve_public_job_id(&job_id)?;
    let snapshot = state
        .scheduler
        .load_job_snapshot(public_job_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load job: {}", e),
            )
        })?;
    let runtime = state.scheduler.get_job_info(public_job_id);

    match snapshot {
        Some(job) => {
            let final_job_id = job
                .job_type
                .is_sync()
                .then(|| internal_final_job_id(&job.job_id));
            let runtime_status = state.scheduler.get_job_status(public_job_id);
            let final_runtime = final_job_id
                .as_deref()
                .and_then(|job_id| state.scheduler.get_job_info(job_id));
            let final_runtime_status = final_job_id
                .as_deref()
                .and_then(|job_id| state.scheduler.get_job_status(job_id));
            let mut snapshot_statuses = HashMap::from([(job.job_id.clone(), job.status)]);
            let retryable_job_ids = if job.job_type.is_sync() {
                state
                    .scheduler
                    .load_retryable_job_ids()
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load retryable jobs: {}", e),
                        )
                    })?
            } else {
                HashSet::new()
            };
            if let Some(final_job_id) = final_job_id.as_deref() {
                let related_statuses = state
                    .scheduler
                    .load_resolved_job_statuses(&[final_job_id.to_string()])
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load related job statuses: {}", e),
                        )
                    })?;
                snapshot_statuses.extend(related_statuses);
            }
            let (round_id, is_last_round, priority) =
                resolve_public_job_runtime_fields(&job, runtime.as_ref(), final_runtime.as_ref());
            let status_view = resolve_public_job_status_view(
                &job,
                runtime_status.as_ref(),
                final_runtime_status.as_ref(),
            );
            let projected_job = project_public_job_snapshot(&job, &status_view);
            let can_finalize =
                can_finalize_job_from_snapshot(&job, &snapshot_statuses, &retryable_job_ids);
            Ok(Json(JobStatusResponse {
                job_id: job.job_id,
                status: status_view.status.as_str().to_string(),
                error_message: status_view.error_message,
                can_cancel: state.scheduler.can_cancel_job_from_snapshot(
                    &projected_job,
                    &snapshot_statuses,
                    &retryable_job_ids,
                ),
                can_finalize,
                progress: status_view.progress,
                current_size: status_view.current_size,
                total_size: status_view.total_size,
                source: JobPath {
                    path: job.source.path,
                    client_id: if job.source.client_id.is_empty() {
                        None
                    } else {
                        Some(job.source.client_id)
                    },
                },
                dest: JobPath {
                    path: job.dest.path,
                    client_id: if job.dest.client_id.is_empty() {
                        None
                    } else {
                        Some(job.dest.client_id)
                    },
                },
                region: job.region,
                job_type: job.job_type.as_str().to_string(),
                round_id,
                is_last_round,
                priority,
                created_at: job.created_at.to_rfc3339(),
                updated_at: status_view.updated_at.to_rfc3339(),
            }))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            format!("Job {} not found", public_job_id),
        )),
    }
}

pub async fn finalize_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<CreateJobResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let job_id = resolve_public_job_id(&job_id)?.to_string();
    info!(
        operation = "http.job_finalize_received",
        job_id = %job_id,
        "job finalize request received"
    );

    match state.scheduler.finalize_job(&job_id).await {
        Ok(_) => {
            let final_job_id = internal_final_job_id(&job_id);
            info!(
                operation = "http.job_finalize_succeeded",
                job_id = %job_id,
                final_job_id = %final_job_id,
                "job finalize request succeeded"
            );
            Ok(Json(CreateJobResponse { job_id }))
        }
        Err(e) => {
            error!(
                operation = "http.job_finalize_failed",
                job_id = %job_id,
                error = %e,
                "job finalize request failed"
            );
            Err((
                map_finalize_error_status(&e),
                format!("Failed to finalize job: {}", e),
            ))
        }
    }
}

pub async fn cancel_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<(), (StatusCode, String)> {
    authorize(&headers, &state)?;
    let public_job_id = resolve_public_job_id(&job_id)?.to_string();
    info!(
        operation = "http.job_cancel_received",
        job_id = %public_job_id,
        requested_job_id = %job_id,
        "job cancel request received"
    );

    match state.scheduler.cancel_job(&public_job_id).await {
        Ok(_) => {
            info!(
                operation = "http.job_cancel_succeeded",
                job_id = %public_job_id,
                "job cancel request succeeded"
            );
            Ok(())
        }
        Err(e) => {
            error!(
                operation = "http.job_cancel_failed",
                job_id = %public_job_id,
                error = %e,
                "job cancel request failed"
            );
            Err((
                map_cancel_error_status(&e),
                format!("Failed to cancel job: {}", e),
            ))
        }
    }
}
