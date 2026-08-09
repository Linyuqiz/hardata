fn is_stale_generation(
    state_generations: &DashMap<String, u64>,
    job_id: &str,
    generation: u64,
) -> bool {
    state_generations
        .get(job_id)
        .map(|current| generation < *current)
        .unwrap_or(false)
}
