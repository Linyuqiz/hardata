use crate::api::{ApiError, JobDetail};
use std::collections::HashSet;

pub(crate) fn cancel_action_label(is_cancelling: bool) -> &'static str {
    if is_cancelling {
        "Cancelling"
    } else {
        "Cancel"
    }
}

pub(crate) fn finalize_action_label(is_finalizing: bool) -> &'static str {
    if is_finalizing {
        "Finalizing"
    } else {
        "Finalize"
    }
}

pub(crate) fn format_cancel_error(job_id: &str, error: &ApiError) -> String {
    if error.is_unauthorized() {
        format!(
            "Failed to cancel job {}: authentication failed, update API token",
            job_id
        )
    } else {
        format!("Failed to cancel job {}: {}", job_id, error)
    }
}

pub(crate) fn format_finalize_error(job_id: &str, error: &ApiError) -> String {
    if error.is_unauthorized() {
        format!(
            "Failed to finalize job {}: authentication failed, update API token",
            job_id
        )
    } else {
        format!("Failed to finalize job {}: {}", job_id, error)
    }
}

pub(crate) fn prune_cancelling_job_ids(
    cancelling_job_ids: &HashSet<String>,
    jobs: &[JobDetail],
) -> HashSet<String> {
    cancelling_job_ids
        .iter()
        .filter(|job_id| {
            !jobs
                .iter()
                .any(|job| job.job_id == **job_id && !job.can_cancel)
        })
        .cloned()
        .collect()
}

pub(crate) fn prune_finalizing_job_ids(
    finalizing_job_ids: &HashSet<String>,
    jobs: &[JobDetail],
) -> HashSet<String> {
    finalizing_job_ids
        .iter()
        .filter(|job_id| {
            !jobs
                .iter()
                .any(|job| job.job_id == **job_id && !job.can_finalize)
        })
        .cloned()
        .collect()
}

pub(crate) fn should_clear_cancel_error(job_id: &str, jobs: &[JobDetail]) -> bool {
    jobs.iter()
        .any(|job| job.job_id == job_id && !job.can_cancel)
}

pub(crate) fn should_clear_finalize_error(job_id: &str, jobs: &[JobDetail]) -> bool {
    jobs.iter()
        .any(|job| job.job_id == job_id && !job.can_finalize)
}

#[cfg(test)]
mod tests {
    use super::{
        cancel_action_label, finalize_action_label, format_cancel_error, format_finalize_error,
        prune_cancelling_job_ids, prune_finalizing_job_ids, should_clear_cancel_error,
        should_clear_finalize_error,
    };
    use crate::api::{ApiError, JobDetail, JobPath};
    use std::collections::HashSet;

    fn sample_job() -> JobDetail {
        JobDetail {
            job_id: "job-1".to_string(),
            status: "pending".to_string(),
            error_message: None,
            can_cancel: true,
            can_finalize: true,
            progress: 0,
            current_size: 0,
            total_size: 0,
            source: JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            dest: JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
            region: "local".to_string(),
            job_type: "sync".to_string(),
            round_id: 0,
            is_last_round: false,
            priority: 1,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn cancel_action_label_marks_in_flight_cancellations() {
        assert_eq!(cancel_action_label(false), "Cancel");
        assert_eq!(cancel_action_label(true), "Cancelling");
    }

    #[test]
    fn finalize_action_label_marks_in_flight_finalizations() {
        assert_eq!(finalize_action_label(false), "Finalize");
        assert_eq!(finalize_action_label(true), "Finalizing");
    }

    #[test]
    fn prune_cancelling_job_ids_keeps_hidden_and_visible_cancellable_jobs() {
        let mut active_job = sample_job();
        active_job.job_id = "job-active".to_string();

        let mut terminal_job = sample_job();
        terminal_job.job_id = "job-terminal".to_string();
        terminal_job.can_cancel = false;

        let cancelling_job_ids = HashSet::from([
            "job-active".to_string(),
            "job-terminal".to_string(),
            "job-missing".to_string(),
        ]);

        assert_eq!(
            prune_cancelling_job_ids(&cancelling_job_ids, &[active_job, terminal_job]),
            HashSet::from(["job-active".to_string(), "job-missing".to_string(),])
        );
    }

    #[test]
    fn should_clear_cancel_error_only_when_job_is_visible_and_terminal() {
        let mut active_job = sample_job();
        active_job.job_id = "job-active".to_string();

        let mut terminal_job = sample_job();
        terminal_job.job_id = "job-terminal".to_string();
        terminal_job.can_cancel = false;

        assert!(!should_clear_cancel_error(
            "job-active",
            &[active_job.clone()]
        ));
        assert!(should_clear_cancel_error("job-terminal", &[terminal_job]));
        assert!(!should_clear_cancel_error("job-missing", &[active_job]));
    }

    #[test]
    fn prune_finalizing_job_ids_keeps_hidden_and_visible_finalizable_jobs() {
        let mut active_job = sample_job();
        active_job.job_id = "job-active".to_string();

        let mut terminal_job = sample_job();
        terminal_job.job_id = "job-terminal".to_string();
        terminal_job.can_finalize = false;

        let finalizing_job_ids = HashSet::from([
            "job-active".to_string(),
            "job-terminal".to_string(),
            "job-missing".to_string(),
        ]);

        assert_eq!(
            prune_finalizing_job_ids(&finalizing_job_ids, &[active_job, terminal_job]),
            HashSet::from(["job-active".to_string(), "job-missing".to_string(),])
        );
    }

    #[test]
    fn should_clear_finalize_error_only_when_job_is_visible_and_non_finalizable() {
        let mut active_job = sample_job();
        active_job.job_id = "job-active".to_string();

        let mut terminal_job = sample_job();
        terminal_job.job_id = "job-terminal".to_string();
        terminal_job.can_finalize = false;

        assert!(!should_clear_finalize_error(
            "job-active",
            &[active_job.clone()]
        ));
        assert!(should_clear_finalize_error("job-terminal", &[terminal_job]));
        assert!(!should_clear_finalize_error("job-missing", &[active_job]));
    }

    #[test]
    fn format_cancel_error_uses_auth_specific_guidance() {
        let message = format_cancel_error("job-1", &ApiError::http(401, "HTTP 401"));

        assert!(message.contains("job-1"));
        assert!(message.contains("authentication failed"));
        assert!(message.contains("update API token"));
    }

    #[test]
    fn format_finalize_error_uses_auth_specific_guidance() {
        let message = format_finalize_error("job-1", &ApiError::http(401, "HTTP 401"));

        assert!(message.contains("job-1"));
        assert!(message.contains("authentication failed"));
        assert!(message.contains("update API token"));
    }
}
