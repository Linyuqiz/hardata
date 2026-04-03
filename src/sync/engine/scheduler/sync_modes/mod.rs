pub mod concurrent;
pub mod files;
pub mod sequential;
pub mod single;

use crate::sync::engine::scheduler::retry::ErrorCategory;
use crate::util::error::HarDataError;

fn category_rank(category: ErrorCategory) -> u8 {
    match category {
        ErrorCategory::Fatal => 4,
        ErrorCategory::Transient => 3,
        ErrorCategory::Retriable => 2,
        ErrorCategory::Unknown => 1,
    }
}

pub(super) fn update_representative_failure(
    representative: &mut Option<(ErrorCategory, String)>,
    category: ErrorCategory,
    message: String,
) {
    let should_replace = representative
        .as_ref()
        .map(|(current, _)| category_rank(category) > category_rank(*current))
        .unwrap_or(true);

    if should_replace {
        *representative = Some((category, message));
    }
}

pub(super) fn aggregate_failed_sync_error(
    failed_count: usize,
    representative: Option<(ErrorCategory, String)>,
) -> HarDataError {
    let (category, message) =
        representative.unwrap_or((ErrorCategory::Unknown, "unknown sync failure".to_string()));
    let summary = format!("{} files failed: {}", failed_count, message);

    match category {
        ErrorCategory::Transient => HarDataError::NetworkError(summary),
        ErrorCategory::Retriable => HarDataError::ConnectionError(summary),
        ErrorCategory::Fatal => HarDataError::Unknown(format!("[fatal] {}", summary)),
        ErrorCategory::Unknown => HarDataError::Unknown(summary),
    }
}

pub(super) fn calculate_progress(current_size: u64, total_size: u64) -> u8 {
    if total_size == 0 {
        return 0;
    }

    ((current_size.min(total_size) as f64 / total_size as f64) * 100.0).min(100.0) as u8
}

#[cfg(test)]
mod tests {
    use super::calculate_progress;

    #[test]
    fn calculate_progress_is_zero_when_total_size_is_zero() {
        assert_eq!(calculate_progress(0, 0), 0);
        assert_eq!(calculate_progress(1024, 0), 0);
    }

    #[test]
    fn calculate_progress_clamps_to_total_size() {
        assert_eq!(calculate_progress(5, 10), 50);
        assert_eq!(calculate_progress(15, 10), 100);
    }
}
