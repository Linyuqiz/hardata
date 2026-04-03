use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const NANOS_PER_SEC: i64 = 1_000_000_000;
const LEGACY_TIMESTAMP_THRESHOLD: i64 = 1_000_000_000_000;

pub fn unix_timestamp_nanos(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .unwrap_or(0)
}

pub fn uses_legacy_second_precision(timestamp: i64) -> bool {
    timestamp.saturating_abs() < LEGACY_TIMESTAMP_THRESHOLD
}

pub fn timestamps_match(left: i64, right: i64) -> bool {
    if left == right {
        return true;
    }

    if uses_legacy_second_precision(left) || uses_legacy_second_precision(right) {
        return timestamp_seconds(left) == timestamp_seconds(right);
    }

    false
}

pub fn optional_timestamps_match(left: Option<i64>, right: Option<i64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => timestamps_match(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn timestamp_seconds(timestamp: i64) -> i64 {
    if uses_legacy_second_precision(timestamp) {
        timestamp
    } else {
        timestamp.div_euclid(NANOS_PER_SEC)
    }
}

pub fn metadata_mtime_nanos(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .map(unix_timestamp_nanos)
        .unwrap_or(0)
}

#[cfg(unix)]
pub fn metadata_ctime_nanos(metadata: &std::fs::Metadata) -> Option<i64> {
    use std::os::unix::fs::MetadataExt;

    let seconds = i128::from(metadata.ctime());
    let nanos = i128::from(metadata.ctime_nsec());
    let total = seconds
        .checked_mul(i128::from(NANOS_PER_SEC))?
        .checked_add(nanos)?;
    i64::try_from(total).ok()
}

#[cfg(not(unix))]
pub fn metadata_ctime_nanos(_metadata: &std::fs::Metadata) -> Option<i64> {
    None
}

#[cfg(unix)]
pub fn metadata_inode(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;

    Some(metadata.ino())
}

#[cfg(not(unix))]
pub fn metadata_inode(_metadata: &std::fs::Metadata) -> Option<u64> {
    None
}

pub fn unix_timestamp_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

pub fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        optional_timestamps_match, timestamps_match, unix_timestamp_millis,
        uses_legacy_second_precision,
    };
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn unix_timestamp_millis_returns_zero_for_pre_epoch_time() {
        let pre_epoch = UNIX_EPOCH - Duration::from_millis(1);
        assert_eq!(unix_timestamp_millis(pre_epoch), 0);
    }

    #[test]
    fn unix_timestamp_millis_converts_regular_timestamp() {
        let timestamp = UNIX_EPOCH + Duration::from_millis(1234);
        assert_eq!(unix_timestamp_millis(timestamp), 1234);
    }

    #[test]
    fn timestamps_match_accepts_legacy_second_precision_against_nanos() {
        assert!(timestamps_match(1_710_000_000, 1_710_000_000_123_456_789));
        assert!(timestamps_match(1_710_000_000_123_456_789, 1_710_000_000));
    }

    #[test]
    fn timestamps_match_keeps_subsecond_checks_for_nanos_values() {
        assert!(!timestamps_match(
            1_710_000_000_123_456_789,
            1_710_000_000_987_654_321
        ));
    }

    #[test]
    fn optional_timestamps_match_requires_same_presence() {
        assert!(optional_timestamps_match(
            Some(1_710_000_000),
            Some(1_710_000_000_123_456_789)
        ));
        assert!(!optional_timestamps_match(Some(1_710_000_000), None));
        assert!(!optional_timestamps_match(None, Some(1_710_000_000)));
        assert!(optional_timestamps_match(None, None));
    }

    #[test]
    fn uses_legacy_second_precision_only_flags_small_epoch_values() {
        assert!(uses_legacy_second_precision(1_710_000_000));
        assert!(!uses_legacy_second_precision(1_710_000_000_000_000_000));
    }
}
