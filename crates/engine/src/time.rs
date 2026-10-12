//! Points in time as pingr stores them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A wall-clock instant: UTC microseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The instant `micros` microseconds after the Unix epoch.
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }

    /// Microseconds since the Unix epoch.
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// This instant moved later by `duration`.
    #[must_use]
    pub fn plus(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(micros(duration)))
    }

    /// This instant moved earlier by `duration`.
    #[must_use]
    pub fn minus(self, duration: Duration) -> Self {
        Self(self.0.saturating_sub(micros(duration)))
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(after) => Self(micros(after)),
            Err(before) => Self(-micros(before.duration())),
        }
    }
}

/// `duration` in whole microseconds, saturating far beyond any real timestamp.
pub(crate) fn micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

/// A span of time: from `since` up to but not including `until`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    /// First instant inside the range.
    pub since: Timestamp,
    /// First instant after the range.
    pub until: Timestamp,
}
