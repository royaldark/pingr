//! The wall clock, as a seam for tests.
//!
//! Elapsed time is measured separately, on the monotonic clock. Comparing the
//! two is how the collector notices a suspend or a clock change.

use std::time::SystemTime;

use crate::time::Timestamp;

/// A source of wall-clock time.
pub trait WallClock: Send + Sync {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// The operating system's clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl WallClock for SystemClock {
    fn now(&self) -> Timestamp {
        SystemTime::now().into()
    }
}
