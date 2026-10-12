//! Statistics over a run of samples.

use std::time::Duration;

use crate::store::Outcome;

/// What a set of probes says about a connection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stats {
    /// Probes sent.
    pub sent: u64,
    /// Probes answered.
    pub received: u64,
    /// Round-trip times of the answered probes, or `None` if none were.
    pub rtt: Option<RttStats>,
}

impl Stats {
    /// Probes that went unanswered.
    pub fn lost(&self) -> u64 {
        self.sent - self.received
    }

    /// The fraction of probes lost, from 0 to 1, or `None` if none were sent.
    pub fn loss(&self) -> Option<f64> {
        (self.sent > 0).then(|| ratio(self.lost(), self.sent))
    }
}

/// The distribution of round-trip times among answered probes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RttStats {
    /// Fastest answer.
    pub min: Duration,
    /// Arithmetic mean.
    pub mean: Duration,
    /// Middle value; with an even count, the mean of the middle two.
    pub median: Duration,
    /// Slowest answer.
    pub max: Duration,
    /// Population standard deviation.
    pub stddev: Duration,
    /// 90th percentile (nearest rank: always an observed value).
    pub p90: Duration,
    /// 95th percentile (nearest rank).
    pub p95: Duration,
    /// 99th percentile (nearest rank).
    pub p99: Duration,
    /// Mean absolute difference between each answer and the previous one, in
    /// the order sent. Lost probes in between are skipped over. `None` with
    /// fewer than two answers.
    pub jitter: Option<Duration>,
}

/// Summarizes `outcomes`, which must be in the order the probes were sent.
pub fn summarize(outcomes: impl IntoIterator<Item = Outcome>) -> Stats {
    let mut sent = 0;
    let mut rtts = Vec::new();
    for outcome in outcomes {
        sent += 1;
        if let Outcome::Reply { rtt } = outcome {
            rtts.push(rtt.as_nanos());
        }
    }
    Stats {
        sent,
        received: rtts.len() as u64,
        rtt: rtt_stats(&rtts),
    }
}

/// Statistics over round-trip times in nanoseconds, in the order sent.
fn rtt_stats(in_order: &[u128]) -> Option<RttStats> {
    let count = in_order.len();
    let (_, rest) = in_order.split_first()?;

    let jitter = (!rest.is_empty()).then(|| {
        let total: u128 = in_order.windows(2).map(|w| w[0].abs_diff(w[1])).sum();
        from_nanos(total / rest.len() as u128)
    });

    let mut sorted = in_order.to_vec();
    sorted.sort_unstable();
    let total: u128 = sorted.iter().sum();
    let mean = total / count as u128;
    let median = u128::midpoint(sorted[(count - 1) / 2], sorted[count / 2]);
    // Nearest rank: the smallest value with at least `percent`% of the
    // answers at or below it.
    let percentile = |percent: usize| from_nanos(sorted[(count * percent).div_ceil(100) - 1]);

    Some(RttStats {
        min: from_nanos(sorted[0]),
        mean: from_nanos(mean),
        median: from_nanos(median),
        max: from_nanos(sorted[count - 1]),
        stddev: stddev(&sorted, total),
        p90: percentile(90),
        p95: percentile(95),
        p99: percentile(99),
        jitter,
    })
}

#[expect(
    clippy::cast_precision_loss,
    reason = "round-trip times in nanoseconds are far below 2^53"
)]
fn stddev(nanos: &[u128], total: u128) -> Duration {
    let count = nanos.len() as f64;
    let mean = total as f64 / count;
    let variance = nanos
        .iter()
        .map(|&rtt| (rtt as f64 - mean).powi(2))
        .sum::<f64>()
        / count;
    Duration::from_secs_f64(variance.sqrt() / 1e9)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "probe counts are far below 2^53"
)]
fn ratio(part: u64, whole: u64) -> f64 {
    part as f64 / whole as f64
}

fn from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}
