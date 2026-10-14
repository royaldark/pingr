//! A target's health at a glance: quick, steady answers with next to no
//! loss, judged over a window of recent probes so that one slow or lost
//! probe does not flip the verdict, and a recovery shows only once the
//! window is clean again.

use std::time::Duration;

use pingr_engine::stats::Stats;

use super::DOWN_AFTER;

/// How a target is doing. Ordered from best to worst.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Health {
    /// Within every [`GOOD`] limit.
    Good,
    /// Within every [`FAIR`] limit.
    Fair,
    /// Worse than that, or not answering right now.
    Poor,
}

/// The most a grade allows of each measure.
pub struct Limits {
    /// Fraction of probes lost, 0 to 1.
    pub loss: f64,
    /// 95th-percentile round-trip time.
    pub p95: Duration,
    /// Mean change in round-trip time from one answer to the next.
    pub jitter: Duration,
}

pub const GOOD: Limits = Limits {
    loss: 0.02,
    p95: Duration::from_millis(100),
    jitter: Duration::from_millis(10),
};

pub const FAIR: Limits = Limits {
    loss: 0.05,
    p95: Duration::from_millis(250),
    jitter: Duration::from_millis(30),
};

impl Limits {
    fn allow(&self, stats: &Stats) -> bool {
        stats.loss().is_none_or(|loss| loss <= self.loss)
            && stats.rtt.is_none_or(|rtt| {
                rtt.p95 <= self.p95 && rtt.jitter.is_none_or(|jitter| jitter <= self.jitter)
            })
    }
}

impl Health {
    /// Judges `window`, the statistics over the latest probes, of which the
    /// most recent `losses_in_a_row` went unanswered. `None` until a probe
    /// has finished.
    pub fn judge(window: &Stats, losses_in_a_row: u32) -> Option<Self> {
        if window.sent == 0 {
            return None;
        }
        Some(if losses_in_a_row >= DOWN_AFTER {
            Self::Poor
        } else if GOOD.allow(window) {
            Self::Good
        } else if FAIR.allow(window) {
            Self::Fair
        } else {
            Self::Poor
        })
    }
}

#[cfg(test)]
mod tests {
    use pingr_engine::stats::summarize;
    use pingr_engine::store::{Loss, Outcome};

    use super::*;

    const LOST: Outcome = Outcome::Lost(Loss::Timeout);

    fn reply(millis: u64) -> Outcome {
        Outcome::Reply {
            rtt: Duration::from_millis(millis),
        }
    }

    fn judge(window: &[Outcome]) -> Option<Health> {
        let losses_in_a_row = window.iter().rev().take_while(|o| **o == LOST).count();
        Health::judge(
            &summarize(window.iter().copied()),
            u32::try_from(losses_in_a_row).expect("a short window"),
        )
    }

    /// A minute of answers that alternate between `low` and `high` ms.
    fn alternating(low: u64, high: u64) -> Vec<Outcome> {
        (0..60)
            .map(|i| reply(if i % 2 == 0 { low } else { high }))
            .collect()
    }

    #[test]
    fn nothing_finished_yet_has_no_verdict() {
        assert_eq!(judge(&[]), None);
    }

    #[test]
    fn quick_steady_answers_are_good() {
        assert_eq!(judge(&alternating(18, 22)), Some(Health::Good));
    }

    #[test]
    fn one_slow_answer_or_one_loss_in_a_minute_stays_good() {
        let mut window = alternating(18, 22);
        window[20] = reply(80);
        window[40] = LOST;

        assert_eq!(judge(&window), Some(Health::Good));
    }

    #[test]
    fn quick_but_jumpy_answers_are_fair() {
        // Every step is 20 ms: jitter 20 ms, though p95 is only 40 ms.
        assert_eq!(judge(&alternating(20, 40)), Some(Health::Fair));
    }

    #[test]
    fn steady_but_slow_answers_are_fair() {
        assert_eq!(judge(&alternating(180, 182)), Some(Health::Fair));
    }

    #[test]
    fn losing_one_probe_in_ten_is_poor() {
        let mut window = alternating(18, 22);
        for i in (5..60).step_by(10) {
            window[i] = LOST;
        }

        assert_eq!(judge(&window), Some(Health::Poor));
    }

    #[test]
    fn a_target_that_just_stopped_answering_is_poor_at_once() {
        let mut window = alternating(18, 22);
        window.extend([LOST; DOWN_AFTER as usize]);

        assert_eq!(judge(&window), Some(Health::Poor));
    }

    #[test]
    fn a_recovery_is_not_good_until_the_outage_leaves_the_window() {
        let mut window = vec![LOST; 5];
        window.extend(alternating(18, 22).into_iter().take(55));

        // Answering again, but 5 of the latest 60 were lost.
        assert_eq!(judge(&window), Some(Health::Poor));
        assert_eq!(judge(&window[5..]), Some(Health::Good));
    }
}
