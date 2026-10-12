//! Statistics checked against values worked out by hand.

use std::time::Duration;

use pingr_engine::stats::summarize;
use pingr_engine::store::{Loss, Outcome};

const LOST: Outcome = Outcome::Lost(Loss::Timeout);

fn reply(millis: u64) -> Outcome {
    Outcome::Reply {
        rtt: Duration::from_millis(millis),
    }
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

#[test]
fn a_mixed_run_matches_hand_computed_values() {
    // Sent in this order; sorted answers are 10, 20, 30, 40 ms.
    let stats = summarize([reply(10), reply(20), LOST, reply(40), reply(30)]);

    assert_eq!((stats.sent, stats.received, stats.lost()), (5, 4, 1));
    assert_eq!(stats.loss(), Some(0.2));
    let rtt = stats.rtt.expect("answers exist");
    assert_eq!(rtt.min, ms(10));
    assert_eq!(rtt.max, ms(40));
    assert_eq!(rtt.mean, ms(25));
    assert_eq!(rtt.median, ms(25));
    // Population variance of 10, 20, 30, 40 is 125 ms², so σ = √125 ms.
    let expected = Duration::from_secs_f64(125f64.sqrt() / 1e3);
    assert!(rtt.stddev.abs_diff(expected) < Duration::from_nanos(10));
    // Nearest rank with four answers: every tail percentile is the slowest.
    assert_eq!((rtt.p90, rtt.p95, rtt.p99), (ms(40), ms(40), ms(40)));
    // Steps in send order, skipping the loss: 10, 20, 10 → mean 13⅓ ms.
    assert_eq!(rtt.jitter, Some(Duration::from_nanos(13_333_333)));
}

#[test]
fn percentiles_pick_the_nearest_rank() {
    // 1 ms, 2 ms, … 100 ms, sent slowest first.
    let stats = summarize((1..=100).rev().map(reply));

    let rtt = stats.rtt.expect("answers exist");
    assert_eq!((rtt.p90, rtt.p95, rtt.p99), (ms(90), ms(95), ms(99)));
    assert_eq!(rtt.median, Duration::from_micros(50_500));
    assert_eq!(rtt.jitter, Some(ms(1)));
}

#[test]
fn nothing_sent_has_no_loss_figure() {
    let stats = summarize([]);

    assert_eq!((stats.sent, stats.received), (0, 0));
    assert_eq!(stats.loss(), None);
    assert_eq!(stats.rtt, None);
}

#[test]
fn everything_lost_is_total_loss_with_no_timings() {
    let stats = summarize([LOST, LOST, LOST]);

    assert_eq!(stats.loss(), Some(1.0));
    assert_eq!(stats.rtt, None);
}

#[test]
fn a_single_answer_is_every_statistic_and_has_no_jitter() {
    let stats = summarize([reply(7)]);

    let rtt = stats.rtt.expect("an answer exists");
    assert_eq!(
        [rtt.min, rtt.mean, rtt.median, rtt.max, rtt.p90, rtt.p99],
        [ms(7); 6]
    );
    assert_eq!(rtt.stddev, Duration::ZERO);
    assert_eq!(rtt.jitter, None);
}
