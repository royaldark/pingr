//! How long statistics over a month of one-second samples take.
//!
//! Not a pass/fail test: run it with `mise run bench` and read the numbers.

use std::time::{Duration, Instant};

use pingr_engine::stats::summarize;
use pingr_engine::store::{Loss, NewSample, Outcome, SessionSettings, Store};
use pingr_engine::time::{TimeRange, Timestamp};

const TARGETS: u32 = 10;
const DAYS: i64 = 30;
const SECONDS: i64 = DAYS * 86_400;

#[test]
#[ignore = "a benchmark: writes about a gigabyte and takes minutes"]
fn stats_over_thirty_days_of_one_second_samples() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("pingr.db");
    let mut collecting = Store::create_or_open(&path)
        .expect("create")
        .collect()
        .expect("claim");
    let targets: Vec<_> = (0..TARGETS)
        .map(|n| collecting.target(&format!("host{n}.test")).expect("target"))
        .collect();
    let settings = SessionSettings {
        interval: Duration::from_secs(1),
        timeout: Duration::from_secs(2),
    };
    let start = Timestamp::from_micros(1_700_000_000_000_000);
    let session = collecting
        .start_session(start, settings, &targets)
        .expect("session");

    let began = Instant::now();
    let mut batch = Vec::new();
    for second in 0..SECONDS {
        let sent = start.plus(Duration::from_secs(second.unsigned_abs()));
        for (n, &target) in targets.iter().enumerate() {
            // A cheap, repeatable spread of latencies with about 1% loss.
            let noise = (second.unsigned_abs() * 2_654_435_761 + n as u64 * 40_503) % 10_000;
            let outcome = if noise < 100 {
                Outcome::Lost(Loss::Timeout)
            } else {
                Outcome::Reply {
                    rtt: Duration::from_micros(8_000 + noise),
                }
            };
            batch.push(NewSample {
                session,
                target,
                sent,
                seq: u32::try_from(second).expect("fits"),
                address: None,
                outcome,
            });
        }
        if batch.len() >= 100_000 {
            collecting.record(session, sent, &batch).expect("record");
            batch.clear();
        }
    }
    collecting
        .record(
            session,
            start.plus(Duration::from_secs(SECONDS.unsigned_abs())),
            &batch,
        )
        .expect("record");
    let store = collecting.store();
    println!(
        "wrote {} samples in {:.0?}; store is {} MB",
        i64::from(TARGETS) * SECONDS,
        began.elapsed(),
        store.size_bytes().expect("size") / 1_000_000
    );

    for (label, days) in [("1 day", 1), ("7 days", 7), ("30 days", DAYS)] {
        let range = TimeRange {
            since: start,
            until: start.plus(Duration::from_secs((days * 86_400).unsigned_abs())),
        };
        let began = Instant::now();
        let samples = store.samples("host3.test", range).expect("samples");
        let read = began.elapsed();
        let stats = summarize(samples.iter().map(|sample| sample.outcome));
        println!(
            "stats over {label}: {} samples, read {read:.0?}, total {:.0?} (loss {:.2}%)",
            stats.sent,
            began.elapsed(),
            stats.loss().unwrap_or(0.0) * 100.0
        );
    }
}
