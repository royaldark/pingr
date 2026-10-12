//! The store's contract, exercised against real `SQLite` files.

use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use pingr_engine::store::{
    Collecting, EndReason, Loss, NewSample, Outcome, SessionId, SessionSettings, Store, StoreError,
    TargetId,
};
use pingr_engine::time::{TimeRange, Timestamp};
use tempfile::TempDir;

const SETTINGS: SessionSettings = SessionSettings {
    interval: Duration::from_secs(1),
    timeout: Duration::from_secs(2),
};

const EVERYTHING: TimeRange = TimeRange {
    since: Timestamp::from_micros(i64::MIN),
    until: Timestamp::from_micros(i64::MAX),
};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_micros(seconds * 1_000_000)
}

fn range(since: i64, until: i64) -> TimeRange {
    TimeRange {
        since: at(since),
        until: at(until),
    }
}

const REPLY: Outcome = Outcome::Reply {
    rtt: Duration::from_micros(12_345),
};

/// A temp directory and the path of a store inside it.
fn scratch() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("pingr.db");
    (dir, path)
}

/// A collector on a fresh store, one target, one open session started at 0 s.
fn collecting(path: &std::path::Path) -> (Collecting, TargetId, SessionId) {
    let mut collecting = Store::create_or_open(path)
        .expect("create store")
        .collect()
        .expect("claim store");
    let target = collecting.target("example.com").expect("target");
    let session = collecting
        .start_session(at(0), SETTINGS, &[target])
        .expect("session");
    (collecting, target, session)
}

fn sample(target: TargetId, sent: i64, seq: u32, outcome: Outcome) -> NewSample {
    NewSample {
        target,
        sent: at(sent),
        seq,
        address: None,
        outcome,
    }
}

#[test]
fn an_unanswered_probe_is_stored_as_lost_not_omitted() {
    let (_dir, path) = scratch();
    let (mut collecting, target, session) = collecting(&path);
    let ip: IpAddr = "192.0.2.7".parse().expect("literal address");
    let address = collecting.address(target, ip).expect("address");

    let answered = NewSample {
        address: Some(address),
        ..sample(target, 1, 0, REPLY)
    };
    let lost = sample(target, 2, 1, Outcome::Lost(Loss::Timeout));
    collecting
        .record(session, at(2), &[answered, lost])
        .expect("record");

    let stored = collecting
        .store()
        .samples("example.com", EVERYTHING)
        .expect("read");
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].outcome, REPLY);
    assert_eq!(stored[0].address, Some(ip));
    assert_eq!(stored[1].outcome, Outcome::Lost(Loss::Timeout));
    assert_eq!(stored[1].address, None);
}

#[test]
fn a_reader_sees_history_grow_while_the_collector_writes() {
    let (_dir, path) = scratch();
    let (mut collecting, target, session) = collecting(&path);
    collecting
        .record(session, at(1), &[sample(target, 1, 0, REPLY)])
        .expect("first batch");

    let reader = Store::open(&path).expect("reader opens a store in use");
    assert_eq!(
        reader
            .samples("example.com", EVERYTHING)
            .expect("read")
            .len(),
        1
    );

    collecting
        .record(session, at(2), &[sample(target, 2, 1, REPLY)])
        .expect("second batch, with a reader attached");
    assert_eq!(
        reader
            .samples("example.com", EVERYTHING)
            .expect("read")
            .len(),
        2
    );
}

#[test]
fn a_second_collector_on_the_same_store_is_refused() {
    let (_dir, path) = scratch();
    let (first, ..) = collecting(&path);

    let second = Store::open(&path).expect("open").collect();
    assert!(matches!(second, Err(StoreError::AlreadyCollecting(_))));
    assert!(
        Store::open(&path)
            .expect("open")
            .is_collecting()
            .expect("status")
    );

    drop(first);
    assert!(
        !Store::open(&path)
            .expect("open")
            .is_collecting()
            .expect("status")
    );
    Store::open(&path)
        .expect("open")
        .collect()
        .expect("claim is free again once the first collector is gone");
}

#[test]
fn a_time_range_includes_its_start_and_excludes_its_end() {
    let (_dir, path) = scratch();
    let (mut collecting, target, session) = collecting(&path);
    let samples: Vec<_> = (0..5)
        .map(|second| sample(target, 10 + i64::from(second), second, REPLY))
        .collect();
    collecting
        .record(session, at(14), &samples)
        .expect("record");

    let sent: Vec<_> = collecting
        .store()
        .samples("example.com", range(11, 13))
        .expect("read")
        .into_iter()
        .map(|sample| sample.sent)
        .collect();
    assert_eq!(sent, [at(11), at(12)]);
}

#[test]
fn an_unknown_target_is_an_error_not_an_empty_result() {
    let (_dir, path) = scratch();
    let (collecting, ..) = collecting(&path);

    let result = collecting.store().samples("nowhere.invalid", EVERYTHING);
    assert!(matches!(result, Err(StoreError::UnknownTarget(name)) if name == "nowhere.invalid"));
}

#[test]
fn prune_removes_only_what_is_older_than_the_cutoff_while_collecting_continues() {
    let (_dir, path) = scratch();
    let (mut collecting, target, session) = collecting(&path);
    let samples: Vec<_> = (0..4)
        .map(|second| sample(target, 10 + i64::from(second), second, REPLY))
        .collect();
    collecting
        .record(session, at(13), &samples)
        .expect("record");

    let pruner = Store::open(&path).expect("open");
    assert_eq!(pruner.count_before(at(12)).expect("count"), 2);
    assert_eq!(pruner.prune(at(12)).expect("prune"), 2);

    collecting
        .record(session, at(14), &[sample(target, 14, 4, REPLY)])
        .expect("the collector keeps recording after a prune");
    let sent: Vec<_> = pruner
        .samples("example.com", EVERYTHING)
        .expect("read")
        .into_iter()
        .map(|sample| sample.sent)
        .collect();
    assert_eq!(sent, [at(12), at(13), at(14)]);
}

#[test]
fn prune_forgets_sessions_that_lie_entirely_before_the_cutoff() {
    let (_dir, path) = scratch();
    let (mut collecting, target, old) = collecting(&path);
    collecting
        .record(old, at(5), &[sample(target, 5, 0, REPLY)])
        .expect("record");
    collecting
        .end_session(old, at(6), EndReason::Stopped)
        .expect("end");
    let current = collecting
        .start_session(at(100), SETTINGS, &[target])
        .expect("session");

    collecting.store().prune(at(50)).expect("prune");

    let sessions = collecting
        .store()
        .sessions("example.com", EVERYTHING)
        .expect("sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].id, current);
}

#[test]
fn a_wall_clock_stepping_backwards_loses_no_samples() {
    let (_dir, path) = scratch();
    let (mut collecting, target, first) = collecting(&path);

    // Within one session the clock repeats an instant…
    collecting
        .record(
            first,
            at(10),
            &[sample(target, 10, 0, REPLY), sample(target, 10, 1, REPLY)],
        )
        .expect("same instant twice in one session");
    // …and a later session lands on that instant again.
    collecting
        .end_session(first, at(10), EndReason::ClockJump)
        .expect("end");
    let second = collecting
        .start_session(at(10), SETTINGS, &[target])
        .expect("session");
    collecting
        .record(second, at(10), &[sample(target, 10, 0, REPLY)])
        .expect("same instant again in a new session");

    let stored = collecting
        .store()
        .samples("example.com", EVERYTHING)
        .expect("read");
    assert_eq!(stored.len(), 3);
}

#[test]
fn sessions_report_when_the_collector_was_and_was_not_running() {
    let (_dir, path) = scratch();
    let (mut collecting, target, first) = collecting(&path);
    collecting
        .record(first, at(30), &[sample(target, 30, 0, REPLY)])
        .expect("record");
    // The first run dies without a clean stop; a second starts later.
    let second = collecting
        .start_session(at(100), SETTINGS, &[target])
        .expect("session");
    collecting
        .end_session(second, at(130), EndReason::Stopped)
        .expect("end");
    let store = collecting.store();

    let all = store.sessions("example.com", EVERYTHING).expect("sessions");
    assert_eq!(all.len(), 2);
    assert_eq!((all[0].started, all[0].last_alive), (at(0), at(30)));
    assert_eq!(all[0].ended, None);
    assert_eq!(all[1].ended, Some((at(130), EndReason::Stopped)));

    // A range that falls in the gap between the two runs overlaps neither.
    let gap = store
        .sessions("example.com", range(40, 90))
        .expect("sessions");
    assert_eq!(gap, []);
}

#[test]
fn a_fresh_store_is_at_the_current_schema_and_reopens_with_its_data() {
    let (_dir, path) = scratch();
    {
        let (mut collecting, target, session) = collecting(&path);
        collecting
            .record(session, at(1), &[sample(target, 1, 0, REPLY)])
            .expect("record");
    }

    let reopened = Store::create_or_open(&path).expect("reopen");
    assert_eq!(reopened.schema_version().expect("version"), 1);
    assert_eq!(
        reopened
            .samples("example.com", EVERYTHING)
            .expect("read")
            .len(),
        1
    );
}

#[test]
fn a_store_from_a_newer_pingr_is_refused_untouched() {
    let (_dir, path) = scratch();
    drop(Store::create_or_open(&path).expect("create"));
    let raw = rusqlite::Connection::open(&path).expect("raw connection");
    raw.pragma_update(None, "user_version", 99)
        .expect("bump version");
    drop(raw);

    let result = Store::open(&path);
    assert!(matches!(
        result,
        Err(StoreError::SchemaTooNew {
            found: 99,
            supported: 1
        })
    ));
    let raw = rusqlite::Connection::open(&path).expect("raw connection");
    let version: u32 = raw
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("version");
    assert_eq!(version, 99);
}

#[test]
fn reading_a_store_that_does_not_exist_does_not_create_one() {
    let (_dir, path) = scratch();

    assert!(matches!(Store::open(&path), Err(StoreError::NotFound(_))));
    assert!(!path.exists());
}
