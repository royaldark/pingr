//! The `pingr` binary from the outside: exit codes, messages, and JSON.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use pingr_engine::store::{EndReason, Loss, NewSample, Outcome, SessionSettings, Store};
use pingr_engine::time::Timestamp;
use serde_json::Value;
use tempfile::TempDir;

/// 2023-11-14T22:13:20Z.
const T0: i64 = 1_700_000_000;

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_micros((T0 + seconds) * 1_000_000)
}

fn scratch() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("pingr.db");
    (dir, path)
}

/// A store holding one ten-second run against `example.com`: answers of 10,
/// 20, 30, and 40 ms and one loss.
fn seeded() -> (TempDir, PathBuf) {
    let (dir, path) = scratch();
    let mut collecting = Store::create_or_open(&path)
        .expect("create")
        .collect()
        .expect("claim");
    let target = collecting.target("example.com").expect("target");
    let settings = SessionSettings {
        interval: Duration::from_secs(1),
        timeout: Duration::from_secs(2),
    };
    let session = collecting
        .start_session(at(0), settings, &[target])
        .expect("session");
    let outcomes = [Some(10), Some(20), None, Some(30), Some(40)];
    let samples: Vec<_> = (0u32..)
        .zip(outcomes)
        .map(|(seq, millis)| NewSample {
            session,
            target,
            sent: at(i64::from(seq)),
            seq,
            address: None,
            outcome: millis.map_or(Outcome::Lost(Loss::Timeout), |millis| Outcome::Reply {
                rtt: Duration::from_millis(millis),
            }),
        })
        .collect();
    collecting
        .record(session, at(10), &samples)
        .expect("record");
    collecting
        .end_session(session, at(10), EndReason::Stopped)
        .expect("end");
    (dir, path)
}

fn pingr(db: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pingr"))
        .args(args)
        .env("PINGR_DB", db)
        .stdin(Stdio::null())
        .output()
        .expect("run pingr")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("utf-8 output")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("utf-8 errors")
}

#[test]
fn stats_as_json_reports_loss_latency_and_time_without_data() {
    let (_dir, db) = seeded();

    // A 100-second range of which the collector ran for the first ten.
    let output = pingr(
        &db,
        &[
            "stats",
            "--since",
            "2023-11-14T22:13:20Z",
            "--until",
            "2023-11-14T22:15:00Z",
            "--json",
        ],
    );

    assert!(output.status.success(), "{}", stderr(&output));
    let json: Value = serde_json::from_str(&stdout(&output)).expect("valid JSON");
    let row = &json[0];
    assert_eq!(row["target"], "example.com");
    assert_eq!(
        (&row["sent"], &row["received"], &row["lost"]),
        (&5.into(), &4.into(), &1.into())
    );
    assert_eq!(row["loss"], 0.2);
    assert_eq!(row["rtt_ms"]["min"], 10.0);
    assert_eq!(row["rtt_ms"]["median"], 25.0);
    assert_eq!(row["rtt_ms"]["p99"], 40.0);
    assert_eq!(row["no_data_seconds"], 90.0);
}

#[test]
fn history_as_json_lists_every_sample_in_the_range() {
    let (_dir, db) = seeded();

    let output = pingr(
        &db,
        &[
            "history",
            "example.com",
            "--since",
            "2023-11-14T22:13:21Z",
            "--until",
            "2023-11-14T22:13:23Z",
            "--json",
        ],
    );

    assert!(output.status.success(), "{}", stderr(&output));
    let json: Value = serde_json::from_str(&stdout(&output)).expect("valid JSON");
    let rows = json.as_array().expect("an array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["rtt_ms"], 20.0);
    assert_eq!(rows[1]["lost"], "timeout");
    assert_eq!(rows[1]["rtt_ms"], Value::Null);
}

#[test]
fn asking_before_any_data_exists_says_how_to_start_and_creates_nothing() {
    let (_dir, db) = scratch();

    let output = pingr(&db, &["stats"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("pingr run"), "{}", stderr(&output));
    assert_eq!(stdout(&output), "");
    assert!(!db.exists());
}

#[test]
fn an_unknown_target_fails_and_lists_the_known_ones() {
    let (_dir, db) = seeded();

    let output = pingr(&db, &["stats", "nowhere.invalid", "--json"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("example.com"),
        "{}",
        stderr(&output)
    );
    assert_eq!(stdout(&output), "", "prose leaked into JSON output");
}

#[test]
fn a_time_that_cannot_be_read_is_rejected_with_examples() {
    let (_dir, db) = seeded();

    let output = pingr(&db, &["stats", "--since", "teatime"]);

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("2026-10-12 14:30"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn prune_without_confirmation_deletes_nothing() {
    let (_dir, db) = seeded();

    let refused = pingr(&db, &["prune", "--before", "0s"]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(stderr(&refused).contains("--yes"), "{}", stderr(&refused));

    let dry = pingr(&db, &["prune", "--before", "0s", "--dry-run"]);
    assert!(dry.status.success());
    assert!(
        stdout(&dry).contains("Would delete 5 samples"),
        "{}",
        stdout(&dry)
    );

    let confirmed = pingr(&db, &["prune", "--before", "0s", "--yes"]);
    assert!(confirmed.status.success());
    assert!(
        stdout(&confirmed).contains("Deleted 5 samples"),
        "{}",
        stdout(&confirmed)
    );
}

#[test]
fn the_terminal_view_refuses_to_run_without_a_terminal() {
    let (_dir, db) = scratch();

    let output = pingr(&db, &["run", "127.0.0.1", "--output", "tui"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("--output summary"),
        "{}",
        stderr(&output)
    );
}

/// Real pings to loopback, as in the engine's loopback test.
#[test]
fn ping_output_prints_a_line_per_probe() {
    let (_dir, db) = scratch();

    let output = pingr(
        &db,
        &[
            "run",
            "127.0.0.1",
            "--output",
            "ping",
            "--interval",
            "50ms",
            "--count",
            "3",
        ],
    );

    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    let probes = text.lines().filter(|line| line.contains("seq=")).count();
    assert!(probes >= 3, "{text}");
    assert!(text.contains("127.0.0.1: "), "{text}");
}

#[test]
fn without_a_terminal_the_default_output_is_periodic_summaries() {
    let (_dir, db) = scratch();

    let output = pingr(
        &db,
        &[
            "run",
            "127.0.0.1",
            "--interval",
            "50ms",
            "--report-every",
            "100ms",
            "--count",
            "6",
        ],
    );

    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(
        text.contains("probes)"),
        "no periodic summary line:\n{text}"
    );
    assert!(
        !text.contains("seq="),
        "per-probe lines in summary mode:\n{text}"
    );
    assert!(text.contains("sent, "), "no closing totals:\n{text}");
}
