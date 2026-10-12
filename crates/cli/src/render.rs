//! The commands that read the store: what they compute and how they print it.

use std::io::{self, Write};
use std::time::Duration;

use pingr_engine::stats::{RttStats, Stats, summarize};
use pingr_engine::store::{Outcome, Sample, Session, Store, StoreError};
use pingr_engine::time::{TimeRange, Timestamp};
use serde::Serialize;

use crate::fmt;

/// Anything that stops a read command.
#[derive(Debug)]
pub enum ReadError {
    /// The store could not be read.
    Store(StoreError),
    /// The output could not be written.
    Io(io::Error),
    /// The request cannot be met; the message says what to do instead.
    Usage(String),
}

impl From<io::Error> for ReadError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<StoreError> for ReadError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// The names to report on: those asked for, or every known target.
fn chosen(store: &Store, asked: &[String]) -> Result<Vec<String>, ReadError> {
    let known: Vec<String> = store
        .targets()?
        .into_iter()
        .map(|target| target.name)
        .collect();
    if let Some(unknown) = asked.iter().find(|name| !known.contains(name)) {
        return Err(ReadError::Usage(format!(
            "no data for target `{unknown}`. Known targets: {}",
            if known.is_empty() {
                "none yet".to_owned()
            } else {
                known.join(", ")
            }
        )));
    }
    Ok(if asked.is_empty() {
        known
    } else {
        asked.to_vec()
    })
}

/// How much of `range` (up to `now`) no collector run covers.
fn uncovered(range: TimeRange, now: Timestamp, sessions: &[Session]) -> Duration {
    let since = range.since.as_micros();
    let until = range.until.min(now).as_micros();
    let mut covered = 0;
    // Sessions arrive oldest first; `reached` guards against overlaps.
    let mut reached = since;
    for session in sessions {
        let end = session.ended.map_or(session.last_alive, |(at, _)| at);
        let from = session.started.as_micros().max(reached);
        let to = end.as_micros().min(until);
        if to > from {
            covered += to - from;
            reached = to;
        }
    }
    let total = (until - since).max(0);
    Duration::from_micros(u64::try_from(total - covered).unwrap_or(0))
}

#[derive(Serialize)]
struct RttJson {
    min: f64,
    mean: f64,
    median: f64,
    max: f64,
    stddev: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    jitter: Option<f64>,
}

impl From<RttStats> for RttJson {
    fn from(rtt: RttStats) -> Self {
        Self {
            min: fmt::millis(rtt.min),
            mean: fmt::millis(rtt.mean),
            median: fmt::millis(rtt.median),
            max: fmt::millis(rtt.max),
            stddev: fmt::millis(rtt.stddev),
            p90: fmt::millis(rtt.p90),
            p95: fmt::millis(rtt.p95),
            p99: fmt::millis(rtt.p99),
            jitter: rtt.jitter.map(fmt::millis),
        }
    }
}

#[derive(Serialize)]
struct StatsJson {
    target: String,
    since: String,
    until: String,
    sent: u64,
    received: u64,
    lost: u64,
    /// Fraction of probes lost, 0 to 1.
    loss: Option<f64>,
    rtt_ms: Option<RttJson>,
    /// Seconds of the range in which no collector was running.
    no_data_seconds: f64,
}

/// `pingr stats`: loss and latency per target over a range.
pub fn stats(
    store: &Store,
    targets: &[String],
    range: TimeRange,
    now: Timestamp,
    json: bool,
    out: &mut impl Write,
) -> Result<(), ReadError> {
    let mut rows = Vec::new();
    for name in chosen(store, targets)? {
        let samples = store.samples(&name, range)?;
        let stats = summarize(samples.iter().map(|sample| sample.outcome));
        let gap = uncovered(range, now, &store.sessions(&name, range)?);
        rows.push((name, stats, gap));
    }

    if json {
        let rows: Vec<_> = rows
            .into_iter()
            .map(|(target, stats, gap)| StatsJson {
                target,
                since: fmt::rfc3339(range.since),
                until: fmt::rfc3339(range.until),
                sent: stats.sent,
                received: stats.received,
                lost: stats.lost(),
                loss: stats.loss(),
                rtt_ms: stats.rtt.map(RttJson::from),
                no_data_seconds: gap.as_secs_f64(),
            })
            .collect();
        return write_json(out, &rows);
    }

    if rows.is_empty() {
        writeln!(out, "No targets recorded yet.")?;
        return Ok(());
    }
    writeln!(
        out,
        "{} to {}",
        fmt::datetime(range.since),
        fmt::datetime(range.until)
    )?;
    let mut table = Table::new(&[
        "target", "sent", "lost", "loss", "min", "mean", "median", "max", "stddev", "p90", "p95",
        "p99", "jitter",
    ]);
    for (name, stats, _) in &rows {
        table.row(stats_row(name, stats));
    }
    table.print(out)?;
    for (name, stats, gap) in &rows {
        if stats.sent == 0 {
            writeln!(out, "{name}: no samples in this range.")?;
        } else if *gap >= Duration::from_secs(60) {
            writeln!(
                out,
                "{name}: no data for {} of this range (the collector was not running).",
                fmt::span(*gap)
            )?;
        }
    }
    Ok(())
}

fn stats_row(name: &str, stats: &Stats) -> Vec<String> {
    let dash = || "-".to_owned();
    let mut row = vec![
        name.to_owned(),
        stats.sent.to_string(),
        stats.lost().to_string(),
        stats.loss().map_or_else(dash, fmt::percent),
    ];
    match stats.rtt {
        Some(rtt) => row.extend(
            [
                Some(rtt.min),
                Some(rtt.mean),
                Some(rtt.median),
                Some(rtt.max),
                Some(rtt.stddev),
                Some(rtt.p90),
                Some(rtt.p95),
                Some(rtt.p99),
                rtt.jitter,
            ]
            .map(|value| value.map_or_else(dash, fmt::rtt)),
        ),
        None => row.extend(std::iter::repeat_with(dash).take(9)),
    }
    row
}

#[derive(Serialize)]
struct SampleJson {
    sent: String,
    seq: u32,
    address: Option<String>,
    rtt_ms: Option<f64>,
    lost: Option<&'static str>,
}

impl From<&Sample> for SampleJson {
    fn from(sample: &Sample) -> Self {
        let (rtt_ms, lost) = match sample.outcome {
            Outcome::Reply { rtt } => (Some(fmt::millis(rtt)), None),
            Outcome::Lost(loss) => (None, Some(fmt::loss(loss))),
        };
        Self {
            sent: fmt::rfc3339(sample.sent),
            seq: sample.seq,
            address: sample.address.map(|ip| ip.to_string()),
            rtt_ms,
            lost,
        }
    }
}

/// `pingr history`: every sample for one target in a range.
pub fn history(
    store: &Store,
    target: &str,
    range: TimeRange,
    json: bool,
    out: &mut impl Write,
) -> Result<(), ReadError> {
    let name = chosen(store, &[target.to_owned()])?.remove(0);
    let samples = store.samples(&name, range)?;
    if json {
        let rows: Vec<SampleJson> = samples.iter().map(SampleJson::from).collect();
        return write_json(out, &rows);
    }
    if samples.is_empty() {
        writeln!(
            out,
            "{name}: no samples between {} and {}.",
            fmt::datetime(range.since),
            fmt::datetime(range.until)
        )?;
        return Ok(());
    }
    let mut previous: Option<&Sample> = None;
    for sample in &samples {
        if let Some(previous) = previous.filter(|previous| previous.session != sample.session) {
            writeln!(
                out,
                "-- collector not running from {} to {} --",
                fmt::datetime(previous.sent),
                fmt::datetime(sample.sent)
            )?;
        }
        writeln!(
            out,
            "{}  {:<15}  {}",
            fmt::datetime_ms(sample.sent),
            sample
                .address
                .map_or_else(|| "-".to_owned(), |ip| ip.to_string()),
            fmt::outcome(sample.outcome)
        )?;
        previous = Some(sample);
    }
    Ok(())
}

#[derive(Serialize)]
struct TargetJson {
    target: String,
    first_sample: Option<String>,
    last_sample: Option<String>,
}

/// `pingr targets`: every target with data and the span it covers.
pub fn targets(store: &Store, json: bool, out: &mut impl Write) -> Result<(), ReadError> {
    let targets = store.targets()?;
    if json {
        let rows: Vec<_> = targets
            .into_iter()
            .map(|target| TargetJson {
                target: target.name,
                first_sample: target.first.map(fmt::rfc3339),
                last_sample: target.last.map(fmt::rfc3339),
            })
            .collect();
        return write_json(out, &rows);
    }
    if targets.is_empty() {
        writeln!(out, "No targets recorded yet.")?;
        return Ok(());
    }
    let mut table = Table::new(&["target", "first sample", "last sample"]);
    for target in targets {
        let time = |at: Option<Timestamp>| at.map_or_else(|| "-".to_owned(), fmt::datetime);
        table.row(vec![target.name, time(target.first), time(target.last)]);
    }
    table.print(out)?;
    Ok(())
}

#[derive(Serialize)]
struct StatusJson {
    store: String,
    size_bytes: u64,
    collecting: bool,
    targets: Vec<TargetJson>,
}

/// `pingr status`: is a collector running, and how big has the store grown.
pub fn status(
    store: &Store,
    now: Timestamp,
    json: bool,
    out: &mut impl Write,
) -> Result<(), ReadError> {
    let collecting = store.is_collecting()?;
    let size = store.size_bytes()?;
    let targets = store.targets()?;
    if json {
        return write_json(
            out,
            &StatusJson {
                store: store.path().display().to_string(),
                size_bytes: size,
                collecting,
                targets: targets
                    .into_iter()
                    .map(|target| TargetJson {
                        target: target.name,
                        first_sample: target.first.map(fmt::rfc3339),
                        last_sample: target.last.map(fmt::rfc3339),
                    })
                    .collect(),
            },
        );
    }
    writeln!(out, "store      {}", store.path().display())?;
    writeln!(out, "size       {}", fmt::bytes(size))?;
    writeln!(
        out,
        "collector  {}",
        if collecting { "running" } else { "not running" }
    )?;
    for target in targets {
        let age = target.last.map_or_else(
            || "no samples".to_owned(),
            |last| {
                let micros = (now.as_micros() - last.as_micros()).max(0);
                let ago = Duration::from_micros(u64::try_from(micros).unwrap_or(0));
                format!("last sample {} ago", fmt::span(ago))
            },
        );
        writeln!(out, "  {}  {age}", target.name)?;
    }
    Ok(())
}

fn write_json(out: &mut impl Write, value: &impl Serialize) -> Result<(), ReadError> {
    serde_json::to_writer_pretty(&mut *out, value).map_err(io::Error::from)?;
    writeln!(out)?;
    Ok(())
}

/// A plain-text table: first column left-aligned, the rest right-aligned.
struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    fn new(header: &[&str]) -> Self {
        Self {
            rows: vec![header.iter().map(|&cell| cell.to_owned()).collect()],
        }
    }

    fn row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    fn print(&self, out: &mut impl Write) -> io::Result<()> {
        let columns = self.rows[0].len();
        let widths: Vec<usize> = (0..columns)
            .map(|column| {
                self.rows
                    .iter()
                    .map(|row| row[column].chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        for row in &self.rows {
            let cells: Vec<String> = row
                .iter()
                .zip(&widths)
                .enumerate()
                .map(|(column, (cell, &width))| {
                    if column == 0 {
                        format!("{cell:<width$}")
                    } else {
                        format!("{cell:>width$}")
                    }
                })
                .collect();
            writeln!(out, "{}", cells.join("  ").trim_end())?;
        }
        Ok(())
    }
}
