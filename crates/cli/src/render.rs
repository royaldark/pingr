//! The commands that read the store: what they compute and how they print it.

use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Duration;

use pingr_engine::stats::{RttStats, Stats, summarize};
use pingr_engine::store::{NetworkId, Outcome, Sample, Session, Store, StoreError};
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

/// Which networks to report on, and what each is called.
struct Networks {
    labels: HashMap<NetworkId, String>,
    /// Only this one, if the user asked for one.
    only: Option<NetworkId>,
}

impl Networks {
    fn new(store: &Store, asked: Option<&str>) -> Result<Self, ReadError> {
        let known = store.networks()?;
        let only = match asked {
            None => None,
            Some(label) => Some(
                known
                    .iter()
                    .find(|network| network.label == label)
                    .ok_or_else(|| {
                        let labels: Vec<_> =
                            known.iter().map(|network| network.label.as_str()).collect();
                        ReadError::Usage(format!(
                            "no network labeled `{label}`. Known networks: {}",
                            if labels.is_empty() {
                                "none yet".to_owned()
                            } else {
                                labels.join(", ")
                            }
                        ))
                    })?
                    .id,
            ),
        };
        let labels = known
            .into_iter()
            .map(|network| (network.id, network.label))
            .collect();
        Ok(Self { labels, only })
    }

    /// `target`'s samples in `range`, on the chosen network if there is one.
    fn samples(
        &self,
        store: &Store,
        target: &str,
        range: TimeRange,
    ) -> Result<Vec<Sample>, ReadError> {
        let mut samples = store.samples(target, range)?;
        if let Some(only) = self.only {
            samples.retain(|sample| sample.network == Some(only));
        }
        Ok(samples)
    }

    /// What `network` is called; `None` for one that could not be told.
    fn label(&self, network: Option<NetworkId>) -> Option<String> {
        network.and_then(|network| self.labels.get(&network).cloned())
    }

    /// What `network` is called, for text output.
    fn name(&self, network: Option<NetworkId>) -> String {
        self.label(network).unwrap_or_else(|| "unknown".to_owned())
    }
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
    /// `None` when the network could not be told, or nothing was sent.
    network: Option<String>,
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

/// One line of `pingr stats`: a target on one network.
struct StatsRow {
    target: String,
    /// `None` when the network could not be told, or nothing was sent.
    network: Option<NetworkId>,
    stats: Stats,
    gap: Duration,
}

/// A target's samples split by the network they were taken on, networks in
/// the order first seen. Never mixes two networks in one group.
fn by_network(samples: &[Sample]) -> Vec<(Option<NetworkId>, Vec<Outcome>)> {
    let mut groups: Vec<(Option<NetworkId>, Vec<Outcome>)> = Vec::new();
    for sample in samples {
        match groups
            .iter_mut()
            .find(|(network, _)| *network == sample.network)
        {
            Some((_, outcomes)) => outcomes.push(sample.outcome),
            None => groups.push((sample.network, vec![sample.outcome])),
        }
    }
    groups
}

/// `pingr stats`: loss and latency per target and network over a range.
pub fn stats(
    store: &Store,
    targets: &[String],
    network: Option<&str>,
    range: TimeRange,
    now: Timestamp,
    json: bool,
    out: &mut impl Write,
) -> Result<(), ReadError> {
    let networks = Networks::new(store, network)?;
    let mut rows = Vec::new();
    // Per target: whether it has samples, the uncovered time, and whether no
    // collector run overlaps the range at all.
    let mut notes = Vec::new();
    for name in chosen(store, targets)? {
        let samples = networks.samples(store, &name, range)?;
        let sessions = store.sessions(&name, range)?;
        let gap = uncovered(range, now, &sessions);
        let groups = by_network(&samples);
        if groups.is_empty() {
            rows.push(StatsRow {
                target: name.clone(),
                network: None,
                stats: summarize([]),
                gap,
            });
        }
        for (network, outcomes) in groups {
            rows.push(StatsRow {
                target: name.clone(),
                network,
                stats: summarize(outcomes),
                gap,
            });
        }
        notes.push((name, samples.is_empty(), gap, sessions.is_empty()));
    }

    if json {
        let rows: Vec<_> = rows
            .into_iter()
            .map(
                |StatsRow {
                     target,
                     network,
                     stats,
                     gap,
                 }| StatsJson {
                    network: networks.label(network),
                    target,
                    since: fmt::rfc3339(range.since),
                    until: fmt::rfc3339(range.until),
                    sent: stats.sent,
                    received: stats.received,
                    lost: stats.lost(),
                    loss: stats.loss(),
                    rtt_ms: stats.rtt.map(RttJson::from),
                    no_data_seconds: gap.as_secs_f64(),
                },
            )
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
        "target", "network", "sent", "lost", "loss", "min", "mean", "median", "max", "stddev",
        "p90", "p95", "p99", "jitter",
    ])
    .text_columns(2);
    for row in &rows {
        let network = if row.stats.sent == 0 {
            "-".to_owned()
        } else {
            networks.name(row.network)
        };
        table.row(stats_row(&row.target, network, &row.stats));
    }
    table.print(out)?;
    for (name, nothing, gap, never_ran) in &notes {
        if *nothing {
            writeln!(out, "{name}: {}", no_samples(network, *never_ran))?;
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

/// Why a range holds no samples (on `network`, if one was asked for), as far
/// as the store can tell. `never_ran` means no collector run on record
/// overlaps the range.
fn no_samples(network: Option<&str>, never_ran: bool) -> String {
    if never_ran {
        // Pruning removes a run's record along with its samples, so the two
        // cannot be told apart afterwards.
        "no samples in this range: the collector was not running then, \
         or the data has since been pruned."
            .to_owned()
    } else if let Some(label) = network {
        format!("no samples on {label} in this range.")
    } else {
        "no samples in this range.".to_owned()
    }
}

fn stats_row(name: &str, network: String, stats: &Stats) -> Vec<String> {
    let dash = || "-".to_owned();
    let mut row = vec![
        name.to_owned(),
        network,
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
    /// `None` when the network could not be told.
    network: Option<String>,
    sent: String,
    seq: u32,
    address: Option<String>,
    rtt_ms: Option<f64>,
    lost: Option<&'static str>,
}

impl SampleJson {
    fn new(sample: &Sample, network: Option<String>) -> Self {
        let (rtt_ms, lost) = match sample.outcome {
            Outcome::Reply { rtt } => (Some(fmt::millis(rtt)), None),
            Outcome::Lost(loss) => (None, Some(fmt::loss(loss))),
        };
        Self {
            network,
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
    network: Option<&str>,
    range: TimeRange,
    json: bool,
    out: &mut impl Write,
) -> Result<(), ReadError> {
    let networks = Networks::new(store, network)?;
    let name = chosen(store, &[target.to_owned()])?.remove(0);
    let samples = networks.samples(store, &name, range)?;
    if json {
        let rows: Vec<SampleJson> = samples
            .iter()
            .map(|sample| SampleJson::new(sample, networks.label(sample.network)))
            .collect();
        return write_json(out, &rows);
    }
    if samples.is_empty() {
        writeln!(
            out,
            "{name}: {} ({} to {})",
            no_samples(network, store.sessions(&name, range)?.is_empty()),
            fmt::datetime(range.since),
            fmt::datetime(range.until)
        )?;
        return Ok(());
    }
    let mut previous: Option<&Sample> = None;
    for sample in &samples {
        match previous {
            // Asked for one network: no need to say which.
            None if network.is_none() => {
                writeln!(out, "-- on {} --", networks.name(sample.network))?;
            }
            Some(previous) if previous.network != sample.network => writeln!(
                out,
                "-- on {} from {} --",
                networks.name(sample.network),
                fmt::datetime(sample.sent)
            )?,
            Some(previous) if previous.session != sample.session => writeln!(
                out,
                "-- {} from {} to {} --",
                network.map_or_else(
                    || "collector not running".to_owned(),
                    |label| format!("not collecting on {label}")
                ),
                fmt::datetime(previous.sent),
                fmt::datetime(sample.sent)
            )?,
            _ => {}
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
struct NetworkJson {
    network: String,
    gateway_mac: Option<String>,
    first_seen: Option<String>,
    last_seen: Option<String>,
}

/// `pingr networks`: every network the collector has run on.
pub fn networks(store: &Store, json: bool, out: &mut impl Write) -> Result<(), ReadError> {
    let networks = store.networks()?;
    if json {
        let rows: Vec<_> = networks
            .into_iter()
            .map(|network| NetworkJson {
                network: network.label,
                gateway_mac: network.gateway_mac,
                first_seen: network.first.map(fmt::rfc3339),
                last_seen: network.last.map(fmt::rfc3339),
            })
            .collect();
        return write_json(out, &rows);
    }
    if networks.is_empty() {
        writeln!(out, "No networks recorded yet.")?;
        return Ok(());
    }
    let mut table = Table::new(&["network", "gateway", "first seen", "last seen"]).text_columns(2);
    for network in networks {
        let time = |at: Option<Timestamp>| at.map_or_else(|| "-".to_owned(), fmt::datetime);
        table.row(vec![
            network.label,
            network.gateway_mac.unwrap_or_else(|| "-".to_owned()),
            time(network.first),
            time(network.last),
        ]);
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

/// A plain-text table: the leading text columns left-aligned, the rest
/// right-aligned.
struct Table {
    rows: Vec<Vec<String>>,
    text_columns: usize,
}

impl Table {
    fn new(header: &[&str]) -> Self {
        Self {
            rows: vec![header.iter().map(|&cell| cell.to_owned()).collect()],
            text_columns: 1,
        }
    }

    /// Left-aligns the first `count` columns.
    fn text_columns(mut self, count: usize) -> Self {
        self.text_columns = count;
        self
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
                    if column < self.text_columns {
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
