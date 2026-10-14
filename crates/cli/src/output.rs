//! What `pingr run` shows while it collects.
//!
//! Every mode is fed the same stream of collector events and computes nothing
//! the others could not.

pub mod health;
pub mod ping;
pub mod summary;
pub mod tui;

use std::io::{self, Write};
use std::net::IpAddr;
use std::time::Duration;

use pingr_engine::collector::Event;
use pingr_engine::store::{EndReason, Outcome};

use crate::fmt;

/// Consecutive losses after which a target counts as not answering.
pub const DOWN_AFTER: u32 = 3;

/// How `pingr run` reports what it is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    /// A live table, redrawn in place (needs a terminal)
    Tui,
    /// A line per target every so often, plus a line when something changes
    Summary,
    /// A line per probe, like `ping`
    Ping,
}

/// Something that turns collector events into lines of output.
pub trait Display {
    /// Called for every collector event, in order.
    fn event(&mut self, event: &Event, out: &mut dyn Write) -> io::Result<()>;

    /// Called once per reporting period.
    fn report(&mut self, out: &mut dyn Write) -> io::Result<()>;

    /// Called several times a second, for modes that repaint a screen.
    fn redraw(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A target as the output modes know it.
#[derive(Debug, Clone)]
pub struct Target {
    pub name: String,
    pub ip: Option<IpAddr>,
}

impl Target {
    /// `name`, or `name (address)` when they differ.
    pub fn label(&self) -> String {
        match self.ip {
            Some(ip) if ip.to_string() != self.name => format!("{} ({ip})", self.name),
            _ => self.name.clone(),
        }
    }
}

/// The line to print for an event that is not a probe result, if any.
/// Also keeps `targets` up to date with what each name resolves to.
pub fn notice(event: &Event, targets: &mut [Target]) -> Option<String> {
    match event {
        Event::Probe { .. } | Event::SessionStarted { .. } => None,
        Event::Resolved { target, ip } => {
            let target = &mut targets[*target];
            let line = match target.ip.replace(*ip) {
                None if ip.to_string() == target.name => return None,
                None => format!("{} is {ip}", target.name),
                Some(old) => format!("{} moved from {old} to {ip}", target.name),
            };
            Some(line)
        }
        Event::ResolveFailed { target, error } => Some(format!(
            "{}: name lookup failed ({error})",
            targets[*target].name
        )),
        Event::SessionEnded { reason, .. } => match reason {
            EndReason::Stopped => None,
            EndReason::ClockJump => {
                Some("the clock jumped (suspend or time change): starting a new session".to_owned())
            }
            EndReason::Stalled => {
                Some("probing stalled for a while: starting a new session".to_owned())
            }
        },
        Event::StoreTrouble { error } => Some(format!(
            "cannot write to the store ({error}): holding samples in memory and retrying"
        )),
    }
}

/// Running totals for one target over the whole run.
#[derive(Debug, Clone, Copy, Default)]
pub struct Totals {
    pub sent: u64,
    pub received: u64,
    fastest: Option<Duration>,
    slowest: Option<Duration>,
    total: Duration,
}

impl Totals {
    /// Counts one finished probe.
    pub fn add(&mut self, outcome: Outcome) {
        self.sent += 1;
        if let Outcome::Reply { rtt } = outcome {
            self.received += 1;
            self.fastest = Some(self.fastest.map_or(rtt, |fastest| fastest.min(rtt)));
            self.slowest = Some(self.slowest.map_or(rtt, |slowest| slowest.max(rtt)));
            self.total += rtt;
        }
    }

    /// One line summing up the run, in the manner of `ping`.
    #[expect(
        clippy::cast_precision_loss,
        reason = "probe counts are far below 2^53"
    )]
    pub fn line(&self, label: &str) -> String {
        let loss = if self.sent == 0 {
            0.0
        } else {
            (self.sent - self.received) as f64 / self.sent as f64
        };
        let timings = match (self.fastest, self.slowest) {
            (Some(fastest), Some(slowest)) => format!(
                ", min/mean/max {} / {} / {}",
                fmt::rtt(fastest),
                fmt::rtt(self.total.div_f64(self.received as f64)),
                fmt::rtt(slowest)
            ),
            _ => String::new(),
        };
        format!(
            "{label}: {} sent, {} received, {} loss{timings}",
            self.sent,
            self.received,
            fmt::percent(loss)
        )
    }
}
