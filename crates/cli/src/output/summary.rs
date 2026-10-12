//! A line per target every reporting period, and a line whenever something
//! changes. Plain appended text: at home in tmux, under a service manager, or
//! redirected to a file.

use std::io::{self, Write};

use pingr_engine::collector::Event;
use pingr_engine::stats::summarize;
use pingr_engine::store::Outcome;
use pingr_engine::time::Timestamp;

use super::{Display, Target, notice};
use crate::fmt;

/// Consecutive losses after which a target is reported as not answering.
const DOWN_AFTER: u32 = 3;

#[derive(Default)]
struct Recent {
    /// Results since the last report.
    window: Vec<Outcome>,
    losses_in_a_row: u32,
    latest: Option<Timestamp>,
}

pub struct Summary {
    targets: Vec<Target>,
    recent: Vec<Recent>,
}

impl Summary {
    pub fn new(targets: Vec<Target>) -> Self {
        let recent = targets.iter().map(|_| Recent::default()).collect();
        Self { targets, recent }
    }
}

impl Display for Summary {
    fn event(&mut self, event: &Event, out: &mut dyn Write) -> io::Result<()> {
        if let Some(line) = notice(event, &mut self.targets) {
            return writeln!(out, "{line}");
        }
        let Event::Probe {
            target,
            sent,
            outcome,
            ..
        } = event
        else {
            return Ok(());
        };
        let label = self.targets[*target].label();
        let recent = &mut self.recent[*target];
        recent.window.push(*outcome);
        recent.latest = Some(*sent);
        match outcome {
            Outcome::Lost(reason) => {
                recent.losses_in_a_row += 1;
                if recent.losses_in_a_row == DOWN_AFTER {
                    writeln!(
                        out,
                        "{} {label}: not answering ({})",
                        fmt::clock(*sent),
                        fmt::loss(*reason)
                    )?;
                }
            }
            Outcome::Reply { .. } => {
                if recent.losses_in_a_row >= DOWN_AFTER {
                    writeln!(out, "{} {label}: answering again", fmt::clock(*sent))?;
                }
                recent.losses_in_a_row = 0;
            }
        }
        Ok(())
    }

    fn report(&mut self, out: &mut dyn Write) -> io::Result<()> {
        for (target, recent) in self.targets.iter().zip(&mut self.recent) {
            let Some(latest) = recent.latest.take() else {
                continue;
            };
            let stats = summarize(recent.window.drain(..));
            let timings = stats.rtt.map_or_else(String::new, |rtt| {
                format!(
                    ", mean {}, p95 {}, max {}",
                    fmt::rtt(rtt.mean),
                    fmt::rtt(rtt.p95),
                    fmt::rtt(rtt.max)
                )
            });
            writeln!(
                out,
                "{} {}: {} loss{timings} ({} probes)",
                fmt::clock(latest),
                target.label(),
                stats.loss().map_or_else(|| "-".to_owned(), fmt::percent),
                stats.sent
            )?;
        }
        Ok(())
    }
}
