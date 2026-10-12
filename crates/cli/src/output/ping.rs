//! One line per probe, in the manner of `ping`.

use std::io::{self, Write};

use pingr_engine::collector::Event;

use super::{Display, Target, notice};
use crate::fmt;

pub struct Ping {
    targets: Vec<Target>,
}

impl Ping {
    pub fn new(targets: Vec<Target>) -> Self {
        Self { targets }
    }
}

impl Display for Ping {
    fn event(&mut self, event: &Event, out: &mut dyn Write) -> io::Result<()> {
        if let Some(line) = notice(event, &mut self.targets) {
            return writeln!(out, "{line}");
        }
        if let Event::Probe {
            target,
            sent,
            seq,
            outcome,
            ..
        } = event
        {
            writeln!(
                out,
                "{} {}: seq={seq} {}",
                fmt::clock(*sent),
                self.targets[*target].label(),
                fmt::outcome(*outcome)
            )?;
        }
        Ok(())
    }

    fn report(&mut self, _: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }
}
