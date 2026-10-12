//! Durations and points in time as typed on the command line.

use std::str::FromStr;
use std::time::Duration;

use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use pingr_engine::time::{TimeRange, Timestamp};

/// A length of time such as `500ms`, `30s`, `15m`, `2h`, `7d`, or `1w`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span(pub Duration);

impl FromStr for Span {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid =
            || format!("`{text}` is not a duration: use e.g. 500ms, 30s, 15m, 2h, 7d, 1w");
        let digits = text
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(invalid)?;
        let (number, unit) = text.split_at(digits);
        let number: f64 = number.parse().map_err(|_| invalid())?;
        let seconds = match unit {
            "ms" => 0.001,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3_600.0,
            "d" => 86_400.0,
            "w" => 604_800.0,
            _ => return Err(invalid()),
        };
        Duration::try_from_secs_f64(number * seconds)
            .map(Self)
            .map_err(|_| invalid())
    }
}

/// A point in time: absolute, or a span before now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    /// A fixed instant.
    At(Timestamp),
    /// This long before the moment the command runs.
    Ago(Duration),
}

impl When {
    /// The instant this means when the command runs at `now`.
    pub fn resolve(self, now: Timestamp) -> Timestamp {
        match self {
            Self::At(at) => at,
            Self::Ago(span) => now.minus(span),
        }
    }
}

impl FromStr for When {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Ok(Span(span)) = text.parse() {
            return Ok(Self::Ago(span));
        }
        if let Ok(instant) = text.parse::<jiff::Timestamp>() {
            return Ok(Self::At(Timestamp::from_micros(instant.as_microsecond())));
        }
        let local: DateTime = text.parse().map_err(|_| {
            format!(
                "`{text}` is not a time: use 2026-10-12, \"2026-10-12 14:30\", \
                 2026-10-12T14:30:00Z, or a span ago such as 2h"
            )
        })?;
        // A local time that a clock change skips or repeats names no single
        // instant; ask for an unambiguous one instead of guessing.
        let zoned = TimeZone::system()
            .to_ambiguous_zoned(local)
            .unambiguous()
            .map_err(|_| {
                format!(
                    "`{text}` is skipped or repeated by a clock change in this time zone: \
                     give it with an offset, e.g. 2026-10-25T02:30:00+01:00"
                )
            })?;
        Ok(Self::At(Timestamp::from_micros(
            zoned.timestamp().as_microsecond(),
        )))
    }
}

/// Which stretch of history a command looks at.
#[derive(Debug, Clone, Copy, clap::Args)]
pub struct RangeArgs {
    /// Look at the most recent span, e.g. 30m, 2h, 7d [default: 24h]
    #[arg(long, value_name = "SPAN", conflicts_with_all = ["since", "until"])]
    pub last: Option<Span>,
    /// Start of the range: a local time, an RFC 3339 time, or a span ago
    #[arg(long, value_name = "WHEN")]
    pub since: Option<When>,
    /// End of the range (exclusive) [default: now]
    #[arg(long, value_name = "WHEN")]
    pub until: Option<When>,
}

impl RangeArgs {
    /// The range these arguments mean when the command runs at `now`.
    pub fn resolve(self, now: Timestamp) -> Result<TimeRange, String> {
        let day = Duration::from_hours(24);
        let until = self.until.map_or(now, |until| until.resolve(now));
        let since = match (self.last, self.since) {
            (Some(Span(last)), _) => now.minus(last),
            (None, Some(since)) => since.resolve(now),
            (None, None) => until.minus(day),
        };
        if since >= until {
            return Err("the range is empty: --since must be earlier than --until".to_owned());
        }
        Ok(TimeRange { since, until })
    }
}
