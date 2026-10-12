//! How pingr writes times, durations, and sizes for people.

use std::time::Duration;

use jiff::tz::TimeZone;
use pingr_engine::store::{Loss, Outcome};
use pingr_engine::time::Timestamp;

fn zoned(at: Timestamp) -> jiff::Zoned {
    jiff::Timestamp::from_microsecond(at.as_micros())
        .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
        .to_zoned(TimeZone::system())
}

/// Local date and time to the second.
pub fn datetime(at: Timestamp) -> String {
    zoned(at).strftime("%Y-%m-%d %H:%M:%S").to_string()
}

/// Local date and time to the millisecond.
pub fn datetime_ms(at: Timestamp) -> String {
    zoned(at).strftime("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

/// Local time of day to the second.
pub fn clock(at: Timestamp) -> String {
    zoned(at).strftime("%H:%M:%S").to_string()
}

/// UTC in RFC 3339, for machines.
pub fn rfc3339(at: Timestamp) -> String {
    jiff::Timestamp::from_microsecond(at.as_micros())
        .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
        .to_string()
}

/// A duration in milliseconds, as a number.
pub fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

/// A round-trip time, e.g. `12.3 ms`.
pub fn rtt(duration: Duration) -> String {
    let ms = millis(duration);
    if ms < 10.0 {
        format!("{ms:.2} ms")
    } else if ms < 100.0 {
        format!("{ms:.1} ms")
    } else {
        format!("{ms:.0} ms")
    }
}

/// A fraction as a percentage to one decimal place, e.g. `0.3%`.
pub fn percent(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

/// A long duration in its two largest units, e.g. `3h 12m`.
pub fn span(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let (days, hours, minutes) = (seconds / 86_400, seconds / 3_600 % 24, seconds / 60 % 60);
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {}s", seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

/// A file size, e.g. `12.4 MB`.
pub fn bytes(count: u64) -> String {
    const UNITS: [&str; 4] = ["KB", "MB", "GB", "TB"];
    if count < 1024 {
        return format!("{count} B");
    }
    #[expect(clippy::cast_precision_loss, reason = "a size for display")]
    let mut size = count as f64 / 1024.0;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.1} {}", UNITS[unit])
}

/// Why a probe was lost, in a word or two.
pub fn loss(loss: Loss) -> &'static str {
    match loss {
        Loss::Timeout => "timeout",
        Loss::Unreachable => "unreachable",
        Loss::Other => "error",
        Loss::Unresolved => "name not resolved",
    }
}

/// A probe's result, e.g. `12.3 ms` or `lost (timeout)`.
pub fn outcome(outcome: Outcome) -> String {
    match outcome {
        Outcome::Reply { rtt: time } => rtt(time),
        Outcome::Lost(reason) => format!("lost ({})", loss(reason)),
    }
}
