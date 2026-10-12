//! Sending one probe to one address and learning what came back.
//!
//! [`Prober`] is the seam between the engine and the operating system. Nothing
//! outside [`icmp`] names the crate that does the real I/O, so that backend can
//! be replaced without touching the rest of the engine.

pub mod icmp;
pub mod sim;

use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::time::Duration;

/// What happened to a single probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The destination answered.
    Reply {
        /// Time from sending the probe to receiving its answer, measured on
        /// the monotonic clock.
        rtt: Duration,
    },
    /// No usable answer arrived.
    Lost(LossReason),
}

/// Why a probe counts as lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossReason {
    /// Nothing came back within the timeout.
    Timeout,
    /// The network reported the destination as unreachable.
    Unreachable,
    /// The probe failed in a way the backend could not classify.
    Other,
}

/// A probe that could not be attempted at all.
///
/// A probe that was sent and went unanswered is not an error: it is
/// [`ProbeOutcome::Lost`].
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// The operating system refused to let an unprivileged process send pings.
    #[error("this system does not allow unprivileged pings{}", unprivileged_hint())]
    NotPermitted(#[source] io::Error),
    /// Any other failure to set up or send the probe.
    #[error("could not send probe to {addr}")]
    Io {
        /// The address being probed.
        addr: IpAddr,
        /// The underlying failure.
        #[source]
        source: io::Error,
    },
}

/// How to open the unprivileged path on this operating system, if we know.
fn unprivileged_hint() -> &'static str {
    if cfg!(target_os = "linux") {
        ": allow them with `sudo sysctl -w net.ipv4.ping_group_range=\"0 2147483647\"`"
    } else {
        ""
    }
}

/// Sends probes. Implementations must never require elevated privileges.
pub trait Prober: Send + Sync {
    /// Checks, without sending anything, that this system lets pingr probe.
    fn preflight(&self) -> Result<(), ProbeError>;

    /// Releases whatever is held for probing `addr`, which is no longer a
    /// target. Probing it again later must still work.
    fn forget(&self, addr: IpAddr);

    /// Probes `addr` once and waits for the answer or the prober's timeout.
    fn probe(&self, addr: IpAddr) -> impl Future<Output = Result<ProbeOutcome, ProbeError>> + Send;
}
