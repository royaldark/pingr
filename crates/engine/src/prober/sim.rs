//! A scripted prober: no network, and time passes on tokio's clock, so tests
//! that pause that clock run instantly and deterministically.

use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use super::{LossReason, ProbeError, ProbeOutcome, Prober};

/// How a simulated destination treats one probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sim {
    /// Answers after this long.
    Reply(Duration),
    /// Never answers; the probe is lost at the timeout.
    Silent,
}

/// A [`Prober`] whose destinations behave as a function says.
pub struct SimProber<F> {
    timeout: Duration,
    permitted: bool,
    behave: F,
    sent: Mutex<HashMap<IpAddr, u64>>,
}

impl<F> SimProber<F>
where
    F: Fn(IpAddr, u64) -> Sim + Send + Sync,
{
    /// A prober where the `n`th probe (from zero) to `addr` is treated as
    /// `behave(addr, n)` says.
    pub fn new(timeout: Duration, behave: F) -> Self {
        Self {
            timeout,
            permitted: true,
            behave,
            sent: Mutex::new(HashMap::new()),
        }
    }

    /// The same prober on a system that refuses unprivileged pings.
    #[must_use]
    pub fn forbidden(mut self) -> Self {
        self.permitted = false;
        self
    }
}

impl<F> Prober for SimProber<F>
where
    F: Fn(IpAddr, u64) -> Sim + Send + Sync,
{
    fn preflight(&self) -> Result<(), ProbeError> {
        if self.permitted {
            Ok(())
        } else {
            Err(ProbeError::NotPermitted(io::Error::from(
                io::ErrorKind::PermissionDenied,
            )))
        }
    }

    async fn probe(&self, addr: IpAddr) -> Result<ProbeOutcome, ProbeError> {
        let nth = {
            let mut sent = self.sent.lock().unwrap_or_else(PoisonError::into_inner);
            let count = sent.entry(addr).or_default();
            *count += 1;
            *count - 1
        };
        match (self.behave)(addr, nth) {
            Sim::Reply(rtt) if rtt <= self.timeout => {
                tokio::time::sleep(rtt).await;
                Ok(ProbeOutcome::Reply { rtt })
            }
            Sim::Reply(_) | Sim::Silent => {
                tokio::time::sleep(self.timeout).await;
                Ok(ProbeOutcome::Lost(LossReason::Timeout))
            }
        }
    }
}
