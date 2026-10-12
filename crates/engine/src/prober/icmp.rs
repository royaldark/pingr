//! ICMP echo through each operating system's unprivileged path: datagram ICMP
//! sockets on Linux and macOS, the `IcmpSendEcho` family on Windows.

use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use ping_async::{IcmpEchoRequestor, IcmpEchoStatus};

use super::{LossReason, ProbeError, ProbeOutcome, Prober};

/// A [`Prober`] that sends real ICMP echo requests.
pub struct IcmpProber {
    timeout: Duration,
    /// One requestor per address, so sequence numbers and the reply socket are
    /// reused across probes to the same destination.
    requestors: Mutex<HashMap<IpAddr, IcmpEchoRequestor>>,
}

impl IcmpProber {
    /// A prober whose probes count as lost after `timeout` without an answer.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            requestors: Mutex::new(HashMap::new()),
        }
    }

    fn requestor(&self, addr: IpAddr) -> Result<IcmpEchoRequestor, ProbeError> {
        let mut requestors = self
            .requestors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(requestor) = requestors.get(&addr) {
            return Ok(requestor.clone());
        }
        let requestor = IcmpEchoRequestor::new(addr, None, None, Some(self.timeout))
            .map_err(|source| setup_error(addr, source))?;
        requestors.insert(addr, requestor.clone());
        Ok(requestor)
    }
}

impl Prober for IcmpProber {
    async fn probe(&self, addr: IpAddr) -> Result<ProbeOutcome, ProbeError> {
        let requestor = self.requestor(addr)?;
        // Timed here rather than taken from the backend, which reports whole
        // milliseconds on Windows and uses the wall clock elsewhere.
        let sent = Instant::now();
        let reply = requestor
            .send()
            .await
            .map_err(|source| ProbeError::Io { addr, source })?;
        let rtt = sent.elapsed();
        Ok(match reply.status() {
            IcmpEchoStatus::Success => ProbeOutcome::Reply { rtt },
            IcmpEchoStatus::TimedOut => ProbeOutcome::Lost(LossReason::Timeout),
            IcmpEchoStatus::Unreachable => ProbeOutcome::Lost(LossReason::Unreachable),
            IcmpEchoStatus::Unknown => ProbeOutcome::Lost(LossReason::Other),
        })
    }
}

fn setup_error(addr: IpAddr, source: io::Error) -> ProbeError {
    if source.kind() == io::ErrorKind::PermissionDenied {
        ProbeError::NotPermitted(source)
    } else {
        ProbeError::Io { addr, source }
    }
}
