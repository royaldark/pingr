//! Real ICMP against this machine's loopback addresses.
//!
//! These are the only tests that prove unprivileged ping works on the
//! operating system they run on. They must pass on Linux, macOS, and Windows;
//! a platform where they cannot pass is a platform pingr does not support.

use std::net::IpAddr;
use std::time::Duration;

use pingr_engine::prober::icmp::IcmpProber;
use pingr_engine::prober::{ProbeOutcome, Prober};

async fn loopback_answers(addr: &str) {
    let addr: IpAddr = addr.parse().expect("literal address");
    let prober = IcmpProber::new(Duration::from_secs(2));

    for attempt in 0..3 {
        let outcome = prober
            .probe(addr)
            .await
            .unwrap_or_else(|error| panic!("probe {attempt} to {addr} not sent: {error:?}"));
        assert!(
            matches!(outcome, ProbeOutcome::Reply { .. }),
            "probe {attempt} to {addr} got no reply: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn ipv4_loopback_answers_without_elevation() {
    loopback_answers("127.0.0.1").await;
}

#[tokio::test]
async fn ipv6_loopback_answers_without_elevation() {
    loopback_answers("::1").await;
}
