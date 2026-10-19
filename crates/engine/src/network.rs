//! Which network the machine is on, so that samples taken on one network are
//! never mixed with those from another.
//!
//! A network is recognized by the hardware address of its default gateway:
//! that needs no privileges, works the same over Wi-Fi and Ethernet, and stays
//! put for as long as the router does. Nothing outside this module names the
//! crate that reads it.

use std::fmt;
use std::future::Future;

/// A network as the system reports it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Observed {
    /// The default gateway's hardware address, lowercase and colon-separated.
    pub gateway_mac: String,
}

/// Which network a session ran on, before it has a store identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionNetwork {
    /// Detected from the system.
    Detected(Observed),
    /// Named by the user, with detection turned off.
    Named(String),
}

impl fmt::Display for SessionNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Detected(observed) => {
                write!(f, "the network with gateway {}", observed.gateway_mac)
            }
            Self::Named(label) => write!(f, "network {label}"),
        }
    }
}

/// Tells which network the machine is on.
pub trait NetworkDetector: Send + Sync {
    /// The current network, or `None` if it cannot be told (no default
    /// route, or the gateway's address is not known yet).
    fn detect(&self) -> impl Future<Output = Option<Observed>> + Send;
}

/// Asks the operating system.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemNetwork;

impl NetworkDetector for SystemNetwork {
    async fn detect(&self) -> Option<Observed> {
        // Reading routes and neighbors is quick, but it is blocking I/O.
        tokio::task::spawn_blocking(|| {
            let gateway = netdev::get_default_gateway().ok()?;
            // An all-zero address means the gateway is known but not resolved.
            let octets = gateway.mac_addr.octets();
            (octets != [0; 6]).then(|| Observed {
                gateway_mac: mac(octets),
            })
        })
        .await
        .ok()
        .flatten()
    }
}

/// `octets` as `8a:2d:ec:c5:f4:5f`.
fn mac(octets: [u8; 6]) -> String {
    octets.map(|octet| format!("{octet:02x}")).join(":")
}
