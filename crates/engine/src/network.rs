//! Which network the machine is on, so that samples taken on one network are
//! never mixed with those from another.
//!
//! A network is recognized by the hardware address of its default gateway:
//! that needs no privileges, works the same over Wi-Fi and Ethernet, and stays
//! put for as long as the router does. Nothing outside this module names the
//! crate that reads it.
//!
//! The Wi-Fi network's name, where the system will tell it, only suggests a
//! label for a network seen for the first time.

mod ssid;

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

/// A network as the system reports it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Observed {
    /// The default gateway's hardware address, lowercase and colon-separated.
    /// This alone identifies the network.
    pub gateway_mac: String,
    /// The Wi-Fi network's name, if the default route is over Wi-Fi and the
    /// system would say.
    pub ssid: Option<String>,
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
            Self::Detected(Observed {
                gateway_mac,
                ssid: Some(ssid),
            }) => write!(f, "Wi-Fi network {ssid} (gateway {gateway_mac})"),
            Self::Detected(Observed {
                gateway_mac,
                ssid: None,
            }) => write!(f, "the network with gateway {gateway_mac}"),
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
#[derive(Debug, Default)]
pub struct SystemNetwork {
    /// The last network seen. Its Wi-Fi name is looked up only when the
    /// gateway changes, since that means running another program.
    last: Arc<Mutex<Option<Observed>>>,
}

impl NetworkDetector for SystemNetwork {
    async fn detect(&self) -> Option<Observed> {
        let last = Arc::clone(&self.last);
        // Reading routes and neighbors is quick, but it is blocking I/O.
        tokio::task::spawn_blocking(move || {
            let interface = netdev::get_default_interface().ok()?;
            // An all-zero address means the gateway is known but not resolved.
            let octets = interface.gateway.as_ref()?.mac_addr.octets();
            if octets == [0; 6] {
                return None;
            }
            let gateway_mac = mac(octets);
            let mut last = last.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(known) = last
                .as_ref()
                .filter(|known| known.gateway_mac == gateway_mac)
            {
                return Some(known.clone());
            }
            let wireless = interface.if_type == netdev::prelude::InterfaceType::Wireless80211;
            let observed = Observed {
                gateway_mac,
                ssid: wireless
                    .then(|| ssid::lookup(&interface.name, interface.friendly_name.as_deref()))
                    .flatten(),
            };
            *last = Some(observed.clone());
            Some(observed)
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
