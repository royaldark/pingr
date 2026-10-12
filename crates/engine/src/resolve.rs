//! Turning a target's name into an address to probe.

use std::future::Future;
use std::io;
use std::net::IpAddr;

/// Looks up the addresses of a host name.
pub trait Resolver: Send + Sync {
    /// The addresses `name` currently resolves to, in the order to prefer them.
    fn resolve(&self, name: &str) -> impl Future<Output = io::Result<Vec<IpAddr>>> + Send;
}

/// The operating system's resolver.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemResolver;

impl Resolver for SystemResolver {
    async fn resolve(&self, name: &str) -> io::Result<Vec<IpAddr>> {
        let addresses = tokio::net::lookup_host((name, 0)).await?;
        Ok(addresses.map(|address| address.ip()).collect())
    }
}
