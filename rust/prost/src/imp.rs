//! Custom implementation for prost types

use std::net::SocketAddr;
use std::str::FromStr;

use crate::common::HostAddress;

impl HostAddress {
    /// Convert `HostAddress` to `SocketAddr`.
    pub fn to_socket_addr(&self) -> Result<SocketAddr, <SocketAddr as FromStr>::Err> {
        SocketAddr::from_str(&format!("{}:{}", self.host, self.port))
    }
}
