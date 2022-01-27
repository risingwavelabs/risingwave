//! Custom implementation for prost types

use std::net::{AddrParseError, SocketAddr};

use crate::common::HostAddress;

impl HostAddress {
    /// Convert `HostAddress` to `SocketAddr`.
    pub fn to_socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        Ok(SocketAddr::new(self.host.parse()?, self.port as u16))
    }
}
