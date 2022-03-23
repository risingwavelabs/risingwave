//! Custom implementation for prost types

use std::net::{SocketAddr, ToSocketAddrs};

impl crate::common::HostAddress {
    /// Convert `HostAddress` to `SocketAddr`.
    /// `HostAddress.host` may be a host name or an IP address.
    pub fn to_socket_addr(&self) -> Result<SocketAddr, std::io::Error> {
        match (self.host.clone(), self.port as u16)
            .to_socket_addrs() {
            Ok(mut addrs) => Ok(addrs.next().unwrap()),
            Err(err) => Err(err),
            }
    }
}

impl crate::meta::Table {
    pub fn is_materialized_view(&self) -> bool {
        matches!(
            self.get_info().unwrap(),
            crate::meta::table::Info::MaterializedView(_)
        )
    }

    pub fn is_stream_source(&self) -> bool {
        matches!(
            self.get_info().unwrap(),
            crate::meta::table::Info::StreamSource(_)
        )
    }

    pub fn is_table_source(&self) -> bool {
        matches!(
            self.get_info().unwrap(),
            crate::meta::table::Info::TableSource(_)
        )
    }
}

impl crate::catalog::Source {
    pub fn is_stream_source(&self) -> bool {
        matches!(
            self.get_info().unwrap(),
            crate::catalog::source::Info::StreamSource(_)
        )
    }

    pub fn is_table_source(&self) -> bool {
        matches!(
            self.get_info().unwrap(),
            crate::catalog::source::Info::TableSource(_)
        )
    }
}
