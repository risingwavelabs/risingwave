//! Custom implementation for prost types

use std::net::{AddrParseError, SocketAddr};

impl crate::common::HostAddress {
    /// Convert `HostAddress` to `SocketAddr`.
    pub fn to_socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        Ok(SocketAddr::new(self.host.parse()?, self.port as u16))
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
