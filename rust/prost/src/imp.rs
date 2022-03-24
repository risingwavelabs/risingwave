//! Custom implementation for prost types

use std::net::{AddrParseError, SocketAddr};

use crate::common::HostAddress;

impl crate::common::HostAddress {
    /// Convert `HostAddress` to `SocketAddr`.
    pub fn to_socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        Ok(SocketAddr::new(self.host.parse()?, self.port as u16))
    }
}

impl<'a> TryInto<SocketAddr> for &'a HostAddress {
    type Error = AddrParseError;

    fn try_into(self) -> Result<SocketAddr, AddrParseError> {
        self.to_socket_addr()
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
