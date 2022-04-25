//! Custom implementation for prost types

use std::net::{AddrParseError, SocketAddr};

use crate::common::HostAddress;
use crate::data::data_type::TypeName;
use crate::data::DataType;
use crate::plan_common::ColumnDesc;

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

impl TryInto<SocketAddr> for HostAddress {
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

    /// Return column_descs from source info
    pub fn get_column_descs(&self) -> Vec<ColumnDesc> {
        let catalogs = match self.get_info().unwrap() {
            crate::catalog::source::Info::StreamSource(info) => &info.columns,
            crate::catalog::source::Info::TableSource(info) => &info.columns,
        };
        catalogs
            .iter()
            .filter(|c| !c.is_hidden)
            .map(|c| c.column_desc.as_ref().cloned().unwrap())
            .collect()
    }
}

impl crate::plan_common::ColumnDesc {
    // Used for test
    pub fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            column_type: Some(data_type),
            column_id,
            name: name.to_string(),
            ..Default::default()
        }
    }

    // Used for test
    pub fn new_struct(
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnDesc>,
    ) -> Self {
        Self {
            column_type: Some(DataType {
                type_name: TypeName::Struct as i32,
                is_nullable: true,
                ..Default::default()
            }),
            column_id,
            name: name.to_string(),
            type_name: type_name.to_string(),
            field_descs: fields,
        }
    }
}
