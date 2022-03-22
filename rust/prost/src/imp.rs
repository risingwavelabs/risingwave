//! Custom implementation for prost types

use std::net::{AddrParseError, SocketAddr};

use crate::data::DataType;
use crate::plan::ColumnDesc;

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

impl crate::plan::ColumnDesc {
    // Used for test
    pub fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            column_type: Some(data_type),
            column_id,
            name: name.to_string(),
            field_descs: vec![],
            type_name: String::new(),
        }
    }

    // Used for test
    pub fn new_struct(
        data_type: DataType,
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnDesc>,
    ) -> Self {
        Self {
            column_type: Some(data_type),
            column_id,
            name: name.to_string(),
            type_name: type_name.to_string(),
            field_descs: fields,
        }
    }
}
