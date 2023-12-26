// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_CONNECTIONS: BuiltinTable = BuiltinTable {
    name: "rw_connections",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "id"),
        (DataType::Varchar, "name"),
        (DataType::Int32, "schema_id"),
        (DataType::Int32, "owner"),
        (DataType::Varchar, "type"),
        (DataType::Varchar, "provider"),
        (DataType::Varchar, "acl"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_rw_connections_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_connections().map(|conn| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(conn.id as i32)),
                        Some(ScalarImpl::Utf8(conn.name.clone().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(conn.owner as i32)),
                        Some(ScalarImpl::Utf8(conn.connection_type().into())),
                        Some(ScalarImpl::Utf8(conn.provider().into())),
                        Some(ScalarImpl::Utf8("".into())),
                    ])
                })
            })
            .collect_vec())
    }
}
