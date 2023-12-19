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

use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{
    get_acl_items, BuiltinTable, SysCatalogReaderImpl, SystemCatalogColumnsDef,
};
use crate::handler::create_source::UPSTREAM_SOURCE_KEY;

pub static RW_SOURCES_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "id"),
        (DataType::Varchar, "name"),
        (DataType::Int32, "schema_id"),
        (DataType::Int32, "owner"),
        (DataType::Varchar, "connector"),
        // [col1, col2]
        (DataType::List(Box::new(DataType::Varchar)), "columns"),
        (DataType::Varchar, "format"),
        (DataType::Varchar, "row_encode"),
        (DataType::Boolean, "append_only"),
        (DataType::Int32, "connection_id"),
        (DataType::Varchar, "definition"),
        (DataType::Varchar, "acl"),
        (DataType::Timestamptz, "initialized_at"),
        (DataType::Timestamptz, "created_at"),
    ]
});

pub static RW_SOURCES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_sources",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &RW_SOURCES_COLUMNS,
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub fn read_rw_sources_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema
                    .iter_source()
                    .filter(|s| s.associated_table_id.is_none())
                    .map(|source| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(source.id as i32)),
                            Some(ScalarImpl::Utf8(source.name.clone().into())),
                            Some(ScalarImpl::Int32(schema.id() as i32)),
                            Some(ScalarImpl::Int32(source.owner as i32)),
                            Some(ScalarImpl::Utf8(
                                source
                                    .with_properties
                                    .get(UPSTREAM_SOURCE_KEY)
                                    .cloned()
                                    .unwrap_or("".to_string())
                                    .to_uppercase()
                                    .into(),
                            )),
                            Some(ScalarImpl::List(ListValue::from_iter(
                                source.columns.iter().map(|c| c.name()),
                            ))),
                            source
                                .info
                                .get_format()
                                .map(|format| Some(ScalarImpl::Utf8(format.as_str_name().into())))
                                .unwrap_or(None),
                            source
                                .info
                                .get_row_encode()
                                .map(|row_encode| {
                                    Some(ScalarImpl::Utf8(row_encode.as_str_name().into()))
                                })
                                .unwrap_or(None),
                            Some(ScalarImpl::Bool(source.append_only)),
                            source.connection_id.map(|id| ScalarImpl::Int32(id as i32)),
                            Some(ScalarImpl::Utf8(source.create_sql().into())),
                            Some(
                                get_acl_items(
                                    &Object::SourceId(source.id),
                                    false,
                                    &users,
                                    username_map,
                                )
                                .into(),
                            ),
                            source.initialized_at_epoch.map(|e| e.as_scalar()),
                            source.created_at_epoch.map(|e| e.as_scalar()),
                        ])
                    })
            })
            .collect_vec())
    }
}
