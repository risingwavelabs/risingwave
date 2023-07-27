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
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{get_acl_items, BuiltinTable, SysCatalogReaderImpl};

pub const PG_NAMESPACE_TABLE_NAME: &str = "pg_namespace";

/// The catalog `pg_namespace` stores namespaces. A namespace is the structure underlying SQL
/// schemas: each namespace can have a separate collection of relations, types, etc. without name
/// conflicts. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-namespace.html`]
pub const PG_NAMESPACE: BuiltinTable = BuiltinTable {
    name: PG_NAMESPACE_TABLE_NAME,
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "nspname"),
        (DataType::Int32, "nspowner"),
        (DataType::Varchar, "nspacl"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_namespace(&self) -> Result<Vec<OwnedRow>> {
        let schemas = self
            .catalog_reader
            .read_guard()
            .get_all_schema_info(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();
        Ok(schemas
            .iter()
            .map(|schema| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(schema.id as i32)),
                    Some(ScalarImpl::Utf8(schema.name.clone().into())),
                    Some(ScalarImpl::Int32(schema.owner as i32)),
                    Some(ScalarImpl::Utf8(
                        get_acl_items(&Object::SchemaId(schema.id), &users, username_map).into(),
                    )),
                ])
            })
            .collect_vec())
    }
}
