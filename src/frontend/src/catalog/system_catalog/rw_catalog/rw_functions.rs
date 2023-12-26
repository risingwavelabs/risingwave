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

pub static RW_FUNCTIONS_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "id"),
        (DataType::Varchar, "name"),
        (DataType::Int32, "schema_id"),
        (DataType::Int32, "owner"),
        (DataType::Varchar, "type"),
        (DataType::List(Box::new(DataType::Int32)), "arg_type_ids"),
        (DataType::Int32, "return_type_id"),
        (DataType::Varchar, "language"),
        (DataType::Varchar, "link"),
        (DataType::Varchar, "acl"),
    ]
});

pub static RW_FUNCTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_functions",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &RW_FUNCTIONS_COLUMNS,
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub fn read_rw_functions_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_function().map(|function| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(function.id.function_id() as i32)),
                        Some(ScalarImpl::Utf8(function.name.clone().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(function.owner as i32)),
                        Some(ScalarImpl::Utf8(function.kind.to_string().into())),
                        Some(ScalarImpl::List(ListValue::from_iter(
                            function.arg_types.iter().map(|t| t.to_oid()),
                        ))),
                        Some(ScalarImpl::Int32(function.return_type.to_oid())),
                        Some(ScalarImpl::Utf8(function.language.clone().into())),
                        Some(ScalarImpl::Utf8(function.link.clone().into())),
                        Some(ScalarImpl::Utf8(
                            get_acl_items(
                                &Object::FunctionId(function.id.function_id()),
                                false,
                                &users,
                                username_map,
                            )
                            .into(),
                        )),
                    ])
                })
            })
            .collect_vec())
    }
}
