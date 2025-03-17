// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{SysCatalogReaderImpl, get_acl_items};
use crate::error::Result;

#[derive(Fields)]
struct RwFunction {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    r#type: String,
    arg_type_ids: Vec<i32>,
    return_type_id: i32,
    language: String,
    link: Option<String>,
    acl: Vec<String>,
    always_retry_on_network_error: bool,
}

#[system_catalog(table, "rw_catalog.rw_functions")]
fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwFunction>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema.iter_function().map(|function| RwFunction {
                id: function.id.function_id() as i32,
                name: function.name.clone(),
                schema_id: schema.id() as i32,
                owner: function.owner as i32,
                r#type: function.kind.to_string(),
                arg_type_ids: function.arg_types.iter().map(|t| t.to_oid()).collect(),
                return_type_id: function.return_type.to_oid(),
                language: function.language.clone(),
                link: function.link.clone(),
                acl: get_acl_items(
                    &Object::FunctionId(function.id.function_id()),
                    false,
                    &users,
                    username_map,
                ),
                always_retry_on_network_error: function.always_retry_on_network_error,
            })
        })
        .collect())
}
