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

use pgwire::pg_response::StatementType;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Ident, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::CatalogError;
use crate::Binder;

pub async fn handle_alter_table_owner(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_owner_name: Ident,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let table_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_table_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        table.id.table_id
    };

    let user_id = {
        let reader = session.env().user_info_reader().read_guard();
        let user_name = new_owner_name.real_value();
        let user_info = reader
            .get_user_by_name(&user_name)
            .ok_or(CatalogError::NotFound("user", user_name))?
            .to_prost();

        user_info.get_id()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.alter_table_owner(table_id, user_id).await?;
    Ok(RwPgResponse::empty_result(StatementType::ALTER_TABLE))
}
