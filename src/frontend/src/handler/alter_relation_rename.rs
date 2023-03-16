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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::Binder;

pub async fn handle_rename_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let new_table_name = Binder::resolve_table_name(new_table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let table_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_table_name)?;
        match table.table_type {
            TableType::Table => {}
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        table.id
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .alter_table_name(table_id.table_id, &new_table_name)
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

pub async fn handle_rename_index(
    handler_args: HandlerArgs,
    index_name: ObjectName,
    new_index_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_index_name) =
        Binder::resolve_schema_qualified_name(db_name, index_name.clone())?;
    let new_index_name = Binder::resolve_index_name(new_index_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let index_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (index, schema_name) =
            reader.get_index_by_name(db_name, schema_path, &real_index_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**index)?;
        index.id
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .alter_index_name(index_id.index_id, &new_index_name)
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_INDEX))
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_alter_table_name_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = "create table t (i int, r real);";
        frontend.run_sql(sql).await.unwrap();

        let table_id = {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
                .unwrap()
                .0
                .id
        };

        // Alter table name.
        let sql = "alter table t rename to t1;";
        frontend.run_sql(sql).await.unwrap();

        let catalog_reader = session.env().catalog_reader().read_guard();
        let altered_table_name = catalog_reader
            .get_table_by_id(&table_id)
            .unwrap()
            .name()
            .to_string();
        assert_eq!(altered_table_name, "t1");
    }
}
