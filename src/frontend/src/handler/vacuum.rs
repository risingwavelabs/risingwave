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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{Engine, TableId};
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_vacuum(
    handler_args: HandlerArgs,
    table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = &handler_args.session;
    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;

    let table_id = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let search_path = session.config().search_path();
        let user_name = session.user_name();
        let schema_path = crate::catalog::root_catalog::SchemaPath::new(
            schema_name.as_deref(),
            &search_path,
            &user_name,
        );

        let (table, _) = catalog_reader
            .get_created_table_by_name(db_name, schema_path, &table_name)
            .map_err(|_| {
                RwError::from(ErrorCode::CatalogError(
                    format!("table {} not found", table_name).into(),
                ))
            })?;

        if table.engine() != Engine::Iceberg {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "VACUUM can only be used on Iceberg engine tables, but table '{}' uses {:?} engine",
                table_name,
                table.engine()
            ))
            .into());
        }

        TableId::new(table.id().table_id)
    };

    session.env().meta_client().compact_table(table_id).await?;

    Ok(PgResponse::builder(StatementType::VACUUM).row_cnt(1).into())
}
