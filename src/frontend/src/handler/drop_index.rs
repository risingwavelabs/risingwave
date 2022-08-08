// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::handler::drop_table::check_source;
use crate::session::OptimizerContext;

pub async fn handle_drop_index(
    context: OptimizerContext,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, index_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader();

    check_source(catalog_reader, session.clone(), &schema_name, &index_name)?;
    let index_id = {
        let reader = catalog_reader.read_guard();
        let index_result = reader.get_index_by_name(session.database(), &schema_name, &index_name);

        if index_result.is_err() {
            let table_result =
                reader.get_table_by_name(session.database(), &schema_name, &index_name);
            if table_result.is_ok() {
                let table = table_result.unwrap();
                // If associated source is `Some`, then it is a actually a materialized source /
                // table v2.
                if table.associated_source_id().is_some() {
                    return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                        "Use `DROP TABLE` to drop a table.".to_owned(),
                    )));
                }

                // If is index on is `None`, then it is a actually a materialized view.
                assert!(table.is_index_on.is_none());
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "Use `DROP MATERIALIZED VIEW` to drop a materialized view.".to_owned(),
                )));
            }

            return Err(index_result.unwrap_err());
        }

        let index = index_result.unwrap();
        if session.user_id() != index.index_table.owner {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }

        index.id
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_index(index_id).await?;

    Ok(PgResponse::empty_result(StatementType::DROP_INDEX))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_index_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_create_index = "create index idx on t(v1);";
        let sql_drop_index = "drop index idx;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_create_index).await.unwrap();
        frontend.run_sql(sql_drop_index).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        let table = catalog_reader
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "idx")
            .ok()
            .cloned();
        assert!(table.is_none());
    }
}
