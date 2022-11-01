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

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::session::OptimizerContext;

pub async fn handle_drop_index(
    context: OptimizerContext,
    index_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let db_name = session.database();
    let (schema_name, index_name) =
        Binder::resolve_schema_qualified_index_name(db_name, index_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = match schema_name.as_deref() {
        Some(schema_name) => SchemaPath::Name(schema_name),
        None => SchemaPath::Path(&search_path, user_name),
    };

    let index_id = {
        let reader = session.env().catalog_reader().read_guard();
        match reader.get_index_by_name(db_name, schema_path, &index_name) {
            Ok((index, _)) => {
                if session.user_id() != index.index_table.owner {
                    return Err(PermissionDenied("Do not have the privilege".to_string()).into());
                }

                index.id
            }
            Err(err) => {
                return match reader.get_table_by_name(db_name, schema_path, &index_name) {
                    Ok((table, _)) => {
                        // If associated source is `Some`, then it is a actually a materialized
                        // source / table v2.
                        if table.associated_source_id().is_some() {
                            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                                "Use `DROP TABLE` to drop a table.".to_owned(),
                            )));
                        }

                        Err(RwError::from(ErrorCode::InvalidInputSyntax(
                            "Use `DROP MATERIALIZED VIEW` to drop a materialized view.".to_owned(),
                        )))
                    }
                    Err(_) => {
                        if if_exists {
                            Ok(RwPgResponse::empty_result_with_notice(
                                StatementType::DROP_INDEX,
                                format!("index \"{}\" does not exist, skipping", index_name),
                            ))
                        } else {
                            Err(err)
                        }
                    }
                };
            }
        }
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_index(index_id).await?;

    Ok(PgResponse::empty_result(StatementType::DROP_INDEX))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
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
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let table = catalog_reader.get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "idx");
        assert!(table.is_err());
    }
}
