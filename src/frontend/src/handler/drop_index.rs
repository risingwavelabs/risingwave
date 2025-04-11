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
use risingwave_common::session_config::RuntimeParameters;
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::ErrorCode::PermissionDenied;
use crate::error::Result;
use crate::handler::HandlerArgs;

pub async fn handle_drop_index(
    handler_args: HandlerArgs,
    index_name: ObjectName,
    if_exists: bool,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, index_name) = Binder::resolve_schema_qualified_name(db_name, index_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let index_id = {
        let reader = session.env().catalog_reader().read_guard();
        match reader.get_index_by_name(db_name, schema_path, &index_name) {
            Ok((index, _)) => {
                if session.user_id() != index.index_table.owner {
                    return Err(PermissionDenied("Do not have the privilege".to_owned()).into());
                }

                index.id
            }
            Err(err) => {
                match err {
                    CatalogError::NotFound("index", _) => {
                        // index not found, try to find table below to give a better error message
                    }
                    _ => return Err(err.into()),
                };
                return match reader.get_created_table_by_name(db_name, schema_path, &index_name) {
                    Ok((table, _)) => match table.table_type() {
                        TableType::Index => unreachable!(),
                        _ => Err(table.bad_drop_error()),
                    },
                    Err(e) => {
                        if if_exists {
                            Ok(RwPgResponse::builder(StatementType::DROP_INDEX)
                                .notice(format!(
                                    "index \"{}\" does not exist, skipping",
                                    index_name
                                ))
                                .into())
                        } else {
                            match e {
                                CatalogError::NotFound("table", name) => {
                                    Err(CatalogError::NotFound("index", name).into())
                                }
                                _ => Err(e.into()),
                            }
                        }
                    }
                };
            }
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.drop_index(index_id, cascade).await?;

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

        let table =
            catalog_reader.get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "idx");
        assert!(table.is_err());
    }
}
