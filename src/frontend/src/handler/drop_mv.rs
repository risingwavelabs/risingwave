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

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::valid_table_name;
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_super_user;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::handler::drop_table::check_source;
use crate::session::OptimizerContext;

pub async fn handle_drop_mv(
    context: OptimizerContext,
    table_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_table_or_source_name(db_name, table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = match schema_name.as_deref() {
        Some(schema_name) => SchemaPath::Name(schema_name),
        None => SchemaPath::Path(&search_path, user_name),
    };

    let (table_id, index_ids) = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            match reader.get_table_by_name(session.database(), schema_path, &table_name) {
                Ok((t, s)) => (t, s),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::empty_result_with_notice(
                            StatementType::DROP_MATERIALIZED_VIEW,
                            format!(
                                "NOTICE: materialized view {} does not exist, skipping",
                                table_name
                            ),
                        ))
                    } else {
                        Err(e)
                    }
                }
            };

        let schema_catalog = reader
            .get_schema_by_name(session.database(), schema_name)
            .unwrap();
        let schema_owner = schema_catalog.owner();
        if session.user_id() != table.owner
            && session.user_id() != schema_owner
            && !check_super_user(&session)
        {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }

        // If associated source is `Some`, then it is actually a materialized source / table v2.
        if table.associated_source_id().is_some() {
            check_source(&reader, db_name, schema_name, &table_name)?;
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "Use `DROP TABLE` to drop a table.".to_owned(),
            )));
        }

        // If is index on is `Some`, then it is actually an index.
        if table.is_index {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "Use `DROP INDEX` to drop an index.".to_owned(),
            )));
        }

        // If the name is not valid, then it is actually an internal table.
        if !valid_table_name(&table_name) {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "Cannot drop an internal table.".to_owned(),
            )));
        }

        let index_ids = schema_catalog
            .get_indexes_by_table_id(&table.id)
            .iter()
            .map(|x| x.id)
            .collect_vec();

        (table.id(), index_ids)
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .drop_materialized_view(table_id, index_ids)
        .await?;

    Ok(PgResponse::empty_result(
        StatementType::DROP_MATERIALIZED_VIEW,
    ))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_mv_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_create_mv = "create materialized view mv as select v1 from t;";
        let sql_drop_mv = "drop materialized view mv;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_create_mv).await.unwrap();
        frontend.run_sql(sql_drop_mv).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let table = catalog_reader.get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv");
        assert!(table.is_err());
    }
}
