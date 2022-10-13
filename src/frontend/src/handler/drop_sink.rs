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
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_super_user;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::handler::drop_table::check_source;
use crate::session::OptimizerContext;

pub async fn handle_drop_sink(
    context: OptimizerContext,
    sink_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let (schema_name, sink_name) = Binder::resolve_table_name(sink_name)?;

    let catalog_reader = session.env().catalog_reader();

    check_source(catalog_reader, session.clone(), &schema_name, &sink_name)?;

    let sink = match catalog_reader.read_guard().get_sink_by_name(
        session.database(),
        &schema_name,
        &sink_name,
    ) {
        Ok(s) => s.clone(),
        Err(e) => {
            return if if_exists {
                Ok(RwPgResponse::empty_result_with_notice(
                    StatementType::DROP_SINK,
                    format!("NOTICE: sink {} does not exist, skipping", sink_name),
                ))
            } else {
                Err(e)
            }
        }
    };

    let schema_owner = catalog_reader
        .read_guard()
        .get_schema_by_name(session.database(), &schema_name)
        .unwrap()
        .owner();
    if sink.owner != session.user_id()
        && session.user_id() != schema_owner
        && !check_super_user(&session)
    {
        return Err(PermissionDenied("Do not have the privilege".to_string()).into());
    }

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_sink(sink.id).await?;

    Ok(PgResponse::empty_result(StatementType::DROP_SINK))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_sink_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_create_mv = "create materialized view mv as select v1 from t;";
        let sql_create_sink = "create sink snk from mv with( connector = 'mysql')";
        let sql_drop_sink = "drop sink snk;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_create_mv).await.unwrap();
        frontend.run_sql(sql_create_sink).await.unwrap();
        frontend.run_sql(sql_drop_sink).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        let sink = catalog_reader
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "snk")
            .ok()
            .cloned();
        assert!(sink.is_none());
    }
}
