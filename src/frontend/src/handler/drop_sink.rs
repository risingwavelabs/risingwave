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

use std::collections::HashSet;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_pb::ddl_service::{ReplaceJobPlan, replace_job_plan};
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::handler::alter_table_column::hijack_merger_for_target_table;
use crate::handler::create_sink::{fetch_incoming_sinks, reparse_table_for_sink};

pub async fn handle_drop_sink(
    handler_args: HandlerArgs,
    sink_name: ObjectName,
    if_exists: bool,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, sink_name) = Binder::resolve_schema_qualified_name(db_name, sink_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let sink = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (sink, schema_name) =
            match catalog_reader.get_sink_by_name(db_name, schema_path, &sink_name) {
                Ok((sink, schema)) => (sink.clone(), schema),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::builder(StatementType::DROP_SINK)
                            .notice(format!("sink \"{}\" does not exist, skipping", sink_name))
                            .into())
                    } else {
                        Err(e.into())
                    };
                }
            };

        session.check_privilege_for_drop_alter(schema_name, &*sink)?;

        sink
    };

    let sink_id = sink.id;

    let mut affected_table_change = None;
    if let Some(target_table_id) = &sink.target_table {
        let table_catalog = {
            let reader = session.env().catalog_reader().read_guard();
            let table = reader.get_any_table_by_id(target_table_id)?;
            table.clone()
        };

        let (mut graph, mut table, source, target_job_type) =
            reparse_table_for_sink(&session, &table_catalog).await?;

        assert!(!table_catalog.incoming_sinks.is_empty());

        table
            .incoming_sinks
            .clone_from(&table_catalog.incoming_sinks);

        let mut incoming_sink_ids: HashSet<_> =
            table_catalog.incoming_sinks.iter().copied().collect();

        assert!(incoming_sink_ids.remove(&sink_id.sink_id));

        let columns_without_rw_timestamp = table_catalog.columns_without_rw_timestamp();
        for sink in fetch_incoming_sinks(&session, &incoming_sink_ids)? {
            hijack_merger_for_target_table(
                &mut graph,
                &columns_without_rw_timestamp,
                &sink,
                Some(&sink.unique_identity()),
            )?;
        }

        affected_table_change = Some(ReplaceJobPlan {
            replace_job: Some(replace_job_plan::ReplaceJob::ReplaceTable(
                replace_job_plan::ReplaceTable {
                    table: Some(table),
                    source,
                    job_type: target_job_type as _,
                },
            )),
            fragment_graph: Some(graph),
            table_col_index_mapping: None,
        });
    }

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .drop_sink(sink_id.sink_id, cascade, affected_table_change)
        .await?;

    Ok(PgResponse::empty_result(StatementType::DROP_SINK))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_sink_handler() {
        let sql_create_table = "create table t (v1 smallint primary key);";
        let sql_create_mv = "create materialized view mv as select v1 from t;";
        let sql_create_sink = "create sink snk from mv with( connector = 'kafka')";
        let sql_drop_sink = "drop sink snk;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_create_mv).await.unwrap();
        frontend.run_sql(sql_create_sink).await.unwrap();
        frontend.run_sql(sql_drop_sink).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sink =
            catalog_reader.get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "snk");
        assert!(sink.is_err());
    }
}
