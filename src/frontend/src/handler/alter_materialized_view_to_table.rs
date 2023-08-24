use anyhow::Context;
use itertools::Itertools;
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
use risingwave_common::catalog::ConflictBehavior;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::handler::create_mv::gen_create_mv_plan;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{PlanTreeNodeUnary, StreamDml, StreamExchange};
use crate::optimizer::property::Distribution;
use crate::optimizer::property::Distribution::HashShard;
use crate::{build_graph, Binder, OptimizerContext, PlanRef};

pub async fn handle_alter_materialized_view_to_table(
    handler_args: HandlerArgs,
    view_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_view_name) =
        Binder::resolve_schema_qualified_name(db_name, view_name.clone())?;

    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_table = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_view_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        table.clone()
    };

    // Retrieve the original table definition and parse it to AST.
    let [definition]: [_; 1] = Parser::parse_sql(&original_table.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;

    let Statement::CreateView { or_replace: _, materialized: _, if_not_exists: _, name, columns, query, emit_mode, with_options: _ } = definition else {
        panic!("unexpected statement: {:?}", definition);
    };

    let (graph, table) = {
        let context = OptimizerContext::from_handler_args(handler_args);

        let (plan, table) = gen_create_mv_plan(
            &session,
            context.into(),
            *query,
            name,
            columns,
            emit_mode,
            Some(ConflictBehavior::Overwrite),
        )?;

        if let Distribution::Single = plan.distribution() {
            return Err(RwError::from(ErrorCode::BindError(
                "cannot alter a single shard materialized view to table.".to_string(),
            )));
        }

        let materialize = plan
            .as_stream_materialize()
            .context("not a materialized view")?;

        let cols = materialize
            .table()
            .columns()
            .iter()
            .flat_map(|c| (!c.is_generated()).then(|| c.column_desc.clone()))
            .collect_vec();

        for col in &cols {
            if matches!(col.data_type, DataType::Serial) {
                return Err(RwError::from(ErrorCode::BindError(
                    "Cannot alter a materialized view with a column of Serial type to table."
                        .to_string(),
                )));
            }
        }

        let pk_index = original_table
            .pk()
            .iter()
            .map(|col| col.column_index)
            .collect_vec();

        let dml = StreamDml::new(materialize.input(), false, cols);
        let exchange = StreamExchange::new(dml.into(), HashShard(pk_index));
        let materialize = materialize.clone_with_input(exchange.into());
        let plan: PlanRef = materialize.into();
        let graph = StreamFragmentGraph {
            parallelism: session
                .config()
                .get_streaming_parallelism()
                .map(|parallelism| Parallelism { parallelism }),
            ..build_graph(plan)
        };

        // Fill the original table ID.
        let table = Table {
            id: original_table.id().table_id(),
            initialized_at_epoch: original_table.initialized_at_epoch.map(|e| e.0),
            created_at_epoch: original_table.created_at_epoch.map(|e| e.0),
            ..table
        };

        (graph, table)
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_materialized_view_to_table(table, graph)
        .await?;

    Ok(PgResponse::empty_result(
        StatementType::ALTER_MATERIALIZED_VIEW,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::catalog::{
        ConflictBehavior, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };

    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::table_catalog::TableType;
    use crate::test_utils::LocalFrontend;
    use crate::TableCatalog;

    #[tokio::test]
    async fn test_alter_mv_to_table_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = "create table t (v int primary key);";
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view m as select * from t;";
        frontend.run_sql(sql).await.unwrap();

        let get_table_by_name = |name: &str| -> Arc<TableCatalog> {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, name)
                .unwrap()
                .0
                .clone()
        };

        let table = get_table_by_name("m");

        assert!(table.version.is_none());
        assert_eq!(table.table_type, TableType::MaterializedView);

        let sql = "alter materialized view m to table";
        frontend.run_sql(sql).await.unwrap();

        let altered_table = get_table_by_name("m");

        assert!(altered_table.version.is_none());
        assert_eq!(altered_table.conflict_behavior, ConflictBehavior::Overwrite);
        assert_eq!(altered_table.table_type, TableType::Table);
    }
}
