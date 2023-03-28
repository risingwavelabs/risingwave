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
use risingwave_common::catalog::{DatabaseId, SchemaId, UserId};
use risingwave_common::error::Result;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_sqlparser::ast::{
    CreateSink, CreateSinkStatement, ObjectName, Query, Select, SelectItem, SetExpr, TableFactor,
    TableWithJoins,
};

use super::create_mv::get_column_names;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::Explain;
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::Planner;

pub fn gen_sink_query_from_name(from_name: ObjectName) -> Result<Query> {
    let table_factor = TableFactor::Table {
        name: from_name,
        alias: None,
        for_system_time_as_of_now: false,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard],
        ..Default::default()
    };
    let body = SetExpr::Select(Box::new(select));
    Ok(Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    })
}

pub fn gen_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSinkStatement,
) -> Result<(PlanRef, SinkCatalog)> {
    let db_name = session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.sink_name.clone())?;

    let query = match stmt.sink_from {
        CreateSink::From(from_name) => Box::new(gen_sink_query_from_name(from_name)?),
        CreateSink::AsQuery(query) => query,
    };

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name)?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_relations, bound) = {
        let mut binder = Binder::new(session);
        let bound = binder.bind_query(*query)?;
        (binder.including_relations(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    // If column names not specified, use the name in materialized view.
    let col_names = get_column_names(&bound, session, stmt.columns)?;

    let properties = context.with_options().clone();

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = col_names {
        plan_root.set_out_names(col_names)?;
    };

    let sink_plan = plan_root.gen_sink_plan(sink_table_name, definition, properties)?;

    let sink_desc = sink_plan.sink_desc().clone();
    let sink_catalog = sink_desc.into_catalog(
        SchemaId::new(sink_schema_id),
        DatabaseId::new(sink_database_id),
        UserId::new(session.user_id()),
        dependent_relations,
    );

    let sink_plan: PlanRef = sink_plan.into();

    let ctx = sink_plan.ctx();

    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string().unwrap());
    }

    Ok((sink_plan, sink_catalog))
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_relation_name_duplicated(stmt.sink_name.clone())?;

    let (sink, graph) = {
        let context = OptimizerContext::from_handler_args(handle_args);
        let (plan, sink) = gen_sink_plan(&session, context.into(), stmt)?;
        let mut graph = build_graph(plan);
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        (sink, graph)
    };

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                sink.database_id.database_id,
                sink.schema_id.schema_id,
                sink.name.clone(),
            ));

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_sink(sink.to_proto(), graph).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}';"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH (connector = 'mysql', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_string();
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, schema_name) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_sink_by_name(DEFAULT_DATABASE_NAME, SchemaPath::Name(schema_name), "snk1")
            .unwrap();
        assert_eq!(sink.name, "snk1");
    }
}
