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
use risingwave_pb::catalog::PbTable;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::user::grant_privilege::Action;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query};

use super::privilege::resolve_relation_privileges;
use super::RwPgResponse;
use crate::binder::{Binder, BoundQuery, BoundSetExpr};
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::Explain;
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef};
use crate::planner::Planner;
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;

pub(super) fn get_column_names(
    bound: &BoundQuery,
    session: &SessionImpl,
    columns: Vec<Ident>,
) -> Result<Option<Vec<String>>> {
    // If columns is empty, it means that the user did not specify the column names.
    // In this case, we extract the column names from the query.
    // If columns is not empty, it means that user specify the column names and the user
    // should guarantee that the column names number are consistent with the query.
    let col_names: Option<Vec<String>> = if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|v| v.real_value()).collect())
    };

    if let BoundSetExpr::Select(select) = &bound.body {
        // `InputRef`'s alias will be implicitly assigned in `bind_project`.
        // If user provide columns name (col_names.is_some()), we don't need alias.
        // For other expressions (col_names.is_none()), we require the user to explicitly assign an
        // alias.
        if col_names.is_none() {
            for (i, alias) in select.aliases.iter().enumerate() {
                if alias.is_none() {
                    return Err(ErrorCode::BindError(format!(
                    "An alias must be specified for the {} expression (counting from 1) in result relation", ordinal(i+1)
                ))
                .into());
                }
            }
        }
        if let Some(relation) = &select.from {
            let mut check_items = Vec::new();
            resolve_relation_privileges(relation, Action::Select, &mut check_items);
            session.check_privileges(&check_items)?;
        }
    }

    Ok(col_names)
}

/// Generate create MV plan, return plan and mv table info.
pub fn gen_create_mv_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    query: Query,
    name: ObjectName,
    columns: Vec<Ident>,
) -> Result<(PlanRef, PbTable)> {
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, name)?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_views, bound) = {
        let mut binder = Binder::new_for_stream(session);
        let bound = binder.bind_query(query)?;
        (binder.shared_views(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    let col_names = get_column_names(&bound, session, columns)?;

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = col_names {
        plan_root.set_out_names(col_names)?;
    }
    let materialize = plan_root.gen_materialize_plan(table_name, definition)?;
    let mut table = materialize.table().to_prost(schema_id, database_id);
    if session.config().get_create_compaction_group_for_mv() {
        table.properties.insert(
            String::from("independent_compaction_group"),
            String::from("1"),
        );
    }
    let plan: PlanRef = materialize.into();
    table.owner = session.user_id();

    // record dependent views.
    table.dependent_relations.extend(dependent_views);

    let ctx = plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Materialized View:");
        ctx.trace(plan.explain_to_string().unwrap());
    }

    Ok((plan, table))
}

pub async fn handle_create_mv(
    handler_args: HandlerArgs,
    name: ObjectName,
    query: Query,
    columns: Vec<Ident>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    let has_order_by = !query.order_by.is_empty();

    session.check_relation_name_duplicated(name.clone())?;
    let mut notice = String::new();

    let (table, graph) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, table) = gen_create_mv_plan(&session, context.into(), query, name, columns)?;
        let context = plan.plan_base().ctx.clone();
        let mut graph = build_graph(plan);
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        // Set the timezone for the stream environment
        let env = graph.env.as_mut().unwrap();
        env.timezone = context.get_session_timezone();
        context.append_notice(&mut notice);

        (table, graph)
    };

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                table.database_id,
                table.schema_id,
                table.name.clone(),
            ));

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_materialized_view(table, graph)
        .await?;

    if has_order_by {
        notice.push_str(r#"
The ORDER BY clause in the CREATE MATERIALIZED VIEW statement does not guarantee that the rows selected out of this materialized view is returned in this order.
It only indicates the physical clustering of the data, which may improve the performance of queries issued against this materialized view."#);
    }
    Ok(PgResponse::empty_result_with_notice(
        StatementType::CREATE_MATERIALIZED_VIEW,
        notice.to_string(),
    ))
}

fn ordinal(i: usize) -> String {
    let s = i.to_string();
    let suffix = if s.ends_with('1') && !s.ends_with("11") {
        "st"
    } else if s.ends_with('2') && !s.ends_with("12") {
        "nd"
    } else if s.ends_with('3') && !s.ends_with("13") {
        "rd"
    } else {
        "th"
    };
    s + suffix
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW;
    use risingwave_common::catalog::{
        row_id_column_name, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_mv_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kinesis')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 with (ttl = 300) as select t1.country from t1";
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
        let (table, _) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let city_type = DataType::new_struct(
            vec![DataType::Varchar, DataType::Varchar],
            vec!["address".to_string(), "zipcode".to_string()],
        );
        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Serial,
            "country" => DataType::new_struct(
                 vec![DataType::Varchar,city_type,DataType::Varchar],
                 vec!["address".to_string(), "city".to_string(), "zipcode".to_string()],
            )
        };
        assert_eq!(columns, expected_columns);
    }

    /// When creating MV, a unique column name must be specified for each column
    #[tokio::test]
    async fn test_no_alias() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = "create table t(x varchar)";
        frontend.run_sql(sql).await.unwrap();

        // Aggregation without alias is ok.
        let sql = "create materialized view mv0 as select count(x) from t";
        frontend.run_sql(sql).await.unwrap();

        // Same aggregations without alias is forbidden, because it make the same column name.
        let sql = "create materialized view mv1 as select count(x), count(*) from t";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid input syntax: column \"count\" specified more than once"
        );

        // Literal without alias is forbidden.
        let sql = "create materialized view mv1 as select 1";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for the 1st expression (counting from 1) in result relation"
        );

        // some expression without alias is forbidden.
        let sql = "create materialized view mv1 as select x is null from t";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for the 1st expression (counting from 1) in result relation"
        );
    }

    /// Creating MV with order by returns a special notice
    #[tokio::test]
    async fn test_create_mv_with_order_by() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = "create table t(x varchar)";
        frontend.run_sql(sql).await.unwrap();

        // Without order by
        let sql = "create materialized view mv1 as select * from t";
        let response = frontend.run_sql(sql).await.unwrap();
        assert_eq!(response.get_stmt_type(), CREATE_MATERIALIZED_VIEW);
        assert_eq!(response.get_notice(), None);

        // With order by
        let sql = "create materialized view mv2 as select * from t order by x";
        let response = frontend.run_sql(sql).await.unwrap();
        assert_eq!(response.get_stmt_type(), CREATE_MATERIALIZED_VIEW);
        assert_eq!(
            response.get_notice().unwrap(),
r#"
The ORDER BY clause in the CREATE MATERIALIZED VIEW statement does not guarantee that the rows selected out of this materialized view is returned in this order.
It only indicates the physical clustering of the data, which may improve the performance of queries issued against this materialized view."#
                .to_string()
        );
    }
}
