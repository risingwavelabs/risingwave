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
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::user::grant_privilege::Action;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query};

use super::privilege::{check_privileges, resolve_relation_privileges};
use super::RwPgResponse;
use crate::binder::{Binder, BoundSetExpr};
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::stream_fragmenter::build_graph;

/// Generate create MV plan, return plan and mv table info.
pub fn gen_create_mv_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    query: Query,
    name: ObjectName,
    columns: Vec<Ident>,
) -> Result<(PlanRef, ProstTable)> {
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, name)?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let definition = query.to_string();

    // If columns is empty, it means that the user did not specify the column names.
    // In this case, we extract the column names from the query.
    // If columns is not empty, it means that user specify the column names and the user
    // should guarantee that the column names number are consistent with the query.
    let col_names: Option<Vec<String>> = if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|v| v.value.clone()).collect())
    };

    let bound = {
        let mut binder = Binder::new(session);
        binder.bind_query(query)?
    };

    if let BoundSetExpr::Select(select) = &bound.body {
        // `InputRef`'s alias will be implicitly assigned in `bind_project`.
        // If user provide columns name (col_names.is_some()), we don't need alias.
        // For other expressions (col_names.is_none()), we require the user to explicitly assign an
        // alias.
        if col_names.is_none() && select.aliases.iter().any(Option::is_none) {
            return Err(ErrorCode::BindError(
                "An alias must be specified for an expression".to_string(),
            )
            .into());
        }
        if let Some(relation) = &select.from {
            let mut check_items = Vec::new();
            resolve_relation_privileges(relation, Action::Select, &mut check_items);
            check_privileges(session, &check_items)?;
        }
    }

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    // Check the col_names match number of columns in the query.
    if let Some(col_names) = &col_names {
        // calculate the number of unhidden columns
        let unhidden_len = plan_root
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| plan_root.out_fields().contains(*i))
            .count();
        if col_names.len() != unhidden_len {
            return Err(InternalError(
                "number of column names does not match number of columns".to_string(),
            )
            .into());
        }
    }
    let materialize = plan_root.gen_create_mv_plan(table_name, definition, col_names)?;
    let mut table = materialize.table().to_prost(schema_id, database_id);
    if session.config().get_create_compaction_group_for_mv() {
        table.properties.insert(
            String::from("independent_compaction_group"),
            String::from("1"),
        );
    }
    let plan: PlanRef = materialize.into();
    table.owner = session.user_id();

    let ctx = plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Materialized View:");
        ctx.trace(plan.explain_to_string().unwrap());
    }

    Ok((plan, table))
}

pub async fn handle_create_mv(
    context: OptimizerContext,
    name: ObjectName,
    query: Query,
    columns: Vec<Ident>,
) -> Result<RwPgResponse> {
    let session = context.session_ctx.clone();

    session.check_relation_name_duplicated(name.clone())?;

    let (table, graph) = {
        let (plan, table) = gen_create_mv_plan(&session, context.into(), query, name, columns)?;
        let graph = build_graph(plan);

        (table, graph)
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_materialized_view(table, graph)
        .await?;

    Ok(PgResponse::empty_result(
        StatementType::CREATE_MATERIALIZED_VIEW,
    ))
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::row_id_column_name;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_mv_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (kafka.topic = 'abc', kafka.servers = 'localhost:1001')
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
            row_id_col_name.as_str() => DataType::Int64,
            "country" => DataType::new_struct(
                 vec![DataType::Varchar,city_type,DataType::Varchar],
                 vec!["address".to_string(), "city".to_string(), "zipcode".to_string()],
            )
        };
        assert_eq!(columns, expected_columns);
    }

    /// When creating MV, The only thing to allow without explicit alias is `InputRef`.
    #[tokio::test]
    async fn test_no_alias() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = "create table t(x varchar)";
        frontend.run_sql(sql).await.unwrap();

        // Aggregation without alias is forbidden.
        let sql = "create materialized view mv1 as select count(x) from t";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for an expression"
        );

        // Literal without alias is forbidden.
        let sql = "create materialized view mv1 as select 1";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for an expression"
        );

        // Function without alias is forbidden.
        let sql = "create materialized view mv1 as select length(x) from t";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for an expression"
        );
    }
}
