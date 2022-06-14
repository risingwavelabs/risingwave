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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_sqlparser::ast::{ObjectName, Query};

use crate::binder::{Binder, BoundSetExpr};
use crate::optimizer::property::RequiredDist;
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::stream_fragmenter::StreamFragmenter;

/// Generate create MV plan, return plan and mv table info.
pub fn gen_create_mv_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    query: Box<Query>,
    name: ObjectName,
) -> Result<(PlanRef, ProstTable)> {
    let (schema_name, table_name) = Binder::resolve_table_name(name)?;
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(session.database(), &schema_name, &table_name)?;

    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind_query(*query)?
    };

    if let BoundSetExpr::Select(select) = &bound.body {
        // `InputRef`'s alias will be implicitly assigned in `bind_project`.
        // For other expressions, we require the user to explicitly assign an alias.
        if select.aliases.iter().any(Option::is_none) {
            return Err(ErrorCode::BindError(
                "An alias must be specified for an expression".to_string(),
            )
            .into());
        }
    }

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    plan_root.set_required_dist(RequiredDist::Any);
    let materialize = plan_root.gen_create_mv_plan(table_name)?;
    let table = materialize.table().to_prost(schema_id, database_id);
    let plan: PlanRef = materialize.into();
    println!("plan is {:#?}", &plan);
    Ok((plan, table))
}

pub async fn handle_create_mv(
    context: OptimizerContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (table, graph) = {
        let (plan, table) = gen_create_mv_plan(&session, context.into(), query, name)?;
        let stream_plan = plan.to_stream_prost();
        let graph = StreamFragmenter::build_graph(stream_plan);

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

    use itertools::Itertools;
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::row_id_column_name;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_mv_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1";
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        // Check source exists.
        let source = catalog_reader
            .read_guard()
            .get_source_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t1")
            .unwrap()
            .clone();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let table = catalog_reader
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "mv1")
            .unwrap()
            .clone();
        assert_eq!(table.name(), "mv1");

        // Get all column descs
        let columns = table
            .columns
            .iter()
            .flat_map(|c| c.column_desc.flatten())
            .collect_vec();

        let columns = columns
            .iter()
            .map(|col| (col.name.as_str(), col.data_type.clone()))
            .collect::<HashMap<&str, DataType>>();

        let city_type = DataType::Struct {
            fields: vec![DataType::Varchar, DataType::Varchar].into(),
        };
        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "country.zipcode" => DataType::Varchar,
            "country.city.address" => DataType::Varchar,
            "country.address" => DataType::Varchar,
            "country.city" => city_type.clone(),
            "country.city.zipcode" => DataType::Varchar,
            "country" => DataType::Struct {fields:vec![DataType::Varchar,city_type,DataType::Varchar].into()},
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
