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

use std::collections::HashMap;
use std::rc::Rc;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::TableDesc;
use risingwave_common::error::Result;
use risingwave_pb::catalog::Sink as ProstSink;
use risingwave_sqlparser::ast::CreateSinkStatement;

use super::util::handle_with_properties;
use crate::binder::Binder;
use crate::catalog::{DatabaseId, SchemaId};
use crate::optimizer::plan_node::{LogicalScan, StreamSink, StreamTableScan};
use crate::optimizer::PlanRef;
use crate::session::{OptimizerContext, OptimizerContextRef};
use crate::stream_fragmenter::StreamFragmenter;

pub(crate) fn make_prost_sink(
    database_id: DatabaseId,
    schema_id: SchemaId,
    name: String,
    associated_table_id: u32,
    properties: HashMap<String, String>,
    owner: String,
) -> Result<ProstSink> {
    Ok(ProstSink {
        id: 0,
        schema_id,
        database_id,
        name,
        associated_table_id,
        properties,
        owner,
    })
}

fn gen_sink_plan(
    context: OptimizerContextRef,
    associated_table_name: String,
    associated_table_desc: TableDesc,
) -> Result<PlanRef> {
    let scan_node: PlanRef = StreamTableScan::new(LogicalScan::create(
        "associated_table_name32463426234".into(),
        false,
        Rc::new(associated_table_desc),
        vec![],
        context,
    ))
    .into();
    
    use risingwave_pb::stream_plan::stream_node::NodeBody;
    use risingwave_pb::stream_plan::ChainNode;

    let chain_node = NodeBody::Chain(ChainNode {
        same_worker_node: false,
        disable_rearrange: false,
        table_ref_id: None,
        upstream_fields: vec![],
        column_ids: vec![]
    });

    Ok(StreamSink::new(chain_node).into())
}

pub async fn handle_create_sink(
    context: OptimizerContext,
    stmt: CreateSinkStatement,
) -> Result<PgResponse> {
    let with_properties = handle_with_properties("create_sink", stmt.with_properties.0)?;
    let session = context.session_ctx.clone();

    let (schema_name, sink_name) = Binder::resolve_table_name(stmt.sink_name.clone())?;
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(session.database(), &schema_name, sink_name.as_str())?;

    let (associated_table_id, associated_table_name, associated_table_desc) = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let table = catalog_reader.get_table_by_name(
            session.database(),
            &schema_name,
            stmt.materialized_view.to_string().as_str(),
        )?;
        (
            table.id().table_id,
            table.name().to_string(),
            table.table_desc(),
        )
    };

    let sink = make_prost_sink(
        database_id,
        schema_id,
        stmt.sink_name.to_string(),
        associated_table_id,
        with_properties.clone(),
        session.user_name().to_string(),
    )?;

    let graph = {
        let plan = gen_sink_plan(context.into(), associated_table_name, associated_table_desc)?;
        let plan = plan.to_stream_prost();
        StreamFragmenter::build_graph(plan)
    };

    dbg!(&graph);

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_sink(sink, graph).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

pub static PROTO_FILE_DATA2: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      string name = 1;
    }"#;


#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(super::PROTO_FILE_DATA2);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}';"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select name from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH ('sink' = 'mysql', 'mysql.endpoint' = '127.0.0.1:3306', 'mysql.table' =
                        '<table_name>', 'mysql.database' = '<database_name>', 'mysql.user' = '<user_name>',
                        'mysql.password' = '<password>');"#.to_string();
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

        // Check sink exists.
        let sink = catalog_reader.read_guard().get_sink_id_by_name(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "snk1",
        );
        assert!(sink.is_ok());
    }
}
