// Copyright 2026 RisingWave Labs
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

use std::rc::Rc;

use risingwave_common::catalog::{ColumnCatalog, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_pb::stream_plan::stream_node::{NodeBody, StreamKind};

use crate::OptimizerContext;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::generic::SourceNodeKind;
use crate::optimizer::plan_node::{BackfillType, LogicalSource, ToStream, ToStreamContext};
use crate::stream_fragmenter::build_graph;
use crate::test_utils::LocalFrontend;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn properties(table: &str, updates: &str) -> String {
    format!(
        "connector='iceberg', catalog.type='mock', catalog.name='demo', database.name='db', table.name='{table}', streaming_updates='{updates}'"
    )
}

fn source(frontend: &LocalFrontend, name: &str) -> SourceCatalog {
    let session = frontend.session_ref();
    let reader = session.env().catalog_reader().read_guard();
    let (source, _) = reader
        .get_source_by_name(
            DEFAULT_DATABASE_NAME,
            SchemaPath::Name(DEFAULT_SCHEMA_NAME),
            name,
        )
        .unwrap();
    (**source).clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_update_catalog_and_retract_graph_round_trip() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    for version in ["v2", "v3"] {
        let name = format!("updates_{version}");
        frontend
            .run_sql(format!(
                "CREATE SOURCE {name} WITH ({})",
                properties(&name, "true")
            ))
            .await?;
        let catalog = source(&frontend, &name);
        assert!(!catalog.append_only);
        assert_eq!(catalog.row_id_index, None);
        assert_eq!(catalog.columns.len(), 3);
        assert!(catalog.columns[2].is_hidden());
        assert_eq!(
            catalog.pk_col_ids,
            vec![
                catalog.columns[2].column_id(),
                catalog.columns[0].column_id()
            ]
        );
        assert!(
            !catalog
                .columns
                .iter()
                .any(ColumnCatalog::is_iceberg_hidden_column)
        );
        let restored = SourceCatalog::from(&catalog.to_prost());
        assert_eq!(restored, catalog);
        assert!(!restored.create_sql_purified().contains("PRIMARY KEY"));
        restored.validate_iceberg_update_source()?;
        let logical = LogicalSource::with_catalog(
            Rc::new(restored),
            SourceNodeKind::CreateMViewOrBatch,
            OptimizerContext::mock(),
            None,
        )?;
        let plan = logical.to_stream(&mut ToStreamContext::new_with_backfill_type(
            false,
            BackfillType::ArrangementBackfill,
        ))?;
        let graph = build_graph(plan, None)?;
        let mut lists = vec![];
        let mut fetches = vec![];
        for fragment in graph.fragments.values() {
            visit_stream_node(fragment.node.as_ref().unwrap(), |node| {
                match node.node_body.as_ref().unwrap() {
                    NodeBody::Source(source) => {
                        assert_eq!(node.stream_kind, StreamKind::Retract as i32);
                        assert_eq!(node.stream_key, vec![0]);
                        lists.push(source.source_inner.as_ref().unwrap().clone());
                    }
                    NodeBody::StreamFsFetch(fetch) => {
                        assert_eq!(node.stream_kind, StreamKind::Retract as i32);
                        assert_eq!(node.stream_key, vec![2, 0]);
                        fetches.push(fetch.node_inner.as_ref().unwrap().clone());
                    }
                    NodeBody::RowIdGen(_) => panic!("stored hidden key must not be regenerated"),
                    _ => {}
                }
            });
        }
        assert_eq!(lists.len(), 1);
        assert_eq!(fetches.len(), 1);
        let list = &lists[0];
        let fetch = &fetches[0];
        let completion = list.iceberg_fetch_state_table.as_ref().unwrap();
        assert_eq!(completion.table_id, fetch.state_table.as_ref().unwrap().id);
        assert_ne!(completion.table_id, list.state_table.as_ref().unwrap().id);
        assert_eq!(completion.dist_key_in_pk_indices, vec![0]);
        assert_eq!(
            list.downstream_columns.as_ref().unwrap().columns,
            fetch.columns
        );
        assert_eq!(fetch.columns.len(), 3);
        assert_eq!(fetch.row_id_index, None);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_update_sql_keeps_logical_keys_and_rejects_physical_columns() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    frontend
        .run_sql(format!(
            "CREATE SOURCE updates WITH ({})",
            properties("updates_v3", "true")
        ))
        .await?;
    for query in [
        "SELECT * FROM updates",
        "SELECT value FROM updates WHERE id > 0",
        "SELECT value, count(*) FROM updates GROUP BY value",
        "SELECT a.value AS left_value, b.value AS right_value FROM updates a JOIN updates b ON a.id=b.id",
        "SELECT _row_id FROM updates",
    ] {
        let plan = frontend
            .get_explain_output(format!("EXPLAIN CREATE MATERIALIZED VIEW mv AS {query}"))
            .await;
        assert!(plan.contains("StreamFsFetch"));
        assert!(!plan.contains("StreamRowIdGen"));
        assert!(!plan.contains("_iceberg_file"));
        assert!(!plan.contains("_iceberg_sequence"));
    }
    for physical in [
        "_iceberg_file_path",
        "_iceberg_file_pos",
        "_iceberg_sequence_number",
    ] {
        for query in [
            format!("SELECT {physical} FROM updates"),
            format!("SELECT u.{physical} FROM updates u"),
            format!("SELECT value FROM updates WHERE {physical} IS NOT NULL"),
            format!("SELECT count(*) FROM updates GROUP BY {physical}"),
            format!("SELECT a.value FROM updates a JOIN updates b ON a.{physical}=b.{physical}"),
        ] {
            let error = frontend
                .run_sql(format!("EXPLAIN CREATE MATERIALIZED VIEW bad AS {query}"))
                .await;
            assert!(error.is_err(), "physical metadata bound: {query}");
            assert!(frontend.run_sql(format!("EXPLAIN {query}")).await.is_err());
        }
    }
    // Batch queries of the opt-in source also use only the stored logical schema.
    frontend.run_sql("EXPLAIN SELECT * FROM updates").await?;
    // LocalFrontend builds and records the actual graph, without starting external services.
    frontend.run_sql("CREATE MATERIALIZED VIEW grouped_updates AS SELECT value, count(*) FROM updates GROUP BY value").await?;
    frontend.run_sql("CREATE MATERIALIZED VIEW joined_updates AS SELECT a.value AS left_value, b.value AS right_value FROM updates a JOIN updates b ON a.id=b.id").await?;
    let mut corrupted = source(&frontend, "updates").to_prost();
    corrupted.columns.extend(
        ColumnCatalog::iceberg_hidden_cols()
            .iter()
            .map(ColumnCatalog::to_protobuf),
    );
    assert!(
        SourceCatalog::from(&corrupted)
            .validate_iceberg_update_source()
            .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_update_opt_in_fails_closed_and_legacy_is_unchanged() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    for sql in [
        format!(
            "CREATE SOURCE bad WITH ({})",
            properties("sparse_table", "true")
        ),
        format!(
            "CREATE SOURCE bad WITH ({})",
            properties("updates_v2", "invalid")
        ),
        format!(
            "CREATE SOURCE bad (PRIMARY KEY (id)) WITH ({})",
            properties("updates_v2", "true")
        ),
        format!(
            "CREATE SOURCE bad INCLUDE OFFSET WITH ({})",
            properties("updates_v2", "true")
        ),
        format!(
            "CREATE SOURCE bad WITH ({}, source_rate_limit='1')",
            properties("updates_v2", "true")
        ),
        format!(
            "CREATE TABLE bad WITH ({})",
            properties("updates_v2", "true")
        ),
    ] {
        assert!(
            frontend.run_sql(&sql).await.is_err(),
            "unexpectedly accepted: {sql}"
        );
    }
    frontend
        .run_sql(format!(
            "CREATE SOURCE legacy WITH ({})",
            properties("sparse_table", "false")
        ))
        .await?;
    let legacy = source(&frontend, "legacy");
    assert!(legacy.append_only && legacy.row_id_index.is_some());
    assert_eq!(
        legacy
            .columns
            .iter()
            .filter(|column| column.is_iceberg_hidden_column())
            .count(),
        3
    );
    frontend
        .run_sql("EXPLAIN SELECT _iceberg_file_path FROM legacy")
        .await?;
    frontend
        .run_sql(format!(
            "CREATE SOURCE updates WITH ({})",
            properties("updates_v2", "true")
        ))
        .await?;
    for sql in [
        "ALTER SOURCE updates CONNECTOR WITH (streaming_updates='false')",
        "ALTER SOURCE legacy CONNECTOR WITH (streaming_updates='true')",
    ] {
        let error = frontend
            .run_sql(sql)
            .await
            .expect_err("mode changes must fail");
        assert!(
            error
                .to_string()
                .contains("streaming_updates cannot be changed"),
            "{error}"
        );
    }
    Ok(())
}
