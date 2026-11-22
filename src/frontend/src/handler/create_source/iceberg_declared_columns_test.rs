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

use risingwave_common::catalog::{
    ColumnCatalog, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, ROW_ID_COLUMN_NAME,
};
use risingwave_common::types::DataType;
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_pb::plan_common::PbColumnCatalog;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::OptimizerContext;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::generic::SourceNodeKind;
use crate::optimizer::plan_node::{BackfillType, LogicalSource, ToStream, ToStreamContext};
use crate::stream_fragmenter::build_graph;
use crate::test_utils::LocalFrontend;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Mock table with `id INT`, `value BIGINT` and a hidden upstream `_row_id BIGINT`.
const UPDATES: &str = "connector='iceberg', catalog.type='mock', catalog.name='demo', database.name='db', table.name='updates_v2'";
/// Mock table with `v1 INT`, `v2 BIGINT`, `v3 VARCHAR` and `v4 TIME`.
const SPARSE: &str = "connector='iceberg', catalog.type='mock', catalog.name='demo', database.name='db', table.name='sparse_table'";

fn source_by_name(frontend: &LocalFrontend, name: &str) -> SourceCatalog {
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

fn table_source(frontend: &LocalFrontend, name: &str) -> SourceCatalog {
    let session = frontend.session_ref();
    let reader = session.env().catalog_reader().read_guard();
    let path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);
    let (table, _) = reader
        .get_any_table_by_name(DEFAULT_DATABASE_NAME, path, name)
        .unwrap();
    let (source, _) = reader
        .get_source_by_id(
            DEFAULT_DATABASE_NAME,
            path,
            table.associated_source_id.unwrap(),
        )
        .unwrap();
    (**source).clone()
}

fn visible(columns: &[ColumnCatalog]) -> Vec<&str> {
    columns
        .iter()
        .filter(|column| !column.is_hidden())
        .map(ColumnCatalog::name)
        .collect()
}

fn table_visible_columns(frontend: &LocalFrontend, name: &str) -> Vec<String> {
    let session = frontend.session_ref();
    let reader = session.env().catalog_reader().read_guard();
    let (table, _) = reader
        .get_any_table_by_name(
            DEFAULT_DATABASE_NAME,
            SchemaPath::Name(DEFAULT_SCHEMA_NAME),
            name,
        )
        .unwrap();
    visible(table.columns())
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn iceberg_hidden_count(catalog: &SourceCatalog) -> usize {
    catalog
        .columns
        .iter()
        .filter(|column| column.is_iceberg_hidden_column())
        .count()
}

/// Returns the columns of the List and Fetch nodes planned for the source.
fn planned_source_columns(
    catalog: &SourceCatalog,
) -> Result<(Vec<PbColumnCatalog>, Vec<PbColumnCatalog>), Box<dyn std::error::Error + Send + Sync>>
{
    let logical = LogicalSource::with_catalog(
        Rc::new(catalog.clone()),
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
                NodeBody::Source(source) => lists.push(
                    source
                        .source_inner
                        .as_ref()
                        .unwrap()
                        .downstream_columns
                        .as_ref()
                        .unwrap()
                        .columns
                        .clone(),
                ),
                NodeBody::StreamFsFetch(fetch) => {
                    fetches.push(fetch.node_inner.as_ref().unwrap().columns.clone())
                }
                _ => {}
            }
        });
    }
    assert_eq!((lists.len(), fetches.len()), (1, 1));
    Ok((lists.remove(0), fetches.remove(0)))
}

async fn expect_error(frontend: &LocalFrontend, sql: String, expected: &str) {
    let error = frontend
        .run_sql(&sql)
        .await
        .expect_err("statement must fail");
    assert!(error.to_string().contains(expected), "{sql}: {error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_full_reload_declared_columns() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    frontend
        .run_sql(format!(
            "CREATE TABLE projected (value, PRIMARY KEY (value)) WITH ({UPDATES}, refresh_mode='FULL_RELOAD')"
        ))
        .await?;
    let catalog = table_source(&frontend, "projected");
    assert_eq!(visible(&catalog.columns), vec!["value"]);
    assert_eq!(catalog.columns[0].data_type(), &DataType::Int64);
    assert_eq!(iceberg_hidden_count(&catalog), 3);
    assert!(
        catalog
            .columns
            .iter()
            .any(|column| column.is_hidden() && column.name() == ROW_ID_COLUMN_NAME)
    );
    assert_eq!(SourceCatalog::from(&catalog.to_prost()), catalog);
    let purified = catalog.create_sql_purified();
    assert!(purified.contains("value BIGINT"), "{purified}");
    frontend.run_sql("EXPLAIN SELECT * FROM projected").await?;
    // The scan projects the visible downstream columns, so only `value` is read upstream.
    let expected = catalog
        .columns
        .iter()
        .map(ColumnCatalog::to_protobuf)
        .collect::<Vec<_>>();
    assert_eq!(
        planned_source_columns(&catalog)?,
        (expected.clone(), expected)
    );

    frontend
        .run_sql(format!(
            "CREATE TABLE reordered (value BIGINT, id, PRIMARY KEY (id)) WITH ({UPDATES}, refresh_mode='FULL_RELOAD')"
        ))
        .await?;
    let catalog = table_source(&frontend, "reordered");
    assert_eq!(visible(&catalog.columns), vec!["value", "id"]);
    assert_eq!(catalog.columns[1].data_type(), &DataType::Int32);

    // Without declared columns, or with a wildcard, every upstream column is ingested.
    for (name, columns) in [("unprojected", ""), ("wildcard", "*, ")] {
        frontend
            .run_sql(format!(
                "CREATE TABLE {name} ({columns}PRIMARY KEY (id)) WITH ({UPDATES}, refresh_mode='FULL_RELOAD')"
            ))
            .await?;
        let catalog = table_source(&frontend, name);
        assert_eq!(visible(&catalog.columns), vec!["id", "value"]);
        assert_eq!(iceberg_hidden_count(&catalog), 3);
    }

    for (columns, expected) in [
        (
            "missing INT, PRIMARY KEY (missing)",
            "not found in the source",
        ),
        ("missing, PRIMARY KEY (missing)", "not found in the source"),
        ("id BIGINT, value, PRIMARY KEY (id)", "Data type mismatch"),
        ("value BIGINT, PRIMARY KEY (id)", "id"),
    ] {
        expect_error(
            &frontend,
            format!("CREATE TABLE bad ({columns}) WITH ({UPDATES}, refresh_mode='FULL_RELOAD')"),
            expected,
        )
        .await;
    }
    expect_error(
        &frontend,
        format!("CREATE TABLE bad (v1) WITH ({SPARSE}, refresh_mode='FULL_RELOAD')"),
        "Refreshable tables must have a PRIMARY KEY",
    )
    .await;
    expect_error(
        &frontend,
        "CREATE TABLE bad (v1)".to_owned(),
        "data type is not specified",
    )
    .await;

    // ALTER replans from the purified definition, which lists the declared columns.
    frontend
        .run_sql("ALTER TABLE projected ADD COLUMN id INT")
        .await?;
    assert_eq!(
        table_visible_columns(&frontend, "projected"),
        ["value", "id"]
    );
    expect_error(
        &frontend,
        "ALTER TABLE projected ADD COLUMN missing INT".to_owned(),
        "not found in the source",
    )
    .await;
    frontend
        .run_sql("ALTER TABLE projected DROP COLUMN id")
        .await?;
    assert_eq!(table_visible_columns(&frontend, "projected"), ["value"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_streaming_source_declared_columns() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    frontend
        .run_sql(format!(
            "CREATE SOURCE projected (v3, v1 INT) WITH ({SPARSE})"
        ))
        .await?;
    let catalog = source_by_name(&frontend, "projected");
    assert_eq!(visible(&catalog.columns), vec!["v3", "v1"]);
    assert_eq!(catalog.columns[0].data_type(), &DataType::Varchar);
    assert!(catalog.append_only && catalog.row_id_index.is_some());
    assert_eq!(iceberg_hidden_count(&catalog), 3);
    let purified = catalog.create_sql_purified();
    assert!(
        purified.contains("v3 CHARACTER VARYING") && !purified.contains("v2"),
        "{purified}"
    );

    frontend
        .run_sql("EXPLAIN CREATE MATERIALIZED VIEW mv AS SELECT * FROM projected")
        .await?;
    frontend
        .run_sql("EXPLAIN SELECT _iceberg_file_path FROM projected")
        .await?;
    assert!(
        frontend
            .run_sql("EXPLAIN SELECT v2 FROM projected")
            .await
            .is_err()
    );
    expect_error(
        &frontend,
        format!("CREATE SOURCE bad (v1, PRIMARY KEY (v1)) WITH ({SPARSE})"),
        "PRIMARY KEY is not supported for Iceberg append-only sources",
    )
    .await;
    for (columns, expected) in [
        ("missing", "not found in the source"),
        ("_iceberg_file_path", "not found in the source"),
        ("v3, v1 BIGINT", "Data type mismatch"),
    ] {
        expect_error(
            &frontend,
            format!("CREATE SOURCE bad ({columns}) WITH ({SPARSE})"),
            expected,
        )
        .await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_update_declared_columns() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    let props = format!("{UPDATES}, streaming_updates='true'");
    frontend
        .run_sql(format!(
            "CREATE SOURCE projected (value, PRIMARY KEY (_row_id)) WITH ({props})"
        ))
        .await?;
    let catalog = source_by_name(&frontend, "projected");
    assert_eq!(visible(&catalog.columns), vec!["value"]);
    assert_eq!(catalog.columns[0].data_type(), &DataType::Int64);
    assert_eq!(iceberg_hidden_count(&catalog), 0);
    let row_id = catalog
        .columns
        .iter()
        .find(|column| column.name() == ROW_ID_COLUMN_NAME)
        .unwrap();
    assert!(row_id.is_hidden());
    assert_eq!(catalog.pk_col_ids, vec![row_id.column_id()]);
    catalog.validate_iceberg_update_source()?;
    let (list, fetch) = planned_source_columns(&catalog)?;
    assert_eq!(list, fetch);
    assert_eq!(
        list.iter()
            .map(|column| column.column_desc.as_ref().unwrap().name.as_str())
            .collect::<Vec<_>>(),
        vec!["value", ROW_ID_COLUMN_NAME]
    );

    frontend
        .run_sql(format!(
            "CREATE TABLE projected_table (id, PRIMARY KEY (id)) WITH ({props})"
        ))
        .await?;
    let catalog = table_source(&frontend, "projected_table");
    assert_eq!(visible(&catalog.columns), vec!["id"]);
    assert_eq!(catalog.columns[0].data_type(), &DataType::Int32);
    catalog.validate_iceberg_update_source()?;

    // The update source persists its projection, so the columns cannot change later.
    for sql in [
        "ALTER TABLE projected_table ADD COLUMN value BIGINT",
        "ALTER TABLE projected_table DROP COLUMN id",
    ] {
        expect_error(&frontend, sql.to_owned(), "streaming_updates").await;
    }
    assert!(
        frontend
            .run_sql(format!(
                "CREATE TABLE bad (id INT, next_id INT AS id + 1, PRIMARY KEY (id)) WITH ({props})"
            ))
            .await
            .is_err()
    );
    Ok(())
}

/// The upstream table may already have a `_row_id` column, for example one written by a
/// RisingWave sink. Without a SQL primary key, RisingWave generates its own `_row_id` instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_upstream_row_id_without_primary_key() -> TestResult {
    let frontend = LocalFrontend::new(Default::default()).await;
    for (name, sql) in [
        (
            "inferred_source",
            format!("CREATE SOURCE inferred_source WITH ({UPDATES})"),
        ),
        (
            "declared_source",
            format!("CREATE SOURCE declared_source (value BIGINT) WITH ({UPDATES})"),
        ),
        (
            "inferred_table",
            format!("CREATE TABLE inferred_table WITH ({UPDATES})"),
        ),
        (
            "declared_table",
            format!("CREATE TABLE declared_table (value) WITH ({UPDATES})"),
        ),
    ] {
        frontend.run_sql(&sql).await?;
        let catalog = if name.ends_with("_source") {
            source_by_name(&frontend, name)
        } else {
            table_source(&frontend, name)
        };
        let row_ids = catalog
            .columns
            .iter()
            .filter(|column| column.name() == ROW_ID_COLUMN_NAME)
            .collect::<Vec<_>>();
        assert_eq!(row_ids.len(), 1, "{sql}");
        assert!(row_ids[0].is_row_id_column(), "{sql}");
        assert_eq!(
            catalog
                .row_id_index
                .map(|index| catalog.columns[index].name()),
            Some(ROW_ID_COLUMN_NAME),
            "{sql}"
        );
        assert_eq!(catalog.pk_col_ids, vec![row_ids[0].column_id()], "{sql}");
        assert_eq!(iceberg_hidden_count(&catalog), 3, "{sql}");
        frontend
            .run_sql(format!("EXPLAIN SELECT * FROM {name}"))
            .await?;
    }

    // A FULL_RELOAD table still reports the missing primary key, not a duplicated `_row_id`.
    expect_error(
        &frontend,
        format!("CREATE TABLE bad (value BIGINT) WITH ({UPDATES}, refresh_mode='FULL_RELOAD')"),
        "Refreshable tables must have a PRIMARY KEY",
    )
    .await;

    // With a primary key, the upstream `_row_id` stays a regular hidden column.
    frontend
        .run_sql(format!(
            "CREATE TABLE keyed (PRIMARY KEY (id)) WITH ({UPDATES}, refresh_mode='FULL_RELOAD')"
        ))
        .await?;
    let catalog = table_source(&frontend, "keyed");
    let row_id = catalog
        .columns
        .iter()
        .find(|column| column.name() == ROW_ID_COLUMN_NAME)
        .unwrap();
    assert!(row_id.is_hidden() && !row_id.is_row_id_column());
    assert_eq!(catalog.row_id_index, None);
    Ok(())
}
