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

use std::sync::Mutex;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
    UnboundPartitionSpec,
};
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Runtime, TableCommit, TableCreation, TableIdent,
};
use risingwave_pb::connector_service::SinkMetadata;
use tempfile::TempDir;

use super::*;

fn report(
    role: PbIcebergPkIndexSinkRole,
    metadata: Option<SinkMetadata>,
) -> PbIcebergPkIndexSinkMetadata {
    PbIcebergPkIndexSinkMetadata {
        role: role as i32,
        metadata,
        ..Default::default()
    }
}

fn serialized_file(path: &str) -> SerializedDataFile {
    serde_json::from_value(serde_json::json!({
        "content": 0,
        "file_path": path,
        "file_format": "PARQUET",
        "partition": {},
        "record_count": 1,
        "file_size_in_bytes": 1
    }))
    .unwrap()
}

fn writer_metadata(
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
) -> SinkMetadata {
    SinkMetadata::try_from(&IcebergCommitResult {
        schema_id,
        partition_spec_id,
        data_files,
    })
    .unwrap()
}

fn merger_metadata(
    schema_id: i32,
    partition_spec_id: i32,
    delete_files: Vec<SerializedDataFile>,
    overwrite_files: Vec<SerializedDataFile>,
) -> SinkMetadata {
    SinkMetadata::try_from(&IcebergPositionDeleteCommitResult {
        schema_id,
        partition_spec_id,
        delete_files,
        overwrite_files,
    })
    .unwrap()
}

fn test_table(temp_dir: &TempDir, format_version: FormatVersion) -> Result<Table> {
    let location = format!("file://{}", temp_dir.path().display());
    test_table_at_location(&location, format_version)
}

fn test_table_at_location(location: &str, format_version: FormatVersion) -> Result<Table> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        location.to_owned(),
        format_version,
        HashMap::new(),
    )?
    .build()?
    .metadata;
    Ok(Table::builder()
        .identifier(TableIdent::new(
            NamespaceIdent::new("db".to_owned()),
            "table".to_owned(),
        ))
        .file_io(FileIO::new_with_fs())
        .runtime(Runtime::try_current()?)
        .metadata(metadata)
        .build()?)
}

#[derive(Debug)]
struct ReloadingTestCatalog {
    table: Mutex<Table>,
}

impl ReloadingTestCatalog {
    fn new(table: Table) -> Self {
        Self {
            table: Mutex::new(table),
        }
    }

    fn set_table(&self, table: Table) {
        *self.table.lock().unwrap() = table;
    }
}

#[async_trait]
impl Catalog for ReloadingTestCatalog {
    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        Ok(self.table.lock().unwrap().clone())
    }

    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        unreachable!("test only loads a table")
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        unreachable!("test only loads a table")
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        unreachable!("test only loads a table")
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        unreachable!("test only loads a table")
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        unreachable!("test only loads a table")
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        unreachable!("test only loads a table")
    }

    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        unreachable!("test only loads a table")
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        unreachable!("test only loads a table")
    }

    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        unreachable!("test only loads a table")
    }
}

fn test_coordinator(
    table: Table,
    catalog: Arc<dyn Catalog>,
    db: DatabaseConnection,
) -> IcebergPkIndexSinkCoordinator {
    IcebergPkIndexSinkCoordinator {
        sink_id: SinkId::new(42),
        db,
        catalog,
        table,
        target_branch: "main".to_owned(),
        retry_num: 0,
        waiting_commit: None,
        prev_committed_epoch: None,
    }
}

#[tokio::test]
async fn reload_table_for_pre_commit_observes_v2_to_v3_upgrade() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let location = format!("file://{}", temp_dir.path().display());
    let stale_v2 = test_table_at_location(&location, FormatVersion::V2)?;
    let current_v3 = test_table_at_location(&location, FormatVersion::V3)?;
    let catalog = Arc::new(ReloadingTestCatalog::new(stale_v2.clone()));
    let mut coordinator =
        test_coordinator(stale_v2, catalog.clone(), DatabaseConnection::Disconnected);

    catalog.set_table(current_v3);
    coordinator.reload_table_for_pre_commit().await?;

    assert_eq!(
        coordinator.table.metadata().format_version(),
        FormatVersion::V3
    );
    Ok(())
}

#[tokio::test]
async fn compactor_output_ids_are_validated_before_physical_rewrite() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let table = test_table(&temp_dir, FormatVersion::V3)?;
    let catalog = Arc::new(ReloadingTestCatalog::new(table.clone()));
    let mut coordinator = test_coordinator(table, catalog, DatabaseConnection::Disconnected);

    let error = coordinator
        .pre_commit(
            1,
            vec![],
            Some(CompactionOverwrite {
                sink_id: SinkId::new(42),
                epoch: 1,
                schema_id: 99,
                partition_spec_id: 0,
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 1,
            }),
        )
        .await
        .expect_err("compactor schema mismatch should fail");

    assert!(error.to_string().contains("compactor output"));
    assert!(coordinator.waiting_commit.is_none());
    Ok(())
}

#[tokio::test]
async fn compaction_pre_commit_rejects_writer_or_merger_files() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let table = test_table(&temp_dir, FormatVersion::V3)?;
    let catalog = Arc::new(ReloadingTestCatalog::new(table.clone()));
    let mut coordinator = test_coordinator(table, catalog, DatabaseConnection::Disconnected);

    let error = coordinator
        .pre_commit(
            1,
            vec![report(
                PbIcebergPkIndexSinkRole::Writer,
                Some(writer_metadata(
                    0,
                    0,
                    vec![serialized_file("ordinary-data")],
                )),
            )],
            Some(CompactionOverwrite {
                sink_id: SinkId::new(42),
                epoch: 1,
                schema_id: 0,
                partition_spec_id: 0,
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 1,
            }),
        )
        .await
        .expect_err("compaction epoch with writer files should fail");

    assert!(error.to_string().contains("writer or merger files"));
    assert!(coordinator.waiting_commit.is_none());
    Ok(())
}

#[test]
fn pre_commit_state_round_trip_preserves_format_version() -> Result<()> {
    let input = IcebergPkIndexSinkAggResult {
        schema_id: 3,
        partition_spec_id: 4,
        format_version: Some(FormatVersion::V3),
        data_files: vec![serialized_file("data")],
        delete_files: vec![serialized_file("delete")],
        overwrite_files: vec![serialized_file("overwrite")],
    };

    let blob = encode_pre_commit_state(&input, 99)?;
    let (decoded, snapshot_id) = decode_pre_commit_state(&blob)?;

    assert_eq!(snapshot_id, 99);
    assert_eq!(decoded.format_version, Some(FormatVersion::V3));
    assert_eq!(decoded.schema_id, 3);
    assert_eq!(decoded.partition_spec_id, 4);
    assert_eq!(decoded.data_files.len(), 1);
    assert_eq!(decoded.delete_files.len(), 1);
    assert_eq!(decoded.overwrite_files.len(), 1);
    Ok(())
}

#[test]
fn pre_commit_state_decodes_legacy_state_without_format_version() -> Result<()> {
    let legacy_aggregate = serde_json::json!({
        "schema_id": 3,
        "partition_spec_id": 4,
        "data_files": [],
        "delete_files": [],
        "overwrite_files": []
    });
    let blob = PbIcebergPkIndexPreCommitState {
        agg_result: serde_json::to_vec(&legacy_aggregate)?,
        snapshot_id: 99,
    }
    .encode_to_vec();

    let (decoded, snapshot_id) = decode_pre_commit_state(&blob)?;

    assert_eq!(snapshot_id, 99);
    assert_eq!(decoded.format_version, None);
    Ok(())
}

#[test]
fn ordinary_aggregate_accepts_table_ids() {
    let aggregate = aggregate_ordinary_reports(
        &[
            report(
                PbIcebergPkIndexSinkRole::Writer,
                Some(writer_metadata(
                    3,
                    4,
                    vec![serialized_file("ordinary-data")],
                )),
            ),
            report(
                PbIcebergPkIndexSinkRole::PositionDeleteMerger,
                Some(merger_metadata(3, 4, vec![], vec![])),
            ),
        ],
        3,
        4,
        FormatVersion::V3,
    )
    .unwrap()
    .expect("ordinary metadata should produce an aggregate");

    assert_eq!(aggregate.schema_id, 3);
    assert_eq!(aggregate.partition_spec_id, 4);
}

#[test]
fn ordinary_aggregate_rejects_schema_id_different_from_table() {
    let error = aggregate_ordinary_reports(
        &[report(
            PbIcebergPkIndexSinkRole::Writer,
            Some(writer_metadata(2, 4, vec![serialized_file("data")])),
        )],
        3,
        4,
        FormatVersion::V3,
    )
    .expect_err("schema mismatch should fail");
    assert!(error.to_string().contains("schema_id 2"));
}

#[test]
fn ordinary_aggregate_rejects_partition_spec_id_different_from_table() {
    let error = aggregate_ordinary_reports(
        &[report(
            PbIcebergPkIndexSinkRole::Writer,
            Some(writer_metadata(3, 5, vec![serialized_file("data")])),
        )],
        3,
        4,
        FormatVersion::V3,
    )
    .expect_err("partition spec mismatch should fail");
    assert!(error.to_string().contains("partition_spec_id 5"));
}

#[test]
fn ordinary_aggregate_all_empty_is_noop_without_ids() {
    let aggregate = aggregate_ordinary_reports(
        &[
            report(PbIcebergPkIndexSinkRole::Writer, None),
            report(PbIcebergPkIndexSinkRole::PositionDeleteMerger, None),
        ],
        3,
        4,
        FormatVersion::V3,
    )
    .unwrap();
    assert!(aggregate.is_none());
}

#[test]
fn combine_compaction_aggregate_merges_every_input() {
    let resolver = decode_compaction_resolver_delete_reports([report(
        PbIcebergPkIndexSinkRole::CompactionResolver,
        Some(merger_metadata(
            3,
            4,
            vec![serialized_file("resolver-delete")],
            vec![],
        )),
    )])
    .unwrap();

    let merged = combine_compaction_aggregate(
        3,
        4,
        FormatVersion::V3,
        vec![serialized_file("compactor-output")],
        resolver,
        vec![serialized_file("resolved-overwrite")],
    )
    .unwrap();

    assert_eq!(merged.schema_id, 3);
    assert_eq!(merged.partition_spec_id, 4);
    assert_eq!(merged.format_version, Some(FormatVersion::V3));
    assert_eq!(merged.data_files.len(), 1);
    assert_eq!(merged.delete_files.len(), 1);
    assert_eq!(merged.overwrite_files.len(), 1);
}

#[test]
fn combine_compaction_aggregate_rejects_resolver_ids_different_from_table() {
    let resolver = decode_compaction_resolver_delete_reports([report(
        PbIcebergPkIndexSinkRole::CompactionResolver,
        Some(merger_metadata(3, 5, vec![], vec![])),
    )])
    .unwrap();
    let error = combine_compaction_aggregate(3, 4, FormatVersion::V3, vec![], resolver, vec![])
        .expect_err("partition spec mismatch should fail");
    assert!(error.to_string().contains("partition_spec_id 5"));
}

#[test]
fn aggregate_reports_ignores_empty_ordinary_metadata() {
    let merger_metadata = SinkMetadata::try_from(&IcebergPositionDeleteCommitResult {
        schema_id: 3,
        partition_spec_id: 4,
        delete_files: vec![],
        overwrite_files: vec![],
    })
    .unwrap();
    let merged = aggregate_reports(&[
        report(PbIcebergPkIndexSinkRole::Writer, None),
        report(
            PbIcebergPkIndexSinkRole::PositionDeleteMerger,
            Some(merger_metadata),
        ),
    ])
    .unwrap()
    .expect("merger metadata should produce an aggregate");

    assert_eq!(merged.schema_id, 3);
    assert_eq!(merged.partition_spec_id, 4);
}
