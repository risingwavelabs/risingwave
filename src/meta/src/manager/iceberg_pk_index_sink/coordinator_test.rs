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
use std::sync::atomic::{AtomicUsize, Ordering};

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
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
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
    loads: AtomicUsize,
}

impl ReloadingTestCatalog {
    fn new(table: Table) -> Self {
        Self {
            table: Mutex::new(table),
            loads: AtomicUsize::new(0),
        }
    }

    fn table(&self) -> Table {
        self.table.lock().unwrap().clone()
    }

    fn load_count(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }

    fn set_table(&self, table: Table) {
        *self.table.lock().unwrap() = table;
    }
}

#[async_trait]
impl Catalog for ReloadingTestCatalog {
    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        self.loads.fetch_add(1, Ordering::SeqCst);
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

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let mut table = self.table.lock().unwrap();
        let updated = commit.apply(table.clone())?;
        *table = updated.clone();
        Ok(updated)
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
        committed_data_file_partitions: None,
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

fn empty_epoch_commit(snapshot_id: i64, staged_on_current_table: bool) -> EpochCommit {
    EpochCommit {
        epoch: 1,
        merged: Arc::new(IcebergPkIndexSinkAggResult {
            schema_id: 0,
            partition_spec_id: 0,
            format_version: Some(FormatVersion::V2),
            data_files: vec![],
            delete_files: vec![],
            overwrite_files: vec![],
        }),
        snapshot_id,
        has_new_data: false,
        materialized_add_files: None,
        staged_on_current_table,
    }
}

fn committable_table(table: Table) -> Result<Table> {
    let metadata_location = format!(
        "{}/metadata/00000-00000000-0000-0000-0000-000000000000.metadata.json",
        table.metadata().location()
    );
    Ok(Table::builder()
        .identifier(table.identifier().clone())
        .file_io(table.file_io().clone())
        .runtime(Runtime::try_current()?)
        .metadata(table.metadata().clone())
        .metadata_location(metadata_location)
        .build()?)
}

/// Same table and metadata with an empty object cache, so manifest reads must hit storage.
fn without_object_cache(table: &Table) -> Result<Table> {
    let mut builder = Table::builder()
        .identifier(table.identifier().clone())
        .file_io(table.file_io().clone())
        .runtime(Runtime::try_current()?)
        .metadata(table.metadata().clone());
    if let Some(location) = table.metadata_location() {
        builder = builder.metadata_location(location.to_owned());
    }
    Ok(builder.build()?)
}

async fn append_data_file(
    catalog: &ReloadingTestCatalog,
    table: &Table,
    data_file: DataFile,
    snapshot_id: i64,
) -> Result<Table> {
    let txn = Transaction::new(table);
    let txn = txn
        .fast_append()
        .set_snapshot_id(snapshot_id)
        .add_data_files(vec![data_file])
        .apply(txn)?;
    Ok(txn.commit(catalog).await?)
}

fn id_partition(id: i64) -> Struct {
    Struct::from_iter([Some(Literal::long(id))])
}

/// A position delete as writers report it: the partition is left for meta to backfill.
fn serialized_delete_without_partition(path: &str, referenced: &str) -> SerializedDataFile {
    serde_json::from_value(serde_json::json!({
        "content": 1,
        "file_path": path,
        "file_format": "PARQUET",
        "partition": {},
        "record_count": 1,
        "file_size_in_bytes": 1,
        "referenced_data_file": referenced
    }))
    .unwrap()
}

async fn pending_sink_state_db() -> Result<DatabaseConnection> {
    let db = Database::connect("sqlite::memory:").await?;
    for sql in [
        "CREATE TABLE object (oid INTEGER PRIMARY KEY)",
        "CREATE TABLE pending_sink_state (\
            sink_id INTEGER NOT NULL, \
            epoch BIGINT NOT NULL, \
            sink_state STRING NOT NULL, \
            metadata BLOB, \
            schema_change BLOB, \
            PRIMARY KEY (sink_id, epoch), \
            FOREIGN KEY (sink_id) REFERENCES object(oid) ON DELETE CASCADE\
        )",
        // `test_coordinator` uses sink 42.
        "INSERT INTO object (oid) VALUES (42)",
    ] {
        db.execute(Statement::from_string(DbBackend::Sqlite, sql))
            .await?;
    }
    Ok(db)
}

fn writer_report(table: &Table, data_files: Vec<DataFile>) -> Result<PbIcebergPkIndexSinkMetadata> {
    Ok(report(
        PbIcebergPkIndexSinkRole::Writer,
        Some(writer_metadata(
            0,
            table.metadata().default_partition_spec_id(),
            serialize_data_files_default_spec(table, data_files)?,
        )),
    ))
}

fn merger_report(
    table: &Table,
    delete_files: Vec<SerializedDataFile>,
    overwrite_files: Vec<DataFile>,
) -> Result<PbIcebergPkIndexSinkMetadata> {
    Ok(report(
        PbIcebergPkIndexSinkRole::PositionDeleteMerger,
        Some(merger_metadata(
            0,
            table.metadata().default_partition_spec_id(),
            delete_files,
            serialize_data_files_default_spec(table, overwrite_files)?,
        )),
    ))
}

fn staged_add_files(coordinator: &IcebergPkIndexSinkCoordinator) -> Vec<DataFile> {
    coordinator
        .waiting_commit
        .as_ref()
        .and_then(|commit| commit.materialized_add_files.clone())
        .expect("pre-commit stages materialized files for a partitioned table")
}

#[tokio::test]
async fn committed_data_file_partitions_follow_own_commits() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let table = committable_table(coalesce_test_table_with_partitioning(
        &temp_dir,
        FormatVersion::V2,
        true,
    )?)?;
    let catalog = Arc::new(ReloadingTestCatalog::new(table.clone()));
    let first = output_data_file_with_partition(&table, "first", id_partition(1))?;
    let table = append_data_file(&catalog, &table, first.clone(), 1).await?;
    let mut coordinator = test_coordinator(
        table.clone(),
        catalog.clone(),
        coalesce_test_config(),
        pending_sink_state_db().await?,
    );
    let loads = catalog.load_count();

    // Epoch 1 builds the index from the committed snapshot and adds a deletion vector.
    coordinator
        .pre_commit(
            1,
            vec![merger_report(
                &table,
                vec![serialized_delete_without_partition(
                    "dv-1",
                    first.file_path(),
                )],
                vec![],
            )?],
            None,
        )
        .await?;
    let first_dv = staged_add_files(&coordinator)
        .pop()
        .expect("epoch 1 stages its deletion vector");
    assert_eq!(first_dv.partition(), &id_partition(1));
    coordinator.commit().await?;
    // Pre-commit loads the table and the transaction refreshes it once on commit. The commit
    // itself starts from the pre-commit table instead of loading it again.
    assert_eq!(catalog.load_count(), loads + 2);

    // Epoch 2 adds a data file and replaces the deletion vector of `first`.
    let second = output_data_file_with_partition(&table, "second", id_partition(2))?;
    coordinator
        .pre_commit(
            2,
            vec![
                writer_report(&table, vec![second.clone()])?,
                merger_report(
                    &table,
                    vec![serialized_delete_without_partition(
                        "dv-2",
                        first.file_path(),
                    )],
                    vec![first_dv],
                )?,
            ],
            None,
        )
        .await?;
    coordinator.commit().await?;
    assert_eq!(catalog.load_count(), loads + 4);
    let committed = coordinator
        .committed_data_file_partitions
        .as_ref()
        .expect("replacing a deletion vector keeps the index");
    assert_eq!(
        Some(committed.snapshot_id),
        coordinator.current_snapshot_id()
    );

    // Epoch 3 can only succeed by resolving both partitions from the advanced index.
    catalog.set_table(without_object_cache(&catalog.table())?);
    std::fs::remove_dir_all(temp_dir.path().join("metadata"))?;
    coordinator
        .pre_commit(
            3,
            vec![merger_report(
                &table,
                vec![
                    serialized_delete_without_partition("dv-3", first.file_path()),
                    serialized_delete_without_partition("dv-4", second.file_path()),
                ],
                vec![],
            )?],
            None,
        )
        .await?;
    let partitions = staged_add_files(&coordinator)
        .iter()
        .map(|file| file.partition().clone())
        .collect::<Vec<_>>();
    assert_eq!(partitions, vec![id_partition(1), id_partition(2)]);
    Ok(())
}
#[tokio::test]
async fn committed_data_file_partitions_reset_on_foreign_or_data_file_removals() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let table = committable_table(coalesce_test_table_with_partitioning(
        &temp_dir,
        FormatVersion::V2,
        true,
    )?)?;
    let catalog = ReloadingTestCatalog::new(table.clone());
    let first = output_data_file_with_partition(&table, "first", id_partition(1))?;
    let table = append_data_file(&catalog, &table, first, 1).await?;
    let index = || CommittedDataFilePartitions {
        snapshot_id: 1,
        partitions: HashMap::new(),
    };

    // Another writer produced the snapshot.
    let second = output_data_file_with_partition(&table, "second", id_partition(2))?;
    let foreign = append_data_file(&catalog, &table, second.clone(), 2).await?;
    let mut commit = empty_epoch_commit(3, true);
    commit.materialized_add_files = Some(vec![second.clone()]);
    assert!(index().advance(&foreign, &commit, false).is_none());

    // Compaction removed data files that the index cannot see.
    let mut commit = empty_epoch_commit(2, true);
    commit.materialized_add_files = Some(vec![second]);
    assert!(index().advance(&foreign, &commit, true).is_none());

    // Recovered epochs carry no materialized files.
    let commit = empty_epoch_commit(2, false);
    assert!(index().advance(&foreign, &commit, false).is_none());
    Ok(())
}
