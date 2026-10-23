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

use std::collections::HashMap;
use std::slice::from_ref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileMetadata, FileRead, FileWrite, InputFile, ListEntry, LocalFsStorage, OutputFile,
    Storage, StorageConfig, StorageFactory,
};
use iceberg::spec::{
    DataFile, DataFileBuilder, ManifestListWriter, ManifestWriterBuilder, NestedField, Operation,
    PrimitiveType, Schema, SortOrder, Struct, TableMetadata, TableMetadataBuilder, Transform,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::{NamespaceIdent, Runtime, TableIdent};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;

use super::*;

/// Count real read/stat/reader requests below `FileIO`, including attempted missing-file reads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CountingIo {
    #[serde(skip)]
    pub metadata_reads: Arc<AtomicUsize>,
    #[serde(skip)]
    pub row_reads: Arc<AtomicUsize>,
    #[serde(skip)]
    manifest_reads: Arc<Mutex<HashMap<String, usize>>>,
}

impl CountingIo {
    fn read_request(&self, path: &str) {
        let counter = if path.ends_with(".avro") {
            if path.contains("/manifest-") {
                *self
                    .manifest_reads
                    .lock()
                    .unwrap()
                    .entry(path.to_owned())
                    .or_default() += 1;
            }
            &self.metadata_reads
        } else {
            &self.row_reads
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[typetag::serde]
impl StorageFactory for CountingIo {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for CountingIo {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.read_request(path);
        LocalFsStorage.exists(path).await
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        self.read_request(path);
        LocalFsStorage.metadata(path).await
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        self.read_request(path);
        LocalFsStorage.read(path).await
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        self.read_request(path);
        LocalFsStorage.reader(path).await
    }

    async fn write(&self, path: &str, bytes: Bytes) -> iceberg::Result<()> {
        LocalFsStorage.write(path, bytes).await
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        LocalFsStorage.writer(path).await
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        LocalFsStorage.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        LocalFsStorage.delete_prefix(path).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        LocalFsStorage.delete_stream(paths).await
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> iceberg::Result<BoxStream<'static, iceberg::Result<ListEntry>>> {
        self.read_request(path);
        LocalFsStorage.list(path, recursive).await
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}

#[test]
fn snapshot_operation_is_authoritative() {
    for operation in [Operation::Append, Operation::Overwrite, Operation::Delete] {
        assert_eq!(
            IcebergCommitKind::from_operation(&operation),
            IcebergCommitKind::Data
        );
    }
    assert_eq!(
        IcebergCommitKind::from_operation(&Operation::Replace),
        IcebergCommitKind::Compaction
    );
}

fn table(temp: &TempDir, version: FormatVersion, partitioned: bool) -> Result<Table> {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "_row_id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let mut properties = HashMap::new();
    properties.insert(
        NAME_MAPPING.to_owned(),
        json!([
            {"field-id": 1, "names": ["value"]}, {"field-id": 2, "names": ["_row_id"]}
        ])
        .to_string(),
    );
    let spec = if partitioned {
        UnboundPartitionSpec::builder()
            .add_partition_fields(vec![UnboundPartitionField {
                source_id: 1,
                field_id: Some(1000),
                name: "value".to_owned(),
                transform: Transform::Identity,
            }])?
            .build()
    } else {
        UnboundPartitionSpec::builder().build()
    };
    let metadata = TableMetadataBuilder::new(
        schema,
        spec,
        SortOrder::unsorted_order(),
        format!("file://{}", temp.path().display()),
        version,
        properties,
    )?
    .build()?
    .metadata;
    Ok(Table::builder()
        .identifier(TableIdent::new(
            NamespaceIdent::new("db".to_owned()),
            "changes".to_owned(),
        ))
        .metadata(metadata)
        .file_io(FileIO::new_with_fs())
        .runtime(Runtime::try_current()?)
        .build()?)
}

fn replace_metadata(table: &Table, metadata: TableMetadata) -> Result<Table> {
    Ok(Table::builder()
        .identifier(table.identifier().clone())
        .metadata(metadata)
        .file_io(table.file_io().clone())
        .runtime(Runtime::try_current()?)
        .build()?)
}

#[tokio::test]
async fn downstream_columns_bind_stored_hidden_keys_in_projection_order() -> Result<()> {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;

    let temp = TempDir::new()?;
    let table = table(&temp, FormatVersion::V3, false)?;
    // RW column IDs need not equal Iceberg field IDs. Hidden stored columns are not virtual.
    let columns = vec![
        ColumnCatalog::hidden(ColumnDesc::named(
            "_row_id",
            ColumnId::new(99),
            DataType::Int64,
        )),
        ColumnCatalog::visible(ColumnDesc::named(
            "value",
            ColumnId::new(100),
            DataType::Int64,
        )),
    ];
    let binding = IcebergUpdateBinding::bind_columns(&table, &columns)?;
    assert_eq!(binding.project_field_ids, vec![2, 1]);
    binding.validate_columns(&columns)?;
    assert!(binding.validate_columns(&columns[1..]).is_err());
    let mut reordered = columns.clone();
    reordered.reverse();
    assert!(binding.validate_columns(&reordered).is_err());
    assert!(IcebergUpdateBinding::bind_columns(&table, &columns[1..]).is_ok());
    let mut virtual_columns = columns.clone();
    virtual_columns.extend(ColumnCatalog::iceberg_hidden_cols());
    assert!(IcebergUpdateBinding::bind_columns(&table, &virtual_columns).is_err());
    let mut duplicate = columns;
    duplicate.push(duplicate[0].clone());
    assert!(IcebergUpdateBinding::bind_columns(&table, &duplicate).is_err());
    Ok(())
}

// These artifacts deliberately do not exist. Planning must only open metadata files.
fn data(table: &Table, name: &str) -> Result<DataFile> {
    Ok(DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/{name}.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(10)
        .first_row_id(
            (table.metadata().format_version() == FormatVersion::V3)
                .then_some(i64::from(name.as_bytes()[0]) * 10),
        )
        .partition(
            if table
                .metadata()
                .default_partition_spec()
                .fields()
                .is_empty()
            {
                Struct::empty()
            } else {
                Struct::from_iter([Some(Literal::long(7))])
            },
        )
        .build()?)
}

fn delete(table: &Table, file: &DataFile, name: &str, offset: i64) -> Result<DataFile> {
    let v3 = table.metadata().format_version() == FormatVersion::V3;
    Ok(DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(format!("{}/delete/{name}", table.metadata().location()))
        .file_format(if v3 {
            DataFileFormat::Puffin
        } else {
            DataFileFormat::Parquet
        })
        .file_size_in_bytes(100)
        .record_count(1)
        .partition(file.partition().clone())
        .referenced_data_file(Some(file.file_path().to_owned()))
        .content_offset(v3.then_some(offset))
        .content_size_in_bytes(v3.then_some(10))
        .build()?)
}

async fn snapshot(
    table: &Table,
    id: i64,
    operation: Operation,
    files: &[DataFile],
    deletes: &[(DataFile, i64)],
) -> Result<Table> {
    snapshot_with_manifest_size(table, id, operation, files, deletes, files.len().max(1)).await
}

async fn snapshot_with_manifest_size(
    table: &Table,
    id: i64,
    operation: Operation,
    files: &[DataFile],
    deletes: &[(DataFile, i64)],
    manifest_size: usize,
) -> Result<Table> {
    let metadata = table.metadata();
    let sequence = metadata.last_sequence_number() + 1;
    let parent = metadata.current_snapshot_id();
    let mut manifests = vec![];
    for (is_delete, entries) in [
        (
            false,
            files
                .iter()
                .cloned()
                .map(|file| (file, 1))
                .collect::<Vec<_>>(),
        ),
        (true, deletes.to_vec()),
    ] {
        if entries.is_empty() {
            continue;
        }
        for (index, entries) in entries.chunks(manifest_size).enumerate() {
            let output = table.file_io().new_output(format!(
                "{}/manifest-{id}-{is_delete}-{index}.avro",
                metadata.location()
            ))?;
            let builder = ManifestWriterBuilder::new(
                output,
                Some(id),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
            );
            let mut writer = match (metadata.format_version(), is_delete) {
                (FormatVersion::V3, true) => builder.build_v3_deletes(),
                (FormatVersion::V3, false) => builder.build_v3_data(),
                (_, true) => builder.build_v2_deletes(),
                (_, false) => builder.build_v2_data(),
            };
            for (file, seq) in entries {
                writer.add_existing_file(file.clone(), id, *seq, Some(1))?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }
    }
    let path = format!("{}/list-{id}.avro", metadata.location());
    let output = table.file_io().new_output(&path)?;
    let mut writer = if metadata.format_version() == FormatVersion::V3 {
        ManifestListWriter::v3(output.writer().await?, id, parent, sequence, Some(0))
    } else {
        ManifestListWriter::v2(output.writer().await?, id, parent, sequence)
    };
    writer.add_manifests(manifests.into_iter())?;
    writer.close().await?;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(sequence)
        .with_timestamp_ms(metadata.last_updated_ms() + 1)
        .with_manifest_list(path)
        .with_summary(iceberg::spec::Summary {
            operation,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(metadata.current_schema_id())
        .with_row_range(0, 0)
        .build();
    let metadata = metadata
        .clone()
        .into_builder(None)
        .set_branch_snapshot(snapshot, "main")?
        .build()?
        .metadata;
    replace_metadata(table, metadata)
}

fn planner(table: &Table) -> Result<IcebergUpdatePlanner> {
    let binding = IcebergUpdateBinding::bind(table, vec![2, 1])?;
    let restored = serde_json::from_slice(&serde_json::to_vec(&binding)?)?;
    assert_eq!(binding, restored);
    Ok(IcebergUpdatePlanner::new(restored))
}

#[tokio::test]
async fn v2_v3_bootstrap_compaction_and_classify_new_versus_retained_files() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, true)?;
        let a = data(&base, "a")?;
        let b = data(&base, "b")?;
        let d1 = delete(&base, &a, "parent", 4)?;
        let first = snapshot(
            &base,
            90,
            Operation::Replace,
            from_ref(&a),
            &[(d1.clone(), 1)],
        )
        .await?;
        let planner = planner(&first)?;
        let bootstrap = planner.plan_bootstrap(&first).await?.unwrap();
        assert!(bootstrap.deletes.is_empty());
        assert_eq!(bootstrap.inserts.len(), 1);
        let d2 = delete(&base, &a, "current", 14)?;
        let db = delete(&base, &b, "new-filter", 24)?;
        // IDs intentionally decrease: ancestry, not numeric sorting, defines the order.
        let second = snapshot(
            &first,
            7,
            Operation::Overwrite,
            &[b.clone(), a.clone()],
            &[(db, 2), (d2, 2)],
        )
        .await?;
        let plan = planner.plan_next(&second, Some(90)).await?.unwrap();
        assert_eq!(plan.snapshot_id, 7);
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.inserts.len(), 1);
        assert_eq!(plan.deletes[0].id.data_file_path, a.file_path());
        assert_eq!(plan.inserts[0].id.data_file_path, b.file_path());
        let restored: IcebergUpdatePlan = serde_json::from_slice(&serde_json::to_vec(&plan)?)?;
        assert_eq!(plan, restored);
        assert_eq!(plan, planner.plan_next(&second, Some(90)).await?.unwrap());
        let (task, mode) = restored.deletes[0].prepare_read(&second)?;
        assert_eq!(task.project_field_ids, vec![2, 1]);
        assert_eq!(task.partition, Some(a.partition().clone()));
        assert_eq!(
            task.partition_spec.as_deref(),
            Some(base.metadata().default_partition_spec().as_ref())
        );
        assert_eq!(task.name_mapping, read_name_mapping(&base)?);
        assert_eq!(task.data_sequence_number, Some(1));
        assert_eq!(task.file_sequence_number, Some(1));
        let IcebergUpdateReadMode::Delete { parent_deletes } = mode else {
            panic!("expected Delete")
        };
        assert_eq!(parent_deletes[0].file_path, d1.file_path());
        assert!(matches!(
            restored.inserts[0].prepare_read(&second)?.1,
            IcebergUpdateReadMode::Insert
        ));
        assert_eq!(
            restored.inserts[0].prepare_read(&second)?.0.deletes.len(),
            1
        );
        assert!(planner.plan_next(&second, Some(7)).await?.is_none());
        // A replayed bootstrap remains pinned, even when the latest snapshot has changed.
        assert_eq!(bootstrap, planner.plan_bootstrap(&second).await?.unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn v2_v3_compactions_refresh_baseline_without_opening_data_or_delete_files() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let a = data(&base, "a")?;
        let b = data(&base, "b")?;
        let c = data(&base, "c")?;
        let first = snapshot(&base, 90, Operation::Overwrite, &[a], &[]).await?;
        let planner = planner(&first)?;
        let second = snapshot(&first, 20, Operation::Replace, &[b], &[]).await?;
        let third = snapshot(&second, 60, Operation::Replace, from_ref(&c), &[]).await?;
        let dc = delete(&base, &c, "delete-c", 4)?;
        let fourth = snapshot(&third, 10, Operation::Overwrite, from_ref(&c), &[(dc, 4)]).await?;
        for (parent, current) in [(90, 20), (20, 60)] {
            let plan = planner.plan_next(&fourth, Some(parent)).await?.unwrap();
            assert_eq!(plan.snapshot_id, current);
            assert!(plan.inserts.is_empty() && plan.deletes.is_empty());
        }
        let plan = planner.plan_next(&fourth, Some(60)).await?.unwrap();
        assert!(plan.inserts.is_empty());
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.deletes[0].id.data_file_path, c.file_path());
    }
    Ok(())
}

#[tokio::test]
async fn v2_v3_new_file_deletes_never_create_a_delete_phase() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let planner = planner(&base)?;
        assert!(planner.plan_bootstrap(&base).await?.is_none());
        assert!(planner.plan_next(&base, None).await?.is_none());
        let a = data(&base, "a")?;
        let d = delete(&base, &a, "new-filter", 4)?;
        let first = snapshot(&base, 1, Operation::Overwrite, &[a], &[(d, 1)]).await?;
        let plan = planner.plan_next(&first, None).await?.unwrap();
        assert!(plan.deletes.is_empty());
        assert_eq!(plan.inserts.len(), 1);
        // Empty bootstrap is fixed, not replaced by the new current snapshot.
        assert!(planner.plan_bootstrap(&first).await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn v3_same_puffin_path_different_blob_is_a_delete_change() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V3, false)?;
    let a = data(&base, "a")?;
    let d1 = delete(&base, &a, "shared.puffin", 4)?;
    let first = snapshot(&base, 1, Operation::Overwrite, from_ref(&a), &[(d1, 1)]).await?;
    let planner = planner(&first)?;
    let d2 = delete(&base, &a, "shared.puffin", 24)?;
    let second = snapshot(&first, 2, Operation::Overwrite, &[a], &[(d2, 2)]).await?;
    let plan = planner.plan_next(&second, Some(1)).await?.unwrap();
    assert_eq!(plan.deletes.len(), 1);
    let task = &plan.deletes[0];
    assert_eq!(
        task.file.deletes[0].file_path,
        task.parent_deletes[0].file_path
    );
    assert_ne!(
        task.file.deletes[0].content_offset,
        task.parent_deletes[0].content_offset
    );
    Ok(())
}

#[tokio::test]
async fn rejects_rollback_expiration_and_divergent_history() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V2, false)?;
    let a = data(&base, "a")?;
    let first = snapshot(&base, 3, Operation::Overwrite, from_ref(&a), &[]).await?;
    let planner = planner(&first)?;
    let second = snapshot(&first, 2, Operation::Overwrite, from_ref(&a), &[]).await?;
    assert!(planner.plan_next(&first, Some(2)).await.is_err());
    assert!(planner.plan_next(&base, Some(3)).await.is_err());
    let expired = replace_metadata(
        &second,
        second
            .metadata()
            .clone()
            .into_builder(None)
            .remove_snapshots(&[3])
            .build()?
            .metadata,
    )?;
    assert!(planner.plan_next(&expired, Some(3)).await.is_err());
    assert!(planner.plan_bootstrap(&expired).await.is_err());
    // Existing but non-ancestor snapshot must not be mistaken for valid progress.
    let rolled_back = replace_metadata(
        &second,
        second
            .metadata()
            .clone()
            .into_builder(None)
            .set_ref(
                "main",
                iceberg::spec::SnapshotReference::new(
                    3,
                    iceberg::spec::SnapshotRetention::branch(None, None, None),
                ),
            )?
            .build()?
            .metadata,
    )?;
    assert!(planner.plan_next(&rolled_back, Some(2)).await.is_err());
    let diverged = snapshot(&rolled_back, 12, Operation::Overwrite, &[a], &[]).await?;
    assert!(planner.plan_next(&diverged, Some(2)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn rejects_duplicate_artifacts_and_descriptor_rebinding() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V2, false)?;
    let a = data(&base, "a")?;
    let d1 = delete(&base, &a, "d1", 4)?;
    let d2 = delete(&base, &a, "d2", 4)?;
    let first = snapshot(&base, 1, Operation::Overwrite, from_ref(&a), &[]).await?;
    let planner = planner(&first)?;
    let invalid = snapshot(
        &first,
        2,
        Operation::Replace,
        from_ref(&a),
        &[(d1, 2), (d2, 2)],
    )
    .await?;
    assert!(planner.plan_next(&invalid, Some(1)).await.is_err());
    let duplicate = snapshot(&first, 3, Operation::Overwrite, &[a.clone(), a], &[]).await?;
    assert!(planner.plan_next(&duplicate, Some(1)).await.is_err());
    let metadata = first
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(NAME_MAPPING.to_owned(), "[]".to_owned())]))?
        .build()?
        .metadata;
    let changed = replace_metadata(&first, metadata)?;
    assert!(planner.plan_bootstrap(&changed).await.is_err());
    let other = table(&tempfile::tempdir()?, FormatVersion::V2, false)?;
    assert!(planner.plan_bootstrap(&other).await.is_err());
    assert!(IcebergUpdateBinding::bind(&first, vec![1]).is_ok());
    assert!(IcebergUpdateBinding::bind(&first, vec![99]).is_err());
    assert!(IcebergUpdateBinding::bind(&first, vec![2, 2]).is_err());
    assert!(
        IcebergUpdateBinding::bind(
            &first,
            vec![2, iceberg::metadata_columns::RESERVED_FIELD_ID_POS]
        )
        .is_err()
    );
    Ok(())
}

async fn write_rows(table: &Table, name: &str, first_key: i64) -> Result<DataFile> {
    use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use parquet::file::properties::WriterProperties;
    use risingwave_common::array::arrow::arrow_array_iceberg::{Int64Array, RecordBatch};

    let schema = table.metadata().current_schema();
    let mut writer = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
        .build(
            table
                .file_io()
                .new_output(format!("{}/{name}.parquet", table.metadata().location()))?,
        )
        .await?;
    writer
        .write(&RecordBatch::try_new(
            Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?),
            vec![
                Arc::new(Int64Array::from_iter_values(0..3)),
                Arc::new(Int64Array::from_iter_values(first_key..first_key + 3)),
            ],
        )?)
        .await?;
    Ok(writer.close().await?.into_iter().next().unwrap().build()?)
}

async fn write_delete(table: &Table, file: &DataFile, position: u64) -> Result<DataFile> {
    use iceberg::delete_vector::DeleteVector;
    use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;

    use crate::sink::iceberg::{
        IcebergConfig, PositionDeleteFileNameGenerators, write_position_delete_file,
    };

    let config = IcebergConfig::from_btreemap(BTreeMap::from([
        ("type".to_owned(), "upsert".to_owned()),
        ("primary_key".to_owned(), "_row_id".to_owned()),
        (
            "warehouse.path".to_owned(),
            table.metadata().location().to_owned(),
        ),
        ("catalog.type".to_owned(), "storage".to_owned()),
        ("database.name".to_owned(), "db".to_owned()),
        ("table.name".to_owned(), "changes".to_owned()),
    ]))?;
    write_position_delete_file(
        table,
        &config,
        &DefaultLocationGenerator::new(table.metadata())?,
        &PositionDeleteFileNameGenerators::new(Uuid::new_v4()),
        table.metadata().format_version(),
        file.file_path().to_owned(),
        &DeleteVector::from([position]),
        None,
    )
    .await
}

async fn read_plan(
    table: &Table,
    plan: &IcebergUpdatePlan,
) -> Result<Vec<(risingwave_common::array::Op, i64, i64)>> {
    use futures::StreamExt;
    use risingwave_common::row::Row;
    use risingwave_common::types::ScalarRefImpl;

    use crate::sink::iceberg::PositionDeleteReader;
    use crate::source::iceberg::update_reader::read_file_updates;

    let restored: IcebergUpdatePlan = serde_json::from_slice(&serde_json::to_vec(plan)?)?;
    let mut reader = PositionDeleteReader::new(table.file_io());
    let mut rows = vec![];
    // This is a local planner/reader test, not the distributed checkpoint fence.
    for task in restored.deletes.iter().chain(&restored.inserts) {
        let (scan, mode) = task.prepare_read(table)?;
        let mut batches = read_file_updates(table.clone(), &mut reader, scan, mode, 2, 0);
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            for (op, row) in batch.chunk.rows() {
                let Some(ScalarRefImpl::Int64(key)) = row.datum_at(0) else {
                    panic!("missing key")
                };
                let Some(ScalarRefImpl::Int64(value)) = row.datum_at(1) else {
                    panic!("missing value")
                };
                rows.push((op, key, value));
            }
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn v2_v3_persisted_plans_feed_reader_without_compaction_events() -> Result<()> {
    use risingwave_common::array::Op;

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let original = write_rows(&base, "original", 100).await?;
        let first = snapshot(&base, 1, Operation::Replace, &[original], &[]).await?;
        let planner = planner(&first)?;
        let bootstrap = planner.plan_bootstrap(&first).await?.unwrap();
        let inserted = read_plan(&first, &bootstrap).await?;
        assert_eq!(
            inserted,
            (0..3).map(|i| (Op::Insert, 100 + i, i)).collect::<Vec<_>>()
        );

        let compacted = write_rows(&base, "compacted", 100).await?;
        let second = snapshot(&first, 2, Operation::Replace, from_ref(&compacted), &[]).await?;
        assert!(
            read_plan(
                &second,
                &planner.plan_next(&second, Some(1)).await?.unwrap()
            )
            .await?
            .is_empty()
        );

        let new = write_rows(&base, "new", 200).await?;
        let old_delete = write_delete(&base, &compacted, 1).await?;
        let new_filter = write_delete(&base, &new, 0).await?;
        let third = snapshot(
            &second,
            3,
            Operation::Overwrite,
            &[compacted, new],
            &[(old_delete, 3), (new_filter, 3)],
        )
        .await?;
        let plan = planner.plan_next(&third, Some(2)).await?.unwrap();
        let rows = read_plan(&third, &plan).await?;
        assert_eq!(
            rows,
            vec![
                (Op::Delete, inserted[1].1, inserted[1].2),
                (Op::Insert, 201, 1),
                (Op::Insert, 202, 2),
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn overwrite_pages_retained_and_removed_files_without_path_ordering() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let a = data(&base, "a")?;
        let z = data(&base, "z")?;
        let first = snapshot(&base, 1, Operation::Overwrite, &[a.clone(), z.clone()], &[]).await?;
        let deletion = delete(&base, &z, "z-delete", 0)?;
        let second = snapshot(
            &first,
            2,
            Operation::Overwrite,
            from_ref(&z),
            &[(deletion, 2)],
        )
        .await?;
        let mut planner = planner(&first)?;
        let mut cursor = planner.start_cursor(&second, true, Some(1))?.unwrap();
        let page = planner.plan_page(&second, &cursor, 1).await?;
        assert_eq!(page.tasks.len(), 1);
        assert_eq!(page.tasks[0].id.data_file_path, z.file_path());
        assert!(!page.tasks[0].removed);
        assert!(!page.phase_finished);
        cursor.after_path = Some(page.tasks[0].id.data_file_path.clone());
        let cursor = serde_json::from_slice(&serde_json::to_vec(&cursor)?)?;
        let mut planner = IcebergUpdatePlanner::new(planner.binding.as_ref().clone());
        let page = planner.plan_page(&second, &cursor, 1).await?;
        assert_eq!(page.tasks.len(), 1);
        assert_eq!(page.tasks[0].id.data_file_path, a.file_path());
        assert!(page.tasks[0].removed);
        assert!(page.phase_finished);
    }
    Ok(())
}

#[tokio::test]
async fn v2_v3_legacy_overwrite_compaction_with_writes_preserves_logical_changes() -> Result<()> {
    use risingwave_common::array::Op;

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let original = write_rows(&base, "original", 100).await?;
        let old_delete = write_delete(&base, &original, 1).await?;
        let first = snapshot(
            &base,
            1,
            Operation::Overwrite,
            &[original],
            &[(old_delete, 1)],
        )
        .await?;
        let planner = planner(&first)?;
        let bootstrap = planner.plan_bootstrap(&first).await?.unwrap();
        let initial = read_plan(&first, &bootstrap).await?;
        assert_eq!(initial, vec![(Op::Insert, 100, 0), (Op::Insert, 102, 2)]);

        // Older coordinators used Overwrite for both pure and mixed compaction epochs.
        let replacement = write_rows(&base, "replacement", 100).await?;
        let replacement_delete = write_delete(&base, &replacement, 1).await?;
        let new = write_rows(&base, "new", 200).await?;
        let new_delete = write_delete(&base, &new, 2).await?;
        let second = snapshot(
            &first,
            2,
            Operation::Overwrite,
            &[replacement, new],
            &[(replacement_delete, 2), (new_delete, 2)],
        )
        .await?;
        let plan = planner.plan_next(&second, Some(1)).await?.unwrap();
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.inserts.len(), 2);
        let rows = read_plan(&second, &plan).await?;
        assert_eq!(&rows[..2], &[(Op::Delete, 100, 0), (Op::Delete, 102, 2)]);
        let mut inserted = rows[2..].to_vec();
        inserted.sort_by_key(|(_, key, _)| *key);
        assert_eq!(
            inserted,
            vec![
                (Op::Insert, 100, 0),
                (Op::Insert, 102, 2),
                (Op::Insert, 200, 0),
                (Op::Insert, 201, 1),
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn paged_list_dispatches_continuously_and_recovers_its_phase_fence() -> Result<()> {
    use crate::source::iceberg::update_state::IcebergUpdateListState;

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let files = ["c", "a", "b"]
            .map(|name| data(&base, name))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let first = snapshot(&base, 9, Operation::Overwrite, &files, &[]).await?;
        let binding = IcebergUpdateBinding::bind(&first, vec![2, 1])?;
        let batch_size = 2;
        let mut enumeration = None;
        let mut state = IcebergUpdateListState::new(42, binding);
        let (page, finished) = state
            .enumerate(&first, batch_size, &mut enumeration)
            .await?;
        assert_eq!(page.len(), 2);
        assert!(!finished);
        assert_eq!(page[0].task.id.data_file_path, files[0].file_path());
        assert_eq!(page[1].task.id.data_file_path, files[1].file_path());
        let encoded = serde_json::to_value(&state)?;
        assert!(encoded.get("pending").is_none());
        state = serde_json::from_value(encoded)?;
        enumeration = None;

        // Latest changes before Fetch completes the first page. Dispatch continues from
        // the persisted exclusive cursor, still against the original bootstrap snapshot.
        let mut next_files = files.clone();
        next_files.push(data(&base, "d")?);
        let next = snapshot(&first, 3, Operation::Overwrite, &next_files, &[]).await?;
        let (last, finished) = state.enumerate(&next, batch_size, &mut enumeration).await?;
        assert_eq!(last.len(), 1);
        assert!(finished);
        assert_eq!(last[0].task.id.snapshot_id, 9);
        assert_eq!(last[0].task.id.data_file_path, files[2].file_path());
        assert!(last[0].generation > page[0].generation);
        state.record_phase_end(100)?;
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        enumeration = None;
        assert_eq!(state.phase_end_epoch(), Some(100));
        assert!(
            state
                .enumerate(&next, batch_size, &mut enumeration)
                .await
                .is_err()
        );
        assert!(state.complete_phase(99).is_err());
        state.complete_phase(100)?;
        assert!(state.complete_phase(100).is_err());

        // An exhaustive empty Delete phase can advance without waiting for a commit.
        assert!(
            state
                .enumerate(&next, batch_size, &mut enumeration)
                .await?
                .0
                .is_empty()
        );
        let (inserts, finished) = state.enumerate(&next, batch_size, &mut enumeration).await?;
        assert!(finished);
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].task.id.snapshot_id, 3);
        assert_eq!(inserts[0].task.id.phase, IcebergUpdatePhase::Insert);
    }
    Ok(())
}
#[tokio::test]
async fn partial_fetch_eof_and_zero_batch_size_fail_closed() -> Result<()> {
    use crate::source::iceberg::update_state::{IcebergUpdateFetchState, IcebergUpdateListState};

    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V3, false)?;
    let first = snapshot(
        &base,
        1,
        Operation::Overwrite,
        &[data(&base, "a")?, data(&base, "b")?],
        &[],
    )
    .await?;
    let binding = IcebergUpdateBinding::bind(&first, vec![2, 1])?;
    let mut enumeration = None;
    let mut state = IcebergUpdateListState::new(1, binding.clone());
    let (page, _) = state.enumerate(&first, 2, &mut enumeration).await?;
    let mut progress = IcebergUpdateFetchState::new(page[0].clone());
    progress.advance(4, false)?;
    progress = serde_json::from_slice(&serde_json::to_vec(&progress)?)?;
    assert_eq!(progress.next_position, 4);
    assert!(progress.advance(3, false).is_err());
    assert!(progress.advance(9, true).is_err());
    assert!(progress.advance(11, false).is_err());
    progress.advance(10, false)?;
    assert!(!progress.finished);
    progress.advance(10, true)?;
    assert!(progress.advance(10, true).is_err());

    let mut enumeration = None;

    let mut state = IcebergUpdateListState::new(1, binding);
    let before = state.clone();
    assert!(state.enumerate(&first, 0, &mut enumeration).await.is_err());
    assert_eq!(state, before);
    Ok(())
}

#[tokio::test]
async fn count_limited_pages_resume_from_the_last_dispatched_task() -> Result<()> {
    use crate::source::iceberg::update_state::IcebergUpdateListState;

    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V3, false)?;
    let first = snapshot(
        &base,
        1,
        Operation::Overwrite,
        &[data(&base, "a")?, data(&base, "b")?],
        &[],
    )
    .await?;
    let binding = IcebergUpdateBinding::bind(&first, vec![2, 1])?;
    let batch_size = 1;
    let mut enumeration = None;
    let mut state = IcebergUpdateListState::new(42, binding);
    let mut paths = vec![];
    for index in 0..2 {
        let (page, finished) = state
            .enumerate(&first, batch_size, &mut enumeration)
            .await?;
        assert_eq!(page.len(), batch_size);
        assert_eq!(finished, index == 1);
        let encoded = serde_json::to_value(&state)?;
        assert!(encoded.get("pending").is_none());
        state = serde_json::from_value(encoded)?;
        paths.push(page[0].task.id.data_file_path.clone());
        if finished {
            state.record_phase_end(2)?;
        }
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        enumeration = None;
    }
    assert!(paths[0] < paths[1]);
    assert!(state.complete_phase(1).is_err());
    state.complete_phase(2)?;
    assert!(
        state
            .enumerate(&first, batch_size, &mut enumeration)
            .await?
            .0
            .is_empty()
    );
    Ok(())
}
#[tokio::test]
async fn compaction_still_validates_delete_descriptors_without_reading_them() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let first = snapshot(&base, 1, Operation::Overwrite, &[data(&base, "a")?], &[]).await?;
        let file = data(&base, "b")?;
        let invalid = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(format!("{}/never-read", base.metadata().location()))
            .file_format(if version == FormatVersion::V3 {
                DataFileFormat::Puffin
            } else {
                DataFileFormat::Parquet
            })
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::empty())
            .referenced_data_file(Some(file.file_path().to_owned()))
            .content_offset(Some(95))
            .content_size_in_bytes(Some(10))
            .build()?;
        let compacted = snapshot(&first, 2, Operation::Replace, &[file], &[(invalid, 2)]).await?;
        let error = planner(&first)?
            .plan_next(&compacted, Some(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("DV range"), "{error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn delete_fence_and_compaction_progress_survive_list_recovery() -> Result<()> {
    use crate::source::iceberg::update_state::IcebergUpdateListState;

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let old = data(&base, "a")?;
        let first = snapshot(&base, 1, Operation::Overwrite, from_ref(&old), &[]).await?;
        let mut enumeration = None;
        let mut state =
            IcebergUpdateListState::new(1, IcebergUpdateBinding::bind(&first, vec![2, 1])?);
        let batch_size = 2;
        let (bootstrap, finished) = state
            .enumerate(&first, batch_size, &mut enumeration)
            .await?;
        assert_eq!(bootstrap.len(), 1);
        assert!(finished);
        state.record_phase_end(1)?;
        state.complete_phase(1)?;

        let compacted = data(&base, "b")?;
        let second = snapshot(&first, 2, Operation::Replace, from_ref(&compacted), &[]).await?;
        assert!(
            state
                .enumerate(&second, batch_size, &mut enumeration)
                .await?
                .0
                .is_empty()
        );
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        enumeration = None;
        assert!(
            state
                .enumerate(&second, batch_size, &mut enumeration)
                .await?
                .0
                .is_empty()
        );
        let new = data(&base, "c")?;
        let d = delete(&base, &compacted, "d", 4)?;
        let third = snapshot(
            &second,
            3,
            Operation::Overwrite,
            &[compacted.clone(), new],
            &[(d, 3)],
        )
        .await?;
        let (deletes, finished) = state
            .enumerate(&third, batch_size, &mut enumeration)
            .await?;
        assert!(finished);
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].task.id.phase, IcebergUpdatePhase::Delete);
        assert_eq!(deletes[0].task.id.data_file_path, compacted.file_path());
        state.record_phase_end(3)?;
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        enumeration = None;
        assert!(
            state
                .enumerate(&third, batch_size, &mut enumeration)
                .await
                .is_err()
        );
        assert!(state.complete_phase(2).is_err());
        state.complete_phase(3)?;
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        enumeration = None;
        let (inserts, finished) = state
            .enumerate(&third, batch_size, &mut enumeration)
            .await?;
        assert!(finished);
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].task.id.phase, IcebergUpdatePhase::Insert);
    }
    Ok(())
}
#[tokio::test]
async fn pages_read_data_manifests_lazily_once_per_scan() -> Result<()> {
    use iceberg::io::FileIOBuilder;

    use crate::source::iceberg::update_state::IcebergUpdateListState;

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let counter = Arc::new(CountingIo::default());
        let base = Table::builder()
            .identifier(base.identifier().clone())
            .metadata(base.metadata().clone())
            .file_io(FileIOBuilder::new(counter.clone()).build())
            .runtime(Runtime::try_current()?)
            .build()?;
        // Neither manifest order nor file order needs to be lexicographic.
        let files = ["z", "a", "y", "b", "x", "c", "w", "d"]
            .map(|name| data(&base, name))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let first =
            snapshot_with_manifest_size(&base, 1, Operation::Replace, &files, &[], 2).await?;
        for batch_size in [1, 2, 32] {
            counter.manifest_reads.lock().unwrap().clear();
            let mut state =
                IcebergUpdateListState::new(42, IcebergUpdateBinding::bind(&first, vec![2, 1])?);
            let mut enumeration = None;
            let mut paths = vec![];
            loop {
                let (page, finished) = state
                    .enumerate(&first, batch_size, &mut enumeration)
                    .await?;
                assert!(page.len() <= batch_size);
                if paths.is_empty() && batch_size == 1 {
                    assert_eq!(counter.manifest_reads.lock().unwrap().len(), 1);
                }
                paths.extend(
                    page.into_iter()
                        .map(|assignment| assignment.task.id.data_file_path),
                );
                if finished {
                    break;
                }
            }
            assert_eq!(
                paths,
                files
                    .iter()
                    .map(|file| file.file_path().to_owned())
                    .collect::<Vec<_>>()
            );
            let reads = counter.manifest_reads.lock().unwrap();
            assert_eq!(reads.len(), 4);
            assert!(reads.values().all(|reads| *reads == 1), "{reads:?}");
        }

        let mut current_files = files[..6].to_vec();
        current_files.extend(
            ["e", "f", "g", "h"]
                .map(|name| data(&base, name))
                .into_iter()
                .collect::<Result<Vec<_>>>()?,
        );
        let deletes = files[..6]
            .iter()
            .enumerate()
            .map(|(index, file)| Ok((delete(&base, file, &format!("delete-{index}"), 4)?, 2)))
            .collect::<Result<Vec<_>>>()?;
        let second = snapshot_with_manifest_size(
            &first,
            2,
            Operation::Overwrite,
            &current_files,
            &deletes,
            2,
        )
        .await?;
        for batch_size in [1, 2, 32] {
            for phase in [IcebergUpdatePhase::Delete, IcebergUpdatePhase::Insert] {
                counter.manifest_reads.lock().unwrap().clear();
                let mut planner = planner(&first)?;
                let mut cursor = planner.start_cursor(&second, true, Some(1))?.unwrap();
                cursor.phase = phase;
                let mut count = 0;
                loop {
                    let page = planner.plan_page(&second, &cursor, batch_size).await?;
                    count += page.tasks.len();
                    cursor.after_path =
                        page.tasks.last().map(|task| task.id.data_file_path.clone());
                    if page.phase_finished {
                        break;
                    }
                }
                assert_eq!(
                    count,
                    if phase == IcebergUpdatePhase::Delete {
                        8
                    } else {
                        4
                    }
                );
                let reads = counter.manifest_reads.lock().unwrap();
                // Four parent data manifests, five current data manifests and three delete manifests.
                assert_eq!(reads.len(), 12);
                assert!(reads.values().all(|reads| *reads == 1), "{reads:?}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn incremental_pages_resume_retained_and_removed_files_across_manifests() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let files = ["z", "a", "y", "b", "c"]
            .map(|name| data(&base, name))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let old_delete = delete(&base, &files[1], "old", 4)?;
        let first = snapshot_with_manifest_size(
            &base,
            1,
            Operation::Overwrite,
            &files[..4],
            &[(old_delete, 1)],
            2,
        )
        .await?;
        let second = snapshot_with_manifest_size(
            &first,
            2,
            Operation::Overwrite,
            &[files[2].clone(), files[1].clone(), files[4].clone()],
            &[
                (delete(&base, &files[1], "new-a", 14)?, 2),
                (delete(&base, &files[2], "new-y", 24)?, 2),
            ],
            1,
        )
        .await?;
        let expected = planner(&first)?.plan_next(&second, Some(1)).await?.unwrap();
        for phase in [IcebergUpdatePhase::Delete, IcebergUpdatePhase::Insert] {
            let mut planner = planner(&first)?;
            let mut cursor = planner.start_cursor(&second, true, Some(1))?.unwrap();
            cursor.phase = phase;
            let mut tasks = vec![];
            loop {
                let page = planner.plan_page(&second, &cursor, 1).await?;
                cursor.after_path = page.tasks.last().map(|task| task.id.data_file_path.clone());
                tasks.extend(page.tasks);
                if page.phase_finished {
                    break;
                }
                cursor = serde_json::from_slice(&serde_json::to_vec(&cursor)?)?;
                // Drop all runtime indexes and lookahead at each simulated recovery.
                planner = IcebergUpdatePlanner::new(planner.binding.as_ref().clone());
            }
            tasks.sort_unstable_by(|left, right| {
                left.id.data_file_path.cmp(&right.id.data_file_path)
            });
            assert_eq!(
                tasks,
                match phase {
                    IcebergUpdatePhase::Delete => expected.deletes.clone(),
                    IcebergUpdatePhase::Insert => expected.inserts.clone(),
                }
            );
        }
        let mut planner = planner(&first)?;
        let cursor = planner.start_cursor(&second, true, Some(1))?.unwrap();
        let mut invalid = cursor.clone();
        invalid.after_path = Some("missing-task".to_owned());
        assert!(planner.plan_page(&second, &invalid, 1).await.is_err());
        assert_eq!(planner.plan_page(&second, &cursor, 1).await?.tasks.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn compaction_has_zero_row_artifact_io() -> Result<()> {
    use std::sync::atomic::Ordering;

    use iceberg::io::FileIOBuilder;

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let counter = Arc::new(CountingIo::default());
        let base = Table::builder()
            .identifier(base.identifier().clone())
            .metadata(base.metadata().clone())
            .file_io(FileIOBuilder::new(counter.clone()).build())
            .runtime(Runtime::try_current()?)
            .build()?;
        let first = snapshot(&base, 1, Operation::Overwrite, &[data(&base, "a")?], &[]).await?;
        let planner = planner(&first)?;
        let b = data(&base, "b")?;
        let second = snapshot(
            &first,
            2,
            Operation::Replace,
            from_ref(&b),
            &[(delete(&base, &b, "d", 4)?, 2)],
        )
        .await?;
        let third = snapshot(&second, 3, Operation::Replace, &[data(&base, "c")?], &[]).await?;
        for (table, parent) in [(&second, 1), (&third, 2)] {
            let plan = planner.plan_next(table, Some(parent)).await?.unwrap();
            assert!(read_plan(table, &plan).await?.is_empty());
        }
        assert!(counter.metadata_reads.load(Ordering::Relaxed) > 0);
        assert_eq!(counter.row_reads.load(Ordering::Relaxed), 0);
    }
    Ok(())
}
