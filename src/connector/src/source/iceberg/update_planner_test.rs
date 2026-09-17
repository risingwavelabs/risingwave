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

use iceberg::io::FileIO;
use iceberg::spec::{
    DataFile, DataFileBuilder, ManifestListWriter, ManifestWriterBuilder, NestedField,
    PrimitiveType, Schema, SortOrder, Struct, TableMetadata, TableMetadataBuilder, Transform,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::{NamespaceIdent, Runtime, TableIdent};
use serde_json::json;
use tempfile::TempDir;

use super::*;

#[path = "update_planner_test_io.rs"]
mod counting_io;

fn table(temp: &TempDir, version: FormatVersion, partitioned: bool) -> Result<Table> {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "_row_id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let mut properties = IcebergSourceContract::new(&schema, vec![2])?.to_properties();
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
    assert!(IcebergUpdateBinding::bind_columns(&table, &columns[1..]).is_err());
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
    kind: Option<IcebergCommitKind>,
    files: &[DataFile],
    deletes: &[(DataFile, i64)],
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
        let output = table.file_io().new_output(format!(
            "{}/manifest-{id}-{is_delete}.avro",
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
            writer.add_existing_file(file, id, seq, Some(1))?;
        }
        manifests.push(writer.write_manifest_file().await?);
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
            operation: iceberg::spec::Operation::Overwrite,
            additional_properties: kind.map(|kind| kind.to_properties()).unwrap_or_default(),
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
            Some(IcebergCommitKind::Compaction),
            from_ref(&a),
            &[(d1.clone(), 1)],
        )
        .await?;
        let planner = planner(&first)?;
        let bootstrap = planner.plan_bootstrap(&first).await?.unwrap();
        assert!(bootstrap.bootstrap);
        assert_eq!(bootstrap.kind, IcebergCommitKind::Compaction);
        assert!(bootstrap.deletes.is_empty());
        assert_eq!(bootstrap.inserts.len(), 1);
        let d2 = delete(&base, &a, "current", 14)?;
        let db = delete(&base, &b, "new-filter", 24)?;
        // IDs intentionally decrease: ancestry, not numeric sorting, defines the order.
        let second = snapshot(
            &first,
            7,
            Some(IcebergCommitKind::Data),
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
        let (task, contract, mode) = restored.deletes[0].read_contract(&second)?;
        assert_eq!(task.project_field_ids, vec![2, 1]);
        assert_eq!(contract.key_field_ids(), &[2]);
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
            restored.inserts[0].read_contract(&second)?.2,
            IcebergUpdateReadMode::Insert
        ));
        assert_eq!(
            restored.inserts[0].read_contract(&second)?.0.deletes.len(),
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
        let first = snapshot(&base, 90, Some(IcebergCommitKind::Data), &[a], &[]).await?;
        let planner = planner(&first)?;
        let second = snapshot(&first, 20, Some(IcebergCommitKind::Compaction), &[b], &[]).await?;
        let third = snapshot(
            &second,
            60,
            Some(IcebergCommitKind::Compaction),
            from_ref(&c),
            &[],
        )
        .await?;
        let dc = delete(&base, &c, "delete-c", 4)?;
        let fourth = snapshot(
            &third,
            10,
            Some(IcebergCommitKind::Data),
            from_ref(&c),
            &[(dc, 4)],
        )
        .await?;
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
        let first = snapshot(&base, 1, Some(IcebergCommitKind::Data), &[a], &[(d, 1)]).await?;
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
    let first = snapshot(
        &base,
        1,
        Some(IcebergCommitKind::Data),
        from_ref(&a),
        &[(d1, 1)],
    )
    .await?;
    let planner = planner(&first)?;
    let d2 = delete(&base, &a, "shared.puffin", 24)?;
    let second = snapshot(&first, 2, Some(IcebergCommitKind::Data), &[a], &[(d2, 2)]).await?;
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
async fn rejects_rollback_expiration_unmarked_history_and_whole_file_removal() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V2, false)?;
    let a = data(&base, "a")?;
    let first = snapshot(&base, 3, Some(IcebergCommitKind::Data), from_ref(&a), &[]).await?;
    let planner = planner(&first)?;
    let second = snapshot(&first, 2, Some(IcebergCommitKind::Data), from_ref(&a), &[]).await?;
    assert!(planner.plan_next(&first, Some(2)).await.is_err());
    assert!(planner.plan_next(&base, Some(3)).await.is_err());
    let unmarked = snapshot(&second, 7, None, from_ref(&a), &[]).await?;
    assert!(planner.plan_next(&unmarked, Some(3)).await.is_err());
    let removed = snapshot(&second, 8, Some(IcebergCommitKind::Data), &[], &[]).await?;
    assert!(planner.plan_next(&removed, Some(2)).await.is_err());
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
    let diverged = snapshot(&rolled_back, 12, Some(IcebergCommitKind::Data), &[a], &[]).await?;
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
    let first = snapshot(&base, 1, Some(IcebergCommitKind::Data), from_ref(&a), &[]).await?;
    let planner = planner(&first)?;
    let invalid = snapshot(
        &first,
        2,
        Some(IcebergCommitKind::Compaction),
        from_ref(&a),
        &[(d1, 2), (d2, 2)],
    )
    .await?;
    assert!(planner.plan_next(&invalid, Some(1)).await.is_err());
    let duplicate = snapshot(
        &first,
        3,
        Some(IcebergCommitKind::Data),
        &[a.clone(), a],
        &[],
    )
    .await?;
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
    assert!(IcebergUpdateBinding::bind(&first, vec![1]).is_err());
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
        &PositionDeleteFileNameGenerators::new(position),
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
        let (scan, contract, mode) = task.read_contract(table)?;
        let mut batches = read_file_updates(table.clone(), &mut reader, scan, contract, mode, 2, 0);
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
        let first = snapshot(
            &base,
            1,
            Some(IcebergCommitKind::Compaction),
            &[original],
            &[],
        )
        .await?;
        let planner = planner(&first)?;
        let bootstrap = planner.plan_bootstrap(&first).await?.unwrap();
        let inserted = read_plan(&first, &bootstrap).await?;
        assert_eq!(
            inserted,
            (0..3).map(|i| (Op::Insert, 100 + i, i)).collect::<Vec<_>>()
        );

        let compacted = write_rows(&base, "compacted", 100).await?;
        let second = snapshot(
            &first,
            2,
            Some(IcebergCommitKind::Compaction),
            from_ref(&compacted),
            &[],
        )
        .await?;
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
            Some(IcebergCommitKind::Data),
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
async fn paged_list_recovery_is_pinned_and_waits_for_every_committed_task() -> Result<()> {
    use crate::source::iceberg::update_state::{IcebergUpdateFetchState, IcebergUpdateListState};

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let files = ["c", "a", "b"]
            .map(|name| data(&base, name))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let first = snapshot(&base, 9, Some(IcebergCommitKind::Data), &files, &[]).await?;
        let binding = IcebergUpdateBinding::bind(&first, vec![2, 1])?;
        let limits = IcebergUpdateLimits {
            page_size: 2,
            ..Default::default()
        };
        let mut state = IcebergUpdateListState::new(42, binding);
        let page = state.enumerate(&first, limits).await?;
        assert_eq!(page.len(), 2);
        assert!(page[0].task.id.data_file_path < page[1].task.id.data_file_path);
        assert!(state.enumerate(&first, limits).await.is_err());
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        assert_eq!(state.assignments(), page);
        let mut done: Vec<_> = page
            .iter()
            .cloned()
            .map(IcebergUpdateFetchState::new)
            .collect();
        done[0].advance(10, true)?;
        assert!(!state.acknowledge_committed(&done)?);
        let mut wrong_job = done[0].clone();
        wrong_job.assignment.job_id += 1;
        assert!(!state.acknowledge_committed(&[wrong_job])?);
        done[1].advance(10, true)?;
        let duplicate = vec![done[0].clone(), done[0].clone(), done[1].clone()];
        assert!(!state.acknowledge_committed(&duplicate)?);
        assert!(state.acknowledge_committed(&done)?);
        assert!(!state.acknowledge_committed(&done)?);
        // Latest changes while bootstrap is only partly applied. Recovery keeps its old cursor.
        let extra = data(&base, "d")?;
        let mut next_files = files.clone();
        next_files.push(extra);
        let next = snapshot(&first, 3, Some(IcebergCommitKind::Data), &next_files, &[]).await?;
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        let last = state.enumerate(&next, limits).await?;
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].task.id.snapshot_id, 9);
        assert!(last[0].generation > page[0].generation);
        assert!(!state.acknowledge_committed(&done)?);
        let mut last_done = IcebergUpdateFetchState::new(last[0].clone());
        last_done.advance(10, true)?;
        assert!(state.acknowledge_committed(&[last_done])?);
        // Exhaustive empty Delete phase permits Insert enumeration, not a fake completion report.
        assert!(state.enumerate(&next, limits).await?.is_empty());
        let inserts = state.enumerate(&next, limits).await?;
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].task.id.snapshot_id, 3);
        assert_eq!(inserts[0].task.id.phase, IcebergUpdatePhase::Insert);
    }
    Ok(())
}

#[tokio::test]
async fn partial_fetch_eof_and_page_limits_fail_closed() -> Result<()> {
    use crate::source::iceberg::update_state::{IcebergUpdateFetchState, IcebergUpdateListState};

    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V3, false)?;
    let first = snapshot(
        &base,
        1,
        Some(IcebergCommitKind::Data),
        &[data(&base, "a")?, data(&base, "b")?],
        &[],
    )
    .await?;
    let binding = IcebergUpdateBinding::bind(&first, vec![2, 1])?;
    let mut state = IcebergUpdateListState::new(1, binding.clone());
    let page = state
        .enumerate(&first, IcebergUpdateLimits::default())
        .await?;
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

    for limits in [
        IcebergUpdateLimits {
            max_files: 1,
            ..Default::default()
        },
        IcebergUpdateLimits {
            max_metadata_bytes: 1,
            ..Default::default()
        },
        IcebergUpdateLimits {
            max_page_bytes: 1,
            ..Default::default()
        },
    ] {
        let mut state = IcebergUpdateListState::new(1, binding.clone());
        assert!(state.enumerate(&first, limits).await.is_err());
        assert!(state.assignments().is_empty());
    }
    assert!(
        IcebergUpdateLimits {
            page_size: 0,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn byte_limited_pages_resume_from_the_last_acknowledged_task() -> Result<()> {
    use crate::source::iceberg::update_state::{IcebergUpdateFetchState, IcebergUpdateListState};

    let temp = tempfile::tempdir()?;
    let base = table(&temp, FormatVersion::V3, false)?;
    let first = snapshot(
        &base,
        1,
        Some(IcebergCommitKind::Data),
        &[data(&base, "a")?, data(&base, "b")?],
        &[],
    )
    .await?;
    let binding = IcebergUpdateBinding::bind(&first, vec![2, 1])?;
    let plan = IcebergUpdatePlanner::new(binding.clone())
        .plan_bootstrap(&first)
        .await?
        .unwrap();
    let limits = IcebergUpdateLimits {
        max_page_bytes: plan
            .inserts
            .iter()
            .map(|task| serde_json::to_vec(task).unwrap().len())
            .max()
            .unwrap(),
        ..Default::default()
    };
    let mut state = IcebergUpdateListState::new(42, binding);
    let mut paths = vec![];
    for _ in 0..2 {
        let page = state.enumerate(&first, limits).await?;
        assert_eq!(page.len(), 1, "byte budget, not task count, ends this page");
        let encoded = serde_json::to_value(&state)?;
        assert!(encoded["pending"].get("generation").is_none());
        assert!(encoded["pending"].get("cursor").is_none());
        assert!(encoded["pending"].get("next_path").is_none());
        state = serde_json::from_value(encoded)?;
        assert_eq!(state.assignments(), page);
        paths.push(page[0].task.id.data_file_path.clone());
        let mut done = IcebergUpdateFetchState::new(page[0].clone());
        done.advance(10, true)?;
        // An identical task ID cannot authorize different persisted read descriptors.
        let mut corrupt = serde_json::to_value(&done)?;
        corrupt["assignment"]["task"]["file"]["size"] = json!(999);
        assert!(!state.acknowledge_committed(&[serde_json::from_value(corrupt)?])?);
        assert!(state.acknowledge_committed(&[done])?);
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
    }
    assert!(paths[0] < paths[1]);
    assert!(state.enumerate(&first, limits).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn compaction_still_validates_delete_descriptors_without_reading_them() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let first = snapshot(
            &base,
            1,
            Some(IcebergCommitKind::Data),
            &[data(&base, "a")?],
            &[],
        )
        .await?;
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
        let compacted = snapshot(
            &first,
            2,
            Some(IcebergCommitKind::Compaction),
            &[file],
            &[(invalid, 2)],
        )
        .await?;
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
    use crate::source::iceberg::update_state::{IcebergUpdateFetchState, IcebergUpdateListState};

    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let base = table(&temp, version, false)?;
        let old = data(&base, "a")?;
        let first = snapshot(&base, 1, Some(IcebergCommitKind::Data), from_ref(&old), &[]).await?;
        let mut state =
            IcebergUpdateListState::new(1, IcebergUpdateBinding::bind(&first, vec![2, 1])?);
        let limits = IcebergUpdateLimits::default();
        let bootstrap = state.enumerate(&first, limits).await?;
        let mut done = IcebergUpdateFetchState::new(bootstrap[0].clone());
        done.advance(10, true)?;
        assert!(state.acknowledge_committed(&[done])?);
        let compacted = data(&base, "b")?;
        let second = snapshot(
            &first,
            2,
            Some(IcebergCommitKind::Compaction),
            from_ref(&compacted),
            &[],
        )
        .await?;
        assert!(state.enumerate(&second, limits).await?.is_empty());
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        assert!(state.enumerate(&second, limits).await?.is_empty());
        let new = data(&base, "c")?;
        let d = delete(&base, &compacted, "d", 4)?;
        let third = snapshot(
            &second,
            3,
            Some(IcebergCommitKind::Data),
            &[compacted.clone(), new],
            &[(d, 3)],
        )
        .await?;
        let deletes = state.enumerate(&third, limits).await?;
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].task.id.phase, IcebergUpdatePhase::Delete);
        assert_eq!(deletes[0].task.id.data_file_path, compacted.file_path());
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        assert!(state.enumerate(&third, limits).await.is_err());
        let mut done = IcebergUpdateFetchState::new(deletes[0].clone());
        done.advance(10, false)?;
        assert!(!state.acknowledge_committed(from_ref(&done))?);
        done.advance(10, true)?;
        assert!(state.acknowledge_committed(from_ref(&done))?);
        state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        let inserts = state.enumerate(&third, limits).await?;
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].task.id.phase, IcebergUpdatePhase::Insert);
        assert!(!state.acknowledge_committed(&[done])?);
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
        let counter = Arc::new(counting_io::CountingIo::default());
        let base = Table::builder()
            .identifier(base.identifier().clone())
            .metadata(base.metadata().clone())
            .file_io(FileIOBuilder::new(counter.clone()).build())
            .runtime(Runtime::try_current()?)
            .build()?;
        let first = snapshot(
            &base,
            1,
            Some(IcebergCommitKind::Data),
            &[data(&base, "a")?],
            &[],
        )
        .await?;
        let planner = planner(&first)?;
        let b = data(&base, "b")?;
        let second = snapshot(
            &first,
            2,
            Some(IcebergCommitKind::Compaction),
            from_ref(&b),
            &[(delete(&base, &b, "d", 4)?, 2)],
        )
        .await?;
        let third = snapshot(
            &second,
            3,
            Some(IcebergCommitKind::Compaction),
            &[data(&base, "c")?],
            &[],
        )
        .await?;
        for (table, parent) in [(&second, 1), (&third, 2)] {
            let plan = planner.plan_next(table, Some(parent)).await?.unwrap();
            assert!(read_plan(table, &plan).await?.is_empty());
        }
        assert!(counter.metadata_reads.load(Ordering::Relaxed) > 0);
        assert_eq!(counter.row_reads.load(Ordering::Relaxed), 0);
    }
    Ok(())
}
