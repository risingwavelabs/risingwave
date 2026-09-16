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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinWriter};
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, NestedField, PrimitiveType, Schema, SortOrder,
    TableMetadataBuilder, Type, UnboundPartitionSpec,
};
use iceberg::writer::base_writer::position_delete_file_writer::POSITION_DELETE_SCHEMA;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use iceberg::{NamespaceIdent, Runtime, TableIdent};
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::arrow_array_iceberg::{ArrayRef, RecordBatch, StringArray};
use risingwave_common::row::Row;
use risingwave_common::types::ScalarRefImpl;
use tempfile::TempDir;

use super::*;
use crate::sink::iceberg::{
    IcebergConfig, PositionDeleteFileNameGenerators, read_position_deletes_from_file,
    write_position_delete_file,
};

fn table(temp: &TempDir, version: FormatVersion) -> Result<Table> {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "_row_id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        format!("file://{}", temp.path().display()),
        version,
        HashMap::new(),
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

fn contract(table: &Table) -> Result<IcebergSourceContract> {
    IcebergSourceContract::from_key_indices(table.metadata().current_schema(), &[1])
}

async fn write_data(table: &Table, name: &str) -> Result<FileScanTask> {
    write_data_with_keys(table, name, Int64Array::from_iter_values(100..110)).await
}

async fn write_data_with_keys(table: &Table, name: &str, keys: Int64Array) -> Result<FileScanTask> {
    let schema = table.metadata().current_schema();
    let path = format!("{}/data/{name}.parquet", table.metadata().location());
    let mut writer = ParquetWriterBuilder::new(
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build(),
        schema.clone(),
    )
    .build(table.file_io().new_output(&path)?)
    .await?;
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema)?);
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let keys: ArrayRef = Arc::new(keys);
    writer
        .write(&RecordBatch::try_new(arrow_schema, vec![values, keys])?)
        .await?;
    let files = writer.close().await?;
    let data = files.into_iter().next().unwrap().build()?;
    Ok(FileScanTask::builder()
        .with_file_size_in_bytes(data.file_size_in_bytes())
        .with_start(0)
        .with_length(data.file_size_in_bytes())
        .with_record_count(Some(10))
        .with_data_file_path(path)
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema.clone())
        .with_project_field_ids(schema.as_struct().fields().iter().map(|f| f.id).collect())
        .with_case_sensitive(true)
        .build())
}

fn descriptor(file: DataFile) -> FileScanTaskDeleteFile {
    FileScanTaskDeleteFile::builder()
        .with_file_path(file.file_path().to_owned())
        .with_file_size_in_bytes(file.file_size_in_bytes())
        .with_file_type(DataContentType::PositionDeletes)
        .with_partition_spec_id(file.partition_spec_id())
        .with_file_format(file.file_format())
        .with_referenced_data_file(file.referenced_data_file())
        .with_content_offset(file.content_offset())
        .with_content_size_in_bytes(file.content_size_in_bytes())
        .with_record_count(Some(file.record_count()))
        .build()
}

async fn write_deletes(
    table: &Table,
    name: &str,
    data: &str,
    positions: impl IntoIterator<Item = u64>,
) -> Result<FileScanTaskDeleteFile> {
    let positions = DeleteVector::from_iter(positions);
    let names = PositionDeleteFileNameGenerators::new(name);
    let location = DefaultLocationGenerator::new(table.metadata())?;
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
    let file = write_position_delete_file(
        table,
        &config,
        &location,
        &names,
        table.metadata().format_version(),
        data.to_owned(),
        &positions,
        None,
    )
    .await?;
    assert_eq!(
        read_position_deletes_from_file(table.file_io(), &file).await?,
        positions
    );
    Ok(descriptor(file))
}

async fn collect_changes(
    table: &Table,
    task: FileScanTask,
    mode: IcebergChangeReadMode,
    resume: u64,
) -> Result<(Vec<(Op, i64, i64)>, Vec<u64>)> {
    let mut reader = PositionDeleteReader::new(table.file_io());
    collect_changes_with_reader(table, &mut reader, task, mode, resume).await
}

async fn collect_changes_with_reader(
    table: &Table,
    reader: &mut PositionDeleteReader,
    task: FileScanTask,
    mode: IcebergChangeReadMode,
    resume: u64,
) -> Result<(Vec<(Op, i64, i64)>, Vec<u64>)> {
    let mut batches = read_file_changes(
        table.clone(),
        reader,
        task,
        contract(table)?,
        mode,
        4,
        resume,
    );
    let mut rows = vec![];
    let mut cursors = vec![];
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        assert!(batch.chunk.capacity() <= 4);
        assert_eq!(batch.chunk.columns().len(), 2);
        if batch.chunk.cardinality() == 0 {
            assert_eq!(batch.chunk.capacity(), 0);
        }
        for (op, row) in batch.chunk.rows() {
            let Some(ScalarRefImpl::Int64(value)) = row.datum_at(0) else {
                panic!("expected value");
            };
            let Some(ScalarRefImpl::Int64(key)) = row.datum_at(1) else {
                panic!("expected stored key");
            };
            rows.push((op, value, key));
        }
        assert!(batch.next_position > cursors.last().copied().unwrap_or(resume));
        cursors.push(batch.next_position);
    }
    Ok((rows, cursors))
}

#[tokio::test]
async fn v2_v3_insert_filters_same_commit_deletes_and_reports_empty_progress() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, version)?;
        let mut task = write_data(&table, "new").await?;
        task.deletes =
            vec![write_deletes(&table, "partial", &task.data_file_path, [1, 3, 7]).await?];
        let (rows, cursors) =
            collect_changes(&table, task.clone(), IcebergChangeReadMode::Insert, 0).await?;
        assert_eq!(
            rows,
            [0, 2, 4, 5, 6, 8, 9].map(|i| (Op::Insert, i, 100 + i))
        );
        assert_eq!(cursors.last(), Some(&10));
        task.deletes = vec![write_deletes(&table, "all", &task.data_file_path, 0..10).await?];
        let (rows, cursors) =
            collect_changes(&table, task, IcebergChangeReadMode::Insert, 0).await?;
        assert!(rows.is_empty());
        assert_eq!(cursors.last(), Some(&10));
    }
    Ok(())
}

#[tokio::test]
async fn v2_v3_retracts_only_new_positions_and_resumes_without_duplicate_rows() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, version)?;
        let mut task = write_data(&table, "retained").await?;
        let parent = write_deletes(&table, "parent", &task.data_file_path, [1, 3]).await?;
        task.deletes =
            vec![write_deletes(&table, "current", &task.data_file_path, [1, 3, 4, 7, 9]).await?];
        let mode = IcebergChangeReadMode::Delete {
            parent_deletes: vec![parent],
        };
        let (rows, cursors) = collect_changes(&table, task.clone(), mode.clone(), 0).await?;
        assert_eq!(rows, [4, 7, 9].map(|i| (Op::Delete, i, 100 + i)));
        for cursor in cursors {
            let (resumed, _) = collect_changes(&table, task.clone(), mode.clone(), cursor).await?;
            assert_eq!(
                resumed,
                rows.iter()
                    .filter(|(_, i, _)| *i as u64 >= cursor)
                    .copied()
                    .collect::<Vec<_>>()
            );
        }
        task.deletes.clear();
        assert!(collect_changes(&table, task, mode, 0).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn v2_v3_compacted_file_preserves_full_logical_before_image() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, version)?;
        let original = write_data(&table, "original").await?;
        let (inserted, _) =
            collect_changes(&table, original, IcebergChangeReadMode::Insert, 0).await?;
        let mut compacted = write_data(&table, "compacted").await?;
        compacted.deletes =
            vec![write_deletes(&table, "delete", &compacted.data_file_path, [2, 8]).await?];
        let (deleted, _) = collect_changes(
            &table,
            compacted,
            IcebergChangeReadMode::Delete {
                parent_deletes: vec![],
            },
            0,
        )
        .await?;
        assert_eq!(
            deleted,
            [inserted[2], inserted[8]].map(|(_, value, key)| (Op::Delete, value, key))
        );
    }
    Ok(())
}

#[tokio::test]
async fn strict_descriptors_reject_unsupported_inputs_before_io() -> Result<()> {
    let base = FileScanTaskDeleteFile::builder()
        .with_file_path("file:///not-read.parquet".to_owned())
        .with_file_size_in_bytes(100)
        .with_file_type(DataContentType::PositionDeletes)
        .with_partition_spec_id(0)
        .with_record_count(Some(1))
        .with_referenced_data_file(Some("data".to_owned()))
        .build();
    let io = FileIO::new_with_fs();
    let mut reader = PositionDeleteReader::new(&io);
    assert!(
        read_deleted_positions(&mut reader, "data", 10, &[base.clone(), base.clone()])
            .await
            .is_err()
    );
    for (index, message) in [
        "expected",
        "references",
        "references",
        "record_count",
        "DV range",
        "unsupported",
        "equality IDs",
        "encrypted",
    ]
    .into_iter()
    .enumerate()
    {
        let mut invalid = base.clone();
        match index {
            0 => invalid.file_type = DataContentType::EqualityDeletes,
            1 => invalid.referenced_data_file = Some("other".to_owned()),
            2 => invalid.referenced_data_file = None,
            3 => invalid.record_count = None,
            4 => invalid.content_offset = Some(1),
            5 => invalid.file_format = DataFileFormat::Avro,
            6 => invalid.equality_ids = Some(vec![1]),
            _ => invalid.key_metadata = Some(Box::new([])),
        }
        let error = read_deleted_positions(&mut reader, "data", 10, &[invalid])
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error:#}");
    }
    for (offset, length) in [(-1, 10), (1, -1), (1, 0), (95, 10), (i64::MAX, i64::MAX)] {
        let mut invalid = base.clone();
        invalid.file_format = DataFileFormat::Puffin;
        invalid.content_offset = Some(offset);
        invalid.content_size_in_bytes = Some(length);
        assert!(
            read_deleted_positions(&mut reader, "data", 10, &[invalid])
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn parquet_checks_every_path_position_and_physical_row_count() -> Result<()> {
    for (paths, values, expected_error) in [
        (
            vec![Some("data"), Some("data")],
            vec![Some(1), Some(1)],
            None,
        ),
        (
            vec![Some("data"), Some("other")],
            vec![Some(1), Some(2)],
            Some("references"),
        ),
        (vec![None], vec![Some(1)], Some("references")),
        (vec![Some("data")], vec![None], Some("null deleted")),
        (vec![Some("data")], vec![Some(-1)], Some("negative")),
        (vec![Some("data")], vec![Some(10)], Some("outside")),
    ] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, FormatVersion::V2)?;
        let path = format!("{}/invalid.parquet", table.metadata().location());
        let trusted_positions = values
            .iter()
            .map(|pos| pos.map(|pos| pos as u64))
            .collect::<Option<DeleteVector>>();
        // A nullable fixture schema lets malformed NULL records reach the reader boundary.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        POSITION_DELETE_SCHEMA.as_struct().fields()[0].id,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::optional(
                        POSITION_DELETE_SCHEMA.as_struct().fields()[1].id,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()?,
        );
        let mut writer =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
                .build(table.file_io().new_output(&path)?)
                .await?;
        let schema = Arc::new(schema_to_arrow_schema(&schema)?);
        writer
            .write(&RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(paths)),
                    Arc::new(Int64Array::from(values)),
                ],
            )?)
            .await?;
        let mut file = writer.close().await?.into_iter().next().unwrap();
        file.content(DataContentType::PositionDeletes)
            .referenced_data_file(Some("data".to_owned()));
        let file = file.build()?;
        // The legacy sink projection must not start validating paths or unsigned bounds.
        let trusted_result = read_position_deletes_from_file(table.file_io(), &file).await;
        match trusted_positions {
            Some(expected) => assert_eq!(trusted_result?, expected),
            None => assert!(trusted_result.is_err()),
        }
        let descriptor = descriptor(file);
        let mut reader = PositionDeleteReader::new(table.file_io());
        let result =
            read_deleted_positions(&mut reader, "data", 10, std::slice::from_ref(&descriptor))
                .await;
        if let Some(message) = expected_error {
            assert!(result.unwrap_err().to_string().contains(message));
        } else {
            assert_eq!(result?, DeleteVector::from([1]));
            let mut invalid_count = descriptor;
            invalid_count.record_count = Some(1);
            assert!(
                read_deleted_positions(&mut reader, "data", 10, &[invalid_count])
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn puffin_checks_blob_path_cardinality_and_position_bounds() -> Result<()> {
    for (path, cardinality, positions, message) in [
        ("other", "1", vec![1], "references"),
        ("data", "2", vec![1], "bitmap"),
        ("data", "1", vec![10], "outside"),
    ] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, FormatVersion::V3)?;
        let location = format!("{}/invalid.puffin", table.metadata().location());
        let output = table.file_io().new_output(&location)?;
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;
        let blob = DeleteVector::from_iter(positions).to_puffin_blob(HashMap::from([
            ("referenced-data-file".to_owned(), path.to_owned()),
            ("cardinality".to_owned(), cardinality.to_owned()),
        ]))?;
        writer.add(blob, CompressionCodec::None).await?;
        let result = writer.close_with_metadata().await?;
        let blob = &result.blobs_metadata[0];
        let delete = FileScanTaskDeleteFile::builder()
            .with_file_path(location)
            .with_file_size_in_bytes(result.file_size_in_bytes)
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .with_file_format(DataFileFormat::Puffin)
            .with_referenced_data_file(Some("data".to_owned()))
            .with_content_offset(Some(blob.offset() as i64))
            .with_content_size_in_bytes(Some(blob.length() as i64))
            .with_record_count(Some(cardinality.parse()?))
            .build();
        let mut reader = PositionDeleteReader::new(table.file_io());
        let error = read_deleted_positions(&mut reader, "data", 10, &[delete])
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn reader_rejects_virtual_metadata_missing_key_and_partial_file_tasks() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let table = table(&temp, FormatVersion::V2)?;
    let task = write_data(&table, "data").await?;
    for index in 0..4 {
        let mut invalid = task.clone();
        match index {
            0 => invalid.project_field_ids.push(RESERVED_FIELD_ID_POS),
            1 => invalid.project_field_ids.truncate(1),
            2 => invalid.start = 1,
            _ => invalid.record_count = Some(11),
        }
        assert!(
            collect_changes(&table, invalid, IcebergChangeReadMode::Insert, 0)
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn puffin_same_file_different_blobs_keep_distinct_delete_sets() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let table = table(&temp, FormatVersion::V3)?;
    let mut task = write_data(&table, "data").await?;
    let location = format!("{}/multiple.puffin", table.metadata().location());
    let output = table.file_io().new_output(&location)?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;
    for positions in [DeleteVector::from([1]), DeleteVector::from([1, 7])] {
        let blob = positions.to_puffin_blob(HashMap::from([
            (
                "referenced-data-file".to_owned(),
                task.data_file_path.clone(),
            ),
            ("cardinality".to_owned(), positions.len().to_string()),
        ]))?;
        writer.add(blob, CompressionCodec::None).await?;
    }
    let result = writer.close_with_metadata().await?;
    let deletes: Vec<_> = result
        .blobs_metadata
        .iter()
        .enumerate()
        .map(|(index, blob)| {
            FileScanTaskDeleteFile::builder()
                .with_file_path(location.clone())
                .with_file_size_in_bytes(result.file_size_in_bytes)
                .with_file_type(DataContentType::PositionDeletes)
                .with_partition_spec_id(0)
                .with_file_format(DataFileFormat::Puffin)
                .with_referenced_data_file(Some(task.data_file_path.clone()))
                .with_content_offset(Some(blob.offset() as i64))
                .with_content_size_in_bytes(Some(blob.length() as i64))
                .with_record_count(Some(index as u64 + 1))
                .build()
        })
        .collect();
    task.deletes = vec![deletes[1].clone()];
    let (rows, _) = collect_changes(
        &table,
        task.clone(),
        IcebergChangeReadMode::Delete {
            parent_deletes: vec![deletes[0].clone()],
        },
        0,
    )
    .await?;
    assert_eq!(rows, vec![(Op::Delete, 7, 107)]);

    let mut reader = PositionDeleteReader::new(table.file_io());
    for (delete, expected) in [
        (&deletes[1], DeleteVector::from([1, 7])),
        (&deletes[0], DeleteVector::from([1])),
    ] {
        assert_eq!(
            read_deleted_positions(
                &mut reader,
                &task.data_file_path,
                10,
                std::slice::from_ref(delete)
            )
            .await?,
            expected
        );
    }
    let mut invalid = deletes[0].clone();
    invalid.content_offset = invalid.content_offset.map(|offset| offset + 1);
    assert!(
        read_deleted_positions(&mut reader, &task.data_file_path, 10, &[invalid])
            .await
            .is_err()
    );
    let mut invalid = deletes[0].clone();
    invalid.file_size_in_bytes += 1;
    assert!(
        read_deleted_positions(&mut reader, &task.data_file_path, 10, &[invalid])
            .await
            .is_err()
    );
    let mut invalid = deletes[0].clone();
    invalid.record_count = Some(2);
    assert!(
        read_deleted_positions(&mut reader, &task.data_file_path, 10, &[invalid])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn puffin_cache_survives_change_reader_api_calls() -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let temp = tempfile::tempdir()?;
    let table = table(&temp, FormatVersion::V3)?;
    let mut task = write_data(&table, "data").await?;
    task.deletes = vec![write_deletes(&table, "delete", &task.data_file_path, [1, 7]).await?];
    let mut reader = PositionDeleteReader::new(table.file_io());
    assert_eq!(
        read_deleted_positions(&mut reader, &task.data_file_path, 10, &task.deletes).await?,
        DeleteVector::from([1, 7])
    );

    // Poison only this fixture's footer after warming the cache. Blobs and file size are unchanged:
    // a fresh reader must fail, while cached API calls must not issue another footer read.
    // Production Puffin files must remain immutable.
    {
        let path = task.deletes[0].file_path.strip_prefix("file://").unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::End(-4))?;
        file.write_all(b"FAIL")?;
    }
    let mut fresh = PositionDeleteReader::new(table.file_io());
    assert!(
        read_deleted_positions(&mut fresh, &task.data_file_path, 10, &task.deletes)
            .await
            .is_err()
    );
    assert_eq!(
        read_deleted_positions(&mut reader, &task.data_file_path, 10, &task.deletes).await?,
        DeleteVector::from([1, 7])
    );
    for resume in [0, 4] {
        let (rows, cursors) = collect_changes_with_reader(
            &table,
            &mut reader,
            task.clone(),
            IcebergChangeReadMode::Delete {
                parent_deletes: vec![],
            },
            resume,
        )
        .await?;
        assert_eq!(
            rows,
            [1, 7]
                .into_iter()
                .filter(|pos| *pos >= resume)
                .map(|pos| (Op::Delete, pos as i64, 100 + pos as i64))
                .collect::<Vec<_>>()
        );
        assert_eq!(cursors.last(), Some(&10));
    }
    let mut invalid = task.deletes[0].clone();
    invalid.file_size_in_bytes += 1;
    let error = read_deleted_positions(&mut reader, &task.data_file_path, 10, &[invalid])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("size differs"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn v2_v3_insert_resume_keeps_filtering_without_retractions() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, version)?;
        let mut task = write_data(&table, "data").await?;
        task.deletes =
            vec![write_deletes(&table, "delete", &task.data_file_path, [0, 2, 4, 6, 8]).await?];
        for cursor in 0..=10 {
            let (rows, _) =
                collect_changes(&table, task.clone(), IcebergChangeReadMode::Insert, cursor)
                    .await?;
            let expected: Vec<_> = (cursor as i64..10)
                .filter(|i| i % 2 == 1)
                .map(|i| (Op::Insert, i, 100 + i))
                .collect();
            assert_eq!(rows, expected);
        }
    }
    Ok(())
}

#[test]
fn cumulative_positions_do_not_repeat_deletes_or_allow_resurrection() -> Result<()> {
    let empty = DeleteVector::default();
    let parent = DeleteVector::from([1, 3]);
    assert_eq!(newly_deleted_positions(&empty, parent.clone())?, parent);
    assert!(newly_deleted_positions(&parent, parent.clone())?.is_empty());
    assert!(newly_deleted_positions(&empty, empty.clone())?.is_empty());
    assert!(newly_deleted_positions(&parent, empty).is_err());
    assert!(newly_deleted_positions(&parent, DeleteVector::from([1, 7])).is_err());
    Ok(())
}

#[tokio::test]
async fn v2_v3_preserves_null_keys_and_reordered_projection() -> Result<()> {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let temp = tempfile::tempdir()?;
        let table = table(&temp, version)?;
        let keys = || Int64Array::from_iter((0..10).map(|i| (i != 3).then_some(100 + i)));
        let mut original = write_data_with_keys(&table, "original", keys()).await?;
        let mut compacted = write_data_with_keys(&table, "compacted", keys()).await?;
        original.project_field_ids.reverse();
        compacted.project_field_ids.reverse();
        compacted.deletes =
            vec![write_deletes(&table, "delete", &compacted.data_file_path, [3, 8]).await?];
        let mut inserted = vec![];
        let mut deleted = vec![];
        let mut reader = PositionDeleteReader::new(table.file_io());
        for (task, mode, rows) in [
            (original, IcebergChangeReadMode::Insert, &mut inserted),
            (
                compacted,
                IcebergChangeReadMode::Delete {
                    parent_deletes: vec![],
                },
                &mut deleted,
            ),
        ] {
            let mut batches = read_file_changes(
                table.clone(),
                &mut reader,
                task,
                contract(&table)?,
                mode,
                4,
                0,
            );
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                assert_eq!(batch.chunk.columns().len(), 2);
                for (op, row) in batch.chunk.rows() {
                    let key = match row.datum_at(0) {
                        None => None,
                        Some(ScalarRefImpl::Int64(key)) => Some(key),
                        _ => panic!("expected nullable stored key as the first column"),
                    };
                    let Some(ScalarRefImpl::Int64(value)) = row.datum_at(1) else {
                        panic!("expected value as the second column");
                    };
                    rows.push((op, key, value));
                }
            }
        }
        assert_eq!(inserted[3], (Op::Insert, None, 3));
        assert_eq!(
            deleted,
            [inserted[3], inserted[8]].map(|(_, key, value)| (Op::Delete, key, value))
        );
    }
    Ok(())
}
