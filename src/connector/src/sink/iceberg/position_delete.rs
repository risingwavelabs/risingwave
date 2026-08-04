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

//! Shared iceberg position-delete (Puffin deletion vector) helpers.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::base_writer::position_delete_file_writer::POSITION_DELETE_SCHEMA;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::{
    FileWriter, FileWriterBuilder, ParquetWriter, ParquetWriterBuilder,
};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::arrow_array_iceberg::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray,
};
use risingwave_common::array::arrow::arrow_schema_iceberg::SchemaRef as ArrowSchemaRef;

use crate::source::iceberg::parquet_file_handler::ParquetFileReader;

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// Reads the deletion-vector positions of a single Puffin DV `DataFile`.
///
/// The `DataFile` must carry a `content_offset` / `content_size_in_bytes` locating its blob inside
/// the Puffin container (as written by [`write_dv_puffin_file`]); the blob body decodes into a
/// [`DeleteVector`] (roaring bitmap) of deleted row positions in the referenced data file.
pub async fn read_dv_positions_from_data_file(
    file_io: &FileIO,
    data_file: &DataFile,
) -> Result<DeleteVector> {
    let blob_offset = data_file.content_offset().with_context(|| {
        format!(
            "DV file {} missing content_offset for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        )
    })?;
    let blob_length = data_file.content_size_in_bytes().with_context(|| {
        format!(
            "DV file {} missing content_size_in_bytes for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        )
    })?;

    let input_file = file_io.new_input(data_file.file_path())?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader.file_metadata().await?;
    let blob_metadata = file_metadata
        .blobs()
        .iter()
        .find(|blob| blob.offset() == blob_offset as u64 && blob.length() == blob_length as u64)
        .with_context(|| {
            format!(
                "DV blob metadata not found in {} at offset={} length={}",
                data_file.file_path(),
                blob_offset,
                blob_length
            )
        })?;
    let blob = puffin_reader.blob(blob_metadata).await?;

    let delete_vector = DeleteVector::from_puffin_blob(blob)?;
    Ok(delete_vector)
}

/// Reads the deleted positions of a single position-delete `DataFile` regardless of on-disk format,
/// dispatching on [`DataFile::file_format`]: a V3 Puffin deletion vector (via
/// [`read_dv_positions_from_data_file`]) or a V2 Parquet position-delete file (via
/// [`read_parquet_position_deletes_from_file`]). Any other format is rejected.
pub async fn read_position_deletes_from_file(
    file_io: &FileIO,
    delete_file: &DataFile,
) -> Result<DeleteVector> {
    match delete_file.file_format() {
        DataFileFormat::Puffin => read_dv_positions_from_data_file(file_io, delete_file).await,
        DataFileFormat::Parquet => {
            read_parquet_position_deletes_from_file(file_io, delete_file).await
        }
        other => bail!(
            "position-delete file {} has unsupported format {:?}; expected Puffin or Parquet",
            delete_file.file_path(),
            other
        ),
    }
}

/// Reads the positions stored in a V2 Parquet position-delete file into a [`DeleteVector`].
///
/// The position-delete schema fixes the column order to `(file_path, pos)`. The files we author are
/// file-scoped (every row shares one `file_path`), so the `file_path` column is redundant here: we
/// project only the `pos` leaf (column index 1) and read every value.
pub async fn read_parquet_position_deletes_from_file(
    file_io: &FileIO,
    delete_file: &DataFile,
) -> Result<DeleteVector> {
    let input_file = file_io.new_input(delete_file.file_path())?;
    let metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let parquet_reader = ParquetFileReader::new(metadata, reader);
    let builder = ParquetRecordBatchStreamBuilder::new(parquet_reader)
        .await
        .map_err(|e| anyhow!(e))?;
    // Project only the `pos` leaf (column index 1) so the `file_path` column is never decoded.
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let mut stream = builder
        .with_projection(projection)
        .build()
        .map_err(|e| anyhow!(e))?;

    let mut delete_vector = DeleteVector::default();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| anyhow!(e))?;
        // Only the projected `pos` column is present in the batch.
        let positions = batch.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("position-delete pos column should be an Int64Array")?;
        for pos in positions {
            let pos = pos.with_context(|| {
                format!(
                    "null value in position-delete file {}",
                    delete_file.file_path()
                )
            })?;
            delete_vector.insert(pos as u64);
        }
    }

    Ok(delete_vector)
}

/// Writes `delete_vector` as a single V3 Puffin deletion-vector blob referencing `data_file_path`,
/// and returns its [`DataFile`] metadata (content `PositionDeletes`, format `Puffin`) with
/// `referenced_data_file` set.
///
/// The blob location is derived from `location_generator` + `file_name_generator` (both borrow-only
/// so callers keep ownership); `partition_key` is attached to the produced `DataFile` for
/// partitioned tables. Callers that need to merge with an existing DV must fold it into
/// `delete_vector` before calling — this writes exactly the positions it is given.
pub async fn write_dv_puffin_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    data_file_path: String,
    delete_vector: DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key, &file_name);
    let output_file = table.file_io().new_output(&location)?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;

    let cardinality = delete_vector.len();
    let properties = HashMap::from([
        (
            DELETION_VECTOR_PROPERTY_CARDINALITY.to_owned(),
            cardinality.to_string(),
        ),
        (
            DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_owned(),
            data_file_path.clone(),
        ),
    ]);
    let blob = delete_vector.to_puffin_blob(properties)?;
    writer.add(blob, CompressionCodec::None).await?;

    let result = writer.close_with_metadata().await?;
    let blob_metadata = result
        .blobs_metadata
        .first()
        .context("blob metadata should be present")?;

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::PositionDeletes)
        .file_path(location)
        .file_format(DataFileFormat::Puffin)
        .record_count(cardinality)
        .file_size_in_bytes(result.file_size_in_bytes)
        .referenced_data_file(Some(data_file_path))
        .content_offset(Some(blob_metadata.offset() as i64))
        .content_size_in_bytes(Some(blob_metadata.length() as i64));
    if let Some(partition_key) = partition_key {
        builder
            .partition(partition_key.data().clone())
            .partition_spec_id(partition_key.spec().spec_id());
    }
    builder
        .build()
        .context("Failed to build deletion vector file metadata")
}

/// How many positions to buffer before flushing one `(file_path, pos)` batch to the writer.
const POSITION_DELETE_WRITE_CHUNK_SIZE: usize = 1024;

/// Default Parquet writer properties for a file-scoped position-delete file, for callers that do not
/// thread an [`super::IcebergConfig`] through (e.g. meta's pk-index compaction coordinator): default
/// Snappy compression and default row-group sizing, tagged with the RisingWave `created_by` marker.
pub fn default_position_delete_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_created_by(super::PARQUET_CREATED_BY.to_owned())
        .build()
}

/// Writes `delete_vector` as a single file-scoped V2 Parquet position-delete file referencing
/// `data_file_path`, and returns its [`DataFile`] metadata (content `PositionDeletes`, format
/// `Parquet`) with `referenced_data_file` set.
///
/// The produced file references exactly one data file (`data_file_path`); every written row shares
/// that path. File-scoped deletes reference exactly one data file, so this writes the file with a
/// plain [`ParquetWriter`] rather than a rolling position-delete writer stack. `writer_properties`
/// carries the desired compression / row-group settings (callers derive these from their iceberg
/// config or use defaults); `partition_key` is attached to the produced `DataFile` for partitioned
/// tables. Callers that need to merge with existing positions must fold them into `delete_vector`
/// before calling — this writes exactly the positions it is given.
pub async fn write_parquet_position_delete_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    writer_properties: WriterProperties,
    data_file_path: String,
    delete_vector: DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key, &file_name);
    let output_file = table.file_io().new_output(&location)?;

    let mut writer =
        ParquetWriterBuilder::new(writer_properties, POSITION_DELETE_SCHEMA.clone().into())
            .build(output_file)
            .await?;

    // The position-delete schema is `(file_path, pos)` with reserved field IDs; derive the matching
    // Arrow schema so the written column field IDs line up.
    let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&POSITION_DELETE_SCHEMA)?);

    let mut positions: Vec<i64> = Vec::with_capacity(POSITION_DELETE_WRITE_CHUNK_SIZE);
    for pos in delete_vector.iter() {
        positions.push(pos as i64);
        if positions.len() == POSITION_DELETE_WRITE_CHUNK_SIZE {
            write_position_delete_chunk(
                &mut writer,
                &arrow_schema,
                &data_file_path,
                std::mem::take(&mut positions),
            )
            .await?;
            positions.reserve(POSITION_DELETE_WRITE_CHUNK_SIZE);
        }
    }
    if !positions.is_empty() {
        write_position_delete_chunk(&mut writer, &arrow_schema, &data_file_path, positions).await?;
    }

    let data_files = writer.close().await?;
    // `close` will yield exactly one builder here.
    let [mut builder] = data_files.try_into().map_err(|_| {
        anyhow!("position-delete writer produced invalid file count for {data_file_path}")
    })?;

    // `ParquetWriter` builds the file as `DataContentType::Data` with an empty partition; override
    // those for a file-scoped V2 position-delete file and attach `referenced_data_file`.
    builder
        .content(DataContentType::PositionDeletes)
        .referenced_data_file(Some(data_file_path));
    if let Some(partition_key) = partition_key {
        builder
            .partition(partition_key.data().clone())
            .partition_spec_id(partition_key.spec().spec_id());
    }
    builder
        .build()
        .context("Failed to build position-delete file metadata")
}

/// Writes one chunk of `positions` as a `(file_path, pos)` batch into `writer`. Every row shares
/// `data_file_path` because the delete file is file-scoped.
async fn write_position_delete_chunk(
    writer: &mut ParquetWriter,
    arrow_schema: &ArrowSchemaRef,
    data_file_path: &str,
    positions: Vec<i64>,
) -> Result<()> {
    let path_column: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        data_file_path,
        positions.len(),
    )));
    let pos_column: ArrayRef = Arc::new(Int64Array::from(positions));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![path_column, pos_column])
        .map_err(|e| anyhow!(e))?;
    writer.write(&batch).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use iceberg::{NamespaceIdent, TableCreation, TableIdent};

    use super::*;

    /// Builds an unpartitioned V2 iceberg table backed by an in-memory `FileIO`, so tests can
    /// author and read back position-delete files without any external object store.
    fn memory_table() -> Table {
        let file_io = FileIO::from_path("memory://").unwrap().build().unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::new(1, "id", Type::Primitive(PrimitiveType::Long), false).into(),
            ])
            .build()
            .unwrap();
        let table_creation = TableCreation {
            name: "t".to_owned(),
            location: Some("memory://root".to_owned()),
            schema,
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            sort_order: None,
            properties: HashMap::new(),
            format_version: FormatVersion::V2,
        };
        Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_owned()),
                "t".to_owned(),
            ))
            .file_io(file_io)
            .metadata(
                TableMetadataBuilder::from_table_creation(table_creation)
                    .unwrap()
                    .build()
                    .unwrap()
                    .metadata,
            )
            .build()
            .unwrap()
    }

    /// Positions written to a file-scoped V2 Parquet position-delete file round-trip exactly through
    /// [`write_parquet_position_delete_file`] and [`read_position_deletes_from_file`], across the
    /// multi-chunk boundary (`> POSITION_DELETE_WRITE_CHUNK_SIZE` positions), with the produced
    /// `DataFile` carrying the expected content type, format, and referenced data file.
    #[tokio::test]
    async fn parquet_position_delete_round_trip() {
        let table = memory_table();
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_generator = DefaultFileNameGenerator::new(
            "test".to_owned(),
            Some("pos-del".to_owned()),
            DataFileFormat::Parquet,
        );

        // Span more than one write chunk to exercise the flush boundary.
        let mut delete_vector = DeleteVector::default();
        for pos in 0u64..(2 * POSITION_DELETE_WRITE_CHUNK_SIZE as u64 + 7) {
            delete_vector.insert(pos);
        }
        let expected: Vec<u64> = delete_vector.iter().collect();

        let referenced = "memory://root/data/file-0.parquet".to_owned();
        let data_file = write_parquet_position_delete_file(
            &table,
            &location_generator,
            &file_name_generator,
            default_position_delete_writer_properties(),
            referenced.clone(),
            delete_vector,
            None,
        )
        .await
        .unwrap();

        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.file_format(), DataFileFormat::Parquet);
        assert_eq!(
            data_file.referenced_data_file().as_deref(),
            Some(referenced.as_str())
        );

        // Read back through the format-dispatching reader (routes to the Parquet reader).
        let read_back = read_position_deletes_from_file(table.file_io(), &data_file)
            .await
            .unwrap();
        let got: Vec<u64> = read_back.iter().collect();
        assert_eq!(got, expected);
    }
}
