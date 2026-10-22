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

//! Shared Iceberg position-delete (Parquet and Puffin deletion vector) helpers.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::scan::FileScanTaskDeleteFile;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, PartitionKey,
};
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
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    DataType as ArrowDataType, SchemaRef as ArrowSchemaRef,
};

use crate::sink::iceberg::{IcebergConfig, PARQUET_CREATED_BY};
use crate::source::iceberg::parquet_file_handler::ParquetFileReader;

/// File-name generators shared by all Iceberg position-delete writers.
///
/// All writers use the same prefix and format-specific suffix pattern. The identity is the only
/// caller-specific part and prevents concurrent actors/epochs from generating the same path.
#[derive(Clone, Debug)]
pub struct PositionDeleteFileNameGenerators {
    pub puffin: DefaultFileNameGenerator,
    pub parquet: DefaultFileNameGenerator,
}

impl PositionDeleteFileNameGenerators {
    pub fn new(identity: impl Display) -> Self {
        let prefix = "position-delete".to_owned();
        let unique_suffix = identity.to_string();
        Self {
            puffin: DefaultFileNameGenerator::new(
                prefix.clone(),
                Some(unique_suffix.clone()),
                DataFileFormat::Puffin,
            ),
            parquet: DefaultFileNameGenerator::new(
                prefix,
                Some(unique_suffix),
                DataFileFormat::Parquet,
            ),
        }
    }

    pub fn for_format(&self, format: DataFileFormat) -> anyhow::Result<&DefaultFileNameGenerator> {
        match format {
            DataFileFormat::Puffin => Ok(&self.puffin),
            DataFileFormat::Parquet => Ok(&self.parquet),
            other => anyhow::bail!(
                "unsupported position-delete output format {:?}; expected Puffin or Parquet",
                other
            ),
        }
    }
}

/// Write one file-scoped position-delete artifact using the table's configured on-disk format.
///
/// All callers share this dispatch so Puffin deletion vectors and V2 Parquet position deletes use
/// identical file-name and partition-path handling.
pub async fn write_position_delete_file(
    table: &Table,
    config: &IcebergConfig,
    location_generator: &DefaultLocationGenerator,
    file_name_generators: &PositionDeleteFileNameGenerators,
    format_version: FormatVersion,
    data_file_path: String,
    delete_vector: &DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let format = if format_version >= FormatVersion::V3 {
        DataFileFormat::Puffin
    } else {
        DataFileFormat::Parquet
    };
    let file_name_generator = file_name_generators.for_format(format)?;
    match format {
        DataFileFormat::Puffin => {
            write_dv_puffin_file(
                table,
                location_generator,
                file_name_generator,
                data_file_path,
                delete_vector,
                partition_key,
            )
            .await
        }
        DataFileFormat::Parquet => {
            write_parquet_position_delete_file(
                table,
                location_generator,
                file_name_generator,
                config,
                data_file_path,
                delete_vector,
                partition_key,
            )
            .await
        }
        _ => unreachable!("position-delete format is selected above"),
    }
}

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// Reads the deletion-vector positions of a single Puffin DV `DataFile`.
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

    PositionDeleteReader::new(file_io)
        .read_dv(
            data_file.file_path(),
            blob_offset as u64,
            blob_length as u64,
            None,
        )
        .await
}

/// Reads the positions stored in a V2 Parquet position-delete file into a [`DeleteVector`].
///
/// The file's schema is `(file_path, pos)`. Callers only invoke this after the entry's
/// `referenced_data_file` already matched the target data file, and the files we write
/// are file-scoped (every row shares one `file_path`), so the `file_path` column is
/// redundant here: we project only the `pos` column and read every value.
pub async fn read_parquet_position_deletes_from_file(
    file_io: &FileIO,
    delete_file: &DataFile,
) -> Result<DeleteVector> {
    PositionDeleteReader::new(file_io)
        .read_parquet(delete_file.file_path(), None)
        .await
}

/// Checks required at the source boundary, but not by trusted sink read-modify-write calls.
#[derive(Clone, Copy)]
struct PositionDeleteValidation<'a> {
    data_file_path: &'a str,
    data_record_count: u64,
    file_size: u64,
    record_count: u64,
}

struct CachedPuffinReader {
    path: String,
    /// Trusted sink reads do not need an additional file-size request.
    file_size: Option<u64>,
    reader: PuffinReader,
}

/// Metadata-only source checks, shared by planning (including compaction) and replay reads.
/// Returns the validated blob range for Puffin, or `None` for Parquet.
pub(crate) fn validate_position_delete_descriptor(
    delete: &FileScanTaskDeleteFile,
    data_file_path: &str,
) -> Result<Option<(u64, u64)>> {
    ensure!(
        delete.file_type == DataContentType::PositionDeletes,
        "expected file-scoped position deletes"
    );
    ensure!(
        delete.equality_ids.is_none(),
        "equality IDs on a position-delete artifact"
    );
    ensure!(
        delete.key_metadata.is_none(),
        "encrypted delete artifacts are not supported by the update reader"
    );
    ensure!(
        delete.referenced_data_file.as_deref() == Some(data_file_path),
        "delete artifact references a different or unspecified data file"
    );
    ensure!(
        delete.record_count.is_some(),
        "delete artifact is missing record_count"
    );
    match delete.file_format {
        DataFileFormat::Puffin => {
            let offset = u64::try_from(
                delete
                    .content_offset
                    .context("DV is missing content_offset")?,
            )
            .context("negative DV offset")?;
            let length = u64::try_from(
                delete
                    .content_size_in_bytes
                    .context("DV is missing content_size_in_bytes")?,
            )
            .context("negative DV length")?;
            ensure!(
                length > 0
                    && offset
                        .checked_add(length)
                        .is_some_and(|end| end <= delete.file_size_in_bytes),
                "DV range is outside its Puffin file"
            );
            Ok(Some((offset, length)))
        }
        DataFileFormat::Parquet => {
            ensure!(
                delete.content_offset.is_none() && delete.content_size_in_bytes.is_none(),
                "Parquet position deletes must not specify a DV range"
            );
            Ok(None)
        }
        _ => anyhow::bail!("unsupported position-delete format"),
    }
}

/// Shared decoder with at most one cached Puffin reader/footer. Callers may reuse it across
/// sequential tasks for the same table/FileIO. Decoded bitmaps are never cached by path.
/// Puffin paths must identify immutable files; drop the reader to release its cache.
pub struct PositionDeleteReader {
    file_io: FileIO,
    puffin: Option<CachedPuffinReader>,
}

impl PositionDeleteReader {
    pub fn new(file_io: &FileIO) -> Self {
        Self {
            file_io: file_io.clone(),
            puffin: None,
        }
    }

    /// Strict source entry: validate descriptors, every Parquet path/position and Puffin metadata.
    /// Reuse this reader across calls to retain its footer cache.
    pub(crate) async fn read_file_scoped(
        &mut self,
        data_file_path: &str,
        data_record_count: u64,
        deletes: &[FileScanTaskDeleteFile],
    ) -> Result<DeleteVector> {
        ensure!(
            deletes.len() <= 1,
            "multiple live delete artifacts for one data file"
        );
        let Some(delete) = deletes.first() else {
            return Ok(DeleteVector::default());
        };
        let blob_range = validate_position_delete_descriptor(delete, data_file_path)?;
        let validation = PositionDeleteValidation {
            data_file_path,
            data_record_count,
            file_size: delete.file_size_in_bytes,
            record_count: delete
                .record_count
                .context("delete artifact is missing record_count")?,
        };
        match blob_range {
            Some((offset, length)) => {
                self.read_dv(&delete.file_path, offset, length, Some(validation))
                    .await
            }
            None => self.read_parquet(&delete.file_path, Some(validation)).await,
        }
    }

    async fn read_dv(
        &mut self,
        file_path: &str,
        offset: u64,
        length: u64,
        validation: Option<PositionDeleteValidation<'_>>,
    ) -> Result<DeleteVector> {
        if self
            .puffin
            .as_ref()
            .is_none_or(|cached| cached.path != file_path)
        {
            // Release the previous footer before opening a different artifact.
            self.puffin = None;
            let input = self.file_io.new_input(file_path)?;
            self.puffin = Some(CachedPuffinReader {
                path: file_path.to_owned(),
                file_size: None,
                reader: PuffinReader::new(input).await?,
            });
        }
        let cached = self
            .puffin
            .as_mut()
            .expect("Puffin reader initialized above");
        if let Some(validation) = validation {
            let size = match cached.file_size {
                Some(size) => size,
                None => {
                    let size = self.file_io.new_input(file_path)?.metadata().await?.size;
                    cached.file_size = Some(size);
                    size
                }
            };
            ensure!(
                size == validation.file_size,
                "delete artifact size differs from its descriptor"
            );
        }
        let metadata = cached.reader.file_metadata().await?;
        let blob_metadata = metadata
            .blobs()
            .iter()
            .find(|blob| blob.offset() == offset && blob.length() == length)
            .context("DV blob range is missing from the Puffin footer")?;
        let blob = cached.reader.blob(blob_metadata).await?;
        if let Some(validation) = validation {
            ensure!(
                blob.properties()
                    .get(DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE)
                    .map(String::as_str)
                    == Some(validation.data_file_path),
                "DV blob references a different or unspecified data file"
            );
            let cardinality: u64 = blob
                .properties()
                .get(DELETION_VECTOR_PROPERTY_CARDINALITY)
                .context("DV blob is missing cardinality")?
                .parse()
                .context("invalid DV cardinality")?;
            ensure!(
                cardinality == validation.record_count,
                "DV cardinality differs from its descriptor"
            );
        }
        let positions = DeleteVector::from_puffin_blob(blob)?;
        if let Some(validation) = validation {
            ensure!(
                positions.len() == validation.record_count,
                "DV cardinality differs from its bitmap"
            );
            ensure!(
                positions
                    .max()
                    .is_none_or(|pos| pos < validation.data_record_count),
                "deleted position is outside the data file"
            );
        }
        Ok(positions)
    }

    async fn read_parquet(
        &self,
        file_path: &str,
        validation: Option<PositionDeleteValidation<'_>>,
    ) -> Result<DeleteVector> {
        let input = self.file_io.new_input(file_path)?;
        let metadata = input.metadata().await?;
        if let Some(validation) = validation {
            ensure!(
                metadata.size == validation.file_size,
                "delete artifact size differs from its descriptor"
            );
        }
        let reader = ParquetFileReader::new(metadata, input.reader().await?);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let builder = if validation.is_some() {
            ensure!(
                builder.schema().fields().len() == 2,
                "invalid position-delete schema"
            );
            ensure!(
                builder.schema().field_with_name("file_path")?.data_type() == &ArrowDataType::Utf8
                    && builder.schema().field_with_name("pos")?.data_type()
                        == &ArrowDataType::Int64,
                "invalid position-delete column types"
            );
            builder.with_batch_size(1024)
        } else {
            // Trusted sink files are file-scoped; preserve the position-only projection.
            let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
            builder.with_projection(projection)
        };
        let mut batches = builder.build()?;
        let mut positions = DeleteVector::default();
        let mut rows = 0u64;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            let (paths, values) = if let Some(validation) = validation {
                let paths = batch
                    .column_by_name("file_path")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                    .context("position-delete file_path must be a string")?;
                let values = batch
                    .column_by_name("pos")
                    .context("missing position-delete pos column")?;
                (Some((paths, validation.data_file_path)), values)
            } else {
                (None, batch.column(0))
            };
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("position-delete pos must be an int64")?;
            for (index, pos) in values.iter().enumerate() {
                if let Some((paths, data_file_path)) = paths {
                    ensure!(
                        !paths.is_null(index) && paths.value(index) == data_file_path,
                        "position-delete row references a different or null data file"
                    );
                }
                let pos = pos.context("null deleted position")?;
                let pos = if let Some(validation) = validation {
                    let pos = u64::try_from(pos).context("negative deleted position")?;
                    ensure!(
                        pos < validation.data_record_count,
                        "deleted position is outside the data file"
                    );
                    pos
                } else {
                    pos as u64
                };
                positions.insert(pos);
            }
            if let Some(validation) = validation {
                rows += batch.num_rows() as u64;
                ensure!(
                    rows <= validation.record_count,
                    "position-delete row count exceeds its descriptor"
                );
            }
        }
        // Duplicates are harmless: Parquet record_count counts physical rows, not distinct positions.
        if let Some(validation) = validation {
            ensure!(
                rows == validation.record_count,
                "position-delete row count differs from its descriptor"
            );
        }
        Ok(positions)
    }
}

/// Reads the deleted positions of a single position-delete `DataFile` regardless of on-disk format,
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

/// Writes `delete_vector` as a single Puffin deletion-vector blob referencing `data_file_path`,
/// and returns its [`DataFile`] metadata (content `PositionDeletes`, format `Puffin`) with
/// `referenced_data_file` set.
pub async fn write_dv_puffin_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    data_file_path: String,
    delete_vector: &DeleteVector,
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

/// Writes `delete_vector` as a single file-scoped Parquet position-delete file referencing
/// `data_file_path`, and returns its [`DataFile`] metadata (content `PositionDeletes`, format
/// `Parquet`) with `referenced_data_file` set.
pub async fn write_parquet_position_delete_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    config: &IcebergConfig,
    data_file_path: String,
    delete_vector: &DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key, &file_name);
    let output_file = table.file_io().new_output(&location)?;

    let parquet_writer_properties = WriterProperties::builder()
        .set_compression(config.get_parquet_compression())
        .set_max_row_group_bytes(config.write_parquet_max_row_group_bytes())
        .set_created_by(PARQUET_CREATED_BY.to_owned())
        .build();
    let mut writer = ParquetWriterBuilder::new(
        parquet_writer_properties,
        POSITION_DELETE_SCHEMA.clone().into(),
    )
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
