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

use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType,
    SerializedDataFile,
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
use risingwave_common::array::arrow::arrow_schema_iceberg::SchemaRef as ArrowSchemaRef;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergPositionDeleteMergerCommitResult, PARQUET_CREATED_BY,
    resolve_partition_type, serialize_data_files_default_spec,
};
use risingwave_connector::sink::{Result as SinkResult, SinkError};
use risingwave_connector::source::iceberg::parquet_file_handler::ParquetFileReader;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::ActorId;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::position_delete_merger::PositionDeleteHandler;
use super::position_delete_staging::StagingVersion;

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// Real implementation of [`PositionDeleteHandler`] using the iceberg-rust crate.
///
/// Loads the iceberg table once at init and keeps the handle for object-store I/O
/// and cached metadata; never reloads through the catalog. The per-shard delete
/// state lives in [`StagingVersion`], seeded once from the delete manifests and
/// appended to on every flush.
pub struct PositionDeleteHandlerImpl {
    config: IcebergConfig,
    table: Table,
    location_generator: DefaultLocationGenerator,
    /// File-name generator for V3 Puffin deletion vector files.
    file_name_generator: DefaultFileNameGenerator,
    /// File-name generator for V2 Parquet position-delete files.
    parquet_file_name_generator: DefaultFileNameGenerator,
    schema_id: i32,
    partition_spec_id: i32,
    format_version: FormatVersion,
    staging: StagingState,
    /// New positions accumulated since the last flush, keyed by data file path.
    pending: HashMap<String, DeleteVector>,
}

impl PositionDeleteHandlerImpl {
    pub async fn new(
        config: IcebergConfig,
        actor_id: ActorId,
        vnode_bitmap: Option<Bitmap>,
    ) -> SinkResult<Self> {
        let table = config.load_table().await?;
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            actor_id.to_string(),
            Some(format!("delvec-{}", uuid_suffix)),
            DataFileFormat::Puffin,
        );
        let parquet_file_name_generator = DefaultFileNameGenerator::new(
            actor_id.to_string(),
            Some(format!("pos-del-{}", uuid_suffix)),
            DataFileFormat::Parquet,
        );

        let schema_id = table.metadata().current_schema_id();
        let partition_spec_id = table.metadata().default_partition_spec_id();
        let format_version = table.metadata().format_version();

        let table_for_seed = table.clone();
        let staging = StagingState::Loading(tokio::spawn(async move {
            let mut res = StagingVersion::new(vnode_bitmap);
            seed_from_delete_manifests(&table_for_seed, &mut res).await?;
            Ok(res)
        }));

        Ok(Self {
            config,
            table,
            location_generator,
            file_name_generator,
            parquet_file_name_generator,
            schema_id,
            partition_spec_id,
            format_version,
            staging,
            pending: HashMap::new(),
        })
    }

    async fn flush_inner(&mut self) -> SinkResult<Option<SinkMetadata>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let staging = self.staging.await_loaded().await?;

        // V3 tables use Puffin deletion vectors; V2 tables use file-scoped Parquet
        // position-delete files. The internal representation is a roaring bitmap
        // (`DeleteVector`) in both cases; only the on-disk format differs.
        let use_puffin = self.format_version >= FormatVersion::V3;
        let pending = std::mem::take(&mut self.pending);

        let mut delete_files = Vec::with_capacity(pending.len());
        let mut overwrite_files = Vec::new();

        for (data_file_path, new_positions) in pending {
            // Merge the new positions with the resident delete vector, lazily loading
            // it on first touch of this path. Scoped so the staging borrow ends before
            // the `&self` write below; `self.table` is a disjoint field from
            // `self.staging`, so the lazy read can still borrow it here.
            let plan = {
                let entry = staging.entry_mut(&data_file_path);
                if entry.needs_load() {
                    let existing_file = entry
                        .current_file()
                        .expect("needs_load implies current_file is Some");
                    let dv = match existing_file.file_format() {
                        DataFileFormat::Puffin => {
                            read_dv_positions_from_data_file(self.table.file_io(), &existing_file)
                                .await?
                        }
                        DataFileFormat::Parquet => {
                            read_v2_positions_from_delete_file(self.table.file_io(), &existing_file)
                                .await?
                        }
                        other => {
                            return Err(SinkError::Iceberg(anyhow!(
                                "unexpected delete file format {:?}",
                                other
                            )));
                        }
                    };
                    entry.set_loaded_delete_vector(dv);
                }
                entry.plan_write(new_positions)
            };
            if let Some(overwrite) = plan.overwrite {
                overwrite_files.push(overwrite);
            }

            let new_file = if use_puffin {
                write_dv_puffin_file(
                    &self.file_name_generator,
                    &self.location_generator,
                    &self.table,
                    data_file_path.clone(),
                    &plan.merged,
                )
                .await?
            } else {
                write_parquet_position_delete_file(
                    &self.parquet_file_name_generator,
                    &self.location_generator,
                    &self.table,
                    &self.config,
                    data_file_path.clone(),
                    &plan.merged,
                )
                .await?
            };

            staging
                .entry_mut(&data_file_path)
                .record_written(new_file.clone(), plan.merged);
            delete_files.push(new_file);
        }

        if delete_files.is_empty() {
            return Ok(None);
        }

        let delete_files = serialize_delete_files(&self.table, delete_files)?;
        let overwrite_files = serialize_overwrite_files(&self.table, overwrite_files)?;

        let sink_metadata = SinkMetadata::try_from(&IcebergPositionDeleteMergerCommitResult {
            schema_id: self.schema_id,
            partition_spec_id: self.partition_spec_id,
            delete_files,
            overwrite_files,
        })?;
        Ok(Some(sink_metadata))
    }
}

/// Writes `delete_vector` as a single V3 Puffin deletion vector blob referencing
/// `data_file_path`, and returns its [`DataFile`] metadata with
/// `referenced_data_file` set. The delete file is written with an empty partition;
/// the coordinator backfills the real partition in a later stage.
async fn write_dv_puffin_file(
    file_name_generator: &DefaultFileNameGenerator,
    location_generator: &DefaultLocationGenerator,
    table: &Table,
    data_file_path: String,
    delete_vector: &DeleteVector,
) -> SinkResult<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    // Empty partition: the coordinator backfills the real partition.
    let location = location_generator.generate_location(None, &file_name);
    let output_file = table.file_io().new_output(&location)?;
    let mut writer = PuffinWriter::new(&output_file, StdHashMap::new(), false).await?;

    let cardinality = delete_vector.len();
    let properties = StdHashMap::from([
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
        .ok_or_else(|| SinkError::Iceberg(anyhow!("blob metadata should be present")))?;

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
    builder.build().map_err(|err| {
        SinkError::Iceberg(anyhow!(err).context("Failed to build deletion vector file metadata"))
    })
}

/// Writes `delete_vector` as a single file-scoped V2 Parquet position-delete file,
/// and returns the resulting [`DataFile`] metadata. The produced file references
/// exactly one data file (`data_file_path`) via `referenced_data_file` and is
/// written with an empty partition; the coordinator backfills the real partition
/// in a later stage.
///
/// File-scoped deletes reference exactly one data file, so this writes the file
/// with a plain [`ParquetWriter`] rather than the sink's rolling position-delete
/// writer stack.
async fn write_parquet_position_delete_file(
    file_name_generator: &DefaultFileNameGenerator,
    location_generator: &DefaultLocationGenerator,
    table: &Table,
    config: &IcebergConfig,
    data_file_path: String,
    delete_vector: &DeleteVector,
) -> SinkResult<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    // Empty partition: the coordinator backfills the real partition.
    let location = location_generator.generate_location(None, &file_name);
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

    // The position-delete schema is `(file_path, pos)` with reserved field IDs;
    // derive the matching Arrow schema so the written column field IDs line up.
    let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&POSITION_DELETE_SCHEMA)?);

    const POSITION_DELETE_WRITE_CHUNK_SIZE: usize = 1024;
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
        SinkError::Iceberg(anyhow!(
            "position-delete writer produced invalid file count for {data_file_path}"
        ))
    })?;

    // `ParquetWriter` builds the file as `DataContentType::Data` with an empty
    // partition; override those for a file-scoped V2 position-delete file and
    // attach `referenced_data_file`.
    builder
        .content(DataContentType::PositionDeletes)
        .referenced_data_file(Some(data_file_path));
    builder.build().map_err(|err| {
        SinkError::Iceberg(anyhow!(err).context("Failed to build position-delete file metadata"))
    })
}

/// Writes one chunk of `positions` as a `(file_path, pos)` batch into `writer`.
/// Every row shares `data_file_path` because the delete file is file-scoped.
async fn write_position_delete_chunk(
    writer: &mut ParquetWriter,
    arrow_schema: &ArrowSchemaRef,
    data_file_path: &str,
    positions: Vec<i64>,
) -> SinkResult<()> {
    let path_column: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        data_file_path,
        positions.len(),
    )));
    let pos_column: ArrayRef = Arc::new(Int64Array::from(positions));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![path_column, pos_column])
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))?;
    writer.write(&batch).await?;
    Ok(())
}

#[async_trait::async_trait]
impl PositionDeleteHandler for PositionDeleteHandlerImpl {
    fn write(&mut self, path: &str, pos: i64) -> SinkResult<()> {
        let pos: u64 = pos.try_into().context("position should be non-negative")?;
        self.pending.entry_ref(path).or_default().insert(pos);
        Ok(())
    }

    async fn flush(&mut self) -> SinkResult<Option<SinkMetadata>> {
        self.flush_inner().await
    }

    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Bitmap) -> SinkResult<()> {
        self.table = self.config.load_table().await?;
        let table = self.table.clone();
        self.staging = StagingState::Loading(tokio::spawn(async move {
            let mut res = StagingVersion::new(Some(vnode_bitmap));
            seed_from_delete_manifests(&table, &mut res).await?;
            Ok(res)
        }));
        Ok(())
    }
}

enum StagingState {
    Loading(JoinHandle<SinkResult<StagingVersion>>),
    Loaded(StagingVersion),
}

impl StagingState {
    async fn await_loaded(&mut self) -> SinkResult<&mut StagingVersion> {
        match self {
            StagingState::Loading(handle) => {
                let staging = handle.await.map_err(|e| SinkError::Iceberg(anyhow!(e)))??;
                *self = StagingState::Loaded(staging);
                let StagingState::Loaded(staging) = self else {
                    unreachable!("staging should be loaded after join");
                };
                Ok(staging)
            }
            StagingState::Loaded(staging) => Ok(staging),
        }
    }
}

/// Scan the table's current-snapshot delete manifests once and seed `staging` with
/// every live position-delete file whose referenced data file belongs to this
/// actor's shard. Contents are not read here; `StagedEntry::delete_vector` stays
/// `None` and is lazily loaded on first touch.
async fn seed_from_delete_manifests(table: &Table, staging: &mut StagingVersion) -> SinkResult<()> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(());
    };
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await?;

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if !entry.is_alive()
                || entry.content_type() != DataContentType::PositionDeletes
                || !matches!(
                    entry.file_format(),
                    DataFileFormat::Puffin | DataFileFormat::Parquet
                )
            {
                continue;
            }
            let data_file = entry.data_file();
            let Some(referenced_data_file) = data_file.referenced_data_file() else {
                continue;
            };
            if !staging.owns(&referenced_data_file) {
                continue;
            }
            staging.seed(referenced_data_file, data_file.clone());
        }
    }
    Ok(())
}

/// Serializes newly written deletion vector files against the current default
/// partition spec, after truncating oversized column statistics.
fn serialize_delete_files(
    table: &Table,
    delete_files: Vec<DataFile>,
) -> SinkResult<Vec<SerializedDataFile>> {
    serialize_data_files_default_spec(table, delete_files)
}

/// Serializes the original DV puffin files that are being overwritten. Each
/// existing DV puffin file was written under its own partition spec (potentially
/// older than the current default), so we resolve its partition type via the
/// file's `partition_spec_id` so the serialized form round-trips correctly,
/// instead of forcing the current default partition type.
fn serialize_overwrite_files(
    table: &Table,
    overwrite_files: Vec<DataFile>,
) -> SinkResult<Vec<SerializedDataFile>> {
    let format_version = table.metadata().format_version();
    let schema = table.metadata().current_schema();
    let mut partition_type_cache: HashMap<i32, iceberg::spec::StructType> = HashMap::new();
    overwrite_files
        .into_iter()
        .map(|f| {
            let spec_id = f.partition_spec_id();
            let pt = match partition_type_cache.entry(spec_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    // Resolve against the file's own (potentially older) partition spec, not the
                    // table default, so the serialized form round-trips correctly.
                    let pt = resolve_partition_type(table, spec_id, schema)?;
                    entry.insert(pt)
                }
            };

            Ok(SerializedDataFile::try_from(f, pt, format_version)?)
        })
        .collect()
}

async fn read_dv_positions_from_data_file(
    file_io: &FileIO,
    data_file: &DataFile,
) -> SinkResult<DeleteVector> {
    let blob_offset = data_file.content_offset().ok_or_else(|| {
        SinkError::Iceberg(anyhow!(
            "DV file {} missing content_offset for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        ))
    })?;
    let blob_length = data_file.content_size_in_bytes().ok_or_else(|| {
        SinkError::Iceberg(anyhow!(
            "DV file {} missing content_size_in_bytes for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        ))
    })?;

    let input_file = file_io.new_input(data_file.file_path())?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader.file_metadata().await?;
    let blob_metadata = file_metadata
        .blobs()
        .iter()
        .find(|blob| blob.offset() == blob_offset as u64 && blob.length() == blob_length as u64)
        .ok_or_else(|| {
            SinkError::Iceberg(anyhow!(
                "DV blob metadata not found in {} at offset={} length={}",
                data_file.file_path(),
                blob_offset,
                blob_length
            ))
        })?;
    let blob = puffin_reader.blob(blob_metadata).await?;

    let delete_vector = DeleteVector::from_puffin_blob(blob)?;
    Ok(delete_vector)
}

/// Reads the positions stored in a V2 Parquet position-delete file into a
/// [`DeleteVector`]. The file's schema is `(file_path, pos)`. Callers only invoke
/// this after the entry's `referenced_data_file` already matched the target data
/// file, and the files we write are file-scoped (every row shares one
/// `file_path`), so the `file_path` column is redundant here: we project only the
/// `pos` column and read every value.
async fn read_v2_positions_from_delete_file(
    file_io: &FileIO,
    data_file: &DataFile,
) -> SinkResult<DeleteVector> {
    let input_file = file_io.new_input(data_file.file_path())?;
    let metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let parquet_reader = ParquetFileReader::new(metadata, reader);
    let builder = ParquetRecordBatchStreamBuilder::new(parquet_reader).await?;
    // The position-delete schema fixes column order to (file_path, pos); project
    // only the `pos` leaf (column index 1) so the file_path column is never decoded.
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let mut stream = builder.with_projection(projection).build()?;

    let mut delete_vector = DeleteVector::default();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Only the projected `pos` column is present in the batch.
        let positions = batch.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                SinkError::Iceberg(anyhow!(
                    "position-delete pos column should be an Int64Array"
                ))
            })?;

        for pos in positions {
            let Some(pos) = pos else {
                return Err(SinkError::Iceberg(anyhow!(
                    "null value in position-delete file {}",
                    data_file.file_path()
                )));
            };
            delete_vector.insert(pos as u64);
        }
    }

    Ok(delete_vector)
}
