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

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestList,
    PartitionKey, SerializedDataFile,
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
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergPositionDeleteMergerCommitResult, PARQUET_CREATED_BY,
    read_dv_positions_from_data_file, resolve_partition_spec, resolve_partition_type,
    serialize_data_files_default_spec, write_dv_puffin_file as connector_write_dv_puffin_file,
};
use risingwave_connector::source::iceberg::parquet_file_handler::ParquetFileReader;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::{ActorId, SinkId};
use uuid::Uuid;

use super::position_delete_merger::PositionDeleteHandler;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// Per-data-file accumulator returned by [`load_delete_vectors`].
///
/// Holds the delete positions loaded from an existing delete file (a V3 Puffin deletion vector
/// or a V2 Parquet position-delete file), the partition key for partitioned tables, and the
/// existing delete file to be overwritten when the merged result is committed.
#[derive(Debug, Default)]
struct LoadedDeleteVectorEntry {
    delete_vector: Option<DeleteVector>,
    partition_key: Option<PartitionKey>,
    overwrite_file: Option<DataFile>,
}

/// Real implementation of [`PositionDeleteHandler`] using the iceberg-rust crate.
pub struct PositionDeleteHandlerImpl {
    config: IcebergConfig,
    location_generator: DefaultLocationGenerator,
    /// File-name generator for V3 Puffin deletion vector files.
    file_name_generator: DefaultFileNameGenerator,
    /// File-name generator for V2 Parquet position-delete files.
    parquet_file_name_generator: DefaultFileNameGenerator,
    delete_vectors: HashMap<String, DeleteVector>,
    sink_id: SinkId,
}

impl PositionDeleteHandlerImpl {
    pub async fn new(
        config: IcebergConfig,
        actor_id: ActorId,
        sink_id: SinkId,
    ) -> StreamExecutorResult<Self> {
        let table = config
            .load_table()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
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
        Ok(Self {
            config,
            location_generator,
            file_name_generator,
            parquet_file_name_generator,
            delete_vectors: HashMap::new(),
            sink_id,
        })
    }

    /// Merges `delete_vector` with any existing DV from `loaded`, writes the result
    /// as a single V3 Puffin deletion vector blob referencing `data_file_path`, and
    /// returns its [`DataFile`] metadata with `referenced_data_file` set.
    ///
    /// The Puffin authoring itself lives in the connector crate
    /// ([`connector_write_dv_puffin_file`]) so it can be shared with meta's pk-index
    /// compaction coordinator; this method only folds in the existing DV / partition key.
    async fn write_dv_puffin_file(
        &mut self,
        table: &Table,
        data_file_path: String,
        mut delete_vector: DeleteVector,
        loaded: LoadedDeleteVectorEntry,
    ) -> StreamExecutorResult<DataFile> {
        let LoadedDeleteVectorEntry {
            delete_vector: existing_delete_vector,
            partition_key,
            ..
        } = loaded;

        if let Some(existing) = existing_delete_vector {
            delete_vector |= existing;
        }

        connector_write_dv_puffin_file(
            table,
            &self.location_generator,
            &self.file_name_generator,
            data_file_path,
            delete_vector,
            partition_key.as_ref(),
        )
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))
    }

    /// Merges `delete_vector` with any existing positions from `loaded`, writes the result
    /// as a single file-scoped V2 Parquet position-delete file, and returns the resulting
    /// [`DataFile`] metadata. The produced file references exactly one data file
    /// (`data_file_path`) via `referenced_data_file`.
    ///
    /// File-scoped deletes reference exactly one data file, so this writes the file
    /// with a plain [`ParquetWriter`] rather than the sink's rolling position-delete
    /// writer stack.
    async fn write_parquet_position_delete_file(
        &self,
        table: &Table,
        data_file_path: String,
        mut delete_vector: DeleteVector,
        loaded: LoadedDeleteVectorEntry,
    ) -> StreamExecutorResult<DataFile> {
        let LoadedDeleteVectorEntry {
            delete_vector: existing_delete_vector,
            partition_key,
            ..
        } = loaded;

        if let Some(existing) = existing_delete_vector {
            delete_vector |= existing;
        }

        let file_name = self.parquet_file_name_generator.generate_file_name();
        let location = self
            .location_generator
            .generate_location(partition_key.as_ref(), &file_name);
        let output_file = table
            .file_io()
            .new_output(&location)
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        let parquet_writer_properties = WriterProperties::builder()
            .set_compression(self.config.get_parquet_compression())
            .set_max_row_group_bytes(self.config.write_parquet_max_row_group_bytes())
            .set_created_by(PARQUET_CREATED_BY.to_owned())
            .build();
        let mut writer = ParquetWriterBuilder::new(
            parquet_writer_properties,
            POSITION_DELETE_SCHEMA.clone().into(),
        )
        .build(output_file)
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        // The position-delete schema is `(file_path, pos)` with reserved field IDs;
        // derive the matching Arrow schema so the written column field IDs line up.
        let arrow_schema: ArrowSchemaRef = Arc::new(
            schema_to_arrow_schema(&POSITION_DELETE_SCHEMA)
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?,
        );

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
                    self.sink_id,
                )
                .await?;
                positions.reserve(POSITION_DELETE_WRITE_CHUNK_SIZE);
            }
        }
        if !positions.is_empty() {
            write_position_delete_chunk(
                &mut writer,
                &arrow_schema,
                &data_file_path,
                positions,
                self.sink_id,
            )
            .await?;
        }

        let data_files = writer
            .close()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        // `close` will yield exactly one builder here.
        let [mut builder] = data_files.try_into().map_err(|_| {
            StreamExecutorError::sink_error(
                anyhow::anyhow!(
                    "position-delete writer produced invalid file count for {data_file_path}"
                ),
                self.sink_id,
            )
        })?;

        // `ParquetWriter` builds the file as `DataContentType::Data` with an empty
        // partition; override those for a file-scoped V2 position-delete file and
        // attach `referenced_data_file`.
        builder
            .content(DataContentType::PositionDeletes)
            .referenced_data_file(Some(data_file_path));
        if let Some(partition_key) = partition_key {
            builder
                .partition(partition_key.data().clone())
                .partition_spec_id(partition_key.spec().spec_id());
        }
        builder.build().map_err(|err| {
            StreamExecutorError::sink_error(
                anyhow::anyhow!(err).context("Failed to build position-delete file metadata"),
                self.sink_id,
            )
        })
    }
}

/// Writes one chunk of `positions` as a `(file_path, pos)` batch into `writer`.
/// Every row shares `data_file_path` because the delete file is file-scoped.
async fn write_position_delete_chunk(
    writer: &mut ParquetWriter,
    arrow_schema: &ArrowSchemaRef,
    data_file_path: &str,
    positions: Vec<i64>,
    sink_id: SinkId,
) -> StreamExecutorResult<()> {
    let path_column: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        data_file_path,
        positions.len(),
    )));
    let pos_column: ArrayRef = Arc::new(Int64Array::from(positions));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![path_column, pos_column])
        .map_err(|e| StreamExecutorError::sink_error(anyhow::anyhow!(e), sink_id))?;
    writer
        .write(&batch)
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))
}

#[async_trait::async_trait]
impl PositionDeleteHandler for PositionDeleteHandlerImpl {
    fn write(&mut self, path: &str, pos: i64) -> StreamExecutorResult<()> {
        let pos = pos.try_into().context("position should be non-negative")?;
        self.delete_vectors.entry_ref(path).or_default().insert(pos);
        Ok(())
    }

    async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>> {
        if self.delete_vectors.is_empty() {
            return Ok(None);
        }

        let table = self
            .config
            .load_table()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        let mut loaded_delete_vectors = load_delete_vectors(
            &table,
            self.sink_id,
            self.delete_vectors.keys().cloned().collect(),
        )
        .await?;

        let overwrite_files = loaded_delete_vectors
            .values()
            .filter_map(|entry| entry.overwrite_file.clone())
            .collect::<Vec<_>>();

        // V3 tables use Puffin deletion vectors; V2 tables use file-scoped Parquet
        // position-delete files. The internal representation is a roaring bitmap
        // (`DeleteVector`) in both cases; only the on-disk format differs.
        let use_puffin = table.metadata().format_version() >= FormatVersion::V3;

        let pending = std::mem::take(&mut self.delete_vectors);
        let mut delete_files = Vec::with_capacity(pending.len());
        for (data_file_path, delete_vector) in pending {
            let loaded = loaded_delete_vectors
                .remove(&data_file_path)
                .unwrap_or_default();
            let file = if use_puffin {
                self.write_dv_puffin_file(&table, data_file_path, delete_vector, loaded)
                    .await?
            } else {
                self.write_parquet_position_delete_file(
                    &table,
                    data_file_path,
                    delete_vector,
                    loaded,
                )
                .await?
            };
            delete_files.push(file);
        }

        if delete_files.is_empty() {
            return Ok(None);
        }

        let delete_files = serialize_delete_files(&table, self.sink_id, delete_files)?;
        let overwrite_files = serialize_overwrite_files(&table, self.sink_id, overwrite_files)?;

        let sink_metadata = SinkMetadata::try_from(&IcebergPositionDeleteMergerCommitResult {
            schema_id: table.metadata().current_schema_id(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            delete_files,
            overwrite_files,
        })
        .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        Ok(Some(sink_metadata))
    }
}

/// Serializes newly written deletion vector files against the current default
/// partition spec, after truncating oversized column statistics.
fn serialize_delete_files(
    table: &Table,
    sink_id: SinkId,
    delete_files: Vec<DataFile>,
) -> StreamExecutorResult<Vec<SerializedDataFile>> {
    serialize_data_files_default_spec(table, delete_files)
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))
}

/// Serializes the original DV puffin files that are being overwritten. Each
/// existing DV puffin file was written under its own partition spec (potentially
/// older than the current default), so we resolve its partition type via the
/// file's `partition_spec_id` so the serialized form round-trips correctly,
/// instead of forcing the current default partition type.
fn serialize_overwrite_files(
    table: &Table,
    sink_id: SinkId,
    overwrite_files: Vec<DataFile>,
) -> StreamExecutorResult<Vec<SerializedDataFile>> {
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
                    let pt = resolve_partition_type(table, spec_id, schema)
                        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
                    entry.insert(pt)
                }
            };

            SerializedDataFile::try_from(f, pt, format_version)
                .map_err(|e| StreamExecutorError::sink_error(e, sink_id))
        })
        .collect()
}

/// Loads existing position deletes for the given data file paths by scanning the
/// table's current snapshot. Returns a map from data file path to the loaded
/// delete positions (as a [`DeleteVector`]), partition key (if partitioned), and
/// original delete file (for overwrite). Handles both V3 Puffin deletion vectors
/// and V2 file-scoped Parquet position-delete files.
///
/// The lookup proceeds in two passes:
/// 1. Scan delete manifests for entries matching the given data file paths. If found,
///    load the existing delete positions, partition key (when partitioned), and the
///    existing delete file (as the overwrite target) from the manifest entry. This
///    reflects the latest delete state for the data file as of the current snapshot.
/// 2. For partitioned tables, scan data manifests for any data files still missing an
///    entry after pass 1 (data files that have not yet accumulated any position deletes).
///    Records the data file as the overwrite target and its partition key.
async fn load_delete_vectors(
    table: &Table,
    sink_id: SinkId,
    mut paths: HashSet<String>,
) -> StreamExecutorResult<HashMap<String, LoadedDeleteVectorEntry>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(HashMap::new());
    };

    let partitioned = !table.metadata().default_partition_spec().is_unpartitioned();
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

    let mut result: HashMap<String, LoadedDeleteVectorEntry> = HashMap::with_capacity(paths.len());

    scan_existing_delete_files(
        table,
        sink_id,
        partitioned,
        &manifest_list,
        &mut paths,
        &mut result,
    )
    .await?;

    if !paths.is_empty() && partitioned {
        scan_data_files_for_partition_keys(table, sink_id, &manifest_list, &mut paths, &mut result)
            .await?;
    }

    Ok(result)
}

/// Pass 1: scan delete manifests for existing position-delete files (V3 Puffin
/// deletion vectors or V2 Parquet position-delete files) referencing any path in
/// `paths`. On hit, removes the path from `paths`, loads the deletes, captures the
/// partition key (for partitioned tables), and records the existing delete file as
/// the overwrite target.
async fn scan_existing_delete_files(
    table: &Table,
    sink_id: SinkId,
    partitioned: bool,
    manifest_list: &ManifestList,
    paths: &mut HashSet<String>,
    result: &mut HashMap<String, LoadedDeleteVectorEntry>,
) -> StreamExecutorResult<()> {
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        for entry in manifest.entries() {
            // Accept both V3 Puffin deletion vectors and V2 Parquet position-delete
            // files; the reader is selected by the entry's file format below.
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
            if !paths.remove(&referenced_data_file) {
                continue;
            }

            let delete_vector = match data_file.file_format() {
                DataFileFormat::Puffin => {
                    read_dv_positions_from_data_file(table.file_io(), data_file)
                        .await
                        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?
                }
                DataFileFormat::Parquet => {
                    read_v2_positions_from_delete_file(table.file_io(), data_file, sink_id).await?
                }
                // Unreachable: the guard above only admits Puffin/Parquet delete files.
                other => {
                    return Err(StreamExecutorError::sink_error(
                        anyhow::anyhow!("unexpected delete file format {:?}", other),
                        sink_id,
                    ));
                }
            };
            let partition_key = if partitioned {
                Some(build_partition_key_from_data_file(
                    table, data_file, sink_id,
                )?)
            } else {
                None
            };
            result.insert(
                referenced_data_file,
                LoadedDeleteVectorEntry {
                    delete_vector: Some(delete_vector),
                    partition_key,
                    overwrite_file: Some(data_file.clone()),
                },
            );

            if paths.is_empty() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Pass 2 (partitioned tables only): scan data manifests for data files whose
/// path is still in `paths` (i.e. no existing DV was found in pass 1). Records
/// the data file as the overwrite target along with its partition key, with no
/// pre-existing delete vector.
async fn scan_data_files_for_partition_keys(
    table: &Table,
    sink_id: SinkId,
    manifest_list: &ManifestList,
    paths: &mut HashSet<String>,
    result: &mut HashMap<String, LoadedDeleteVectorEntry>,
) -> StreamExecutorResult<()> {
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        for entry in manifest.entries() {
            if !entry.is_alive() || entry.content_type() != DataContentType::Data {
                continue;
            }

            let data_file = entry.data_file();
            if !paths.remove(data_file.file_path()) {
                continue;
            }

            let partition_key = build_partition_key_from_data_file(table, data_file, sink_id)?;
            result.insert(
                data_file.file_path().to_owned(),
                LoadedDeleteVectorEntry {
                    delete_vector: None,
                    partition_key: Some(partition_key),
                    overwrite_file: None,
                },
            );

            if paths.is_empty() {
                return Ok(());
            }
        }
    }
    Ok(())
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
    sink_id: SinkId,
) -> StreamExecutorResult<DeleteVector> {
    let input_file = file_io
        .new_input(data_file.file_path())
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    let metadata = input_file
        .metadata()
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    let reader = input_file
        .reader()
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    let parquet_reader = ParquetFileReader::new(metadata, reader);
    let builder = ParquetRecordBatchStreamBuilder::new(parquet_reader)
        .await
        .map_err(|e| StreamExecutorError::sink_error(anyhow::anyhow!(e), sink_id))?;
    // The position-delete schema fixes column order to (file_path, pos); project
    // only the `pos` leaf (column index 1) so the file_path column is never decoded.
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let mut stream = builder
        .with_projection(projection)
        .build()
        .map_err(|e| StreamExecutorError::sink_error(anyhow::anyhow!(e), sink_id))?;

    let mut delete_vector = DeleteVector::default();
    while let Some(batch) = stream.next().await {
        let batch =
            batch.map_err(|e| StreamExecutorError::sink_error(anyhow::anyhow!(e), sink_id))?;
        // Only the projected `pos` column is present in the batch.
        let positions = batch.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("position-delete pos column should be an Int64Array")?;

        for pos in positions {
            let Some(pos) = pos else {
                return Err(StreamExecutorError::sink_error(
                    anyhow::anyhow!(
                        "null value in position-delete file {}",
                        data_file.file_path()
                    ),
                    sink_id,
                ));
            };
            delete_vector.insert(pos as u64);
        }
    }

    Ok(delete_vector)
}

fn build_partition_key_from_data_file(
    table: &Table,
    data_file: &DataFile,
    sink_id: SinkId,
) -> StreamExecutorResult<PartitionKey> {
    let partition_spec = resolve_partition_spec(table, data_file.partition_spec_id())
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

    Ok(PartitionKey::new(
        partition_spec.as_ref().clone(),
        table.metadata().current_schema().clone(),
        data_file.partition().clone(),
    ))
}
