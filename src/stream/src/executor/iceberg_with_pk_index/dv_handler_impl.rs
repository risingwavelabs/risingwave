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

use std::collections::{HashMap as StdHashMap, HashSet};

use anyhow::Context;
use hashbrown::HashMap;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, PartitionKey,
    SerializedDataFile,
};
use iceberg::table::Table;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use risingwave_connector::sink::iceberg::{IcebergCommitResult, IcebergConfig, truncate_datafile};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::{ActorId, SinkId};
use uuid::Uuid;

use super::dv_merger::DvHandler;
use crate::executor::iceberg_with_pk_index::deserialize_partition_struct;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

#[derive(Debug, Default)]
struct LoadedDeleteVectorEntry {
    delete_vector: Option<DeleteVector>,
    partition_key: Option<PartitionKey>,
}

/// Real implementation of [`DvHandler`] using the iceberg-rust crate.
pub struct DvHandlerImpl {
    config: IcebergConfig,
    location_generator: DefaultLocationGenerator,
    file_name_generator: DefaultFileNameGenerator,
    delete_vectors: HashMap<String, DeleteVector>,
    partition_keys: HashMap<String, Vec<u8>>,
    sink_id: SinkId,
}

impl DvHandlerImpl {
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
        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            actor_id.to_string(),
            Some(unique_uuid_suffix.to_string()),
            DataFileFormat::Puffin,
        );
        Ok(Self {
            config,
            location_generator,
            file_name_generator,
            delete_vectors: HashMap::new(),
            partition_keys: HashMap::new(),
            sink_id,
        })
    }
}

#[async_trait::async_trait]
impl DvHandler for DvHandlerImpl {
    fn write(&mut self, path: &str, pos: i64) -> StreamExecutorResult<()> {
        let pos = pos.try_into().context("position should be non-negative")?;
        self.delete_vectors.entry_ref(path).or_default().insert(pos);
        Ok(())
    }

    fn add_partition_info(
        &mut self,
        path: &str,
        partition_info: Vec<u8>,
    ) -> StreamExecutorResult<()> {
        self.partition_keys
            .entry_ref(path)
            .or_insert(partition_info);
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
            std::mem::take(&mut self.partition_keys),
        )
        .await?;

        let mut data_files = Vec::with_capacity(self.delete_vectors.len());
        for (data_file_path, mut delete_vector) in self.delete_vectors.drain() {
            let LoadedDeleteVectorEntry {
                delete_vector: existing_delete_vector,
                partition_key,
            } = loaded_delete_vectors
                .remove(&data_file_path)
                .unwrap_or_default();

            if let Some(existing) = existing_delete_vector {
                delete_vector |= existing;
            }

            let file_name = self.file_name_generator.generate_file_name();
            let location = self
                .location_generator
                .generate_location(partition_key.as_ref(), &file_name);
            let output_file = table
                .file_io()
                .new_output(&location)
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
            let mut writer = PuffinWriter::new(&output_file, StdHashMap::new(), false)
                .await
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

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
            let blob = delete_vector
                .to_puffin_blob(properties)
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
            writer
                .add(blob, CompressionCodec::None)
                .await
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

            let result = writer
                .close_with_metadata()
                .await
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
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
            let data_file = builder.build().map_err(|err| {
                StreamExecutorError::sink_error(
                    anyhow::anyhow!(err).context("Failed to build deletion vector file metadata"),
                    self.sink_id,
                )
            })?;

            data_files.push(data_file);
        }

        if data_files.is_empty() {
            return Ok(None);
        }

        let format_version = table.metadata().format_version();
        let partition_type = table.metadata().default_partition_type();
        let data_files = data_files
            .into_iter()
            .map(|f| {
                // Truncate large column statistics BEFORE serialization
                let truncated = truncate_datafile(f);
                SerializedDataFile::try_from(truncated, partition_type, format_version)
                    .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))
            })
            .collect::<StreamExecutorResult<Vec<_>>>()?;
        let sink_metadata = SinkMetadata::try_from(&IcebergCommitResult {
            data_files,
            schema_id: table.metadata().current_schema_id(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
        })
        .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        Ok(Some(sink_metadata))
    }
}

/// Loads existing delete vectors for the given data file paths by scanning the
/// table's current snapshot. Returns a map from data file path to the loaded
/// delete vector and partition key (if partitioned).
///
/// The priority of loading is:
/// 1. It scans delete manifests for delete entries matching the given data file paths.
///    If found, loads the DV and partition key (if partitioned) from the delete entry.
///    This reflects the latest DV state for the data file as of the current snapshot.
/// 2. If not found in delete manifests, this means the data file has no delete entries
///    in the current snapshot. We scan the buffered partition information from input messages,
///    and construct partition key if partition info is available.
/// 3. Finally, it scans data manifests for entries matching the given data file paths.
///    This is a fallback path, and usually it shouldn't enter this path.
async fn load_delete_vectors(
    table: &Table,
    sink_id: SinkId,
    mut paths: HashSet<String>,
    partition_keys: HashMap<String, Vec<u8>>,
) -> StreamExecutorResult<HashMap<String, LoadedDeleteVectorEntry>> {
    let mut result: HashMap<String, LoadedDeleteVectorEntry> = HashMap::with_capacity(paths.len());

    let Some(snapshot) = table.metadata().current_snapshot() else {
        let partitioned = !table.metadata().default_partition_spec().is_unpartitioned();
        if !partitioned {
            return Ok(HashMap::new());
        }

        scan_partition_info_from_input_messages(
            table,
            sink_id,
            partition_keys,
            &mut paths,
            &mut result,
        )?;

        if !paths.is_empty() {
            return Err(StreamExecutorError::sink_error(
                anyhow::anyhow!("failed to find manifest entries for data files {:?}", paths),
                sink_id,
            ));
        }
        return Ok(result);
    };

    let partitioned = !table.metadata().default_partition_spec().is_unpartitioned();
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

    'deletes_outer: for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        for entry in manifest.entries() {
            if !entry.is_alive()
                || entry.content_type() != DataContentType::PositionDeletes
                || entry.file_format() != DataFileFormat::Puffin
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

            let delete_vector =
                read_dv_positions_from_data_file(table.file_io(), data_file, sink_id).await?;
            let partition_key = if partitioned {
                Some(build_partition_key_from_data_file(
                    table, data_file, sink_id,
                )?)
            } else {
                None
            };
            let duplicated = result
                .insert(
                    referenced_data_file,
                    LoadedDeleteVectorEntry {
                        delete_vector: Some(delete_vector),
                        partition_key,
                    },
                )
                .is_some();
            if duplicated {
                return Err(StreamExecutorError::sink_error(
                    anyhow::anyhow!(
                        "duplicate DV file {} for data file {} in manifest entries",
                        data_file.file_path(),
                        data_file.referenced_data_file().unwrap()
                    ),
                    sink_id,
                ));
            }

            if paths.is_empty() {
                break 'deletes_outer;
            }
        }
    }

    if paths.is_empty() || !partitioned {
        return Ok(result);
    }

    scan_partition_info_from_input_messages(
        table,
        sink_id,
        partition_keys,
        &mut paths,
        &mut result,
    )?;

    if paths.is_empty() {
        return Ok(result);
    }

    'data_outer: for manifest_file in manifest_list.entries() {
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
            let duplicated = result
                .insert(
                    data_file.file_path().to_owned(),
                    LoadedDeleteVectorEntry {
                        delete_vector: None,
                        partition_key: Some(partition_key),
                    },
                )
                .is_some();
            if duplicated {
                return Err(StreamExecutorError::sink_error(
                    anyhow::anyhow!(
                        "duplicate DV file {} for data file {} in manifest entries",
                        data_file.file_path(),
                        data_file.referenced_data_file().unwrap()
                    ),
                    sink_id,
                ));
            }

            if paths.is_empty() {
                break 'data_outer;
            }
        }
    }

    if !paths.is_empty() {
        return Err(StreamExecutorError::sink_error(
            anyhow::anyhow!("failed to find manifest entries for data files {:?}", paths),
            sink_id,
        ));
    }

    Ok(result)
}

fn scan_partition_info_from_input_messages(
    table: &Table,
    sink_id: SinkId,
    partition_keys: HashMap<String, Vec<u8>>,
    paths: &mut HashSet<String>,
    result: &mut HashMap<String, LoadedDeleteVectorEntry>,
) -> StreamExecutorResult<()> {
    let mut new_paths = HashSet::new();
    let partition_type = table.metadata().default_partition_type();
    for path in std::mem::take(paths) {
        if let Some(partition_info) = partition_keys.get(&path) {
            let partition_key = build_partition_key_from_partition_info(
                table,
                partition_info,
                partition_type,
                sink_id,
            )?;
            result.insert(
                path,
                LoadedDeleteVectorEntry {
                    delete_vector: None,
                    partition_key: Some(partition_key),
                },
            );
        } else {
            new_paths.insert(path);
        }
    }
    *paths = new_paths;
    Ok(())
}

fn build_partition_key_from_partition_info(
    table: &Table,
    partition_info: &[u8],
    partition_type: &iceberg::spec::StructType,
    sink_id: SinkId,
) -> StreamExecutorResult<PartitionKey> {
    let data = deserialize_partition_struct(partition_info, partition_type)
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    Ok(PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table.metadata().current_schema().clone(),
        data,
    ))
}

async fn read_dv_positions_from_data_file(
    file_io: &FileIO,
    data_file: &DataFile,
    sink_id: SinkId,
) -> StreamExecutorResult<DeleteVector> {
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

    let input_file = file_io
        .new_input(data_file.file_path())
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader
        .file_metadata()
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
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
    let blob = puffin_reader
        .blob(blob_metadata)
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

    let delete_vector = DeleteVector::from_puffin_blob(blob)
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    Ok(delete_vector)
}

fn build_partition_key_from_data_file(
    table: &Table,
    data_file: &DataFile,
    sink_id: SinkId,
) -> StreamExecutorResult<PartitionKey> {
    let partition_spec_id = data_file.partition_spec_id();
    let partition_spec = table
        .metadata()
        .partition_spec_by_id(partition_spec_id)
        .with_context(|| {
            format!(
                "failed to find partition spec {} for manifest file {}",
                partition_spec_id,
                data_file.file_path()
            )
        })
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

    Ok(PartitionKey::new(
        partition_spec.as_ref().clone(),
        table.metadata().current_schema().clone(),
        data_file.partition().clone(),
    ))
}
