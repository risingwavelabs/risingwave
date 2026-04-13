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

//! Real implementation of [`DvHandler`] backed by the iceberg-rust crate.
//!
//! Uses `DeletionVectorWriter` to write Puffin-format DV files and returns
//! commit metadata for the coordinator.
//!
//! ## Reading existing DVs
//!
//! Scans the current Iceberg snapshot's manifest entries to find DV files
//! (`content_type=PositionDeletes`, `file_format=Puffin`) that reference the
//! target data file. Reads the Puffin blob and deserializes the `RoaringTreemap`.
//!
//! ## Writing DVs
//!
//! Uses the iceberg crate's `DeletionVectorWriter` which accepts
//! `Vec<PositionDeleteInput>` and produces Puffin-format DV files.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::Context;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, ManifestContentType, SerializedDataFile,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::deletion_vector_writer::DeletionVectorWriterBuilder;
use iceberg::writer::base_writer::position_delete_file_writer::PositionDeleteInput;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter as _, IcebergWriterBuilder as _};
use risingwave_connector::sink::iceberg::common::{DvFileCommitMetadata, DvFileInfo};
use risingwave_pb::id::{ActorId, SinkId};

use super::dv_blob::read_dv_positions_from_data_file;
use super::dv_merger::DvHandler;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

type DvWriterBuilderType =
    DeletionVectorWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Real implementation of [`DvHandler`] using the iceberg-rust crate.
pub struct DvHandlerImpl {
    /// Iceberg table for reading manifest entries and existing DVs.
    table: Table,
    /// File I/O for reading existing DV files.
    file_io: FileIO,
    /// Reusable builder for creating `DeletionVectorWriter` instances.
    dv_writer_builder: DvWriterBuilderType,
    /// Table format version.
    format_version: FormatVersion,
    sink_id: SinkId,
}

impl DvHandlerImpl {
    pub fn new(table: Table, actor_id: ActorId, sink_id: SinkId) -> StreamExecutorResult<Self> {
        let file_io = table.file_io().clone();
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            actor_id.to_string(),
            Some(format!(
                "delvec-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            )),
            DataFileFormat::Puffin,
        );
        let format_version = table.metadata().format_version();
        let dv_writer_builder = DeletionVectorWriterBuilder::new(
            file_io.clone(),
            location_generator,
            file_name_generator,
        );
        Ok(Self {
            table,
            file_io,
            dv_writer_builder,
            format_version,
            sink_id,
        })
    }

    async fn load_existing_positions(
        &self,
        file_path: &str,
    ) -> StreamExecutorResult<BTreeSet<i64>> {
        let Some(snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(BTreeSet::new());
        };

        let manifest_list = self
            .table
            .object_cache()
            .get_manifest_list(snapshot, &self.table.metadata_ref())
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        let mut positions = BTreeSet::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(&self.file_io)
                .await
                .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

            for entry in manifest.entries() {
                if !entry.is_alive()
                    || entry.content_type() != DataContentType::PositionDeletes
                    || entry.file_format() != DataFileFormat::Puffin
                {
                    continue;
                }

                let data_file = entry.data_file();
                if data_file.referenced_data_file().as_deref() != Some(file_path) {
                    continue;
                }

                positions.extend(
                    read_dv_positions_from_data_file(&self.file_io, data_file, self.sink_id)
                        .await?,
                );
            }
        }

        Ok(positions)
    }
}

#[async_trait::async_trait]
impl DvHandler for DvHandlerImpl {
    async fn append_dv(
        &self,
        pending_deletes: HashMap<String, BTreeSet<i64>>,
    ) -> StreamExecutorResult<Option<DvFileCommitMetadata>> {
        if pending_deletes.is_empty() {
            return Ok(None);
        }

        if self.format_version < FormatVersion::V3 {
            return Err(anyhow::anyhow!(
                "deletion vectors require Iceberg format v3, got {:?}",
                self.format_version
            )
            .into());
        }

        // Collect all PositionDeleteInputs, merging with existing DVs per file.
        let mut all_inputs = Vec::new();
        for (file_path, new_positions) in &pending_deletes {
            let path: Arc<str> = Arc::from(file_path.as_str());
            let mut merged = self.load_existing_positions(file_path).await?;
            merged.extend(new_positions.iter().copied());
            for pos in merged {
                all_inputs.push(PositionDeleteInput::new(path.clone(), pos));
            }
        }

        // Write all DVs in a single writer session.
        let mut writer = self
            .dv_writer_builder
            .build(None)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        writer
            .write(all_inputs)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        let data_files = writer
            .close()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        if data_files.is_empty() {
            return Ok(None);
        }

        let partition_type = self.table.metadata().default_partition_type();
        let dv_files = data_files
            .iter()
            .map(|df| DvFileInfo {
                data_file_path: df.referenced_data_file().unwrap_or_default(),
                dv_file_path: df.file_path().to_owned(),
                num_deletes: df.record_count(),
            })
            .collect();
        let serialized_dv_data_files = data_files
            .into_iter()
            .map(|df| {
                let serialized =
                    SerializedDataFile::try_from(df, partition_type, self.format_version)
                        .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
                let res = serde_json::to_value(&serialized)
                    .context("failed to serialize DV SerializedDataFile to JSON")?;
                Ok(res)
            })
            .collect::<Result<Vec<_>, StreamExecutorError>>()?;

        Ok(Some(DvFileCommitMetadata {
            dv_files,
            serialized_dv_data_files,
        }))
    }
}
