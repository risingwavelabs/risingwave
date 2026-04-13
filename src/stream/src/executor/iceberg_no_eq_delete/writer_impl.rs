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

//! Real implementation of [`IcebergWriter`] backed by the iceberg-rust crate.
//!
//! Reuses the existing Iceberg sink's writer construction pattern from
//! `IcebergSinkWriterInner::build_append_only`, including:
//! - `ParquetWriterBuilder` with configurable compression and row group size
//! - `RollingFileWriterBuilder` with `target_file_size_mb`
//! - `DataFileWriterBuilder` for data file abstraction
//! - `SerializedDataFile` for metadata serialization (same format as existing sink)
//!
//! ## Position tracking
//!
//! Uses `CurrentFileStatus` from the iceberg-rust crate to obtain the real
//! file path and row offset for each written row. Before `write()`,
//! `current_row_num()` provides the starting offset; after `write()`,
//! `current_file_path()` provides the data file path the batch was written to.

use std::sync::Arc;

use anyhow::Context;
use iceberg::spec::{DataFile, FormatVersion, SerializedDataFile};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{
    CurrentFileStatus, IcebergWriter as IcebergCrateWriter,
    IcebergWriterBuilder as IcebergCrateWriterBuilder,
};
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_schema_iceberg::SchemaRef;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::sink::iceberg::common::{
    DataFileCommitMetadata, build_data_file_writer_builder,
};
use risingwave_pb::id::{ActorId, SinkId};

use super::writer::{IcebergWriter, RowPosition};
use crate::executor::{StreamExecutorError, StreamExecutorResult};

type DataFileWriterBuilderType =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;
type DataFileWriterType =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Real implementation of [`IcebergWriter`] using the iceberg-rust crate.
///
/// Constructed from `IcebergConfig` + `Table`, mirroring
/// `IcebergSinkWriterInner::build_append_only`.
pub struct IcebergWriterImpl {
    /// The underlying iceberg writer (`DataFileWriter` wrapped in layers).
    inner: Option<DataFileWriterType>,
    /// Builder used to recreate the writer after each flush.
    writer_builder: DataFileWriterBuilderType,
    /// Arrow schema for `StreamChunk` → `RecordBatch` conversion.
    arrow_schema: SchemaRef,
    /// Iceberg schema and partition spec IDs for commit metadata.
    schema_id: i32,
    partition_spec_id: i32,
    /// Partition type (needed for `SerializedDataFile` conversion).
    partition_type: iceberg::spec::StructType,
    /// Table format version.
    format_version: FormatVersion,
    sink_id: SinkId,
}

impl IcebergWriterImpl {
    /// Build a `RealIcebergWriter` from an `IcebergConfig` and loaded `Table`.
    ///
    /// This mirrors `IcebergSinkWriterInner::build_append_only` but is adapted
    /// for the streaming executor context (no metrics, no partition columns).
    pub fn build(
        config: &IcebergConfig,
        table: &Table,
        actor_id: ActorId,
        sink_id: SinkId,
    ) -> StreamExecutorResult<Self> {
        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();
        let format_version = table.metadata().format_version();
        let schema_id = table.metadata().current_schema_id();
        let partition_spec_id = table.metadata().default_partition_spec_id();
        let partition_type = partition_spec
            .partition_type(schema.as_ref())
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        // Build the writer using the shared builder utility from common.
        let data_file_builder = build_data_file_writer_builder(
            config,
            table,
            &actor_id.to_string(),
            &unique_suffix.to_string(),
        )?;

        // Build Arrow schema for StreamChunk conversion.
        let arrow_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(schema.as_ref())
                .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?,
        );

        Ok(Self {
            inner: None,
            writer_builder: data_file_builder,
            arrow_schema,
            schema_id,
            partition_spec_id,
            partition_type,
            format_version,
            sink_id,
        })
    }
}

#[async_trait::async_trait]
impl IcebergWriter for IcebergWriterImpl {
    async fn write_chunk(&mut self, chunk: &DataChunk) -> StreamExecutorResult<Vec<RowPosition>> {
        let n = chunk.cardinality();

        // Convert DataChunk to RecordBatch (same as existing sink).
        let record_batch = IcebergArrowConvert.to_record_batch(self.arrow_schema.clone(), chunk)?;

        // Lazily initialize the writer on first write (same pattern as DataFileWriter).
        if self.inner.is_none() {
            self.inner = Some(
                IcebergCrateWriterBuilder::build(&self.writer_builder, None)
                    .await
                    .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?,
            );
        }
        let writer = self.inner.as_mut().unwrap();

        // Write to iceberg writer (may trigger file rolling before writing).
        writer
            .write(record_batch)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        // After write, `current_file_path()` returns the file this batch was written to,
        // and `current_row_num()` returns the total row count in that file after this write.
        // The starting offset of this batch is (current_row_num - batch_size).
        // This is correct even when file rolling occurs: rolling closes the old file and
        // starts a new one (with row_num reset to 0) before writing the batch.
        let file_path = writer.current_file_path();
        let start_offset = (writer.current_row_num() - n) as i64;

        let mut positions = Vec::with_capacity(n);
        for i in 0..n {
            positions.push(RowPosition {
                file_path: file_path.clone(),
                offset: start_offset + i as i64,
            });
        }

        Ok(positions)
    }

    async fn flush(&mut self) -> StreamExecutorResult<Option<DataFileCommitMetadata>> {
        // If no writes happened this epoch, inner is None — nothing to flush.
        let Some(mut writer) = self.inner.take() else {
            return Ok(None);
        };

        // Close the writer to finalize data files.
        let data_files: Vec<DataFile> = writer
            .close()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        if data_files.is_empty() {
            return Ok(None);
        }

        // Convert DataFile to serializable JSON values.
        let serialized_files: Vec<serde_json::Value> = data_files
            .into_iter()
            .map(|df| {
                let serialized =
                    SerializedDataFile::try_from(df, &self.partition_type, self.format_version)
                        .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
                let res = serde_json::to_value(&serialized)
                    .context("failed to serialize SerializedDataFile to JSON")?;
                Ok(res)
            })
            .collect::<Result<_, StreamExecutorError>>()?;

        Ok(Some(DataFileCommitMetadata {
            schema_id: self.schema_id,
            partition_spec_id: self.partition_spec_id,
            data_files: serialized_files,
        }))
    }
}
