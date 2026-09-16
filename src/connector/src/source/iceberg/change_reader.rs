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

//! Per-file change reads for the PK-index source contract. The snapshot planner, not the reader's
//! local history, determines whether a file is new or retained. This does not schedule phases or
//! commit progress: callers must checkpoint cursors with output and enforce Delete-before-Insert.

use std::collections::HashSet;

use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use futures_async_stream::try_stream;
use iceberg::delete_vector::DeleteVector;
use iceberg::metadata_columns::{RESERVED_FIELD_ID_POS, is_metadata_field};
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_array_iceberg::Int64Array;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};

use crate::connector_common::IcebergSourceContract;
use crate::sink::iceberg::PositionDeleteReader;

/// The planner must retain this classification and the parent descriptors across replay.
#[derive(Clone)]
pub enum IcebergChangeReadMode {
    /// Bootstrap or newly added file: current deletes only filter Inserts.
    Insert,
    /// A file present in both snapshots: retract only the new deleted positions.
    Delete {
        parent_deletes: Vec<FileScanTaskDeleteFile>,
    },
}

/// An empty chunk still carries read progress. EOF, not the last nonempty chunk, completes a task.
pub struct IcebergChangeBatch {
    pub chunk: StreamChunk,
    pub next_position: u64,
}

/// Strict decoding for the source boundary. Unlike the sink's trusted-file helper, this checks
/// every Parquet path and position and validates Puffin blob metadata against the descriptor.
/// Reuse the caller's reader to retain its Puffin footer cache across calls.
pub async fn read_deleted_positions(
    delete_reader: &mut PositionDeleteReader,
    data_file_path: &str,
    data_record_count: u64,
    deletes: &[FileScanTaskDeleteFile],
) -> Result<DeleteVector> {
    delete_reader
        .read_file_scoped(data_file_path, data_record_count, deletes)
        .await
}

pub fn newly_deleted_positions(
    parent: &DeleteVector,
    mut current: DeleteVector,
) -> Result<DeleteVector> {
    ensure!(
        parent.is_subset(&current),
        "deleted positions shrank for a retained data file"
    );
    current -= parent;
    Ok(current)
}

/// Read one whole immutable file with bounded row batches. SDK `_pos` is used internally and
/// removed before creating logical chunks. No shredded-VARIANT-to-NULL fallback is allowed here.
/// Resume rereads from the beginning; seeking is an optimization, not part of the cursor contract.
/// The caller supplies a reader for this table's `FileIO` and controls its cache lifetime. Sequential
/// tasks can reuse it after dropping the returned stream; parallel tasks need separate readers.
#[try_stream(boxed, ok = IcebergChangeBatch, error = anyhow::Error)]
pub async fn read_file_changes(
    table: Table,
    delete_reader: &mut PositionDeleteReader,
    mut task: FileScanTask,
    contract: IcebergSourceContract,
    mode: IcebergChangeReadMode,
    chunk_size: usize,
    resume_position: u64,
) {
    ensure!(chunk_size > 0, "change reader batch size must be positive");
    ensure!(
        task.data_file_format == DataFileFormat::Parquet,
        "change reader requires Parquet data files"
    );
    ensure!(
        task.key_metadata.is_none(),
        "encrypted data files are not supported by the change reader"
    );
    ensure!(
        task.start == 0 && task.length == task.file_size_in_bytes && task.predicate.is_none(),
        "change reader requires a whole-file task without predicate pushdown"
    );
    let record_count = task
        .record_count
        .context("change task is missing data record_count")?;
    ensure!(
        record_count <= i64::MAX as u64 && resume_position <= record_count,
        "invalid change task position"
    );
    contract.validate_schema(&task.schema)?;
    let mut projected = HashSet::new();
    for id in &task.project_field_ids {
        ensure!(
            projected.insert(*id)
                && !is_metadata_field(*id)
                && task
                    .schema
                    .as_struct()
                    .fields()
                    .iter()
                    .any(|field| field.id == *id),
            "change reader projection must contain unique stored top-level fields, not virtual metadata"
        );
    }
    ensure!(
        contract
            .key_field_ids()
            .iter()
            .all(|id| projected.contains(id)),
        "change reader projection must retain the complete sink key"
    );

    let current = read_deleted_positions(
        delete_reader,
        &task.data_file_path,
        record_count,
        &task.deletes,
    )
    .await?;
    let (op, positions) = match mode {
        IcebergChangeReadMode::Insert => (Op::Insert, current),
        IcebergChangeReadMode::Delete { parent_deletes } => {
            let parent = read_deleted_positions(
                delete_reader,
                &task.data_file_path,
                record_count,
                &parent_deletes,
            )
            .await?;
            (Op::Delete, newly_deleted_positions(&parent, current)?)
        }
    };
    task.deletes.clear();
    let logical_column_count = task.project_field_ids.len();
    task.project_field_ids.push(RESERVED_FIELD_ID_POS);
    let reader = table
        .reader_builder()
        .with_batch_size(chunk_size)
        .with_data_file_concurrency_limit(1)
        .with_row_group_filtering_enabled(false)
        .build();
    let mut batches = reader.read(tokio_stream::once(Ok(task)).boxed())?.stream();
    let projection: Vec<_> = (0..logical_column_count).collect();
    let mut next_position = 0u64;
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        ensure!(
            batch.num_columns() == logical_column_count + 1,
            "unexpected change reader output schema"
        );
        let row_positions = batch
            .column(logical_column_count)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("SDK row positions must be int64")?;
        let mut visibility = BitmapBuilder::with_capacity(batch.num_rows());
        for pos in row_positions {
            let pos = u64::try_from(pos.context("null SDK row position")?)
                .context("negative SDK row position")?;
            ensure!(
                pos == next_position && pos < record_count,
                "SDK row positions disagree with the whole-file task"
            );
            next_position = pos + 1;
            visibility.append(
                pos >= resume_position
                    && match op {
                        Op::Insert => !positions.contains(pos),
                        Op::Delete => positions.contains(pos),
                        _ => unreachable!(),
                    },
            );
        }
        if next_position <= resume_position {
            continue;
        }
        let mut visibility = visibility.finish();
        let mut projected_batch = batch.project(&projection)?;
        if visibility.count_ones() == 0 {
            // Preserve the typed schema and physical cursor without converting filtered row values.
            projected_batch = projected_batch.slice(0, 0);
            visibility = Bitmap::zeros(0);
        }
        let mut chunk = IcebergArrowConvert.chunk_from_record_batch(&projected_batch)?;
        chunk.set_visibility(visibility);
        yield IcebergChangeBatch {
            chunk: StreamChunk::from_parts(vec![op; chunk.capacity()], chunk),
            next_position,
        };
    }
    ensure!(
        next_position == record_count,
        "data row count differs from the change task"
    );
}

#[cfg(test)]
#[path = "change_reader_test.rs"]
mod tests;
