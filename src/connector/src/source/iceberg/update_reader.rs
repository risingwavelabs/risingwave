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

//! Per-file updates (Insert and Delete) for the PK-index source contract. The planner, not the reader's
//! local history, determines whether a file is new or retained. This does not schedule phases or
//! commit progress: callers must checkpoint cursors with output and enforce Delete-before-Insert.

use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use futures_async_stream::try_stream;
use iceberg::delete_vector::DeleteVector;
use iceberg::metadata_columns::RESERVED_FIELD_ID_POS;
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
pub enum IcebergUpdateReadMode {
    /// Bootstrap or newly added file: current deletes only filter Inserts.
    Insert,
    /// A file present in both snapshots: retract only the new deleted positions.
    Delete {
        parent_deletes: Vec<FileScanTaskDeleteFile>,
    },
}

/// An empty chunk still carries read progress. EOF, not the last nonempty chunk, completes a task.
pub struct IcebergUpdateBatch {
    pub chunk: StreamChunk,
    pub next_position: u64,
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
#[try_stream(boxed, ok = IcebergUpdateBatch, error = anyhow::Error)]
pub async fn read_file_updates(
    table: Table,
    delete_reader: &mut PositionDeleteReader,
    mut task: FileScanTask,
    contract: IcebergSourceContract,
    mode: IcebergUpdateReadMode,
    chunk_size: usize,
    resume_position: u64,
) {
    ensure!(chunk_size > 0, "update reader batch size must be positive");
    ensure!(
        task.data_file_format == DataFileFormat::Parquet,
        "update reader requires Parquet data files"
    );
    ensure!(
        task.key_metadata.is_none(),
        "encrypted data files are not supported by the update reader"
    );
    ensure!(
        task.start == 0 && task.length == task.file_size_in_bytes && task.predicate.is_none(),
        "update reader requires a whole-file task without predicate pushdown"
    );
    let record_count = task
        .record_count
        .context("update task is missing data record_count")?;
    ensure!(
        record_count <= i64::MAX as u64 && resume_position <= record_count,
        "invalid update task position"
    );
    contract.validate_projection(&task.schema, &task.project_field_ids)?;

    let current = delete_reader
        .read_file_scoped(&task.data_file_path, record_count, &task.deletes)
        .await?;
    let (op, positions) = match mode {
        IcebergUpdateReadMode::Insert => (Op::Insert, current),
        IcebergUpdateReadMode::Delete { parent_deletes } => {
            let parent = delete_reader
                .read_file_scoped(&task.data_file_path, record_count, &parent_deletes)
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
            "unexpected update reader output schema"
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
        yield IcebergUpdateBatch {
            chunk: StreamChunk::from_parts(vec![op; chunk.capacity()], chunk),
            next_position,
        };
    }
    ensure!(
        next_position == record_count,
        "data row count differs from the update task"
    );
}

#[cfg(test)]
#[path = "update_reader_test.rs"]
mod tests;
