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

// The reader is introduced before planner/executor plumbing so it can be reviewed independently.
#![allow(dead_code)]

use std::ops::Bound;

use futures::{Stream, pin_mut};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::ColumnId;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_common::{bail, row};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;

use super::diff::{SnapshotReadOutput, diff_ordered_row_streams};
use super::external::ExternalStorageTable;
use super::snapshot::{
    SnapshotReadArgs, SplitSnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::compute_bounds;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

pub struct ResnapshotDiffRead<S: StateStore> {
    table: BatchTable<S>,
    read_epoch: u64,
}

impl<S: StateStore> ResnapshotDiffRead<S> {
    pub fn new(
        state_store: S,
        table_desc: &StorageTableDesc,
        output_column_ids: &[ColumnId],
        read_epoch: u64,
    ) -> Self {
        let vnodes = Some(Bitmap::ones(table_desc.vnode_count()).into());
        Self {
            table: BatchTable::new_partial(
                state_store,
                output_column_ids.to_vec(),
                vnodes,
                table_desc,
            ),
            read_epoch,
        }
    }

    pub fn snapshot_read_full_table_diff<'a>(
        &'a self,
        upstream_table_reader: &'a UpstreamTableReader<ExternalStorageTable>,
        read_args: SnapshotReadArgs,
        batch_size: u32,
        pk_order: Vec<OrderType>,
        compare_indices: Vec<usize>,
        data_types: Vec<DataType>,
        chunk_size: usize,
    ) -> impl Stream<Item = StreamExecutorResult<SnapshotReadOutput>> + Send + 'a {
        let pk_indices = read_args.pk_indices.clone();
        let left = snapshot_read_rows(
            upstream_table_reader.snapshot_read_full_table(read_args.clone(), batch_size),
        );
        let right = self.storage_table_read(read_args);
        diff_ordered_row_streams(
            left,
            right,
            pk_indices,
            pk_order,
            compare_indices,
            data_types,
            chunk_size,
        )
    }

    pub fn snapshot_read_table_split_diff<'a>(
        &'a self,
        upstream_table_reader: &'a UpstreamTableReader<ExternalStorageTable>,
        read_args: SplitSnapshotReadArgs,
        pk_indices: Vec<usize>,
        pk_order: Vec<OrderType>,
        compare_indices: Vec<usize>,
        data_types: Vec<DataType>,
        chunk_size: usize,
        split_pk_column_index: usize,
    ) -> impl Stream<Item = StreamExecutorResult<SnapshotReadOutput>> + Send + 'a {
        let left =
            snapshot_read_rows(upstream_table_reader.snapshot_read_table_split(read_args.clone()));
        let right = self.storage_table_split_read(read_args, split_pk_column_index);
        diff_ordered_row_streams(
            left,
            right,
            pk_indices,
            pk_order,
            compare_indices,
            data_types,
            chunk_size,
        )
    }

    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    async fn storage_table_read(&self, read_args: SnapshotReadArgs) {
        let table = &self.table;
        let range_bounds = match compute_bounds(table.pk_indices(), read_args.current_pos) {
            Some(range_bounds) => range_bounds,
            None => return Ok(()),
        };
        let row_iter = table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::Committed(self.read_epoch),
                row::empty(),
                range_bounds,
                true,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;

        #[for_await]
        for row in row_iter {
            yield row?;
        }
    }

    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    async fn storage_table_split_read(
        &self,
        read_args: SplitSnapshotReadArgs,
        split_pk_column_index: usize,
    ) {
        let table = &self.table;
        let Some(pk_in_output_indices) = table.pk_in_output_indices() else {
            bail!("CDC snapshot diff output projection must contain every primary-key column");
        };
        let Some(&split_output_column_index) = pk_in_output_indices.get(split_pk_column_index)
        else {
            bail!(
                "CDC snapshot split primary-key index {split_pk_column_index} is out of bounds for \
                 {} storage primary-key columns",
                pk_in_output_indices.len()
            );
        };
        let range_bounds = if split_pk_column_index == 0 {
            split_pk_range_bounds(
                &read_args.left_bound_inclusive,
                &read_args.right_bound_exclusive,
            )?
        } else {
            // A non-prefix primary-key component cannot be represented as a contiguous storage
            // range. Scan the committed table and apply the same split predicate as the executor.
            (Bound::Unbounded, Bound::Unbounded)
        };
        let row_iter = table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::Committed(self.read_epoch),
                row::empty(),
                range_bounds,
                true,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;

        #[for_await]
        for row in row_iter {
            let row = row?;
            if split_pk_column_index != 0
                && !row_matches_split(
                    &row,
                    split_output_column_index,
                    &read_args.left_bound_inclusive,
                    &read_args.right_bound_exclusive,
                )?
            {
                continue;
            }
            yield row;
        }
    }
}

#[try_stream(ok = OwnedRow, error = StreamExecutorError)]
async fn snapshot_read_rows(
    input: impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send,
) {
    pin_mut!(input);
    #[for_await]
    for output in input {
        match output? {
            Some(chunk) => {
                for (_, row) in chunk.rows() {
                    yield row.to_owned_row();
                }
            }
            None => break,
        }
    }
}

fn split_pk_range_bounds(
    left: &OwnedRow,
    right: &OwnedRow,
) -> StreamExecutorResult<(Bound<OwnedRow>, Bound<OwnedRow>)> {
    if left.len() != 1 || right.len() != 1 {
        bail!("CDC resnapshot diff backfill only supports a single split column");
    }
    let start = if is_unbounded_split_bound(left) {
        Bound::Unbounded
    } else {
        Bound::Included(left.clone())
    };
    let end = if is_unbounded_split_bound(right) {
        Bound::Unbounded
    } else {
        Bound::Excluded(right.clone())
    };
    Ok((start, end))
}

fn is_unbounded_split_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

fn row_matches_split(
    row: &OwnedRow,
    split_output_column_index: usize,
    left: &OwnedRow,
    right: &OwnedRow,
) -> StreamExecutorResult<bool> {
    if left.len() != 1 || right.len() != 1 {
        bail!("CDC snapshot diff backfill only supports a single split column");
    }
    let datum = row.datum_at(split_output_column_index);
    let after_left = is_unbounded_split_bound(left)
        || cmp_datum(datum, left.datum_at(0), OrderType::ascending_nulls_first()).is_ge();
    let before_right = is_unbounded_split_bound(right)
        || cmp_datum(datum, right.datum_at(0), OrderType::ascending_nulls_first()).is_lt();
    Ok(after_left && before_right)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_pb::plan_common::StorageTableDesc;
    use risingwave_storage::memory::MemoryStateStore;

    use super::{ResnapshotDiffRead, row_matches_split};

    #[test]
    fn resnapshot_projects_only_cdc_scan_output_columns() {
        let columns = vec![
            ColumnDesc::named("id", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("payload", ColumnId::new(2), DataType::Varchar),
            ColumnDesc::named("generated", ColumnId::new(3), DataType::Boolean),
        ];
        let table_desc = StorageTableDesc {
            table_id: 1.into(),
            columns: columns.iter().map(ColumnDesc::to_protobuf).collect(),
            pk: vec![ColumnOrder::new(0, OrderType::ascending()).to_protobuf()],
            dist_key_in_pk_indices: vec![],
            value_indices: vec![0, 1, 2],
            read_prefix_len_hint: 0,
            versioned: false,
            stream_key: vec![0],
            vnode_col_idx_in_pk: None,
            retention_seconds: None,
            maybe_vnode_count: Some(1),
        };

        let read = ResnapshotDiffRead::new(
            MemoryStateStore::new(),
            &table_desc,
            &[ColumnId::new(1), ColumnId::new(2)],
            0,
        );
        assert_eq!(
            read.table.schema().data_types(),
            vec![DataType::Int32, DataType::Varchar]
        );
    }

    #[test]
    fn filters_storage_by_non_prefix_split_key() {
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(100)),
            Some(ScalarImpl::Int32(7)),
        ]);
        assert!(
            row_matches_split(
                &row,
                1,
                &OwnedRow::new(vec![Some(ScalarImpl::Int32(5))]),
                &OwnedRow::new(vec![Some(ScalarImpl::Int32(10))]),
            )
            .unwrap()
        );
        assert!(
            !row_matches_split(
                &row,
                1,
                &OwnedRow::new(vec![Some(ScalarImpl::Int32(7))]),
                &OwnedRow::new(vec![Some(ScalarImpl::Int32(7))]),
            )
            .unwrap()
        );
        assert!(
            row_matches_split(
                &row,
                1,
                &OwnedRow::new(vec![None]),
                &OwnedRow::new(vec![None]),
            )
            .unwrap()
        );
    }
}
