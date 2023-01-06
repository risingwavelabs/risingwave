// Copyright 2023 Singularity Data
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
#![cfg_attr(not(test), expect(dead_code))]

use std::collections::BTreeMap;
use std::ops::Bound;

use futures::stream::select_all;
use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::for_await;
use gen_iter::GenIter;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{ScalarImpl, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_storage::StateStore;

use super::{Barrier, PkIndices, StreamExecutorResult, Watermark};
use crate::common::table::state_table::StateTable;

/// [`SortBufferKey`] contains a record's timestamp and pk.
type SortBufferKey = (ScalarImpl, OwnedRow);

/// [`SortBufferValue`] contains a record's value and a flag indicating whether the record has been
/// persisted to storage.
/// NOTE: There is an exhausting trade-off for which structure to use for the in-memory buffer. For
/// example, up to 8x memory can be used with [`OwnedRow`] compared to the `CompactRow`. However, if
/// there are only a few rows that will be temporarily stored in the buffer during an epoch,
/// [`OwnedRow`] will be more efficient instead due to no ser/de needed. So here we could do further
/// optimizations.
type SortBufferValue = (OwnedRow, bool);

/// [`SortBuffer`] is a common component that consume an unordered stream and produce an ordered
/// stream by watermark. This component maintains a state table internally, which schema is same as
/// its input and output. Generally, the component acts as a buffer that output the data it received
/// with a delay.
pub struct SortBuffer<S: StateStore> {
    schema: Schema,

    pk_indices: PkIndices,

    sort_column_index: usize,

    state_table: StateTable<S>,

    chunk_size: usize,

    last_watermark: Option<ScalarImpl>,

    buffer: BTreeMap<SortBufferKey, SortBufferValue>,
}

impl<S: StateStore> SortBuffer<S> {
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        sort_column_index: usize,
        state_table: StateTable<S>,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            sort_column_index,
            state_table,
            chunk_size: 1024,
            last_watermark: None,
            buffer: BTreeMap::new(),
        }
    }

    pub async fn recover(
        schema: Schema,
        pk_indices: PkIndices,
        sort_column_index: usize,
        state_table: StateTable<S>,
    ) -> StreamExecutorResult<Self> {
        let vnodes = state_table.vnode_bitmap().to_owned();
        let mut buffer = BTreeMap::new();

        let pk_range = (
            Bound::<row::Empty>::Unbounded,
            Bound::<row::Empty>::Unbounded,
        );
        let streams = stream::iter(vnodes.iter_ones())
            .map(|vnode| {
                let vnode = VirtualNode::from_index(vnode);
                state_table.iter_with_pk_range(&pk_range, vnode)
            })
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(Box::pin);

        #[for_await]
        for row in select_all(streams) {
            let row = row?.to_owned_row();
            let timestamp_datum = row.datum_at(sort_column_index).to_owned_datum().unwrap();
            let pk = (&row).project(&pk_indices).into_owned_row();
            // Null event time should not exist in the row since the `WatermarkFilter` before
            // the `Sort` will filter out the Null event time.
            buffer.insert((timestamp_datum, pk), (row, true));
        }

        Ok(Self {
            schema,
            pk_indices,
            sort_column_index,
            state_table,
            chunk_size: 1024,
            last_watermark: None,
            buffer,
        })
    }

    /// Store all rows in one [`StreamChunk`] to the buffer.
    /// TODO: Only insertions are supported now.
    pub fn handle_chunk(&mut self, chunk: &StreamChunk) {
        for (op, row_ref) in chunk.rows() {
            assert_eq!(
                op,
                Op::Insert,
                "operations other than insert currently are not supported by sort executor"
            );
            let timestamp_datum = row_ref
                .datum_at(self.sort_column_index)
                .to_owned_datum()
                .unwrap();
            let pk = row_ref.project(&self.pk_indices).into_owned_row();
            let row = row_ref.into_owned_row();
            self.buffer.insert((timestamp_datum, pk), (row, false));
        }
    }

    /// Handle the watermark and output a stream that is ordered by the sort key.
    pub fn handle_watermark<'a, 'b: 'a>(
        &'a mut self,
        watermark: &'b Watermark,
    ) -> impl Iterator<Item = DataChunk> + 'a {
        let Watermark {
            col_idx,
            data_type: _,
            val,
        } = watermark;
        let last_watermark = self.last_watermark.replace(val.clone());
        let g = move || {
            if *col_idx != self.sort_column_index {
                return;
            }
            let mut data_chunk_builder =
                DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
            let watermark_value = val;
            // Only records with timestamp greater than the last watermark will be output, so
            // records will only be emitted exactly once unless recovery.
            let start_bound = if let Some(last_watermark) = last_watermark {
                Bound::Excluded((last_watermark, OwnedRow::empty()))
            } else {
                Bound::Unbounded
            };
            // TODO: `end_bound` = `Bound::Inclusive((watermark_value + 1, OwnedRow::empty()))`, but
            // it's hard to represent now, so we end the loop by an explicit break.
            let end_bound = Bound::Unbounded;
            for (key, (row, _)) in self.buffer.range((start_bound, end_bound)) {
                // Only when a record's timestamp is prior to the watermark should it be
                // sent to downstream.
                if &key.0 <= watermark_value {
                    // Add the record to stream chunk data. Note that we retrieve the
                    // record from a BTreeMap, so data in this chunk should be ordered
                    // by timestamp and pk.
                    if let Some(data_chunk) = data_chunk_builder.append_one_row(row) {
                        // When the chunk size reaches its maximum, we construct a data chunk and
                        // send it to downstream.
                        yield data_chunk;
                    }
                } else {
                    // We have collected all data below watermark.
                    break;
                }
            }

            // Construct and send a data chunk message. Rows in this message are
            // always ordered by timestamp.
            if let Some(data_chunk) = data_chunk_builder.consume_all() {
                yield data_chunk;
            }
        };
        GenIter::from(g)
    }

    /// Delete a record from the buffer.
    /// TODO: The delete is always be called in order per group, so should we support range_delete
    /// here?
    ///
    /// # Panics
    ///
    /// Panics if the record is not found in the buffer.
    pub fn delete(&mut self, row: impl Row) {
        let timestamp_datum = row
            .datum_at(self.sort_column_index)
            .to_owned_datum()
            .unwrap();
        let pk = row.project(&self.pk_indices).into_owned_row();
        let (_, (row, persisted)) = self.buffer.remove_entry(&(timestamp_datum, pk)).unwrap();
        if persisted {
            self.state_table.delete(&row);
        }
    }

    pub async fn handle_barrier(&mut self, barrier: &Barrier) -> StreamExecutorResult<()> {
        if barrier.checkpoint {
            // If the barrier is a checkpoint, then we should persist all records in
            // buffer that have not been persisted before to state store.
            for (row, persisted) in self.buffer.values_mut() {
                if !*persisted {
                    self.state_table.insert(&*row);
                    // Update `persisted` so if the next barrier arrives before the
                    // next watermark, this record will not be persisted redundantly.
                    *persisted = true;
                }
            }
            // Commit the epoch.
            self.state_table.commit(barrier.epoch).await?;
        } else {
            // If the barrier is not a checkpoint, then there is no actual data to
            // commit. Therefore, we simply update the epoch of state table.
            self.state_table.commit_no_data_expected(barrier.epoch);
        }

        // TODO: Handle mutations.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableId};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::SortBuffer;
    use crate::common::table::state_table::StateTable;
    use crate::executor::{Barrier, PkIndices, Watermark};

    fn chunks_to_rows(chunks: impl Iterator<Item = DataChunk>) -> Vec<OwnedRow> {
        let mut rows = vec![];
        for chunk in chunks {
            for row in chunk.rows() {
                rows.push(row.into_owned_row());
            }
        }
        rows
    }

    #[tokio::test]
    async fn test_basic() {
        let state_store = MemoryStateStore::default();
        let pk_indices: PkIndices = vec![0];
        let sort_column_index = 1;
        let table_id = TableId::new(1);
        let column_descs = vec![
            // Pk
            ColumnDesc::unnamed(0.into(), DataType::Int64),
            // Sk
            ColumnDesc::unnamed(1.into(), DataType::Int64),
        ];
        let fields: Vec<Field> = column_descs.iter().map(Into::into).collect();
        let schema = Schema::new(fields);
        let tys = schema.data_types();
        let order_types = vec![OrderType::Ascending];
        let mut state_table = StateTable::new_without_distribution(
            state_store,
            table_id,
            column_descs,
            order_types,
            pk_indices.clone(),
        )
        .await;
        state_table.init_epoch(EpochPair::new_test_epoch(2));
        let mut sort_buffer =
            SortBuffer::new(schema.clone(), pk_indices, sort_column_index, state_table);

        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        );
        let watermark1 = Watermark::new(1, DataType::Int64, 3i64.into());
        let chunk2 = StreamChunk::from_pretty(
            " I  I
            + 98 4
            + 37 5
            + 60 8",
        );
        let watermark2 = Watermark::new(1, DataType::Int64, 7i64.into());
        let barrier1 = Barrier::new_test_barrier(3);
        sort_buffer.handle_chunk(&chunk1);
        let output = sort_buffer.handle_watermark(&watermark1);
        let rows1 = chunks_to_rows(output);
        assert_eq!(
            rows1,
            vec![
                OwnedRow::from_pretty_with_tys(&tys, "1 1"),
                OwnedRow::from_pretty_with_tys(&tys, "2 2"),
            ]
        );
        sort_buffer.handle_chunk(&chunk2);
        let output = sort_buffer.handle_watermark(&watermark2);
        let rows2 = chunks_to_rows(output);
        assert_eq!(
            rows2,
            vec![
                OwnedRow::from_pretty_with_tys(&tys, "98 4"),
                OwnedRow::from_pretty_with_tys(&tys, "37 5"),
                OwnedRow::from_pretty_with_tys(&tys, "3 6"),
                OwnedRow::from_pretty_with_tys(&tys, "4 7"),
            ]
        );
        sort_buffer.delete(&rows1[0]);
        sort_buffer.delete(&rows1[1]);
        sort_buffer.handle_barrier(&barrier1).await.unwrap();
    }
}
