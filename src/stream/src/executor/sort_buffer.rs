// Copyright 2023 RisingWave Labs
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
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, AscentOwnedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{ScalarImpl, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_storage::StateStore;

use super::{Barrier, PkIndices, StreamExecutorResult};
use crate::common::table::state_table::StateTable;

/// [`SortBufferKey`] contains a record's timestamp and pk.
type SortBufferKey = (ScalarImpl, AscentOwnedRow);

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

    last_output: Option<ScalarImpl>,

    watermark: Option<ScalarImpl>,

    buffer: BTreeMap<SortBufferKey, SortBufferValue>,
}

impl<S: StateStore> SortBuffer<S> {
    /// Update the watermark value.
    pub fn set_watermark(&mut self, watermark: ScalarImpl) {
        assert!(
            {
                if let Some(ref last_watermark) = self.watermark {
                    &watermark >= last_watermark
                } else {
                    true
                }
            },
            "watermark should always increase"
        );
        self.watermark = Some(watermark);
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
        let streams = stream::iter(vnodes.iter_vnodes())
            .map(|vnode| state_table.iter_with_pk_range(&pk_range, vnode))
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(Box::pin);

        #[for_await]
        for row in select_all(streams) {
            let row = row?.to_owned_row();
            let timestamp_datum = row.datum_at(sort_column_index).to_owned_datum().unwrap();
            let pk = (&row).project(&pk_indices).into_owned_row().into();
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
            last_output: None,
            watermark: None,
            buffer,
        })
    }

    /// Store all rows in one [`StreamChunk`] to the buffer.
    /// TODO: Only insertions are supported now.
    pub fn insert(&mut self, chunk: &StreamChunk) {
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
            let pk = row_ref.project(&self.pk_indices).into_owned_row().into();
            let row = row_ref.into_owned_row();
            self.buffer.insert((timestamp_datum, pk), (row, false));
        }
    }

    /// Output a stream that is ordered by the sort key.
    ///
    /// The output stream will not output the same record except during failover and recovery.
    /// However, if you want to delete one record permanently, you should call
    /// [`SortBuffer::delete`] manually.
    pub fn consume(&mut self) -> impl Iterator<Item = DataChunk> + '_ {
        if let Some(watermark) = self.watermark.clone() {
            let last_output = self.last_output.replace(watermark);
            let g = move || {
                let watermark = self.watermark.as_ref().unwrap();
                let mut data_chunk_builder =
                    DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
                // Only records with timestamp greater than the last watermark will be output, so
                // records will only be emitted exactly once unless recovery.
                let start_bound = if let Some(last_output) = last_output.clone() {
                    Bound::Excluded((
                        // TODO: unsupported type or watermark overflow. Do we have better ways
                        // instead of unwrap?
                        last_output.successor().unwrap(),
                        OwnedRow::empty().into(),
                    ))
                } else {
                    Bound::Unbounded
                };
                let end_bound =
                    Bound::Excluded(((watermark.successor().unwrap()), OwnedRow::empty().into()));

                for (_, (row, _)) in self.buffer.range((start_bound, end_bound)) {
                    if let Some(data_chunk) = data_chunk_builder.append_one_row(row) {
                        // When the chunk size reaches its maximum, we construct a data chunk and
                        // send it to downstream.
                        yield data_chunk;
                    }
                }

                // Construct and send a data chunk message. Rows in this message are
                // always ordered by timestamp.
                if let Some(data_chunk) = data_chunk_builder.consume_all() {
                    yield data_chunk;
                }
            };
            Some(std::iter::from_generator(g))
        } else {
            None
        }
        .into_iter()
        .flatten()
    }

    /// Delete a record from the buffer.
    /// TODO: `delete` is always be called in order per group, so should we support `range_delete`
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
        if let Some((_, (row, persisted))) = self.buffer.remove_entry(&(timestamp_datum, pk.into()))
        {
            if persisted {
                self.state_table.delete(&row);
            }
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
    use crate::executor::{Barrier, PkIndices};

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

        let row_pretty = |s: &str| OwnedRow::from_pretty_with_tys(&tys, s);

        let order_types = vec![OrderType::Ascending];
        let mut state_table = StateTable::new_without_distribution(
            state_store.clone(),
            table_id,
            column_descs.clone(),
            order_types.clone(),
            pk_indices.clone(),
        )
        .await;
        state_table.init_epoch(EpochPair::new_test_epoch(2));
        let mut sort_buffer = SortBuffer::recover(
            schema.clone(),
            pk_indices.clone(),
            sort_column_index,
            state_table,
        )
        .await
        .unwrap();

        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        );
        let watermark1 = 3i64.into();
        let chunk2 = StreamChunk::from_pretty(
            " I  I
            + 98 4
            + 37 5
            + 60 8
            + 13 9",
        );
        let watermark2 = 7i64.into();
        let barrier1 = Barrier::new_test_barrier(3);
        sort_buffer.insert(&chunk1);
        sort_buffer.set_watermark(watermark1);
        let output = sort_buffer.consume();
        let rows1 = chunks_to_rows(output);
        assert_eq!(rows1, vec![row_pretty("1 1"), row_pretty("2 2")]);
        sort_buffer.insert(&chunk2);
        sort_buffer.set_watermark(watermark2);
        let output = sort_buffer.consume();
        let rows2 = chunks_to_rows(output);
        assert_eq!(
            rows2,
            vec![
                row_pretty("98 4"),
                row_pretty("37 5"),
                row_pretty("3 6"),
                row_pretty("4 7"),
            ]
        );

        // Rows that shouldn't be re-emitted after recovery.
        sort_buffer.delete(&rows1[0]);
        sort_buffer.delete(&rows1[1]);

        sort_buffer.handle_barrier(&barrier1).await.unwrap();

        // Rows that should be re-emitted after recovery.
        sort_buffer.delete(&rows2[0]);
        sort_buffer.delete(&rows2[1]);

        // Failover and recover
        drop(sort_buffer);

        let mut state_table = StateTable::new_without_distribution(
            state_store.clone(),
            table_id,
            column_descs,
            order_types,
            pk_indices.clone(),
        )
        .await;

        state_table.init_epoch(EpochPair::new_test_epoch(3));
        let mut sort_buffer =
            SortBuffer::recover(schema, pk_indices.clone(), sort_column_index, state_table)
                .await
                .unwrap();

        let watermark3 = 8i64.into();

        sort_buffer.set_watermark(watermark3);
        let output = sort_buffer.consume();
        let rows3 = chunks_to_rows(output);
        assert_eq!(
            rows3,
            vec![
                row_pretty("98 4"),
                row_pretty("37 5"),
                row_pretty("3 6"),
                row_pretty("4 7"),
                row_pretty("60 8"),
            ]
        );

        let watermark4 = 9i64.into();
        sort_buffer.set_watermark(watermark4);
        let output = sort_buffer.consume();
        let rows4 = chunks_to_rows(output);
        assert_eq!(rows4, vec![row_pretty("13 9")]);
    }
}
