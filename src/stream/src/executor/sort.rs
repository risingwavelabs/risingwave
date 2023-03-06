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

use std::collections::BTreeMap;
use std::ops::Bound;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, AscentOwnedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{ScalarImpl, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::select_all;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    PkIndices, StreamExecutorResult, Watermark,
};
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

/// [`SortExecutor`] consumes unordered input data and outputs ordered data to downstream.
pub struct SortExecutor<S: StateStore> {
    context: ActorContextRef,

    /// We make it `Option` here due to lifetime restrictions. It will be taken (`Option.take()`)
    /// after executing.
    input: Option<BoxedExecutor>,

    pk_indices: PkIndices,

    identity: String,

    schema: Schema,

    state_table: StateTable<S>,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// The index of the column on which the sort executor sorts data.
    sort_column_index: usize,

    /// Stores data in memory ordered by the column indexed by `sort_column_index`. Once a
    /// watermark of `sort_column_index` arrives, data below watermark (i.e. value of that column
    /// being less than the watermark) should be sent to downstream and cleared from the buffer.
    buffer: BTreeMap<SortBufferKey, SortBufferValue>,

    /// The last received watermark. `None` on initialization. Used for range delete.
    _prev_watermark: Option<ScalarImpl>,
}

impl<S: StateStore> SortExecutor<S> {
    pub fn new(
        context: ActorContextRef,
        input: BoxedExecutor,
        pk_indices: PkIndices,
        executor_id: u64,
        state_table: StateTable<S>,
        chunk_size: usize,
        sort_column_index: usize,
    ) -> Self {
        let schema = input.schema().clone();
        Self {
            context,
            input: Some(input),
            pk_indices,
            identity: format!("SortExecutor {:X}", executor_id),
            schema,
            state_table,
            chunk_size,
            sort_column_index,
            buffer: BTreeMap::new(),
            _prev_watermark: None,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);

        // Consume the first barrier message and initialize state table.
        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        // Fill the buffer on initialization. If the executor has recovered from a failed state, the
        // buffer should be refilled by its previous data.
        let vnode_bitmap = self.state_table.vnode_bitmap().to_owned();
        self.fill_buffer(None, &vnode_bitmap).await?;

        #[for_await]
        for msg in input {
            match msg? {
                // Sort executor only sends a stream chunk to downstream when
                // `self.sort_column_index` matches the watermark's column index.
                Message::Watermark(watermark @ Watermark { col_idx, .. })
                    if col_idx == self.sort_column_index =>
                {
                    let watermark_value = watermark.val.clone();
                    // Find out the records to send to downstream.
                    while let Some(entry) = self.buffer.first_entry() {
                        // Only when a record's timestamp is prior to the watermark should it be
                        // sent to downstream.
                        if entry.key().0 < watermark_value {
                            // Remove the record from memory.
                            let (row, persisted) = entry.remove();
                            // Remove the record from state store. It is possible that a record
                            // is not present in state store because this watermark arrives
                            // before a barrier since last watermark.
                            // TODO: Use range delete instead.
                            if persisted {
                                self.state_table.delete(&row);
                            }
                            // Add the record to stream chunk data. Note that we retrieve the
                            // record from a BTreeMap, so data in this chunk should be ordered
                            // by timestamp and pk.
                            if let Some(data_chunk) = data_chunk_builder.append_one_row(row) {
                                // When the chunk size reaches its maximum, we construct a
                                // stream chunk and send it to downstream.
                                let ops = vec![Op::Insert; data_chunk.capacity()];
                                let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
                                yield Message::Chunk(stream_chunk);
                            }
                        } else {
                            // We have collected all data below watermark.
                            break;
                        }
                    }

                    // Construct and send a stream chunk message. Rows in this message are
                    // always ordered by timestamp.
                    if let Some(data_chunk) = data_chunk_builder.consume_all() {
                        let ops = vec![Op::Insert; data_chunk.capacity()];
                        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
                        yield Message::Chunk(stream_chunk);
                    }

                    // Update previous watermark, which is used for range delete.
                    self._prev_watermark = Some(watermark_value);

                    // Forward the watermark message.
                    yield Message::Watermark(watermark);
                }
                // Otherwise, it just forwards the watermark message to downstream without sending a
                // stream chunk message.
                Message::Watermark(w) => yield Message::Watermark(w),
                Message::Chunk(chunk) => {
                    for (op, row_ref) in chunk.rows() {
                        match op {
                            Op::Insert => {
                                // For insert operation, we buffer the record in memory.
                                let timestamp_datum = row_ref.datum_at(self.sort_column_index).to_owned_datum().unwrap();
                                let pk = row_ref.project(&self.pk_indices).into_owned_row();
                                let row = row_ref.into_owned_row();
                                // Null event time should not exist in the row since the `WatermarkFilter`
                                // before the `Sort` will filter out the Null event time.
                                self.buffer.insert((timestamp_datum, pk.into()), (row, false));
                            },
                            // Other operations are not supported currently.
                            _ => unimplemented!("operations other than insert currently are not supported by sort executor")
                        }
                    }
                }

                Message::Barrier(barrier) => {
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

                    // Update the vnode bitmap for the state table if asked. Also update the buffer.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.context.id) {
                        let prev_vnode_bitmap =
                            self.state_table.update_vnode_bitmap(vnode_bitmap.clone());
                        self.fill_buffer(Some(&prev_vnode_bitmap), &vnode_bitmap)
                            .await?;
                    }

                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    /// Fill the buffer when the sort executor initializes or scales. On initialization (including
    /// recovering), `prev_vnode_bitmap` will be `None`. On scaling, `prev_vnode_bitmap` should be
    /// set as the previous vnode bitmap to perform buffer update. Note that we do not assume
    /// set relations between `prev_vnode_bitmap` and `curr_vnode_bitmap` when scaling occurs. That
    /// is to say, `prev_vnode_bitmap` does not necessarily contain `curr_vnode_bitmap` on scaling
    /// out, and vice versa. Therefore, we always check vnodes that are no longer owned and newly
    /// owned by the sort executor regardless of the scaling type (scale in or scale out).
    async fn fill_buffer(
        &mut self,
        prev_vnode_bitmap: Option<&Bitmap>,
        curr_vnode_bitmap: &Bitmap,
    ) -> StreamExecutorResult<()> {
        // When scaling occurs, we remove data with vnodes that are no longer owned by this executor
        // from buffer.
        if let Some(prev_vnode_bitmap) = prev_vnode_bitmap {
            let no_longer_owned_vnodes =
                Bitmap::bit_saturate_subtract(prev_vnode_bitmap, curr_vnode_bitmap);
            self.buffer.retain(|(_, pk), _| {
                let vnode = self.state_table.compute_vnode(pk);
                !no_longer_owned_vnodes.is_set(vnode.to_index())
            });
        }

        // Read data with vnodes that are newly owned by this executor from state store. This is
        // performed both on initialization and on scaling.
        let newly_owned_vnodes = if let Some(prev_vnode_bitmap) = prev_vnode_bitmap {
            Bitmap::bit_saturate_subtract(curr_vnode_bitmap, prev_vnode_bitmap)
        } else {
            curr_vnode_bitmap.to_owned()
        };
        let mut values_per_vnode = Vec::new();
        for owned_vnode in newly_owned_vnodes.iter_vnodes() {
            let value_iter = self
                .state_table
                .iter_with_pk_range(
                    &(
                        Bound::<row::Empty>::Unbounded,
                        Bound::<row::Empty>::Unbounded,
                    ),
                    owned_vnode,
                    PrefetchOptions { exhaust_iter: true },
                )
                .await?;
            let value_iter = Box::pin(value_iter);
            values_per_vnode.push(value_iter);
        }
        if !values_per_vnode.is_empty() {
            let mut stream = select_all(values_per_vnode);
            while let Some(storage_result) = stream.next().await {
                // Insert the data into buffer.
                let row: OwnedRow = storage_result?;
                let timestamp_datum = row
                    .datum_at(self.sort_column_index)
                    .to_owned_datum()
                    .unwrap();
                let pk = (&row).project(&self.pk_indices).into_owned_row();
                // Null event time should not exist in the row since the `WatermarkFilter` before
                // the `Sort` will filter out the Null event time.
                self.buffer
                    .insert((timestamp_datum, pk.into()), (row, true));
            }
        }
        Ok(())
    }
}

impl<S: StateStore> Executor for SortExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::{ActorContext, BoxedMessageStream, Executor};

    #[tokio::test]
    async fn test_sort_executor() {
        let sort_column_index = 1;
        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I
            + 98 4
            + 37 5
            + 60 8",
        );
        let watermark1 = 3_i64;
        let watermark2 = 7_i64;

        let state_table = create_state_table(MemoryStateStore::new()).await;
        let (mut tx, mut sort_executor) = create_executor(sort_column_index, state_table);

        // Init barrier
        tx.push_barrier(1, false);

        // Consume the barrier
        sort_executor.next().await.unwrap().unwrap();

        // Init watermark
        tx.push_int64_watermark(0, 0_i64);
        tx.push_int64_watermark(sort_column_index, 0_i64);

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();
        sort_executor.next().await.unwrap().unwrap();

        // Push data chunk1
        tx.push_chunk(chunk1);

        // Push watermark1 on an irrelevant column
        tx.push_int64_watermark(0, watermark1);

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();

        // Push watermark1 on sorted column
        tx.push_int64_watermark(sort_column_index, watermark1);

        // Consume the data chunk
        let chunk_msg = sort_executor.next().await.unwrap().unwrap();

        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2"
            )
        );

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();

        // Push data chunk2
        tx.push_chunk(chunk2);

        // Push barrier
        tx.push_barrier(2, false);

        // Consume the barrier
        sort_executor.next().await.unwrap().unwrap();

        // Push watermark2 on an irrelevant column
        tx.push_int64_watermark(0, watermark2);

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();

        // Push watermark2 on sorted column
        tx.push_int64_watermark(sort_column_index, watermark2);

        // Consume the data chunk
        let chunk_msg = sort_executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 98 4
                + 37 5
                + 3 6"
            )
        );

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_sort_executor_fail_over() {
        let sort_column_index = 1;
        let chunk = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        );
        let watermark = 3_i64;

        let state_store = MemoryStateStore::new();
        let state_table = create_state_table(state_store.clone()).await;
        let (mut tx, mut sort_executor) = create_executor(sort_column_index, state_table);

        // Init barrier
        tx.push_barrier(1, false);

        // Consume the barrier
        sort_executor.next().await.unwrap().unwrap();

        // Init watermark
        tx.push_int64_watermark(0, 0_i64);
        tx.push_int64_watermark(sort_column_index, 0_i64);

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();
        sort_executor.next().await.unwrap().unwrap();

        // Push data chunk
        tx.push_chunk(chunk);

        // Push barrier
        tx.push_barrier(2, false);

        // Consume the barrier
        sort_executor.next().await.unwrap().unwrap();

        let state_table = create_state_table(state_store.clone()).await;
        // Mock fail over
        let (mut recovered_tx, mut recovered_sort_executor) =
            create_executor(sort_column_index, state_table);

        // Push barrier
        recovered_tx.push_barrier(3, false);

        // Consume the barrier
        recovered_sort_executor.next().await.unwrap().unwrap();

        // Push watermark on sorted column
        recovered_tx.push_int64_watermark(sort_column_index, watermark);

        // Consume the data chunk
        let chunk_msg = recovered_sort_executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2"
            )
        );

        // Consume the watermark
        recovered_sort_executor.next().await.unwrap().unwrap();
    }

    #[inline]
    fn create_pk_indices() -> PkIndices {
        vec![0]
    }

    async fn create_state_table(
        memory_state_store: MemoryStateStore,
    ) -> StateTable<MemoryStateStore> {
        let table_id = TableId::new(1);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];
        let order_types = vec![OrderType::Ascending];
        let pk_indices = create_pk_indices();
        StateTable::new_without_distribution(
            memory_state_store,
            table_id,
            column_descs,
            order_types,
            pk_indices,
        )
        .await
    }

    fn create_executor(
        sort_column_index: usize,
        state_table: StateTable<MemoryStateStore>,
    ) -> (MessageSender, BoxedMessageStream) {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let pk_indices = create_pk_indices();
        let (tx, source) = MockSource::channel(schema, pk_indices.clone());
        let sort_executor = SortExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            pk_indices,
            1,
            state_table,
            1024,
            sort_column_index,
        );
        (tx, Box::new(sort_executor).execute())
    }
}
