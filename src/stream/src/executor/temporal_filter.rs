// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::ops::{Bound, Deref};

use chrono::NaiveDateTime;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{ArrayImpl, DataChunk, Op, Row, StreamChunk};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::must_match;
use risingwave_common::types::{DataType, IntervalUnit, NaiveDateTimeWrapper, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::select_all;
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{BoxedExpression, InputRefExpression, LiteralExpression};
use risingwave_expr::vector_op::arithmetic_op::timestamp_interval_sub;
use risingwave_expr::ExprError;
use risingwave_pb::expr::expr_node::Type;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    PkIndices, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::common::InfallibleExpression;

/// [`TemporalBufferKey`] contains a record's timestamp and pk.
type TemporalBufferKey = (ScalarImpl, Row);

pub struct TemporalFilterExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Option<BoxedExecutor>,
    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
    state_table: StateTable<S>,
    time_col_idx: usize,
    window_size: ScalarImpl,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// Stores data in memory ordered by the column indexed by `time_column_index`.
    buffer: BTreeMap<TemporalBufferKey, Row>,
}

impl<S: StateStore> TemporalFilterExecutor<S> {
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: BoxedExecutor,
        pk_indices: PkIndices,
        executor_id: u64,
        state_table: StateTable<S>,
        time_col_idx: usize,
        window_size: ScalarImpl,
        chunk_size: usize,
    ) -> Self {
        let schema = input.schema().clone();
        Self {
            ctx,
            input: Some(input),
            pk_indices,
            identity: format!("TemporalFilterExecutor {:X}", executor_id),
            schema,
            state_table,
            time_col_idx,
            window_size,
            chunk_size,
            buffer: BTreeMap::new(),
        }
    }

    fn apply_batch(
        &mut self,
        data_chunk: &DataChunk,
        ops: Vec<Op>,
        lower_condition: &BoxedExpression,
    ) -> (Vec<Op>, Bitmap) {
        debug_assert_eq!(ops.len(), data_chunk.capacity());
        let mut new_ops = Vec::with_capacity(ops.len());
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        let mut last_res = false;

        let lower_results_array = lower_condition.eval_infallible(data_chunk, |err| {
            self.ctx.on_compute_error(err, self.identity())
        });
        let lower_results = must_match!(
            lower_results_array.deref(), ArrayImpl::Bool(eval_results) => eval_results);

        for ((row_ref, op), lower_result) in data_chunk
            .rows_with_holes()
            .zip_eq(ops.into_iter())
            .zip_eq(lower_results.to_bitmap().iter())
        {
            if let Some(row_ref) = row_ref {
                let res = lower_result;
                match op {
                    Op::Insert | Op::Delete => {
                        new_ops.push(op);
                        new_visibility.append(res);
                    }
                    Op::UpdateDelete => {
                        last_res = res;
                    }
                    Op::UpdateInsert => {
                        if last_res == res {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                        } else {
                            new_ops.push(Op::Delete);
                            new_ops.push(Op::Insert);
                        }
                        new_visibility.append(last_res);
                        new_visibility.append(res);
                    }
                }
                if lower_result {
                    let row = row_ref.to_owned_row();
                    // Null event time should not exist in the row since it passed the filter.
                    let timestamp = row[self.time_col_idx].as_ref().unwrap().clone();
                    let pk = row.by_indices(self.pk_indices());
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            self.buffer.insert((timestamp, pk), row.clone());
                            self.state_table.insert(row);
                        }
                        Op::Delete | Op::UpdateDelete => {
                            self.buffer.remove(&(timestamp, pk));
                            self.state_table.delete(row);
                        }
                    }
                }
            } else {
                new_ops.push(op);
                new_visibility.append(false);
            }
        }

        let new_visibility = new_visibility.finish();

        (new_ops, new_visibility)
    }

    /// Fill the buffer when the temporal executor initializes or scales. On initialization
    /// (including recovering), `prev_vnode_bitmap` will be `None`. On scaling,
    /// `prev_vnode_bitmap` should be set as the previous vnode bitmap to perform buffer update.
    /// Note that we do not assume set relations between `prev_vnode_bitmap` and
    /// `curr_vnode_bitmap` when scaling occurs. That is to say, `prev_vnode_bitmap` does not
    /// necessarily contain `curr_vnode_bitmap` on scaling out, and vice versa. Therefore, we
    /// always check vnodes that are no longer owned and newly owned by the temporal executor
    /// regardless of the scaling type (scale in or scale out).
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
                !no_longer_owned_vnodes.is_set(vnode as _)
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
        for owned_vnode in newly_owned_vnodes.ones() {
            let value_iter = self
                .state_table
                .iter_with_pk_range(&(Bound::Unbounded, Bound::Unbounded), owned_vnode as _)
                .await?;
            let value_iter = Box::pin(value_iter);
            values_per_vnode.push(value_iter);
        }
        if !values_per_vnode.is_empty() {
            let mut stream = select_all(values_per_vnode);
            while let Some(storage_result) = stream.next().await {
                // Insert the data into buffer.
                let row = storage_result?.into_owned();
                let timestamp = row[self.time_col_idx].as_ref().unwrap().clone();
                let pk = row.by_indices(&self.pk_indices);
                self.buffer.insert((timestamp, pk), row);
            }
        }
        Ok(())
    }

    fn sync(
        &mut self,
        lower_condition: &BoxedExpression,
    ) -> StreamExecutorResult<Vec<StreamChunk>> {
        let identity = self.identity.clone();
        let mut ret = vec![];
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.schema().data_types(), self.chunk_size);
        while let Some(entry) = self.buffer.first_entry() {
            if lower_condition
                .eval_row_infallible(entry.get(), |err| self.ctx.on_compute_error(err, &identity))
                .map_or(false, |scalar| *scalar.as_bool())
            {
                break;
            } else {
                // Remove the record from buffer.
                let row = entry.remove();

                // If there is higher condition, we need to check existence before yield.
                if let Some(data_chunk) =
                    data_chunk_builder.append_one_row_from_datums(row.values())
                {
                    // When the chunk size reaches its maximum, we construct a
                    // stream chunk
                    let ops = vec![Op::Delete; data_chunk.capacity()];
                    ret.push(StreamChunk::from_parts(ops, data_chunk));
                }

                self.state_table.delete(row);
            }
        }
        if let Some(data_chunk) = data_chunk_builder.consume_all() {
            let ops = vec![Op::Delete; data_chunk.capacity()];
            ret.push(StreamChunk::from_parts(ops, data_chunk));
        }
        Ok(ret)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let time_col_data_type = self.schema().fields()[self.time_col_idx].data_type();
        let window_size = *self.window_size.as_interval();
        let dynamic_cond = move |time_millis: u64| -> Result<BoxedExpression, ExprError> {
            new_binary_expr(
                Type::GreaterThanOrEqual,
                DataType::Boolean,
                Box::new(InputRefExpression::new(
                    time_col_data_type.clone(),
                    self.time_col_idx,
                )),
                Box::new(LiteralExpression::new(
                    DataType::Timestamp,
                    Some(ScalarImpl::NaiveDateTime(timestamp_interval_sub::<
                        NaiveDateTimeWrapper,
                        IntervalUnit,
                        NaiveDateTimeWrapper,
                    >(
                        NaiveDateTimeWrapper::new(NaiveDateTime::from_timestamp(
                            (time_millis / 1000) as i64,
                            (time_millis % 1000 * 1_000_000) as u32,
                        )),
                        window_size,
                    )?)),
                )),
            )
        };

        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier message and initialize state table.
        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);
        let mut time_millis = Epoch::from(barrier.epoch.curr).as_unix_millis();
        let mut sync_expr = None;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        // Fill the buffer on initialization. If the executor has recovered from a failed state, the
        // buffer should be refilled by its previous data.
        let vnode_bitmap = self.state_table.vnode_bitmap().to_owned();
        self.fill_buffer(None, &vnode_bitmap).await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(_) => {
                    todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                }
                Message::Chunk(chunk) => {
                    if sync_expr.is_none() {
                        sync_expr = Some(dynamic_cond(time_millis)?);
                        for stream_chunk in self.sync(sync_expr.as_ref().unwrap())? {
                            yield Message::Chunk(stream_chunk);
                        }
                    }

                    let (data_chunk, ops) = chunk.into_parts();
                    let (new_ops, new_visibility) =
                        self.apply_batch(&data_chunk, ops, sync_expr.as_ref().unwrap());
                    if new_visibility.num_high_bits() > 0 {
                        let (columns, _) = data_chunk.into_parts();
                        let new_chunk = StreamChunk::new(new_ops, columns, Some(new_visibility));
                        yield Message::Chunk(new_chunk);
                    }
                }
                Message::Barrier(barrier) => {
                    time_millis = Epoch::from(barrier.epoch.curr).as_unix_millis();
                    if barrier.is_update() {
                        sync_expr = None;
                    } else {
                        sync_expr = Some(dynamic_cond(time_millis)?);
                        for stream_chunk in self.sync(sync_expr.as_ref().unwrap())? {
                            yield Message::Chunk(stream_chunk);
                        }
                    }
                    self.state_table.commit(barrier.epoch).await?;
                    // Update the vnode bitmap for the state table if asked. Also update the buffer.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
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
}

impl<S: StateStore> Executor for TemporalFilterExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;

    use super::TemporalFilterExecutor;
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::{ActorContext, BoxedMessageStream, Executor, PkIndices};

    #[tokio::test]
    async fn test_temporal_filter() {
        let time_column_index = 1;

        let chunk = StreamChunk::from_pretty(
            " I TS
            + 1 2015-09-18T23:56:04
            + 2 2021-03-31T00:00:00",
        );

        let state_table = create_state_table().await;
        let (mut tx, mut temporal_filter_executor) =
            create_executor(time_column_index, state_table);

        // Init barrier
        tx.push_barrier(1, false, None);

        // Consume the barrier
        temporal_filter_executor.next().await.unwrap().unwrap();

        // Push data chunk
        tx.push_chunk(chunk);

        // Consume the data chunk
        let chunk_msg = temporal_filter_executor.next().await.unwrap().unwrap();

        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I TS
                + 2 2021-03-31T00:00:00"
            )
        );

        // Init barrier
        tx.push_barrier(1 << 16, false, Some(1));

        // Consume the data chunk
        let chunk_msg = temporal_filter_executor.next().await.unwrap().unwrap();

        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I TS
                - 2 2021-03-31T00:00:00"
            )
        );

        // Consume the barrier
        temporal_filter_executor.next().await.unwrap().unwrap();
    }

    #[inline]
    fn create_pk_indices() -> PkIndices {
        vec![0]
    }

    async fn create_state_table() -> StateTable<MemoryStateStore> {
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Timestamp),
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
        time_column_index: usize,
        state_table: StateTable<MemoryStateStore>,
    ) -> (MessageSender, BoxedMessageStream) {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Timestamp),
        ]);
        let pk_indices = create_pk_indices();
        let (tx, source) = MockSource::channel(schema, pk_indices.clone());
        let temporal_filter_executor = TemporalFilterExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            pk_indices,
            1,
            state_table,
            time_column_index,
            ScalarImpl::Interval(IntervalUnit::new(0, 1, 0)),
            1024,
        );
        (tx, Box::new(temporal_filter_executor).execute())
    }
}
