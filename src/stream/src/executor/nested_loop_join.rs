// Copyright 2024 RisingWave Labs
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
use std::sync::Arc;
use std::time::{Duration, Instant};

use await_tree::InstrumentAwait;
use futures::{pin_mut, stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::EpochPair;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use self::row::row_concat;
use super::join::builder::JoinStreamChunkBuilder;
use super::join::*;
use super::test_utils::prelude::StateTable;
use super::watermark::BufferedWatermarks;
use super::{Executor, ExecutorInfo, Message, StreamExecutorError, StreamExecutorResult};
use crate::executor::barrier_align::{barrier_align, AlignedMessage};
use crate::executor::join::SideType;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier_from_aligned_stream, ActorContextRef, BoxedExecutor, JoinType, Watermark,
};
use crate::task::AtomicU64Ref;

pub struct NestedLoopJoinExecutor<S: StateStore, const T: JoinTypePrimitive> {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Left input executor
    input_l: Option<BoxedExecutor>,
    /// Right input executor (broadcast side)
    input_r: Option<BoxedExecutor>,
    /// The data types of the formed new columns
    actual_output_data_types: Vec<DataType>,
    /// The parameters of the left join executor
    side_l: JoinSide<S>,
    /// The parameters of the right join executor
    side_r: JoinSide<S>,
    /// Optional non-equi join conditions
    cond: Option<NonStrictExpression>,

    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time
    chunk_size: usize,
    /// watermark column index -> `BufferedWatermarks`
    watermark_buffers: BTreeMap<usize, BufferedWatermarks<SideTypePrimitive>>,
}

struct JoinArgs<'a, S: StateStore> {
    ctx: &'a ActorContextRef,
    side_l: &'a mut JoinSide<S>,
    side_r: &'a mut JoinSide<S>,
    actual_output_data_types: &'a [DataType],
    cond: &'a mut Option<NonStrictExpression>,
    chunk: StreamChunk,
    chunk_size: usize,
}

struct JoinSide<S: StateStore> {
    /// State table. Contains the data from upstream.
    state: StateTable<S>,
    /// `Some(true)`: state table is right table and broadcasted singleton and need to write.
    write_singleton: Option<bool>,
    /// The start position for the side in output new columns
    start_pos: usize,
    /// The mapping from input indices of a side to output columes.
    i2o_mapping: Vec<(usize, usize)>,
}

impl<S: StateStore> JoinSide<S> {
    fn init(&mut self, epoch: EpochPair) {
        self.state.init_epoch(epoch);
    }

    /// Update the vnode bitmap and manipulate the cache if necessary.
    fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) -> bool {
        let (_previous_vnode_bitmap, cache_may_stale) =
            self.state.update_vnode_bitmap(vnode_bitmap.clone());

        cache_may_stale
    }

    /// Iter the rows in the table.
    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    async fn rows(&mut self) {
        let range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let streams =
            futures::future::try_join_all(self.state.vnodes().iter_vnodes().map(|vnode| {
                self.state.iter_with_vnode(
                    vnode,
                    range,
                    PrefetchOptions::prefetch_for_large_range_scan(),
                )
            }))
            .await?
            .into_iter()
            .map(Box::pin);

        #[for_await]
        for entry in stream::select_all(streams) {
            let row = entry?;
            yield row.into_owned_row();
        }
    }

    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state.commit(epoch).await?;
        Ok(())
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state.try_flush().await?;
        Ok(())
    }

    /// Insert a row.
    /// Used when the side does not need to update degree.
    #[allow(clippy::unused_async)]
    pub async fn insert_row(&mut self, value: impl Row) -> StreamExecutorResult<()> {
        // Update the flush buffer.
        if matches!(self.write_singleton, Some(true)) {
            self.state.insert(value);
        }
        Ok(())
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self, value: impl Row) -> StreamExecutorResult<()> {
        // If no cache maintained, only update the state table.
        if matches!(self.write_singleton, Some(true)) {
            self.state.delete(value);
        }
        Ok(())
    }
}

impl<S: StateStore, const T: JoinTypePrimitive> NestedLoopJoinExecutor<S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input_l: BoxedExecutor,
        input_r: BoxedExecutor,
        output_indices: Vec<usize>,
        cond: Option<NonStrictExpression>,
        state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        _watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> Self {
        let side_l_column_n = input_l.schema().len();

        let schema_fields = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => input_l.schema().fields.clone(),
            JoinType::RightSemi | JoinType::RightAnti => input_r.schema().fields.clone(),
            _ => [
                input_l.schema().fields.clone(),
                input_r.schema().fields.clone(),
            ]
            .concat(),
        };

        let original_output_data_types = schema_fields
            .iter()
            .map(|field| field.data_type())
            .collect_vec();
        let actual_output_data_types = output_indices
            .iter()
            .map(|&idx| original_output_data_types[idx].clone())
            .collect_vec();

        // Data types of of hash join state.
        let state_all_data_types_l = input_l.schema().data_types();
        let state_all_data_types_r = input_r.schema().data_types();

        let (left_to_output, right_to_output) = 
            JoinStreamChunkBuilder::get_i2o_mapping(&output_indices, state_all_data_types_l.len(), state_all_data_types_r.len())
        ;

        let watermark_buffers = BTreeMap::new();

        let need_write_right_table = state_table_l.vnodes().is_set(0);

        Self {
            ctx: ctx.clone(),
            info,
            input_l: Some(input_l),
            input_r: Some(input_r),
            actual_output_data_types,
            side_l: JoinSide {
                state: state_table_l,
                write_singleton: None,
                i2o_mapping: left_to_output,
                start_pos: 0,
            },
            side_r: JoinSide {
                state: state_table_r,
                write_singleton: Some(need_write_right_table),
                start_pos: side_l_column_n,
                i2o_mapping: right_to_output,
            },
            cond,
            metrics,
            chunk_size,
            watermark_buffers,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let input_l = self.input_l.take().unwrap();
        let input_r = self.input_r.take().unwrap();

        let aligned_stream = barrier_align(
            input_l.execute(),
            input_r.execute(),
            self.ctx.id,
            self.ctx.fragment_id,
            self.metrics.clone(),
        );
        pin_mut!(aligned_stream);

        pin_mut!(aligned_stream);

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        self.side_l.init(barrier.epoch);
        self.side_r.init(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.fragment_id.to_string();

        // initialized some metrics
        let join_actor_input_waiting_duration_ns = self
            .metrics
            .join_actor_input_waiting_duration_ns
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str]);
        let left_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str, "left"]);
        let right_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str, "right"]);

        let barrier_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str, "barrier"]);

        let mut start_time = Instant::now();

        while let Some(msg) = aligned_stream
            .next()
            .instrument_await("nested_loop_join_barrier_align")
            .await
        {
            join_actor_input_waiting_duration_ns.inc_by(start_time.elapsed().as_nanos() as u64);
            match msg? {
                AlignedMessage::WatermarkLeft(watermark) => {
                    for watermark_to_emit in
                        self.handle_watermark(SideType::Left, watermark).await?
                    {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::WatermarkRight(watermark) => {
                    for watermark_to_emit in
                        self.handle_watermark(SideType::Right, watermark).await?
                    {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::Left(chunk) => {
                    let mut left_time = Duration::from_nanos(0);
                    let mut left_start_time = Instant::now();
                    #[for_await]
                    for chunk in Self::eq_join_oneside::<{ SideType::Left }>(JoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        chunk,
                        chunk_size: self.chunk_size,
                    }) {
                        left_time += left_start_time.elapsed();
                        yield Message::Chunk(chunk?);
                        left_start_time = Instant::now();
                    }
                    left_time += left_start_time.elapsed();
                    left_join_match_duration_ns.inc_by(left_time.as_nanos() as u64);
                    self.try_flush_data().await?;
                }
                AlignedMessage::Right(chunk) => {
                    let mut right_time = Duration::from_nanos(0);
                    let mut right_start_time = Instant::now();
                    #[for_await]
                    for chunk in Self::eq_join_oneside::<{ SideType::Right }>(JoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        chunk,
                        chunk_size: self.chunk_size,
                    }) {
                        right_time += right_start_time.elapsed();
                        yield Message::Chunk(chunk?);
                        right_start_time = Instant::now();
                    }
                    right_time += right_start_time.elapsed();
                    right_join_match_duration_ns.inc_by(right_time.as_nanos() as u64);
                    self.try_flush_data().await?;
                }
                AlignedMessage::Barrier(barrier) => {
                    let barrier_start_time = Instant::now();
                    self.flush_data(barrier.epoch).await?;

                    // Update the vnode bitmap for state tables of both sides if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        if self.side_l.update_vnode_bitmap(vnode_bitmap.clone()) {
                            self.watermark_buffers
                                .values_mut()
                                .for_each(|buffers| buffers.clear());
                        }
                        self.side_r.update_vnode_bitmap(vnode_bitmap);
                    }

                    barrier_join_match_duration_ns
                        .inc_by(barrier_start_time.elapsed().as_nanos() as u64);
                    yield Message::Barrier(barrier);
                }
            }
            start_time = Instant::now();
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_oneside<const SIDE: SideTypePrimitive>(args: JoinArgs<'_, S>) {
        let JoinArgs {
            ctx,
            side_l,
            side_r,
            actual_output_data_types,
            cond,
            chunk,
            chunk_size,
            ..
        } = args;

        let (side_update, side_match) = if SIDE == SideType::Left {
            (side_l, side_r)
        } else {
            (side_r, side_l)
        };

        let mut join_chunk_builder = JoinStreamChunkBuilder::new(
            chunk_size,
            actual_output_data_types.to_vec(),
            side_update.i2o_mapping.clone(),
            side_match.i2o_mapping.clone(),
        );


        let join_matched_join_keys = ctx
            .streaming_metrics
            .join_matched_join_keys
            .with_label_values(&[
                &ctx.id.to_string(),
                &ctx.fragment_id.to_string(),
                &side_update.state.table_id().to_string(),
            ]);

        let side_match_start_pos = side_match.start_pos;

        for r in chunk.rows_with_holes() {
            let Some((op, row)) = r else {
                continue;
            };

            let rows = side_match.rows();
            let mut matched_rows_cnt = 0;

            match op {
                Op::Insert | Op::UpdateInsert => {
                    #[for_await]
                    for matched_row in rows {
                        matched_rows_cnt += 1;
                        let matched_row = matched_row?;
                        dbg!(matched_row);
                        // TODO(yuhao-su): We should find a better way to eval the expression
                        // without concat two rows.
                        // if there are non-equi expressions
                        let check_join_condition = if let Some(ref mut cond) = cond {
                            let new_row = row_concat(
                                &row,
                                side_update.start_pos,
                                &matched_row,
                                side_match_start_pos,
                            );

                            cond.eval_row_infallible(&new_row)
                                .await
                                .map(|s| *s.as_bool())
                                .unwrap_or(false)
                        } else {
                            true
                        };
                        dbg!(check_join_condition);
                        if check_join_condition {
                         dbg!("fuck");

                            if let Some(chunk) =
                                join_chunk_builder.append_row(Op::Insert, row, &matched_row)
                            {
                                yield chunk;
                            }
                        }
                    }
                    side_update.insert_row(row).await?;
                }
                Op::Delete | Op::UpdateDelete => {
                    #[for_await]
                    for matched_row in rows {
                        matched_rows_cnt += 1;
                        let matched_row = matched_row?;
                        // TODO(yuhao-su): We should find a better way to eval the expression
                        // without concat two rows.
                        // if there are non-equi expressions
                        let check_join_condition = if let Some(ref mut cond) = cond {
                            let new_row = row_concat(
                                &row,
                                side_update.start_pos,
                                &matched_row,
                                side_match_start_pos,
                            );

                            cond.eval_row_infallible(&new_row)
                                .await
                                .map(|s| *s.as_bool())
                                .unwrap_or(false)
                        } else {
                            true
                        };
                        if check_join_condition {
                            if let Some(chunk) =
                                join_chunk_builder.append_row(Op::Delete, row, &matched_row)
                            {
                                yield chunk;
                            }
                        }
                    }
                    side_update.delete_row(row)?;
                }
            }
            join_matched_join_keys.observe(matched_rows_cnt as _);
        }
        if let Some(chunk) = join_chunk_builder.take() {
            yield chunk;
        }
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        self.side_l.flush(epoch).await?;
        self.side_r.flush(epoch).await?;
        Ok(())
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        self.side_l.try_flush().await?;
        self.side_r.try_flush().await?;
        Ok(())
    }

    #[expect(clippy::unused_async)]
    async fn handle_watermark(
        &mut self,
        _side: SideTypePrimitive,
        _watermark: Watermark,
    ) -> StreamExecutorResult<Vec<Watermark>> {
        // TODO: State cleaning

        // TODO: Select watermarks to yield.
        Ok(vec![])
    }
}

impl<S: StateStore, const T: JoinTypePrimitive> Executor for NestedLoopJoinExecutor<S, T> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[cfg(test)]
mod test{
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::executor::test_utils::*;
    use risingwave_common::{array::StreamChunkTestExt, catalog::{ColumnDesc, ColumnId, Field, TableId}, util::sort_util::OrderType};
    use risingwave_storage::memory::MemoryStateStore;
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::{BoxedMessageStream, ActorContext};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
    ) -> StateTable<MemoryStateStore>  {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        StateTable::new_without_distribution(
            mem_state.clone(),
            TableId::new(table_id),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
        )
        .await

    }
    
    fn create_cond(condition_text: Option<String>) -> NonStrictExpression {
        build_from_pretty(
            condition_text
                .as_deref()
                .unwrap_or("(less_than:boolean $1:int8 $3:int8)"),
        )
    }

    async fn create_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
        condition_text: Option<String>,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel(schema, vec![1]);
        let cond = with_condition.then(|| create_cond(condition_text));

        let mem_state = MemoryStateStore::new();

        let state_l = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            0,
        )
        .await;

        let state_r= create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            2,
        )
        .await;

        let schema: Schema = 
            [source_l.schema().fields(), source_r.schema().fields()]
                .concat()
                .into_iter()
                .collect();
        let schema_len = schema.len();
        let info = ExecutorInfo {
            schema,
            pk_indices: vec![1],
            identity: "NestedLoopJoinExecutor".to_string(),
        };

        let executor = NestedLoopJoinExecutor::<MemoryStateStore, T>::new(
            ActorContext::for_test(123),
            info,
            Box::new(source_l),
            Box::new(source_r),
            (0..schema_len).collect_vec(),
            cond,
            state_l,
            state_r,
            Arc::new(AtomicU64::new(0)),
            Arc::new(StreamingMetrics::unused()),
            1024,
        );
        (tx_l, tx_r, Box::new(executor).execute())
    }

    #[tokio::test]
    async fn test_streaming_nested_loop_inner_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor::<{ JoinType::Inner }>(true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10"
            )
        );

        Ok(())
    }
}