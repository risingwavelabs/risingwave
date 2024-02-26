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
use futures::stream;
use futures::{pin_mut, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;


use multimap::MultiMap;
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

use super::join::*;
use super::test_utils::prelude::StateTable;
use super::watermark::BufferedWatermarks;
use super::{
    Barrier, Executor, ExecutorInfo, Message, StreamExecutorError, StreamExecutorResult
};

use crate::executor::join::SideType;
use super::join::builder::JoinStreamChunkBuilder;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ expect_first_barrier_from_aligned_stream, ActorContextRef, BoxedExecutor, JoinType, Watermark};
use crate::executor::barrier_align::{barrier_align, AlignedMessage};
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

    /// Whether the logic can be optimized for append-only stream
    append_only_optimize: bool,

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
    append_only_optimize: bool,
    chunk_size: usize,
}

struct JoinSide<S: StateStore> {
    /// State table. Contains the data from upstream.
    state: StateTable<S>,
    /// Degree table.
    ///
    /// The degree is generated from the hash join executor.
    /// Each row in `state` has a corresponding degree in `degree state`.
    /// A degree value `d` in for a row means the row has `d` matched row in the other join side.
    ///
    /// It will only be used when needed in a side.
    ///
    /// - Full Outer: both side
    /// - Left Outer/Semi/Anti: left side
    /// - Right Outer/Semi/Anti: right side
    /// - Inner: None.
    degree_state: StateTable<S>,
    /// The pk indices of the state table.
    state_pk_indices: Vec<usize>,
    /// The data type of all columns without degree.
    all_data_types: Vec<DataType>,
    /// The start position for the side in output new columns
    start_pos: usize,
    /// The mapping from input indices of a side to output columes.
    i2o_mapping: Vec<(usize, usize)>,
    i2o_mapping_indexed: MultiMap<usize, usize>,

    /// Whether degree table is needed for this side.
    need_degree_table: bool,
}

impl<S: StateStore> JoinSide<S> {
    fn init(&mut self, epoch: EpochPair) {
        self.state.init_epoch(epoch);
        self.degree_state.init_epoch(epoch);
    }

    /// Update the vnode bitmap and manipulate the cache if necessary.
    fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) -> bool {
        let (_previous_vnode_bitmap, cache_may_stale) =
            self.state.update_vnode_bitmap(vnode_bitmap.clone());
        let _ = self.degree_state.update_vnode_bitmap(vnode_bitmap);

        cache_may_stale
    }

    /// Iter the rows in the table.
    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    async fn rows(&mut self) {
        
            let range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let streams = futures::future::try_join_all(
                    self.state.vnodes().iter_vnodes().map(|vnode| {
                        self.state.iter_with_vnode(
                            vnode,
                            range,
                            PrefetchOptions::prefetch_for_large_range_scan(),
                        )
                    })
                ).await?.into_iter().map(Box::pin);
            
            #[for_await]
            for entry in stream::select_all(streams) {
                let row = entry?;
                yield row.into_owned_row();
            }
    }

    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state.commit(epoch).await?;
        self.degree_state.commit(epoch).await?;
        Ok(())
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state.try_flush().await?;
        self.degree_state.try_flush().await?;
        Ok(())
    }
    
    /// Insert a row.
    /// Used when the side does not need to update degree.
    #[allow(clippy::unused_async)]
    pub async fn insert_row(&mut self, value: impl Row) -> StreamExecutorResult<()> {
        // Update the flush buffer.
        self.state.insert(value);
        Ok(())
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self,value: impl Row) -> StreamExecutorResult<()> {
        // If no cache maintained, only update the state table.
        self.state.delete(value);
        Ok(())
    }    
}

enum InternalMessage {
    Chunk(StreamChunk),
    Barrier(Vec<StreamChunk>, Barrier),
    WaterMark(Watermark),
}


impl<S: StateStore, const T: JoinTypePrimitive> NestedLoopJoinExecutor<S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input_l: BoxedExecutor,
        input_r: BoxedExecutor,
        _null_safe: Vec<bool>,
        output_indices: Vec<usize>,
        cond: Option<NonStrictExpression>,
        inequality_pairs: Vec<(usize, usize, bool, Option<NonStrictExpression>)>,
        state_table_l: StateTable<S>,
        degree_state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        degree_state_table_r: StateTable<S>,
        _watermark_epoch: AtomicU64Ref,
        is_append_only: bool,
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

        let state_pk_indices_l = input_l.pk_indices().to_vec();
        let state_pk_indices_r = input_r.pk_indices().to_vec();

        let state_order_key_indices_l = state_table_l.pk_indices();
        let state_order_key_indices_r = state_table_r.pk_indices();

        let _degree_pk_indices_l = input_l.pk_indices();
        let _degree_pk_indices_r = input_r.pk_indices();

        // check whether join key contains pk in both side
        let append_only_optimize = is_append_only;

        let _degree_all_data_types_l = state_order_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();
        let _degree_all_data_types_r = state_order_key_indices_r
            .iter()
            .map(|idx| state_all_data_types_r[*idx].clone())
            .collect_vec();

        let need_degree_table_l = need_left_degree(T);
        let need_degree_table_r = need_right_degree(T);

        let (left_to_output, right_to_output) = {
            let (left_len, right_len) = if is_left_semi_or_anti(T) {
                (state_all_data_types_l.len(), 0usize)
            } else if is_right_semi_or_anti(T) {
                (0usize, state_all_data_types_r.len())
            } else {
                (state_all_data_types_l.len(), state_all_data_types_r.len())
            };
            JoinStreamChunkBuilder::get_i2o_mapping(&output_indices, left_len, right_len)
        };

        let l2o_indexed = MultiMap::from_iter(left_to_output.iter().copied());
        let r2o_indexed = MultiMap::from_iter(right_to_output.iter().copied());

        let left_input_len = input_l.schema().len();
        let right_input_len = input_r.schema().len();
        let mut l2inequality_index = vec![vec![]; left_input_len];
        let mut r2inequality_index = vec![vec![]; right_input_len];
        let mut l_state_clean_columns = vec![];
        let mut r_state_clean_columns = vec![];
        let _inequality_pairs = inequality_pairs
            .into_iter()
            .enumerate()
            .map(
                |(
                    index,
                    (key_required_larger, key_required_smaller, clean_state, delta_expression),
                )| {
                    let output_indices = if key_required_larger < key_required_smaller {
                        if clean_state {
                            l_state_clean_columns.push((key_required_larger, index));
                        }
                        l2inequality_index[key_required_larger].push((index, false));
                        r2inequality_index[key_required_smaller - left_input_len]
                            .push((index, true));
                        l2o_indexed
                            .get_vec(&key_required_larger)
                            .cloned()
                            .unwrap_or_default()
                    } else {
                        if clean_state {
                            r_state_clean_columns
                                .push((key_required_larger - left_input_len, index));
                        }
                        l2inequality_index[key_required_smaller].push((index, true));
                        r2inequality_index[key_required_larger - left_input_len]
                            .push((index, false));
                        r2o_indexed
                            .get_vec(&(key_required_larger - left_input_len))
                            .cloned()
                            .unwrap_or_default()
                    };
                    (output_indices, delta_expression)
                },
            )
            .collect_vec();

        let watermark_buffers = BTreeMap::new();

        Self {
            ctx: ctx.clone(),
            info,
            input_l: Some(input_l),
            input_r: Some(input_r),
            actual_output_data_types,
            side_l: JoinSide {
                state: state_table_l,
                degree_state: degree_state_table_l,
                state_pk_indices: state_pk_indices_l,
                all_data_types: state_all_data_types_l,
                i2o_mapping: left_to_output,
                i2o_mapping_indexed: l2o_indexed,
                start_pos: 0,
                need_degree_table: need_degree_table_l,
            },
            side_r: JoinSide {
                state: state_table_r,
                degree_state: degree_state_table_r,
                state_pk_indices: state_pk_indices_r,
                all_data_types: state_all_data_types_r,
                start_pos: side_l_column_n,
                i2o_mapping: right_to_output,
                i2o_mapping_indexed: r2o_indexed,
                need_degree_table: need_degree_table_r,
            },
            cond,
            append_only_optimize,
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
                    for chunk in Self::eq_join_oneside::<{SideType::Left}>(JoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        chunk,
                        append_only_optimize: self.append_only_optimize,
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
                    for chunk in Self::eq_join_oneside::<{SideType::Right}>(JoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        chunk,
                        append_only_optimize: self.append_only_optimize,
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
            append_only_optimize,
            chunk_size,
            ..
        } = args;

        let (side_update, side_match) = if SIDE == SideType::Left {
            (side_l, side_r)
        } else {
            (side_r, side_l)
        };

        let mut join_chunk_builder = 
            JoinStreamChunkBuilder::new(
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
                    let mut append_only_matched_row: Option<OwnedRow> = None;
                        #[for_await]
                        for matched_row in rows
                        {
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
                            if check_join_condition && !forward_exactly_once(T, SIDE) {
                                if let Some(chunk) = join_chunk_builder
                                    .append_row(Op::Insert, row, &matched_row)
                                {
                                    yield chunk;
                                }
                            }
                            // If the stream is append-only and the join key covers pk in both side,
                            // then we can remove matched rows since pk is unique and will not be
                            // inserted again
                            if append_only_optimize {
                                // Since join key contains pk and pk is unique, there should be only
                                // one row if matched.
                                assert!(append_only_matched_row.is_none());
                                append_only_matched_row = Some(matched_row);
                            }
                        }

                        if append_only_optimize && let Some(row) = append_only_matched_row {
                            side_match.delete_row(row)?;
                        
                        } else {
                            side_update.insert_row(row).await?;
                        }

                }
                Op::Delete | Op::UpdateDelete => {
                        #[for_await]
                        for matched_row in rows
                        {
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
                                    if let Some(chunk) = join_chunk_builder
                                        .append_row(Op::Delete, row, &matched_row)
                                    {
                                        yield chunk;
                                    }
                            }
                        }


                        if append_only_optimize {
                            unreachable!();
                        } else {
                            side_update.delete_row(row)?;
                        };

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
        self.side_l.state.commit(epoch).await?;
        self.side_r.degree_state.commit(epoch).await?;
        Ok(())
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        self.side_l.state.try_flush().await?;
        self.side_r.degree_state.try_flush().await?;
        Ok(())
    }

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

impl<S: StateStore, const T: JoinTypePrimitive> Executor
    for NestedLoopJoinExecutor<S, T>
{
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
