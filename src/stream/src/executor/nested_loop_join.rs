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

use std::alloc::Global;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Bound, Deref, DerefMut};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use either::Either;
use futures::stream::{self, PollNext};
use futures::{pin_mut, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use lru::DefaultHasher;
use multimap::MultiMap;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::estimate_size::{EstimateSize, KvSize};
use risingwave_common::hash::{HashKey, NullBitmap, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_expr::ExprError;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use self::builder::JoinChunkBuilder;
use self::row::{row_concat, DegreeType, JoinRow};

use super::join::*;
use super::test_utils::prelude::StateTable;
use super::watermark::BufferedWatermarks;
use super::{
    Barrier, Executor, ExecutorInfo, Message, MessageStream, StreamExecutorError, StreamExecutorResult
};
use crate::cache::{cache_may_stale, new_with_hasher_in, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
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
    /// Count the messages received, clear to 0 when counted to `EVICT_EVERY_N_MESSAGES`
    cnt_rows_received: u32,

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
    cnt_rows_received: &'a mut u32,
}

struct CachedJoinSide<S: StateStore> {
    /// pk -> row
    cached: ManagedLruCache<OwnedRow, OwnedRow>,
    kv_heap_size: KvSize,
    inner: JoinSide<S>,
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
    #[try_stream(ok = JoinRow<OwnedRow>, error = StreamExecutorError)]
    async fn rows(&mut self) {
        if self.need_degree_table {
            let range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let streams = futures::future::try_join_all(
                    self.state.vnodes().iter_vnodes().map(|vnode| {
                        let state_iter = self.state.iter_with_vnode(
                            vnode,
                            &range,
                            PrefetchOptions::prefetch_for_large_range_scan(),
                        );
                        let degree_state_iter = self.degree_state.iter_with_vnode(
                            vnode,
                            &range,
                            PrefetchOptions::prefetch_for_large_range_scan(),
                        );
                        futures::future::try_join(state_iter, degree_state_iter)
                    })
                ).await?.into_iter().map(|(state_iter, degree_state_iter)| Box::pin(state_iter.zip(degree_state_iter)));
            
            #[for_await]
            for (row, degree) in stream::select_all(streams) {
                let row = row?;
                let degree_row = degree?;
                let pk1 = row.key();
                let pk2 = degree_row.key();
                debug_assert_eq!(
                    pk1, pk2,
                    "mismatched pk in degree table: pk1: {pk1:?}, pk2: {pk2:?}",
                );
                let degree_i64 = degree_row
                    .datum_at(degree_row.len() - 1)
                    .expect("degree should not be NULL");
                yield JoinRow::new(row.into_owned_row(), degree_i64.into_int64() as u64);
            }
        } else {
            let range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let streams = futures::future::try_join_all(
                    self.state.vnodes().iter_vnodes().map(|vnode| {
                        self.state.iter_with_vnode(
                            vnode,
                            &range,
                            PrefetchOptions::prefetch_for_large_range_scan(),
                        )
                    })
                ).await?.into_iter().map(Box::pin);
            
            #[for_await]
            for entry in stream::select_all(streams) {
                let row = entry?;
                yield JoinRow::new(row.into_owned_row(), 0);
            }
        };
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

    /// Insert a join row
    #[allow(clippy::unused_async)]
    pub async fn insert(&mut self, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        // Update the flush buffer.
        let (row, degree) = value.to_table_rows(&self.state_pk_indices);
        self.state.insert(row);
        self.degree_state.insert(degree);
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

    /// Delete a join row
    pub fn delete(&mut self, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        // If no cache maintained, only update the state table.
        let (row, degree) = value.to_table_rows(&self.state_pk_indices);
        self.state.delete(row);
        self.degree_state.delete(degree);
        Ok(())
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self,value: impl Row) -> StreamExecutorResult<()> {
        // If no cache maintained, only update the state table.
        self.state.delete(value);
        Ok(())
    }

    /// Manipulate the degree of the given [`JoinRow`] and update the degree table.
    fn manipulate_degree(
        &mut self,
        join_row: &mut JoinRow<OwnedRow>,
        action: impl Fn(&mut DegreeType),
    ) {
        // TODO: no need to `into_owned_row` here due to partial borrow.
        let old_degree = join_row
            .to_table_rows(&self.state_pk_indices)
            .1
            .into_owned_row();

        action(&mut join_row.degree);

        let new_degree = join_row.to_table_rows(&self.state_pk_indices).1;

        self.degree_state.update(old_degree, new_degree);
    }

    /// Increment the degree of the given [`JoinRow`] and [`EncodedJoinRow`] with `action`, both in
    /// memory and in the degree table.
    pub fn inc_degree(
        &mut self,
        join_row: &mut JoinRow<OwnedRow>,
    ) {
        self.manipulate_degree(join_row, |d| *d += 1)
    }

    /// Decrement the degree of the given [`JoinRow`] and [`EncodedJoinRow`] with `action`, both in
    /// memory and in the degree table.
    pub fn dec_degree(
        &mut self,
        join_row: &mut JoinRow<OwnedRow>,
    ) {
        self.manipulate_degree(join_row, |d| {
            *d = d
                .checked_sub(1)
                .expect("Tried to decrement zero join row degree")
        })
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
        null_safe: Vec<bool>,
        output_indices: Vec<usize>,
        cond: Option<NonStrictExpression>,
        inequality_pairs: Vec<(usize, usize, bool, Option<NonStrictExpression>)>,
        state_table_l: StateTable<S>,
        degree_state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        degree_state_table_r: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
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

        let degree_pk_indices_l = input_l.pk_indices().clone();
        let degree_pk_indices_r = input_r.pk_indices().clone();

        // check whether join key contains pk in both side
        let append_only_optimize = is_append_only;

        let degree_all_data_types_l = state_order_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();
        let degree_all_data_types_r = state_order_key_indices_r
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
        let inequality_pairs = inequality_pairs
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
            cnt_rows_received: 0,
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

        let left_join_cached_entry_count = self
            .metrics
            .join_cached_entry_count
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str, "left"]);

        let right_join_cached_entry_count = self
            .metrics
            .join_cached_entry_count
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str, "right"]);

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
                        cnt_rows_received: &mut self.cnt_rows_received,
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
                        cnt_rows_received: &mut self.cnt_rows_received,
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
            cnt_rows_received,
            ..
        } = args;

        let (side_update, side_match) = if SIDE == SideType::Left {
            (side_l, side_r)
        } else {
            (side_r, side_l)
        };

        let mut join_chunk_builder = JoinChunkBuilder::<T, SIDE>::new(
            JoinStreamChunkBuilder::new(
                chunk_size,
                actual_output_data_types.to_vec(),
                side_update.i2o_mapping.clone(),
                side_match.i2o_mapping.clone(),
            )
        );

        let join_matched_join_keys = ctx
            .streaming_metrics
            .join_matched_join_keys
            .with_label_values(&[
                &ctx.id.to_string(),
                &ctx.fragment_id.to_string(),
                &side_update.state.table_id().to_string(),
            ]);

        for r in chunk.rows_with_holes() {
            let Some((op, row)) = r else {
                continue;
            };


            let rows = side_match.rows();
            
            let mut matched_rows_cnt = 0;

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let mut degree = 0;
                    let mut append_only_matched_row: Option<JoinRow<OwnedRow>> = None;
                        #[for_await]
                        for matched_row in rows
                        {
                            matched_rows_cnt += 1;
                            let mut matched_row = matched_row?;
                            // TODO(yuhao-su): We should find a better way to eval the expression
                            // without concat two rows.
                            // if there are non-equi expressions
                            let check_join_condition = if let Some(ref mut cond) = cond {
                                let new_row = row_concat(
                                    &row,
                                    side_update.start_pos,
                                    &matched_row.row,
                                    side_match.start_pos,
                                );

                                cond.eval_row_infallible(&new_row)
                                    .await
                                    .map(|s| *s.as_bool())
                                    .unwrap_or(false)
                            } else {
                                true
                            };
                            if check_join_condition {
                                degree += 1;
                                if !forward_exactly_once(T, SIDE) {
                                    if let Some(chunk) = join_chunk_builder
                                        .with_match_on_insert(&row, &matched_row)
                                    {
                                        yield chunk;
                                    }
                                }
                                if side_match.need_degree_table {
                                    side_match.inc_degree(&mut matched_row);
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
                        if degree == 0 {
                            if let Some(chunk) =
                                join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            join_chunk_builder.forward_exactly_once_if_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }

                        if append_only_optimize && let Some(row) = append_only_matched_row {
                            side_match.delete(row)?;
                        } else if side_update.need_degree_table {
                            side_update
                                .insert(JoinRow::new(row, degree))
                                .await?;
                        } else {
                            side_update.insert_row(row).await?;
                        }
                    if matched_rows_cnt == 0 {
                        // Row which violates null-safe bitmap will never be matched so we need not
                        // store.
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    let mut degree = 0;
                        #[for_await]
                        for matched_row in rows
                        {
                            matched_rows_cnt += 1;
                            let mut matched_row = matched_row?;
                            // TODO(yuhao-su): We should find a better way to eval the expression
                            // without concat two rows.
                            // if there are non-equi expressions
                            let check_join_condition = if let Some(ref mut cond) = cond {
                                let new_row = row_concat(
                                    &row,
                                    side_update.start_pos,
                                    &matched_row.row,
                                    side_match.start_pos,
                                );

                                cond.eval_row_infallible(&new_row)
                                    .await
                                    .map(|s| *s.as_bool())
                                    .unwrap_or(false)
                            } else {
                                true
                            };
                            let mut need_state_clean = false;
                            if check_join_condition {
                                degree += 1;
                                if side_match.need_degree_table {
                                    side_match.dec_degree(&mut matched_row);
                                }
                                if !forward_exactly_once(T, SIDE) {
                                    if let Some(chunk) = join_chunk_builder
                                        .with_match_on_delete(&row, &matched_row)
                                    {
                                        yield chunk;
                                    }
                                }
                            }
                        }
                        if degree == 0 {
                            if let Some(chunk) =
                                join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            join_chunk_builder.forward_exactly_once_if_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }

                        if append_only_optimize {
                            unreachable!();
                        } else if side_update.need_degree_table {
                            side_update.delete(JoinRow::new(row, degree))?;
                        } else {
                            side_update.delete_row(row)?;
                        };
                    if matched_rows_cnt == 0 {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                    }
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
        side: SideTypePrimitive,
        watermark: Watermark,
    ) -> StreamExecutorResult<Vec<Watermark>> {
        let (side_update, side_match) = if side == SideType::Left {
            (&mut self.side_l, &mut self.side_r)
        } else {
            (&mut self.side_r, &mut self.side_l)
        };

        // State cleaning


        // Select watermarks to yield.
        let wm_in_jk = side_update
            .join_key_indices
            .iter()
            .positions(|idx| *idx == watermark.col_idx);
        let mut watermarks_to_emit = vec![];
        for idx in wm_in_jk {
            let buffers = self
                .watermark_buffers
                .entry(idx)
                .or_insert_with(|| BufferedWatermarks::with_ids([SideType::Left, SideType::Right]));
            if let Some(selected_watermark) = buffers.handle_watermark(side, watermark.clone()) {
                let empty_indices = vec![];
                let output_indices = side_update
                    .i2o_mapping_indexed
                    .get_vec(&side_update.join_key_indices[idx])
                    .unwrap_or(&empty_indices)
                    .iter()
                    .chain(
                        side_match
                            .i2o_mapping_indexed
                            .get_vec(&side_match.join_key_indices[idx])
                            .unwrap_or(&empty_indices),
                    );
                for output_idx in output_indices {
                    watermarks_to_emit.push(selected_watermark.clone().with_idx(*output_idx));
                }
            };
        }
        for (inequality_index, need_offset) in
            &side_update.input2inequality_index[watermark.col_idx]
        {
            let buffers = self
                .watermark_buffers
                .entry(side_update.join_key_indices.len() + inequality_index)
                .or_insert_with(|| BufferedWatermarks::with_ids([SideType::Left, SideType::Right]));
            let mut input_watermark = watermark.clone();
            if *need_offset
                && let Some(delta_expression) = self.inequality_pairs[*inequality_index].1.as_ref()
            {
                // allow since we will handle error manually.
                #[allow(clippy::disallowed_methods)]
                let eval_result = delta_expression
                    .inner()
                    .eval_row(&OwnedRow::new(vec![Some(input_watermark.val)]))
                    .await;
                match eval_result {
                    Ok(value) => input_watermark.val = value.unwrap(),
                    Err(err) => {
                        if !matches!(err, ExprError::NumericOutOfRange) {
                            self.ctx.on_compute_error(err, &self.info.identity);
                        }
                        continue;
                    }
                }
            };
            if let Some(selected_watermark) = buffers.handle_watermark(side, input_watermark) {
                for output_idx in &self.inequality_pairs[*inequality_index].0 {
                    watermarks_to_emit.push(selected_watermark.clone().with_idx(*output_idx));
                }
                self.inequality_watermarks[*inequality_index] = Some(selected_watermark);
            }
        }
        Ok(watermarks_to_emit)
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
