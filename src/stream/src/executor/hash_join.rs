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
use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::sync::LazyLock;
use std::time::Duration;

use governor::{Quota, RateLimiter};
use itertools::Itertools;
use multimap::MultiMap;
use risingwave_common::array::Op;
use risingwave_common::hash::{HashKey, NullBitmap};
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{DefaultOrd, ToOwnedDatum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_expr::ExprError;
use tokio::time::Instant;

use self::builder::JoinChunkBuilder;
use super::barrier_align::*;
use super::join::hash_join::*;
use super::join::row::JoinRow;
use super::join::*;
use super::watermark::*;
use crate::executor::join::builder::JoinStreamChunkBuilder;
use crate::executor::prelude::*;

/// Evict the cache every n rows.
const EVICT_EVERY_N_ROWS: u32 = 16;

fn is_subset(vec1: Vec<usize>, vec2: Vec<usize>) -> bool {
    HashSet::<usize>::from_iter(vec1).is_subset(&vec2.into_iter().collect())
}

pub struct JoinParams {
    /// Indices of the join keys
    pub join_key_indices: Vec<usize>,
    /// Indices of the input pk after dedup
    pub deduped_pk_indices: Vec<usize>,
}

impl JoinParams {
    pub fn new(join_key_indices: Vec<usize>, deduped_pk_indices: Vec<usize>) -> Self {
        Self {
            join_key_indices,
            deduped_pk_indices,
        }
    }
}

struct JoinSide<K: HashKey, S: StateStore> {
    /// Store all data from a one side stream
    ht: JoinHashMap<K, S>,
    /// Indices of the join key columns
    join_key_indices: Vec<usize>,
    /// The data type of all columns without degree.
    all_data_types: Vec<DataType>,
    /// The start position for the side in output new columns
    start_pos: usize,
    /// The mapping from input indices of a side to output columes.
    i2o_mapping: Vec<(usize, usize)>,
    i2o_mapping_indexed: MultiMap<usize, usize>,
    /// The first field of the ith element indicates that when a watermark at the ith column of
    /// this side comes, what band join conditions should be updated in order to possibly
    /// generate a new watermark at that column or the corresponding column in the counterpart
    /// join side.
    ///
    /// The second field indicates that whether the column is required less than the
    /// the corresponding column in the counterpart join side in the band join condition.
    input2inequality_index: Vec<Vec<(usize, bool)>>,
    /// Some fields which are required non null to match due to inequalities.
    non_null_fields: Vec<usize>,
    /// (i, j) in this `Vec` means that state data in this join side can be cleaned if the value of
    /// its ith column is less than the synthetic watermark of the jth band join condition.
    state_clean_columns: Vec<(usize, usize)>,
    /// Whether degree table is needed for this side.
    need_degree_table: bool,
}

impl<K: HashKey, S: StateStore> std::fmt::Debug for JoinSide<K, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinSide")
            .field("join_key_indices", &self.join_key_indices)
            .field("col_types", &self.all_data_types)
            .field("start_pos", &self.start_pos)
            .field("i2o_mapping", &self.i2o_mapping)
            .field("need_degree_table", &self.need_degree_table)
            .finish()
    }
}

impl<K: HashKey, S: StateStore> JoinSide<K, S> {
    // WARNING: Please do not call this until we implement it.
    fn is_dirty(&self) -> bool {
        unimplemented!()
    }

    #[expect(dead_code)]
    fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while states of hash join are dirty"
        );

        // TODO: not working with rearranged chain
        // self.ht.clear();
    }

    pub fn init(&mut self, epoch: EpochPair) {
        self.ht.init(epoch);
    }
}

/// `HashJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct HashJoinExecutor<K: HashKey, S: StateStore, const T: JoinTypePrimitive> {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Left input executor
    input_l: Option<Executor>,
    /// Right input executor
    input_r: Option<Executor>,
    /// The data types of the formed new columns
    actual_output_data_types: Vec<DataType>,
    /// The parameters of the left join executor
    side_l: JoinSide<K, S>,
    /// The parameters of the right join executor
    side_r: JoinSide<K, S>,
    /// Optional non-equi join conditions
    cond: Option<NonStrictExpression>,
    /// Column indices of watermark output and offset expression of each inequality, respectively.
    inequality_pairs: Vec<(Vec<usize>, Option<NonStrictExpression>)>,
    /// The output watermark of each inequality condition and its value is the minimum of the
    /// calculation result of both side. It will be used to generate watermark into downstream
    /// and do state cleaning if `clean_state` field of that inequality is `true`.
    inequality_watermarks: Vec<Option<Watermark>>,

    /// Whether the logic can be optimized for append-only stream
    append_only_optimize: bool,

    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time
    chunk_size: usize,
    /// Count the messages received, clear to 0 when counted to `EVICT_EVERY_N_MESSAGES`
    cnt_rows_received: u32,

    /// watermark column index -> `BufferedWatermarks`
    watermark_buffers: BTreeMap<usize, BufferedWatermarks<SideTypePrimitive>>,

    high_join_amplification_threshold: usize,
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> std::fmt::Debug
    for HashJoinExecutor<K, S, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashJoinExecutor")
            .field("join_type", &T)
            .field("input_left", &self.input_l.as_ref().unwrap().identity())
            .field("input_right", &self.input_r.as_ref().unwrap().identity())
            .field("side_l", &self.side_l)
            .field("side_r", &self.side_r)
            .field("pk_indices", &self.info.pk_indices)
            .field("schema", &self.info.schema)
            .field("actual_output_data_types", &self.actual_output_data_types)
            .finish()
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> Execute for HashJoinExecutor<K, S, T> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

struct EqJoinArgs<'a, K: HashKey, S: StateStore> {
    ctx: &'a ActorContextRef,
    side_l: &'a mut JoinSide<K, S>,
    side_r: &'a mut JoinSide<K, S>,
    actual_output_data_types: &'a [DataType],
    cond: &'a mut Option<NonStrictExpression>,
    inequality_watermarks: &'a [Option<Watermark>],
    chunk: StreamChunk,
    append_only_optimize: bool,
    chunk_size: usize,
    cnt_rows_received: &'a mut u32,
    high_join_amplification_threshold: usize,
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> HashJoinExecutor<K, S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input_l: Executor,
        input_r: Executor,
        params_l: JoinParams,
        params_r: JoinParams,
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
        high_join_amplification_threshold: usize,
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

        let state_join_key_indices_l = params_l.join_key_indices;
        let state_join_key_indices_r = params_r.join_key_indices;

        let degree_join_key_indices_l = (0..state_join_key_indices_l.len()).collect_vec();
        let degree_join_key_indices_r = (0..state_join_key_indices_r.len()).collect_vec();

        let degree_pk_indices_l = (state_join_key_indices_l.len()
            ..state_join_key_indices_l.len() + params_l.deduped_pk_indices.len())
            .collect_vec();
        let degree_pk_indices_r = (state_join_key_indices_r.len()
            ..state_join_key_indices_r.len() + params_r.deduped_pk_indices.len())
            .collect_vec();

        // If pk is contained in join key.
        let pk_contained_in_jk_l =
            is_subset(state_pk_indices_l.clone(), state_join_key_indices_l.clone());
        let pk_contained_in_jk_r =
            is_subset(state_pk_indices_r.clone(), state_join_key_indices_r.clone());

        // check whether join key contains pk in both side
        let append_only_optimize = is_append_only && pk_contained_in_jk_l && pk_contained_in_jk_r;

        let join_key_data_types_l = state_join_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();

        let join_key_data_types_r = state_join_key_indices_r
            .iter()
            .map(|idx| state_all_data_types_r[*idx].clone())
            .collect_vec();

        assert_eq!(join_key_data_types_l, join_key_data_types_r);

        let degree_all_data_types_l = state_order_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();
        let degree_all_data_types_r = state_order_key_indices_r
            .iter()
            .map(|idx| state_all_data_types_r[*idx].clone())
            .collect_vec();

        let null_matched = K::Bitmap::from_bool_vec(null_safe);

        let need_degree_table_l = need_left_degree(T) && !pk_contained_in_jk_r;
        let need_degree_table_r = need_right_degree(T) && !pk_contained_in_jk_l;

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

        let mut l_non_null_fields = l2inequality_index
            .iter()
            .positions(|inequalities| !inequalities.is_empty())
            .collect_vec();
        let mut r_non_null_fields = r2inequality_index
            .iter()
            .positions(|inequalities| !inequalities.is_empty())
            .collect_vec();

        if append_only_optimize {
            l_state_clean_columns.clear();
            r_state_clean_columns.clear();
            l_non_null_fields.clear();
            r_non_null_fields.clear();
        }

        let inequality_watermarks = vec![None; inequality_pairs.len()];
        let watermark_buffers = BTreeMap::new();

        Self {
            ctx: ctx.clone(),
            info,
            input_l: Some(input_l),
            input_r: Some(input_r),
            actual_output_data_types,
            side_l: JoinSide {
                ht: JoinHashMap::new(
                    watermark_epoch.clone(),
                    join_key_data_types_l,
                    state_join_key_indices_l.clone(),
                    state_all_data_types_l.clone(),
                    state_table_l,
                    params_l.deduped_pk_indices,
                    degree_join_key_indices_l,
                    degree_all_data_types_l,
                    degree_state_table_l,
                    degree_pk_indices_l,
                    null_matched.clone(),
                    need_degree_table_l,
                    pk_contained_in_jk_l,
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "left",
                ),
                join_key_indices: state_join_key_indices_l,
                all_data_types: state_all_data_types_l,
                i2o_mapping: left_to_output,
                i2o_mapping_indexed: l2o_indexed,
                input2inequality_index: l2inequality_index,
                non_null_fields: l_non_null_fields,
                state_clean_columns: l_state_clean_columns,
                start_pos: 0,
                need_degree_table: need_degree_table_l,
            },
            side_r: JoinSide {
                ht: JoinHashMap::new(
                    watermark_epoch,
                    join_key_data_types_r,
                    state_join_key_indices_r.clone(),
                    state_all_data_types_r.clone(),
                    state_table_r,
                    params_r.deduped_pk_indices,
                    degree_join_key_indices_r,
                    degree_all_data_types_r,
                    degree_state_table_r,
                    degree_pk_indices_r,
                    null_matched,
                    need_degree_table_r,
                    pk_contained_in_jk_r,
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "right",
                ),
                join_key_indices: state_join_key_indices_r,
                all_data_types: state_all_data_types_r,
                start_pos: side_l_column_n,
                i2o_mapping: right_to_output,
                i2o_mapping_indexed: r2o_indexed,
                input2inequality_index: r2inequality_index,
                non_null_fields: r_non_null_fields,
                state_clean_columns: r_state_clean_columns,
                need_degree_table: need_degree_table_r,
            },
            cond,
            inequality_pairs,
            inequality_watermarks,
            append_only_optimize,
            metrics,
            chunk_size,
            cnt_rows_received: 0,
            watermark_buffers,
            high_join_amplification_threshold,
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
            "Join",
        );
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
            .instrument_await("hash_join_barrier_align")
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
                    for chunk in Self::eq_join_left(EqJoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        inequality_watermarks: &self.inequality_watermarks,
                        chunk,
                        append_only_optimize: self.append_only_optimize,
                        chunk_size: self.chunk_size,
                        cnt_rows_received: &mut self.cnt_rows_received,
                        high_join_amplification_threshold: self.high_join_amplification_threshold,
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
                    for chunk in Self::eq_join_right(EqJoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        inequality_watermarks: &self.inequality_watermarks,
                        chunk,
                        append_only_optimize: self.append_only_optimize,
                        chunk_size: self.chunk_size,
                        cnt_rows_received: &mut self.cnt_rows_received,
                        high_join_amplification_threshold: self.high_join_amplification_threshold,
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
                        if self.side_l.ht.update_vnode_bitmap(vnode_bitmap.clone()) {
                            self.watermark_buffers
                                .values_mut()
                                .for_each(|buffers| buffers.clear());
                            self.inequality_watermarks.fill(None);
                        }
                        self.side_r.ht.update_vnode_bitmap(vnode_bitmap);
                    }

                    // Report metrics of cached join rows/entries
                    for (join_cached_entry_count, ht) in [
                        (&left_join_cached_entry_count, &self.side_l.ht),
                        (&right_join_cached_entry_count, &self.side_r.ht),
                    ] {
                        join_cached_entry_count.set(ht.entry_count() as i64);
                    }

                    barrier_join_match_duration_ns
                        .inc_by(barrier_start_time.elapsed().as_nanos() as u64);
                    yield Message::Barrier(barrier);
                }
            }
            start_time = Instant::now();
        }
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        self.side_l.ht.flush(epoch).await?;
        self.side_r.ht.flush(epoch).await?;
        Ok(())
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        self.side_l.ht.try_flush().await?;
        self.side_r.ht.try_flush().await?;
        Ok(())
    }

    // We need to manually evict the cache.
    fn evict_cache(
        side_update: &mut JoinSide<K, S>,
        side_match: &mut JoinSide<K, S>,
        cnt_rows_received: &mut u32,
    ) {
        *cnt_rows_received += 1;
        if *cnt_rows_received == EVICT_EVERY_N_ROWS {
            side_update.ht.evict();
            side_match.ht.evict();
            *cnt_rows_received = 0;
        }
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
        if side_update.join_key_indices[0] == watermark.col_idx {
            side_match.ht.update_watermark(watermark.val.clone());
        }

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

    /// the data the hash table and match the coming
    /// data chunk with the executor state
    async fn hash_eq_match(
        key: &K,
        ht: &mut JoinHashMap<K, S>,
    ) -> StreamExecutorResult<Option<HashValueType>> {
        if !key.null_bitmap().is_subset(ht.null_matched()) {
            Ok(None)
        } else {
            ht.take_state(key).await.map(Some)
        }
    }

    fn row_concat(
        row_update: &RowRef<'_>,
        update_start_pos: usize,
        row_matched: &OwnedRow,
        matched_start_pos: usize,
    ) -> OwnedRow {
        let mut new_row = vec![None; row_update.len() + row_matched.len()];

        for (i, datum_ref) in row_update.iter().enumerate() {
            new_row[i + update_start_pos] = datum_ref.to_owned_datum();
        }
        for i in 0..row_matched.len() {
            new_row[i + matched_start_pos].clone_from(&row_matched[i]);
        }
        OwnedRow::new(new_row)
    }

    /// Used to forward `eq_join_oneside` to show join side in stack.
    fn eq_join_left(
        args: EqJoinArgs<'_, K, S>,
    ) -> impl Stream<Item = Result<StreamChunk, StreamExecutorError>> + '_ {
        Self::eq_join_oneside::<{ SideType::Left }>(args)
    }

    /// Used to forward `eq_join_oneside` to show join side in stack.
    fn eq_join_right(
        args: EqJoinArgs<'_, K, S>,
    ) -> impl Stream<Item = Result<StreamChunk, StreamExecutorError>> + '_ {
        Self::eq_join_oneside::<{ SideType::Right }>(args)
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_oneside<const SIDE: SideTypePrimitive>(args: EqJoinArgs<'_, K, S>) {
        let EqJoinArgs {
            ctx,
            side_l,
            side_r,
            actual_output_data_types,
            cond,
            inequality_watermarks,
            chunk,
            append_only_optimize,
            chunk_size,
            cnt_rows_received,
            high_join_amplification_threshold,
            ..
        } = args;

        let (side_update, side_match) = if SIDE == SideType::Left {
            (side_l, side_r)
        } else {
            (side_r, side_l)
        };

        let useful_state_clean_columns = side_match
            .state_clean_columns
            .iter()
            .filter_map(|(column_idx, inequality_index)| {
                inequality_watermarks[*inequality_index]
                    .as_ref()
                    .map(|watermark| (*column_idx, watermark))
            })
            .collect_vec();

        let mut hashjoin_chunk_builder =
            JoinChunkBuilder::<T, SIDE>::new(JoinStreamChunkBuilder::new(
                chunk_size,
                actual_output_data_types.to_vec(),
                side_update.i2o_mapping.clone(),
                side_match.i2o_mapping.clone(),
            ));

        let join_matched_join_keys = ctx
            .streaming_metrics
            .join_matched_join_keys
            .with_label_values(&[
                &ctx.id.to_string(),
                &ctx.fragment_id.to_string(),
                &side_update.ht.table_id().to_string(),
            ]);

        let keys = K::build_many(&side_update.join_key_indices, chunk.data_chunk());
        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.iter()) {
            let Some((op, row)) = r else {
                continue;
            };
            Self::evict_cache(side_update, side_match, cnt_rows_received);

            let matched_rows: Option<HashValueType> = if side_update
                .non_null_fields
                .iter()
                .all(|column_idx| unsafe { row.datum_at_unchecked(*column_idx).is_some() })
            {
                Self::hash_eq_match(key, &mut side_match.ht).await?
            } else {
                None
            };

            if let Some(rows) = &matched_rows {
                join_matched_join_keys.observe(rows.len() as _);
                if rows.len() > high_join_amplification_threshold {
                    static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(|| {
                        LogSuppresser::new(RateLimiter::direct(Quota::per_minute(
                            NonZeroU32::new(1).unwrap(),
                        )))
                    });
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        let join_key_data_types = side_update.ht.join_key_data_types();
                        let key = key.deserialize(join_key_data_types)?;
                        tracing::warn!(target: "high_join_amplification",
                            suppressed_count,
                            matched_rows_len = rows.len(),
                            update_table_id = side_update.ht.table_id(),
                            match_table_id = side_match.ht.table_id(),
                            join_key = ?key,
                            actor_id = ctx.id,
                            fragment_id = ctx.fragment_id,
                            "large rows matched for join key"
                        );
                    }
                }
            } else {
                join_matched_join_keys.observe(0.0)
            }

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let mut degree = 0;
                    let mut append_only_matched_row: Option<JoinRow<OwnedRow>> = None;
                    if let Some(mut matched_rows) = matched_rows {
                        let mut matched_rows_to_clean = vec![];
                        for (matched_row_ref, matched_row) in
                            matched_rows.values_mut(&side_match.all_data_types)
                        {
                            let mut matched_row = matched_row?;
                            // TODO(yuhao-su): We should find a better way to eval the expression
                            // without concat two rows.
                            // if there are non-equi expressions
                            let check_join_condition = if let Some(ref mut cond) = cond {
                                let new_row = Self::row_concat(
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
                                if !forward_exactly_once(T, SIDE) {
                                    if let Some(chunk) = hashjoin_chunk_builder
                                        .with_match_on_insert(&row, &matched_row)
                                    {
                                        yield chunk;
                                    }
                                }
                                if side_match.need_degree_table {
                                    side_match.ht.inc_degree(matched_row_ref, &mut matched_row);
                                }
                            } else {
                                for (column_idx, watermark) in &useful_state_clean_columns {
                                    if matched_row.row.datum_at(*column_idx).map_or(
                                        false,
                                        |scalar| {
                                            scalar
                                                .default_cmp(&watermark.val.as_scalar_ref_impl())
                                                .is_lt()
                                        },
                                    ) {
                                        need_state_clean = true;
                                        break;
                                    }
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
                            } else if need_state_clean {
                                // `append_only_optimize` and `need_state_clean` won't both be true.
                                // 'else' here is only to suppress compiler error.
                                matched_rows_to_clean.push(matched_row);
                            }
                        }
                        if degree == 0 {
                            if let Some(chunk) =
                                hashjoin_chunk_builder.forward_if_not_matched(Op::Insert, row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            hashjoin_chunk_builder.forward_exactly_once_if_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                        // Insert back the state taken from ht.
                        side_match.ht.update_state(key, matched_rows);
                        for matched_row in matched_rows_to_clean {
                            if side_match.need_degree_table {
                                side_match.ht.delete(key, matched_row)?;
                            } else {
                                side_match.ht.delete_row(key, matched_row.row)?;
                            }
                        }

                        if append_only_optimize && let Some(row) = append_only_matched_row {
                            side_match.ht.delete(key, row)?;
                        } else if side_update.need_degree_table {
                            side_update
                                .ht
                                .insert(key, JoinRow::new(row, degree))
                                .await?;
                        } else {
                            side_update.ht.insert_row(key, row).await?;
                        }
                    } else {
                        // Row which violates null-safe bitmap will never be matched so we need not
                        // store.
                        if let Some(chunk) =
                            hashjoin_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    let mut degree = 0;
                    if let Some(mut matched_rows) = matched_rows {
                        let mut matched_rows_to_clean = vec![];
                        for (matched_row_ref, matched_row) in
                            matched_rows.values_mut(&side_match.all_data_types)
                        {
                            let mut matched_row = matched_row?;
                            // TODO(yuhao-su): We should find a better way to eval the expression
                            // without concat two rows.
                            // if there are non-equi expressions
                            let check_join_condition = if let Some(ref mut cond) = cond {
                                let new_row = Self::row_concat(
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
                                    side_match.ht.dec_degree(matched_row_ref, &mut matched_row);
                                }
                                if !forward_exactly_once(T, SIDE) {
                                    if let Some(chunk) = hashjoin_chunk_builder
                                        .with_match_on_delete(&row, &matched_row)
                                    {
                                        yield chunk;
                                    }
                                }
                            } else {
                                for (column_idx, watermark) in &useful_state_clean_columns {
                                    if matched_row.row.datum_at(*column_idx).map_or(
                                        false,
                                        |scalar| {
                                            scalar
                                                .default_cmp(&watermark.val.as_scalar_ref_impl())
                                                .is_lt()
                                        },
                                    ) {
                                        need_state_clean = true;
                                        break;
                                    }
                                }
                            }
                            if need_state_clean {
                                matched_rows_to_clean.push(matched_row);
                            }
                        }
                        if degree == 0 {
                            if let Some(chunk) =
                                hashjoin_chunk_builder.forward_if_not_matched(Op::Delete, row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            hashjoin_chunk_builder.forward_exactly_once_if_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                        // Insert back the state taken from ht.
                        side_match.ht.update_state(key, matched_rows);
                        for matched_row in matched_rows_to_clean {
                            if side_match.need_degree_table {
                                side_match.ht.delete(key, matched_row)?;
                            } else {
                                side_match.ht.delete_row(key, matched_row.row)?;
                            }
                        }

                        if append_only_optimize {
                            unreachable!();
                        } else if side_update.need_degree_table {
                            side_update.ht.delete(key, JoinRow::new(row, degree))?;
                        } else {
                            side_update.ht.delete_row(key, row)?;
                        };
                    } else {
                        // We do not store row which violates null-safe bitmap.
                        if let Some(chunk) =
                            hashjoin_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                    }
                }
            }
        }
        if let Some(chunk) = hashjoin_chunk_builder.take() {
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::hash::{Key128, Key64};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        let state_table = StateTable::new_without_distribution(
            mem_state.clone(),
            TableId::new(table_id),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
        )
        .await;

        // Create degree table
        let mut degree_table_column_descs = vec![];
        pk_indices.iter().enumerate().for_each(|(pk_id, idx)| {
            degree_table_column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(pk_id as i32),
                data_types[*idx].clone(),
            ))
        });
        degree_table_column_descs.push(ColumnDesc::unnamed(
            ColumnId::new(pk_indices.len() as i32),
            DataType::Int64,
        ));
        let degree_state_table = StateTable::new_without_distribution(
            mem_state,
            TableId::new(table_id + 1),
            degree_table_column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
        )
        .await;
        (state_table, degree_state_table)
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
        null_safe: bool,
        condition_text: Option<String>,
        inequality_pairs: Vec<(usize, usize, bool, Option<NonStrictExpression>)>,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![1]);
        let params_l = JoinParams::new(vec![0], vec![1]);
        let params_r = JoinParams::new(vec![0], vec![1]);
        let cond = with_condition.then(|| create_cond(condition_text));

        let mem_state = MemoryStateStore::new();

        let (state_l, degree_state_l) = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            0,
        )
        .await;

        let (state_r, degree_state_r) = create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            2,
        )
        .await;

        let schema = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => source_l.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => source_r.schema().clone(),
            _ => [source_l.schema().fields(), source_r.schema().fields()]
                .concat()
                .into_iter()
                .collect(),
        };
        let schema_len = schema.len();
        let info = ExecutorInfo {
            schema,
            pk_indices: vec![1],
            identity: "HashJoinExecutor".to_string(),
        };

        let executor = HashJoinExecutor::<Key64, MemoryStateStore, T>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![null_safe],
            (0..schema_len).collect_vec(),
            cond,
            inequality_pairs,
            state_l,
            degree_state_l,
            state_r,
            degree_state_r,
            Arc::new(AtomicU64::new(0)),
            false,
            Arc::new(StreamingMetrics::unused()),
            1024,
            2048,
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    async fn create_classical_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
        null_safe: bool,
        condition_text: Option<String>,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        create_executor::<T>(with_condition, null_safe, condition_text, vec![]).await
    }

    async fn create_append_only_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![0]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![0]);
        let params_l = JoinParams::new(vec![0, 1], vec![]);
        let params_r = JoinParams::new(vec![0, 1], vec![]);
        let cond = with_condition.then(|| create_cond(None));

        let mem_state = MemoryStateStore::new();

        let (state_l, degree_state_l) = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, 1, 0],
            0,
        )
        .await;

        let (state_r, degree_state_r) = create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, 1, 1],
            0,
        )
        .await;

        let schema = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => source_l.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => source_r.schema().clone(),
            _ => [source_l.schema().fields(), source_r.schema().fields()]
                .concat()
                .into_iter()
                .collect(),
        };
        let schema_len = schema.len();
        let info = ExecutorInfo {
            schema,
            pk_indices: vec![1],
            identity: "HashJoinExecutor".to_string(),
        };

        let executor = HashJoinExecutor::<Key128, MemoryStateStore, T>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![false],
            (0..schema_len).collect_vec(),
            cond,
            vec![],
            state_l,
            degree_state_l,
            state_r,
            degree_state_r,
            Arc::new(AtomicU64::new(0)),
            true,
            Arc::new(StreamingMetrics::unused()),
            1024,
            2048,
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_interval_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 3
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
             + 2 6
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 2 3
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::Inner }>(
            true,
            false,
            Some(String::from("(and:boolean (greater_than:boolean $1:int8 (subtract:int8 $3:int8 2:int8)) (greater_than:boolean $3:int8 (subtract:int8 $1:int8 2:int8)))")),
            vec![(1, 3, true, Some(build_from_pretty("(subtract:int8 $0:int8 2:int8)"))), (3, 1, true, Some(build_from_pretty("(subtract:int8 $0:int8 2:int8)")))],
        )
        .await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        tx_l.push_watermark(1, DataType::Int64, ScalarImpl::Int64(10));
        hash_join.next_unwrap_pending();

        tx_r.push_watermark(1, DataType::Int64, ScalarImpl::Int64(6));
        let output_watermark = hash_join.next_unwrap_ready_watermark()?;
        assert_eq!(
            output_watermark,
            Watermark::new(1, DataType::Int64, ScalarImpl::Int64(4))
        );
        let output_watermark = hash_join.next_unwrap_ready_watermark()?;
        assert_eq!(
            output_watermark,
            Watermark::new(3, DataType::Int64, ScalarImpl::Int64(6))
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        // data "2 3" should have been cleaned
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 6"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        // pending means that state clean is successful, or the executor will yield a chunk "+ 2 3
        // 2 3" here.
        hash_join.next_unwrap_pending();

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join() -> StreamExecutorResult<()> {
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
            create_classical_executor::<{ JoinType::Inner }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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

    #[tokio::test]
    async fn test_streaming_null_safe_hash_inner_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + . 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + . 8
             - . 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + . 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(false, true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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
                + . 6 . 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_semi_join() -> StreamExecutorResult<()> {
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
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             + 6 10",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I  I
             - 6 11",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I  I
             - 6 9",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftSemi }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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
                " I I
                + 2 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 3 6"
            )
        );

        // push the 3rd left chunk (tests forward_exactly_once)
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 6 10"
            )
        );

        // push the 3rd right chunk
        // (tests that no change if there are still matches)
        tx_r.push_chunk(chunk_r3);
        hash_join.next_unwrap_pending();

        // push the 3rd left chunk
        // (tests that deletion occurs when there are no more matches)
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 6 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_null_safe_hash_left_semi_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + . 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + . 8
             - . 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + . 10
             + 6 11",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             + 6 10",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I  I
             - 6 11",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I  I
             - 6 9",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftSemi }>(false, true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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
                " I I
                + 2 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + . 6"
            )
        );

        // push the 3rd left chunk (tests forward_exactly_once)
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 6 10"
            )
        );

        // push the 3rd right chunk
        // (tests that no change if there are still matches)
        tx_r.push_chunk(chunk_r3);
        hash_join.next_unwrap_pending();

        // push the 3rd left chunk
        // (tests that deletion occurs when there are no more matches)
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 6 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::Inner }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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
                " I I I I I I
                + 2 5 2 2 5 1
                + 4 9 4 4 9 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 1 4 1 1 4 4
                + 3 6 3 3 6 5"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_semi_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::LeftSemi }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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
                " I I I
                + 2 5 2
                + 4 9 4"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 1 4 1
                + 3 6 3"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_semi_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::RightSemi }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
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
                " I I I
                + 2 5 1
                + 4 9 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 1 4 4
                + 3 6 5"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_semi_join() -> StreamExecutorResult<()> {
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I
             + 6 10",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I  I
             - 6 11",
        );
        let chunk_l4 = StreamChunk::from_pretty(
            "  I  I
             - 6 9",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::RightSemi }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        hash_join.next_unwrap_pending();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 2 5"
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 3 6"
            )
        );

        // push the 3rd right chunk (tests forward_exactly_once)
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 6 10"
            )
        );

        // push the 3rd left chunk
        // (tests that no change if there are still matches)
        tx_l.push_chunk(chunk_l3);
        hash_join.next_unwrap_pending();

        // push the 3rd right chunk
        // (tests that deletion occurs when there are no more matches)
        tx_l.push_chunk(chunk_l4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 6 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_anti_join() -> StreamExecutorResult<()> {
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
             + 6 11
             + 1 2
             + 1 3",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             + 9 10",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I
             - 1 2",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I I
             - 1 3",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftAnti }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4
                + 2 5
                + 3 6",
            )
        );

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I
                 + 3 8
                 - 3 8",
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 2 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 3 6
                - 1 4"
            )
        );

        // push the 3rd left chunk (tests forward_exactly_once)
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 9 10"
            )
        );

        // push the 3rd right chunk
        // (tests that no change if there are still matches)
        tx_r.push_chunk(chunk_r3);
        hash_join.next_unwrap_pending();

        // push the 4th right chunk
        // (tests that insertion occurs when there are no more matches)
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_anti_join() -> StreamExecutorResult<()> {
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11
             + 1 2
             + 1 3",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I
             + 9 10",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             - 1 2",
        );
        let chunk_l4 = StreamChunk::from_pretty(
            "  I I
             - 1 3",
        );
        let (mut tx_r, mut tx_l, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftAnti }>(false, false, None).await;

        // push the init barrier for left and right
        tx_r.push_barrier(test_epoch(1), false);
        tx_l.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4
                + 2 5
                + 3 6",
            )
        );

        // push the init barrier for left and right
        tx_r.push_barrier(test_epoch(2), false);
        tx_l.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I
                 + 3 8
                 - 3 8",
            )
        );

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 2 5"
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 3 6
                - 1 4"
            )
        );

        // push the 3rd right chunk (tests forward_exactly_once)
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 9 10"
            )
        );

        // push the 3rd left chunk
        // (tests that no change if there are still matches)
        tx_l.push_chunk(chunk_l3);
        hash_join.next_unwrap_pending();

        // push the 4th left chunk
        // (tests that insertion occurs when there are no more matches)
        tx_l.push_chunk(chunk_l4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_barrier() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 6 8
             + 3 8",
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
            create_classical_executor::<{ JoinType::Inner }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push a barrier to left side
        tx_l.push_barrier(test_epoch(2), false);

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);

        // join the first right chunk
        tx_r.push_chunk(chunk_r1);

        // Consume stream chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push a barrier to right side
        tx_r.push_barrier(test_epoch(2), false);

        // get the aligned barrier here
        let expected_epoch = EpochPair::new_test_epoch(test_epoch(2));
        assert!(matches!(
            hash_join.next_unwrap_ready_barrier()?,
            Barrier {
                epoch,
                mutation: None,
                ..
            } if epoch == expected_epoch
        ));

        // join the 2nd left chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 6 8 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10
                + 3 8 3 10
                + 6 8 6 11"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_null_and_barrier() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 .
             + 3 .",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 6 .
             + 3 8",
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
            create_classical_executor::<{ JoinType::Inner }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push a barrier to left side
        tx_l.push_barrier(test_epoch(2), false);

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);

        // join the first right chunk
        tx_r.push_chunk(chunk_r1);

        // Consume stream chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 . 2 7"
            )
        );

        // push a barrier to right side
        tx_r.push_barrier(test_epoch(2), false);

        // get the aligned barrier here
        let expected_epoch = EpochPair::new_test_epoch(test_epoch(2));
        assert!(matches!(
            hash_join.next_unwrap_ready_barrier()?,
            Barrier {
                epoch,
                mutation: None,
                ..
            } if epoch == expected_epoch
        ));

        // join the 2nd left chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 6 . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 3 10
                + 3 . 3 10
                + 6 . 6 11"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join() -> StreamExecutorResult<()> {
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
            create_classical_executor::<{ JoinType::LeftOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . .
                - 3 8 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I
                U- 2 5 . .
                U+ 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I
                U- 3 6 . .
                U+ 3 6 3 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_null_safe_hash_left_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + . 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + . 8
             - . 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + . 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftOuter }>(false, true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + . 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . 8 . .
                - . 8 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I
                U- 2 5 . .
                U+ 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I
                U- . 6 . .
                U+ . 6 . 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join() -> StreamExecutorResult<()> {
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
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::RightOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

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
                + 2 5 2 7
                + . . 4 8
                + . . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10
                - . . 5 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::LeftOuter }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 1 4 1 . . .
                + 2 5 2 . . .
                + 3 6 3 . . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 4 9 4 . . .
                + 5 10 5 . . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I I I
                U- 2 5 2 . . .
                U+ 2 5 2 2 5 1
                U- 4 9 4 . . .
                U+ 4 9 4 4 9 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I I I
                U- 1 4 1 . . .
                U+ 1 4 1 1 4 4
                U- 3 6 3 . . .
                U+ 3 6 3 3 6 5"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5
             + 7 7 6",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::RightOuter }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I I I
                + 2 5 2 2 5 1
                + 4 9 4 4 9 2
                + . . . 6 9 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I I I
                + 1 4 1 1 4 4
                + 3 6 3 3 6 5
                + . . . 7 7 6"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join() -> StreamExecutorResult<()> {
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
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::FullOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . .
                - 3 8 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I
                U-  2 5 . .
                U+  2 5 2 7
                +  . . 4 8
                +  . . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10
                - . . 5 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join_update() -> StreamExecutorResult<()> {
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::FullOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_chunk(StreamChunk::from_pretty(
            "  I I
             + 1 1
            ",
        ));
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 . ."
            )
        );

        tx_r.push_chunk(StreamChunk::from_pretty(
            "  I I
             + 1 1
            ",
        ));
        let chunk = hash_join.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                U- 1 1 . .
                U+ 1 1 1 1"
            )
        );

        tx_l.push_chunk(StreamChunk::from_pretty(
            "   I I
              - 1 1
              + 1 2
            ",
        ));
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        let chunk = chunk.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 1 1 1 1
                + 1 2 1 1
                "
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join_with_nonequi_condition() -> StreamExecutorResult<()>
    {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6
             + 3 7",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8
             - 1 4", // delete row to cause an empty JoinHashEntry
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 6
             + 4 8
             + 3 4",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10
             + 1 2",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::FullOuter }>(true, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . .
                + 3 7 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . .
                - 3 8 . .
                - 1 4 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I
                U-  2 5 . .
                U+  2 5 2 6
                +  . . 4 8
                +  . . 3 4" /* regression test (#2420): 3 4 should be forwarded only once
                             * despite matching on eq join on 2
                             * entries */
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10
                - . . 5 10
                + . . 1 2" /* regression test (#2420): 1 2 forwarded even if matches on an empty
                            * join entry */
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_nonequi_condition() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 10
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
            create_classical_executor::<{ JoinType::Inner }>(true, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        hash_join.next_unwrap_pending();

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

    #[tokio::test]
    async fn test_streaming_hash_join_watermark() -> StreamExecutorResult<()> {
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(true, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_int64_watermark(0, 100);

        tx_l.push_int64_watermark(0, 200);

        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_r.push_int64_watermark(0, 50);

        let w1 = hash_join.next().await.unwrap().unwrap();
        let w1 = w1.as_watermark().unwrap();

        let w2 = hash_join.next().await.unwrap().unwrap();
        let w2 = w2.as_watermark().unwrap();

        tx_r.push_int64_watermark(0, 100);

        let w3 = hash_join.next().await.unwrap().unwrap();
        let w3 = w3.as_watermark().unwrap();

        let w4 = hash_join.next().await.unwrap().unwrap();
        let w4 = w4.as_watermark().unwrap();

        assert_eq!(
            w1,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(50)
            }
        );

        assert_eq!(
            w2,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(50)
            }
        );

        assert_eq!(
            w3,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        assert_eq!(
            w4,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        Ok(())
    }
}
