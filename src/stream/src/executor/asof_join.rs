// Copyright 2025 RisingWave Labs
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
use std::ops::Bound;
use std::time::Duration;

use either::Either;
use itertools::Itertools;
use multimap::MultiMap;
use risingwave_common::array::Op;
use risingwave_common::hash::{HashKey, NullBitmap};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use tokio::time::Instant;

use self::builder::JoinChunkBuilder;
use super::barrier_align::*;
use super::join::hash_join::*;
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

    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.ht.init(epoch).await
    }
}

/// `AsOfJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct AsOfJoinExecutor<K: HashKey, S: StateStore, const T: AsOfJoinTypePrimitive> {
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

    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time
    chunk_size: usize,
    /// Count the messages received, clear to 0 when counted to `EVICT_EVERY_N_MESSAGES`
    cnt_rows_received: u32,

    /// watermark column index -> `BufferedWatermarks`
    watermark_buffers: BTreeMap<usize, BufferedWatermarks<SideTypePrimitive>>,

    high_join_amplification_threshold: usize,
    /// `AsOf` join description
    asof_desc: AsOfDesc,
}

impl<K: HashKey, S: StateStore, const T: AsOfJoinTypePrimitive> std::fmt::Debug
    for AsOfJoinExecutor<K, S, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsOfJoinExecutor")
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

impl<K: HashKey, S: StateStore, const T: AsOfJoinTypePrimitive> Execute
    for AsOfJoinExecutor<K, S, T>
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

struct EqJoinArgs<'a, K: HashKey, S: StateStore> {
    ctx: &'a ActorContextRef,
    side_l: &'a mut JoinSide<K, S>,
    side_r: &'a mut JoinSide<K, S>,
    asof_desc: &'a AsOfDesc,
    actual_output_data_types: &'a [DataType],
    // inequality_watermarks: &'a Watermark,
    chunk: StreamChunk,
    chunk_size: usize,
    cnt_rows_received: &'a mut u32,
    high_join_amplification_threshold: usize,
}

impl<K: HashKey, S: StateStore, const T: AsOfJoinTypePrimitive> AsOfJoinExecutor<K, S, T> {
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
        state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        high_join_amplification_threshold: usize,
        asof_desc: AsOfDesc,
    ) -> Self {
        let side_l_column_n = input_l.schema().len();

        let schema_fields = [
            input_l.schema().fields.clone(),
            input_r.schema().fields.clone(),
        ]
        .concat();

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

        let state_join_key_indices_l = params_l.join_key_indices;
        let state_join_key_indices_r = params_r.join_key_indices;

        // If pk is contained in join key.
        let pk_contained_in_jk_l =
            is_subset(state_pk_indices_l.clone(), state_join_key_indices_l.clone());
        let pk_contained_in_jk_r =
            is_subset(state_pk_indices_r.clone(), state_join_key_indices_r.clone());

        let join_key_data_types_l = state_join_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();

        let join_key_data_types_r = state_join_key_indices_r
            .iter()
            .map(|idx| state_all_data_types_r[*idx].clone())
            .collect_vec();

        assert_eq!(join_key_data_types_l, join_key_data_types_r);

        let null_matched = K::Bitmap::from_bool_vec(null_safe);

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

        // handle inequality watermarks
        // https://github.com/risingwavelabs/risingwave/issues/18503
        // let inequality_watermarks = None;
        let watermark_buffers = BTreeMap::new();

        let inequal_key_idx_l = Some(asof_desc.left_idx);
        let inequal_key_idx_r = Some(asof_desc.right_idx);

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
                    None,
                    null_matched.clone(),
                    pk_contained_in_jk_l,
                    inequal_key_idx_l,
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "left",
                ),
                join_key_indices: state_join_key_indices_l,
                all_data_types: state_all_data_types_l,
                i2o_mapping: left_to_output,
                i2o_mapping_indexed: l2o_indexed,
                start_pos: 0,
                need_degree_table: false,
            },
            side_r: JoinSide {
                ht: JoinHashMap::new(
                    watermark_epoch,
                    join_key_data_types_r,
                    state_join_key_indices_r.clone(),
                    state_all_data_types_r.clone(),
                    state_table_r,
                    params_r.deduped_pk_indices,
                    None,
                    null_matched,
                    pk_contained_in_jk_r,
                    inequal_key_idx_r,
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
                need_degree_table: false,
            },
            metrics,
            chunk_size,
            cnt_rows_received: 0,
            watermark_buffers,
            high_join_amplification_threshold,
            asof_desc,
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
        let actor_id = self.ctx.id;

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        let first_epoch = barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        self.side_l.init(first_epoch).await?;
        self.side_r.init(first_epoch).await?;

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
                    for watermark_to_emit in self.handle_watermark(SideType::Left, watermark)? {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::WatermarkRight(watermark) => {
                    for watermark_to_emit in self.handle_watermark(SideType::Right, watermark)? {
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
                        asof_desc: &self.asof_desc,
                        actual_output_data_types: &self.actual_output_data_types,
                        // inequality_watermarks: &self.inequality_watermarks,
                        chunk,
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
                        asof_desc: &self.asof_desc,
                        actual_output_data_types: &self.actual_output_data_types,
                        // inequality_watermarks: &self.inequality_watermarks,
                        chunk,
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
                    let (left_post_commit, right_post_commit) =
                        self.flush_data(barrier.epoch).await?;

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);
                    yield Message::Barrier(barrier);

                    // Update the vnode bitmap for state tables of both sides if asked.
                    right_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?;
                    if left_post_commit
                        .post_yield_barrier(update_vnode_bitmap)
                        .await?
                        .unwrap_or(false)
                    {
                        self.watermark_buffers
                            .values_mut()
                            .for_each(|buffers| buffers.clear());
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
                }
            }
            start_time = Instant::now();
        }
    }

    async fn flush_data(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<(
        JoinHashMapPostCommit<'_, K, S>,
        JoinHashMapPostCommit<'_, K, S>,
    )> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        let left = self.side_l.ht.flush(epoch).await?;
        let right = self.side_r.ht.flush(epoch).await?;
        Ok((left, right))
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

    fn handle_watermark(
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

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_left(args: EqJoinArgs<'_, K, S>) {
        let EqJoinArgs {
            ctx: _,
            side_l,
            side_r,
            asof_desc,
            actual_output_data_types,
            // inequality_watermarks,
            chunk,
            chunk_size,
            cnt_rows_received,
            high_join_amplification_threshold: _,
        } = args;

        let (side_update, side_match) = (side_l, side_r);

        let mut join_chunk_builder =
            JoinChunkBuilder::<T, { SideType::Left }>::new(JoinStreamChunkBuilder::new(
                chunk_size,
                actual_output_data_types.to_vec(),
                side_update.i2o_mapping.clone(),
                side_match.i2o_mapping.clone(),
            ));

        let keys = K::build_many(&side_update.join_key_indices, chunk.data_chunk());
        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.iter()) {
            let Some((op, row)) = r else {
                continue;
            };
            Self::evict_cache(side_update, side_match, cnt_rows_received);

            let matched_rows = if !side_update.ht.check_inequal_key_null(&row) {
                Self::hash_eq_match(key, &mut side_match.ht).await?
            } else {
                None
            };
            let inequal_key = side_update.ht.serialize_inequal_key_from_row(row);

            if let Some(matched_rows) = matched_rows {
                let matched_row_by_inequality = match asof_desc.inequality_type {
                    AsOfInequalityType::Lt => matched_rows.lower_bound_by_inequality(
                        Bound::Excluded(&inequal_key),
                        &side_match.all_data_types,
                    ),
                    AsOfInequalityType::Le => matched_rows.lower_bound_by_inequality(
                        Bound::Included(&inequal_key),
                        &side_match.all_data_types,
                    ),
                    AsOfInequalityType::Gt => matched_rows.upper_bound_by_inequality(
                        Bound::Excluded(&inequal_key),
                        &side_match.all_data_types,
                    ),
                    AsOfInequalityType::Ge => matched_rows.upper_bound_by_inequality(
                        Bound::Included(&inequal_key),
                        &side_match.all_data_types,
                    ),
                };
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if let Some(matched_row_by_inequality) = matched_row_by_inequality {
                            let matched_row = matched_row_by_inequality?;

                            if let Some(chunk) =
                                join_chunk_builder.with_match_on_insert(&row, &matched_row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                        side_update.ht.insert_row(key, row)?;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(matched_row_by_inequality) = matched_row_by_inequality {
                            let matched_row = matched_row_by_inequality?;

                            if let Some(chunk) =
                                join_chunk_builder.with_match_on_delete(&row, &matched_row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                        side_update.ht.delete_row(key, row)?;
                    }
                }
                // Insert back the state taken from ht.
                side_match.ht.update_state(key, matched_rows);
            } else {
                // Row which violates null-safe bitmap will never be matched so we need not
                // store.
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                    }
                }
            }
        }
        if let Some(chunk) = join_chunk_builder.take() {
            yield chunk;
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_right(args: EqJoinArgs<'_, K, S>) {
        let EqJoinArgs {
            ctx,
            side_l,
            side_r,
            asof_desc,
            actual_output_data_types,
            // inequality_watermarks,
            chunk,
            chunk_size,
            cnt_rows_received,
            high_join_amplification_threshold,
        } = args;

        let (side_update, side_match) = (side_r, side_l);

        let mut join_chunk_builder = JoinStreamChunkBuilder::new(
            chunk_size,
            actual_output_data_types.to_vec(),
            side_update.i2o_mapping.clone(),
            side_match.i2o_mapping.clone(),
        );

        let join_matched_rows_metrics = ctx
            .streaming_metrics
            .join_matched_join_keys
            .with_guarded_label_values(&[
                &ctx.id.to_string(),
                &ctx.fragment_id.to_string(),
                &side_update.ht.table_id().to_string(),
            ]);

        let keys = K::build_many(&side_update.join_key_indices, chunk.data_chunk());
        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.iter()) {
            let Some((op, row)) = r else {
                continue;
            };
            let mut join_matched_rows_cnt = 0;

            Self::evict_cache(side_update, side_match, cnt_rows_received);

            let matched_rows = if !side_update.ht.check_inequal_key_null(&row) {
                Self::hash_eq_match(key, &mut side_match.ht).await?
            } else {
                None
            };
            let inequal_key = side_update.ht.serialize_inequal_key_from_row(row);

            if let Some(matched_rows) = matched_rows {
                let update_rows = Self::hash_eq_match(key, &mut side_update.ht).await?.expect("None is not expected because we have checked null in key when getting matched_rows");
                let right_inequality_index = update_rows.inequality_index();
                let (row_to_delete_r, row_to_insert_r) =
                    if let Some(pks) = right_inequality_index.get(&inequal_key) {
                        assert!(!pks.is_empty());
                        let row_pk = side_update.ht.serialize_pk_from_row(row);
                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                // If there are multiple rows match the inequality key in the right table, we use one with smallest pk.
                                let smallest_pk = pks.first_key_sorted().unwrap();
                                if smallest_pk > &row_pk {
                                    // smallest_pk is in the cache index, so it must exist in the cache.
                                    if let Some(to_delete_row) = update_rows
                                        .get_by_indexed_pk(smallest_pk, &side_update.all_data_types)
                                    {
                                        (
                                            Some(Either::Left(to_delete_row?.row)),
                                            Some(Either::Right(row)),
                                        )
                                    } else {
                                        // Something wrong happened. Ignore this row in non strict consistency mode.
                                        (None, None)
                                    }
                                } else {
                                    // No affected row in the right table.
                                    (None, None)
                                }
                            }
                            Op::Delete | Op::UpdateDelete => {
                                let smallest_pk = pks.first_key_sorted().unwrap();
                                if smallest_pk == &row_pk {
                                    if let Some(second_smallest_pk) = pks.second_key_sorted() {
                                        if let Some(to_insert_row) = update_rows.get_by_indexed_pk(
                                            second_smallest_pk,
                                            &side_update.all_data_types,
                                        ) {
                                            (
                                                Some(Either::Right(row)),
                                                Some(Either::Left(to_insert_row?.row)),
                                            )
                                        } else {
                                            // Something wrong happened. Ignore this row in non strict consistency mode.
                                            (None, None)
                                        }
                                    } else {
                                        (Some(Either::Right(row)), None)
                                    }
                                } else {
                                    // No affected row in the right table.
                                    (None, None)
                                }
                            }
                        }
                    } else {
                        match op {
                            // Decide the row_to_delete later
                            Op::Insert | Op::UpdateInsert => (None, Some(Either::Right(row))),
                            // Decide the row_to_insert later
                            Op::Delete | Op::UpdateDelete => (Some(Either::Right(row)), None),
                        }
                    };

                // 4 cases for row_to_delete_r and row_to_insert_r:
                // 1. Some(_), Some(_): delete row_to_delete_r and insert row_to_insert_r
                // 2. None, Some(_)   : row_to_delete to be decided by the nearest inequality key
                // 3. Some(_), None   : row_to_insert to be decided by the nearest inequality key
                // 4. None, None      : do nothing
                if row_to_delete_r.is_none() && row_to_insert_r.is_none() {
                    // no row to delete or insert.
                } else {
                    let prev_inequality_key =
                        right_inequality_index.upper_bound_key(Bound::Excluded(&inequal_key));
                    let next_inequality_key =
                        right_inequality_index.lower_bound_key(Bound::Excluded(&inequal_key));
                    let affected_row_r = match asof_desc.inequality_type {
                        AsOfInequalityType::Lt | AsOfInequalityType::Le => next_inequality_key
                            .and_then(|k| {
                                update_rows.get_first_by_inequality(k, &side_update.all_data_types)
                            }),
                        AsOfInequalityType::Gt | AsOfInequalityType::Ge => prev_inequality_key
                            .and_then(|k| {
                                update_rows.get_first_by_inequality(k, &side_update.all_data_types)
                            }),
                    }
                    .transpose()?
                    .map(|r| Either::Left(r.row));

                    let (row_to_delete_r, row_to_insert_r) =
                        match (&row_to_delete_r, &row_to_insert_r) {
                            (Some(_), Some(_)) => (row_to_delete_r, row_to_insert_r),
                            (None, Some(_)) => (affected_row_r, row_to_insert_r),
                            (Some(_), None) => (row_to_delete_r, affected_row_r),
                            (None, None) => unreachable!(),
                        };
                    let range = match asof_desc.inequality_type {
                        AsOfInequalityType::Lt => (
                            prev_inequality_key.map_or_else(|| Bound::Unbounded, Bound::Included),
                            Bound::Excluded(&inequal_key),
                        ),
                        AsOfInequalityType::Le => (
                            prev_inequality_key.map_or_else(|| Bound::Unbounded, Bound::Excluded),
                            Bound::Included(&inequal_key),
                        ),
                        AsOfInequalityType::Gt => (
                            Bound::Excluded(&inequal_key),
                            next_inequality_key.map_or_else(|| Bound::Unbounded, Bound::Included),
                        ),
                        AsOfInequalityType::Ge => (
                            Bound::Included(&inequal_key),
                            next_inequality_key.map_or_else(|| Bound::Unbounded, Bound::Excluded),
                        ),
                    };

                    let rows_l =
                        matched_rows.range_by_inequality(range, &side_match.all_data_types);
                    for row_l in rows_l {
                        join_matched_rows_cnt += 1;
                        let row_l = row_l?.row;
                        if let Some(row_to_delete_r) = &row_to_delete_r {
                            if let Some(chunk) =
                                join_chunk_builder.append_row(Op::Delete, row_to_delete_r, &row_l)
                            {
                                yield chunk;
                            }
                        } else if is_as_of_left_outer(T) {
                            if let Some(chunk) =
                                join_chunk_builder.append_row_matched(Op::Delete, &row_l)
                            {
                                yield chunk;
                            }
                        }
                        if let Some(row_to_insert_r) = &row_to_insert_r {
                            if let Some(chunk) =
                                join_chunk_builder.append_row(Op::Insert, row_to_insert_r, &row_l)
                            {
                                yield chunk;
                            }
                        } else if is_as_of_left_outer(T) {
                            if let Some(chunk) =
                                join_chunk_builder.append_row_matched(Op::Insert, &row_l)
                            {
                                yield chunk;
                            }
                        }
                    }
                }
                // Insert back the state taken from ht.
                side_match.ht.update_state(key, matched_rows);
                side_update.ht.update_state(key, update_rows);

                match op {
                    Op::Insert | Op::UpdateInsert => {
                        side_update.ht.insert_row(key, row)?;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        side_update.ht.delete_row(key, row)?;
                    }
                }
            } else {
                // Row which violates null-safe bitmap will never be matched so we need not
                // store.
                // Noop here because we only support left outer AsOf join.
            }
            join_matched_rows_metrics.observe(join_matched_rows_cnt as _);
            if join_matched_rows_cnt > high_join_amplification_threshold {
                let join_key_data_types = side_update.ht.join_key_data_types();
                let key = key.deserialize(join_key_data_types)?;
                tracing::warn!(target: "high_join_amplification",
                    matched_rows_len = join_matched_rows_cnt,
                    update_table_id = side_update.ht.table_id(),
                    match_table_id = side_match.ht.table_id(),
                    join_key = ?key,
                    actor_id = ctx.id,
                    fragment_id = ctx.fragment_id,
                    "large rows matched for join key when AsOf join updating right side",
                );
            }
        }
        if let Some(chunk) = join_chunk_builder.take() {
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::hash::Key64;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(table_id),
                column_descs,
                order_types.to_vec(),
                pk_indices.to_vec(),
                0,
            ),
            mem_state.clone(),
            None,
        )
        .await
    }

    async fn create_executor<const T: AsOfJoinTypePrimitive>(
        asof_desc: AsOfDesc,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![1]);
        let params_l = JoinParams::new(vec![0], vec![1]);
        let params_r = JoinParams::new(vec![0], vec![1]);

        let mem_state = MemoryStateStore::new();

        let state_l = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, asof_desc.left_idx, 1],
            0,
        )
        .await;

        let state_r = create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, asof_desc.right_idx, 1],
            1,
        )
        .await;

        let schema: Schema = [source_l.schema().fields(), source_r.schema().fields()]
            .concat()
            .into_iter()
            .collect();
        let schema_len = schema.len();
        let info = ExecutorInfo {
            schema,
            pk_indices: vec![1],
            identity: "HashJoinExecutor".to_owned(),
        };

        let executor = AsOfJoinExecutor::<Key64, MemoryStateStore, T>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![false],
            (0..schema_len).collect_vec(),
            state_l,
            state_r,
            Arc::new(AtomicU64::new(0)),
            Arc::new(StreamingMetrics::unused()),
            1024,
            2048,
            asof_desc,
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_as_of_inner_join() -> StreamExecutorResult<()> {
        let asof_desc = AsOfDesc {
            left_idx: 0,
            right_idx: 2,
            inequality_type: AsOfInequalityType::Lt,
        };

        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 7
             + 2 5 8
             + 3 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 3 8 1
             - 3 8 1",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 1 7
             + 2 2 1
             + 2 3 4
             + 2 4 2
             + 6 1 9
             + 6 2 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             - 2 3 4",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I I
             + 2 3 3",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I I
             - 2 5 8",
        );
        let chunk_l4 = StreamChunk::from_pretty(
            "  I I I
             + 6 3 1
             + 6 4 1",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I I I
             - 6 1 9",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor::<{ AsOfJoinType::Inner }>(asof_desc).await;

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
                + 2 5 8 2 1 7
                - 2 5 8 2 1 7
                + 2 5 8 2 3 4"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 3 4
                + 2 5 8 2 1 7"
            )
        );

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 1 7
                + 2 5 8 2 3 3"
            )
        );

        // push the 3rd left chunk
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 3 3"
            )
        );

        // push the 4th left chunk
        tx_l.push_chunk(chunk_l4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 6 3 1 6 1 9
                + 6 4 1 6 1 9"
            )
        );

        // push the 4th right chunk
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 6 3 1 6 1 9
                + 6 3 1 6 2 9
                - 6 4 1 6 1 9
                + 6 4 1 6 2 9"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_as_of_left_outer_join() -> StreamExecutorResult<()> {
        let asof_desc = AsOfDesc {
            left_idx: 1,
            right_idx: 2,
            inequality_type: AsOfInequalityType::Ge,
        };

        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 7
             + 2 5 8
             + 3 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 3 8 1
             - 3 8 1",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 3 4
             + 2 2 5
             + 2 1 5
             + 6 1 8
             + 6 2 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             - 2 3 4
             - 2 1 5
             - 2 2 5",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I I
             + 6 8 9",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I I
             - 6 1 8",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor::<{ AsOfJoinType::LeftOuter }>(asof_desc).await;

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
                + 1 4 7 . . .
                + 2 5 8 . . .
                + 3 6 9 . . ."
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
                " I I I I I I
                + 3 8 1 . . .
                - 3 8 1 . . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 . . .
                + 2 5 8 2 3 4
                - 2 5 8 2 3 4
                + 2 5 8 2 2 5
                - 2 5 8 2 2 5
                + 2 5 8 2 1 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 1 5
                + 2 5 8 2 2 5
                - 2 5 8 2 2 5
                + 2 5 8 . . ."
            )
        );

        // push the 3rd left chunk
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 6 8 9 6 1 8"
            )
        );

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 6 8 9 6 1 8
                + 6 8 9 . . ."
            )
        );
        Ok(())
    }
}
