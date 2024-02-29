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
use std::collections::HashMap;
use std::pin::pin;
use std::sync::Arc;

use either::Either;
use futures::stream::{self, PollNext};
use futures::{pin_mut, StreamExt, TryStreamExt};
use futures_async_stream::{for_await, try_stream};
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use lru::DefaultHasher;
use risingwave_common::array::{ArrayImpl, Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::estimate_size::{EstimateSize, KvSize};
use risingwave_common::hash::{HashKey, NullBitmap};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::join::{JoinType, JoinTypePrimitive};
use super::{
    Barrier, Execute, ExecutorInfo, Message, MessageStream, StreamExecutorError,
    StreamExecutorResult,
};
use crate::cache::{cache_may_stale, new_with_hasher_in, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::executor::join::builder::JoinStreamChunkBuilder;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, Executor, Watermark};
use crate::task::AtomicU64Ref;

pub struct TemporalJoinExecutor<K: HashKey, S: StateStore, const T: JoinTypePrimitive> {
    ctx: ActorContextRef,
    #[allow(dead_code)]
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: TemporalSide<K, S>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    chunk_size: usize,
    // TODO: update metrics
    #[allow(dead_code)]
    metrics: Arc<StreamingMetrics>,
}

#[derive(Default)]
pub struct JoinEntry {
    /// pk -> row
    cached: HashMap<OwnedRow, OwnedRow>,
    kv_heap_size: KvSize,
}

impl EstimateSize for JoinEntry {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl JoinEntry {
    /// Insert into the cache.
    pub fn insert(&mut self, key: OwnedRow, value: OwnedRow) {
        // Lookup might refill the cache before the `insert` messages from the temporal side
        // upstream.
        if let Entry::Vacant(e) = self.cached.entry(key) {
            self.kv_heap_size.add(e.key(), &value);
            e.insert(value);
        }
    }

    /// Delete from the cache.
    pub fn remove(&mut self, key: &OwnedRow) {
        if let Some(value) = self.cached.remove(key) {
            self.kv_heap_size.sub(key, &value);
        } else {
            panic!("key {:?} should be in the cache", key);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cached.is_empty()
    }
}

struct TemporalSide<K: HashKey, S: StateStore> {
    source: StorageTable<S>,
    table_stream_key_indices: Vec<usize>,
    table_output_indices: Vec<usize>,
    cache: ManagedLruCache<K, JoinEntry, DefaultHasher, SharedStatsAlloc<Global>>,
    ctx: ActorContextRef,
    join_key_data_types: Vec<DataType>,
}

impl<K: HashKey, S: StateStore> TemporalSide<K, S> {
    /// Fetch records from temporal side table and ensure the entry in the cache.
    /// If already exists, the entry will be promoted.
    async fn fetch_or_promote_keys(
        &mut self,
        keys: impl Iterator<Item = &K>,
        epoch: HummockEpoch,
    ) -> StreamExecutorResult<()> {
        let table_id_str = self.source.table_id().to_string();
        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.id.to_string();

        let mut futs = Vec::with_capacity(keys.size_hint().1.unwrap_or(0));
        for key in keys {
            self.ctx
                .streaming_metrics
                .temporal_join_total_query_cache_count
                .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                .inc();

            if self.cache.get(key).is_none() {
                self.ctx
                    .streaming_metrics
                    .temporal_join_cache_miss_count
                    .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                    .inc();

                futs.push(async {
                    let pk_prefix = key.deserialize(&self.join_key_data_types)?;

                    let iter = self
                        .source
                        .batch_iter_with_pk_bounds(
                            HummockReadEpoch::NoWait(epoch),
                            &pk_prefix,
                            ..,
                            false,
                            PrefetchOptions::default(),
                        )
                        .await?;

                    let mut entry = JoinEntry::default();

                    pin_mut!(iter);
                    while let Some(row) = iter.next_row().await? {
                        entry.insert(
                            row.as_ref()
                                .project(&self.table_stream_key_indices)
                                .into_owned_row(),
                            row.project(&self.table_output_indices).into_owned_row(),
                        );
                    }
                    let key = key.clone();
                    Ok((key, entry)) as StreamExecutorResult<_>
                });
            }
        }

        #[for_await]
        for res in stream::iter(futs).buffered(16) {
            let (key, entry) = res?;
            self.cache.put(key, entry);
        }

        Ok(())
    }

    fn force_peek(&self, key: &K) -> &JoinEntry {
        self.cache.peek(key).expect("key should exists")
    }

    fn update(
        &mut self,
        chunks: Vec<StreamChunk>,
        join_keys: &[usize],
        right_stream_key_indices: &[usize],
    ) -> StreamExecutorResult<()> {
        for chunk in chunks {
            let keys = K::build(join_keys, chunk.data_chunk())?;
            for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
                let Some((op, row)) = r else {
                    continue;
                };
                if self.cache.contains(&key) {
                    // Update cache
                    let mut entry = self.cache.get_mut(&key).unwrap();
                    let stream_key = row.project(right_stream_key_indices).into_owned_row();
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            entry.insert(stream_key, row.into_owned_row())
                        }
                        Op::Delete | Op::UpdateDelete => entry.remove(&stream_key),
                    };
                }
            }
        }
        Ok(())
    }
}

enum InternalMessage {
    Chunk(StreamChunk),
    Barrier(Vec<StreamChunk>, Barrier),
    WaterMark(Watermark),
}

#[try_stream(ok = StreamChunk, error = StreamExecutorError)]
async fn chunks_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(_) => {
                // ignore
            }
            Message::Chunk(c) => yield c,
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
async fn internal_messages_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(w) => {
                yield InternalMessage::WaterMark(w);
            }
            Message::Chunk(c) => yield InternalMessage::Chunk(c),
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

// Align the left and right inputs according to their barriers,
// such that in the produced stream, an aligned interval starts with
// any number of `InternalMessage::Chunk(left_chunk)` and followed by
// `InternalMessage::Barrier(right_chunks, barrier)`.
#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
async fn align_input(left: Executor, right: Executor) {
    let mut left = pin!(left.execute());
    let mut right = pin!(right.execute());
    // Keep producing intervals until stream exhaustion or errors.
    loop {
        let mut right_chunks = vec![];
        // Produce an aligned interval.
        'inner: loop {
            let mut combined = stream::select_with_strategy(
                left.by_ref().map(Either::Left),
                right.by_ref().map(Either::Right),
                |_: &mut ()| PollNext::Left,
            );
            match combined.next().await {
                Some(Either::Left(Ok(Message::Chunk(c)))) => yield InternalMessage::Chunk(c),
                Some(Either::Right(Ok(Message::Chunk(c)))) => right_chunks.push(c),
                Some(Either::Left(Ok(Message::Barrier(b)))) => {
                    let mut remain = chunks_until_barrier(right.by_ref(), b.clone())
                        .try_collect()
                        .await?;
                    right_chunks.append(&mut remain);
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Right(Ok(Message::Barrier(b)))) => {
                    #[for_await]
                    for internal_message in
                        internal_messages_until_barrier(left.by_ref(), b.clone())
                    {
                        yield internal_message?;
                    }
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Left(Err(e)) | Either::Right(Err(e))) => return Err(e),
                Some(Either::Left(Ok(Message::Watermark(w)))) => {
                    yield InternalMessage::WaterMark(w);
                }
                Some(Either::Right(Ok(Message::Watermark(_)))) => {
                    // ignore right side watermark
                }
                None => return Ok(()),
            }
        }
    }
}

mod phase1 {
    use futures_async_stream::try_stream;
    use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::hash::{HashKey, NullBitmap};
    use risingwave_common::row::{self, Row, RowExt};
    use risingwave_common::types::{DataType, DatumRef};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_hummock_sdk::HummockEpoch;
    use risingwave_storage::StateStore;

    use super::{StreamExecutorError, TemporalSide};

    pub(super) trait Phase1Evaluation {
        /// Called when a matched row is found.
        #[must_use = "consume chunk if produced"]
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk>;

        /// Called when all matched rows of a join key are appended.
        #[must_use = "consume chunk if produced"]
        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            matched: bool,
        ) -> Option<StreamChunk>;
    }

    pub(super) struct Inner;
    pub(super) struct LeftOuter;
    pub(super) struct LeftOuterWithCond;

    impl Phase1Evaluation for Inner {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            _builder: &mut StreamChunkBuilder,
            _op: Op,
            _left_row: impl Row,
            _right_size: usize,
            _matched: bool,
        ) -> Option<StreamChunk> {
            None
        }
    }

    impl Phase1Evaluation for LeftOuter {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            matched: bool,
        ) -> Option<StreamChunk> {
            if !matched {
                // If no rows matched, a marker row should be inserted.
                builder.append_row(
                    op,
                    left_row.chain(row::repeat_n(DatumRef::None, right_size)),
                )
            } else {
                None
            }
        }
    }

    impl Phase1Evaluation for LeftOuterWithCond {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            _matched: bool,
        ) -> Option<StreamChunk> {
            // A marker row should always be inserted and mark as invisible for non-lookup filters evaluation.
            // The row will be converted to visible in the further steps if no rows matched after all filters evaluated.
            builder.append_row_invisible(
                op,
                left_row.chain(row::repeat_n(DatumRef::None, right_size)),
            )
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_chunk<'a, K: HashKey, S: StateStore, E: Phase1Evaluation>(
        chunk_size: usize,
        right_size: usize,
        full_schema: Vec<DataType>,
        epoch: HummockEpoch,
        left_join_keys: &'a [usize],
        right_table: &'a mut TemporalSide<K, S>,
        null_matched: &'a K::Bitmap,
        chunk: StreamChunk,
    ) {
        let mut builder = StreamChunkBuilder::new(chunk_size, full_schema);
        let keys = K::build(left_join_keys, chunk.data_chunk())?;
        let to_fetch_keys = chunk
            .visibility()
            .iter()
            .zip_eq_debug(keys.iter())
            .filter_map(|(vis, key)| if vis { Some(key) } else { None });
        right_table
            .fetch_or_promote_keys(to_fetch_keys, epoch)
            .await?;
        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
            let Some((op, left_row)) = r else {
                continue;
            };
            let mut matched = false;
            if key.null_bitmap().is_subset(null_matched)
                && let join_entry = right_table.force_peek(&key)
                && !join_entry.is_empty()
            {
                matched = true;
                for right_row in join_entry.cached.values() {
                    if let Some(chunk) =
                        E::append_matched_row(op, &mut builder, left_row, right_row)
                    {
                        yield chunk;
                    }
                }
            }
            if let Some(chunk) = E::match_end(&mut builder, op, left_row, right_size, matched) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> TemporalJoinExecutor<K, S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        left: Executor,
        right: Executor,
        table: StorageTable<S>,
        left_join_keys: Vec<usize>,
        right_join_keys: Vec<usize>,
        null_safe: Vec<bool>,
        condition: Option<NonStrictExpression>,
        output_indices: Vec<usize>,
        table_output_indices: Vec<usize>,
        table_stream_key_indices: Vec<usize>,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        join_key_data_types: Vec<DataType>,
    ) -> Self {
        let alloc = StatsAlloc::new(Global).shared();

        let metrics_info = MetricsInfo::new(
            metrics.clone(),
            table.table_id().table_id,
            ctx.id,
            "temporal join",
        );

        let cache = new_with_hasher_in(
            watermark_epoch,
            metrics_info,
            DefaultHasher::default(),
            alloc,
        );

        Self {
            ctx: ctx.clone(),
            info,
            left,
            right,
            right_table: TemporalSide {
                source: table,
                table_stream_key_indices,
                table_output_indices,
                cache,
                ctx,
                join_key_data_types,
            },
            left_join_keys,
            right_join_keys,
            null_safe,
            condition,
            output_indices,
            chunk_size,
            metrics,
        }
    }

    fn apply_indices_map(chunk: StreamChunk, indices: &[usize]) -> StreamChunk {
        let (data_chunk, ops) = chunk.into_parts();
        let (columns, vis) = data_chunk.into_parts();
        let output_columns = indices
            .iter()
            .cloned()
            .map(|idx| columns[idx].clone())
            .collect();
        StreamChunk::with_visibility(ops, output_columns, vis)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let right_size = self.right.schema().len();

        let (left_map, _right_map) = JoinStreamChunkBuilder::get_i2o_mapping(
            &self.output_indices,
            self.left.schema().len(),
            right_size,
        );

        let left_to_output: HashMap<usize, usize> = HashMap::from_iter(left_map.iter().cloned());

        let right_stream_key_indices = self.right.pk_indices().to_vec();

        let null_matched = K::Bitmap::from_bool_vec(self.null_safe);

        let mut prev_epoch = None;

        let table_id_str = self.right_table.source.table_id().to_string();
        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.fragment_id.to_string();
        let full_schema: Vec<_> = self
            .left
            .schema()
            .data_types()
            .into_iter()
            .chain(self.right.schema().data_types().into_iter())
            .collect();

        #[for_await]
        for msg in align_input(self.left, self.right) {
            self.right_table.cache.evict();
            self.ctx
                .streaming_metrics
                .temporal_join_cached_entry_count
                .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                .set(self.right_table.cache.len() as i64);
            match msg? {
                InternalMessage::WaterMark(watermark) => {
                    let output_watermark_col_idx = *left_to_output.get(&watermark.col_idx).unwrap();
                    yield Message::Watermark(watermark.with_idx(output_watermark_col_idx));
                }
                InternalMessage::Chunk(chunk) => {
                    let epoch = prev_epoch.expect("Chunk data should come after some barrier.");

                    let full_schema = full_schema.clone();

                    if T == JoinType::Inner {
                        let st1 = phase1::handle_chunk::<K, S, phase1::Inner>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            epoch,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &null_matched,
                            chunk,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = if let Some(ref cond) = self.condition {
                                let (data_chunk, ops) = chunk.into_parts();
                                let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                                let ArrayImpl::Bool(passed_bitmap) = &*passed_bitmap else {
                                    panic!("unmatched type: filter expr returns a non-null array");
                                };
                                let passed_bitmap = passed_bitmap.to_bitmap();
                                let (columns, vis) = data_chunk.into_parts();
                                let new_vis = vis & passed_bitmap;
                                StreamChunk::with_visibility(ops, columns, new_vis)
                            } else {
                                chunk
                            };
                            let new_chunk =
                                Self::apply_indices_map(new_chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    } else if let Some(ref cond) = self.condition {
                        // Joined result without evaluating non-lookup conditions.
                        let st1 = phase1::handle_chunk::<K, S, phase1::LeftOuterWithCond>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            epoch,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &null_matched,
                            chunk,
                        );
                        let mut matched_count = 0usize;
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let (data_chunk, ops) = chunk.into_parts();
                            let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                            let ArrayImpl::Bool(passed_bitmap) = &*passed_bitmap else {
                                panic!("unmatched type: filter expr returns a non-null array");
                            };
                            let passed_bitmap = passed_bitmap.to_bitmap();
                            let (columns, vis) = data_chunk.into_parts();
                            let mut new_vis = BitmapBuilder::with_capacity(vis.len());
                            for (passed, not_match_end) in
                                passed_bitmap.iter().zip_eq_debug(vis.iter())
                            {
                                let is_match_end = !not_match_end;
                                let vis = if is_match_end && matched_count == 0 {
                                    // Nothing is matched, so the marker row should be visible.
                                    true
                                } else if is_match_end {
                                    // reset the count
                                    matched_count = 0;
                                    // rows found, so the marker row should be invisible.
                                    false
                                } else {
                                    if passed {
                                        matched_count += 1;
                                    }
                                    passed
                                };
                                new_vis.append(vis);
                            }
                            let new_chunk = Self::apply_indices_map(
                                StreamChunk::with_visibility(ops, columns, new_vis.finish()),
                                &self.output_indices,
                            );
                            yield Message::Chunk(new_chunk);
                        }
                        // The last row should always be marker row,
                        assert_eq!(matched_count, 0);
                    } else {
                        let st1 = phase1::handle_chunk::<K, S, phase1::LeftOuter>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            epoch,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &null_matched,
                            chunk,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = Self::apply_indices_map(chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    }
                }
                InternalMessage::Barrier(updates, barrier) => {
                    if let Some(vnodes) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let prev_vnodes =
                            self.right_table.source.update_vnode_bitmap(vnodes.clone());
                        if cache_may_stale(&prev_vnodes, &vnodes) {
                            self.right_table.cache.clear();
                        }
                    }
                    self.right_table.cache.update_epoch(barrier.epoch.curr);
                    self.right_table.update(
                        updates,
                        &self.right_join_keys,
                        &right_stream_key_indices,
                    )?;
                    prev_epoch = Some(barrier.epoch.curr);
                    yield Message::Barrier(barrier)
                }
            }
        }
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> Execute
    for TemporalJoinExecutor<K, S, T>
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }
}
