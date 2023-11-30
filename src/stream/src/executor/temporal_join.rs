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

use std::alloc::Global;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::pin::pin;
use std::sync::Arc;

use either::Either;
use futures::stream::{self, PollNext};
use futures::{pin_mut, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use lru::DefaultHasher;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
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

use super::{
    Barrier, Executor, ExecutorInfo, Message, MessageStream, StreamExecutorError,
    StreamExecutorResult,
};
use crate::cache::{cache_may_stale, new_with_hasher_in, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::common::JoinStreamChunkBuilder;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, BoxedExecutor, JoinType, JoinTypePrimitive, Watermark};
use crate::task::AtomicU64Ref;

pub struct TemporalJoinExecutor<K: HashKey, S: StateStore, const T: JoinTypePrimitive> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    left: BoxedExecutor,
    right: BoxedExecutor,
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

struct JoinEntryWrapper(Option<JoinEntry>);

impl EstimateSize for JoinEntryWrapper {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl JoinEntryWrapper {
    const MESSAGE: &'static str = "the state should always be `Some`";

    /// Take the value out of the wrapper. Panic if the value is `None`.
    pub fn take(&mut self) -> JoinEntry {
        self.0.take().expect(Self::MESSAGE)
    }
}

impl Deref for JoinEntryWrapper {
    type Target = JoinEntry;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect(Self::MESSAGE)
    }
}

impl DerefMut for JoinEntryWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect(Self::MESSAGE)
    }
}

struct TemporalSide<K: HashKey, S: StateStore> {
    source: StorageTable<S>,
    table_stream_key_indices: Vec<usize>,
    table_output_indices: Vec<usize>,
    cache: ManagedLruCache<K, JoinEntryWrapper, DefaultHasher, SharedStatsAlloc<Global>>,
    ctx: ActorContextRef,
    join_key_data_types: Vec<DataType>,
}

impl<K: HashKey, S: StateStore> TemporalSide<K, S> {
    /// Lookup the temporal side table and return a `JoinEntry` which could be empty if there are no
    /// matched records.
    async fn lookup(&mut self, key: &K, epoch: HummockEpoch) -> StreamExecutorResult<JoinEntry> {
        let table_id_str = self.source.table_id().to_string();
        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.id.to_string();
        self.ctx
            .streaming_metrics
            .temporal_join_total_query_cache_count
            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
            .inc();

        let res = if self.cache.contains(key) {
            let mut state = self.cache.peek_mut(key).unwrap();
            state.take()
        } else {
            // cache miss
            self.ctx
                .streaming_metrics
                .temporal_join_cache_miss_count
                .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                .inc();

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

            entry
        };

        Ok(res)
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

    pub fn insert_back(&mut self, key: K, state: JoinEntry) {
        self.cache.put(key, JoinEntryWrapper(Some(state)));
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
async fn align_input(left: Box<dyn Executor>, right: Box<dyn Executor>) {
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

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> TemporalJoinExecutor<K, S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        left: BoxedExecutor,
        right: BoxedExecutor,
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

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let (left_map, right_map) = JoinStreamChunkBuilder::get_i2o_mapping(
            &self.output_indices,
            self.left.schema().len(),
            self.right.schema().len(),
        );

        let left_to_output: HashMap<usize, usize> = HashMap::from_iter(left_map.iter().cloned());

        let right_stream_key_indices = self.right.pk_indices().to_vec();

        let null_matched = K::Bitmap::from_bool_vec(self.null_safe);

        let mut prev_epoch = None;

        let table_id_str = self.right_table.source.table_id().to_string();
        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.fragment_id.to_string();
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
                    let mut builder = JoinStreamChunkBuilder::new(
                        self.chunk_size,
                        self.info.schema.data_types(),
                        left_map.clone(),
                        right_map.clone(),
                    );
                    let epoch = prev_epoch.expect("Chunk data should come after some barrier.");
                    let keys = K::build(&self.left_join_keys, chunk.data_chunk())?;
                    for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
                        let Some((op, left_row)) = r else {
                            continue;
                        };
                        if key.null_bitmap().is_subset(&null_matched)
                            && let join_entry = self.right_table.lookup(&key, epoch).await?
                            && !join_entry.is_empty()
                        {
                            for right_row in join_entry.cached.values() {
                                // check join condition
                                let ok = if let Some(ref mut cond) = self.condition {
                                    let concat_row = left_row.chain(&right_row).into_owned_row();
                                    cond.eval_row_infallible(&concat_row)
                                        .await
                                        .map(|s| *s.as_bool())
                                        .unwrap_or(false)
                                } else {
                                    true
                                };

                                if ok {
                                    if let Some(chunk) = builder.append_row(op, left_row, right_row)
                                    {
                                        yield Message::Chunk(chunk);
                                    }
                                }
                            }
                            // Insert back the state taken from ht.
                            self.right_table.insert_back(key.clone(), join_entry);
                        } else if T == JoinType::LeftOuter {
                            if let Some(chunk) = builder.append_row_update(op, left_row) {
                                yield Message::Chunk(chunk);
                            }
                        }
                    }
                    if let Some(chunk) = builder.take() {
                        yield Message::Chunk(chunk);
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

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> Executor
    for TemporalJoinExecutor<K, S, T>
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
