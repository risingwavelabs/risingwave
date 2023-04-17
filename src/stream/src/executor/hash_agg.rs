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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

use futures::{stream, Stream, StreamExt};
use futures_async_stream::try_stream;
use iter_chunks::IterChunks;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::function::aggregate::AggCall;
use risingwave_storage::StateStore;

use super::agg_common::{AggExecutorArgs, GroupAggExecutorExtraArgs};
use super::aggregation::{
    agg_call_filter_res, iter_table_storage, AggStateStorage, ChunkBuilder, DistinctDeduplicater,
    OnlyOutputIfHasInput,
};
use super::sort_buffer::SortBuffer;
use super::{
    expect_first_barrier, ActorContextRef, ExecutorInfo, PkIndicesRef, StreamExecutorResult,
    Watermark,
};
use crate::cache::{cache_may_stale, new_with_hasher, ExecutorCache};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::aggregation::{generate_agg_schema, AggGroup as GenericAggGroup};
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{BoxedMessageStream, Executor, Message};
use crate::task::AtomicU64Ref;

type AggGroup<S> = GenericAggGroup<S, OnlyOutputIfHasInput>;
type AggGroupCache<K, S> = ExecutorCache<K, AggGroup<S>, PrecomputedBuildHasher>;

/// [`HashAggExecutor`] could process large amounts of data using a state backend. It works as
/// follows:
///
/// * The executor pulls data from the upstream, and apply the data chunks to the corresponding
///   aggregation states.
/// * While processing, it will record which keys have been modified in this epoch using
///   `group_change_set`.
/// * Upon a barrier is received, the executor will call `.flush` on the storage backend, so that
///   all modifications will be flushed to the storage backend. Meanwhile, the executor will go
///   through `group_change_set`, and produce a stream chunk based on the state changes.
pub struct HashAggExecutor<K: HashKey, S: StateStore> {
    input: Box<dyn Executor>,
    inner: ExecutorInner<K, S>,
}

struct ExecutorInner<K: HashKey, S: StateStore> {
    _phantom: PhantomData<K>,

    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Pk indices from input.
    input_pk_indices: Vec<usize>,

    /// Schema from input.
    input_schema: Schema,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    group_key_indices: Vec<usize>,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// Index of row count agg call (`count(*)`) in the call list.
    row_count_index: usize,

    /// State storages for each aggregation calls.
    /// `None` means the agg call need not to maintain a state table by itself.
    storages: Vec<AggStateStorage<S>>,

    /// State table for the previous result of all agg calls.
    /// The outputs of all managed agg states are collected and stored in this
    /// table when `flush_data` is called.
    /// Also serves as EOWC sort buffer table.
    result_table: StateTable<S>,

    /// State tables for deduplicating rows on distinct key for distinct agg calls.
    /// One table per distinct column (may be shared by multiple agg calls).
    distinct_dedup_tables: HashMap<usize, StateTable<S>>,

    /// Watermark epoch.
    watermark_epoch: AtomicU64Ref,

    /// State cache size for extreme agg.
    extreme_cache_size: usize,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// Should emit on window close according to watermark?
    emit_on_window_close: bool,

    metrics: Arc<StreamingMetrics>,
}

impl<K: HashKey, S: StateStore> ExecutorInner<K, S> {
    fn all_state_tables_mut(&mut self) -> impl Iterator<Item = &mut StateTable<S>> {
        iter_table_storage(&mut self.storages)
            .chain(self.distinct_dedup_tables.values_mut())
            .chain(std::iter::once(&mut self.result_table))
    }
}

trait Emitter {
    type StateStore: StateStore;

    fn new(result_table: &StateTable<Self::StateStore>) -> Self;

    fn emit_from_changes<'a>(
        &'a mut self,
        chunk_builder: &'a mut ChunkBuilder,
        result_table: &'a mut StateTable<Self::StateStore>,
        watermark: Option<&'a ScalarImpl>,
        changes: impl IntoIterator<Item = Record<OwnedRow>> + 'a,
    ) -> impl Stream<Item = StreamExecutorResult<StreamChunk>> + 'a;

    fn emit_from_result_table<'a>(
        &'a mut self,
        chunk_builder: &'a mut ChunkBuilder,
        result_table: &'a mut StateTable<Self::StateStore>,
        watermark: Option<&'a ScalarImpl>,
    ) -> impl Stream<Item = StreamExecutorResult<StreamChunk>> + 'a;
}

struct EmitOnUpdates<S: StateStore> {
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Emitter for EmitOnUpdates<S> {
    type StateStore = S;

    fn new(_result_table: &StateTable<Self::StateStore>) -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn emit_from_changes<'a>(
        &'a mut self,
        chunk_builder: &'a mut ChunkBuilder,
        result_table: &'a mut StateTable<Self::StateStore>,
        _watermark: Option<&'a ScalarImpl>,
        changes: impl IntoIterator<Item = Record<OwnedRow>> + 'a,
    ) {
        for change in changes {
            // For EOU, write change to result table and directly yield the change.
            result_table.write_record(change.as_ref());
            if let Some(chunk) = chunk_builder.append_record(change) {
                yield chunk;
            }
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn emit_from_result_table<'a>(
        &'a mut self,
        _chunk_builder: &'a mut ChunkBuilder,
        _result_table: &'a mut StateTable<Self::StateStore>,
        _watermark: Option<&'a ScalarImpl>,
    ) {
        // do nothing
    }
}

struct EmitOnWindowClose<S: StateStore> {
    buffer: SortBuffer<S>,
}

impl<S: StateStore> Emitter for EmitOnWindowClose<S> {
    type StateStore = S;

    fn new(result_table: &StateTable<Self::StateStore>) -> Self {
        Self {
            buffer: SortBuffer::new(0, result_table),
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn emit_from_changes<'a>(
        &'a mut self,
        _chunk_builder: &'a mut ChunkBuilder,
        result_table: &'a mut StateTable<Self::StateStore>,
        _watermark: Option<&'a ScalarImpl>,
        changes: impl IntoIterator<Item = Record<OwnedRow>> + 'a,
    ) {
        for change in changes {
            // For EOWC, write change to the sort buffer.
            self.buffer.apply_change(change, result_table);
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn emit_from_result_table<'a>(
        &'a mut self,
        chunk_builder: &'a mut ChunkBuilder,
        result_table: &'a mut StateTable<Self::StateStore>,
        watermark: Option<&'a ScalarImpl>,
    ) {
        if let Some(watermark) = watermark {
            #[for_await]
            for row in self.buffer.consume(watermark.clone(), result_table) {
                let row = row?;
                if let Some(chunk) = chunk_builder.append_row(Op::Insert, row) {
                    yield chunk;
                }
            }
        }
    }
}

struct ExecutionVars<K: HashKey, S: StateStore, E: Emitter<StateStore = S>> {
    stats: ExecutionStats,

    /// Cache for [`AggGroup`]s. `HashKey` -> `AggGroup`.
    agg_group_cache: AggGroupCache<K, S>,

    /// Changed group keys in the current epoch (before next flush).
    group_change_set: HashSet<K>,

    /// Distinct deduplicater to deduplicate input rows for each distinct agg call.
    distinct_dedup: DistinctDeduplicater<S>,

    /// Buffer watermarks on group keys received since last barrier.
    buffered_watermarks: Vec<Option<Watermark>>,

    /// Latest watermark on window column.
    window_watermark: Option<ScalarImpl>,

    /// Stream chunk builder.
    chunk_builder: ChunkBuilder,

    /// Emitter for emit-on-updates/emit-on-window-close semantics.
    chunk_emitter: E,
}

struct ExecutionStats {
    /// How many times have we hit the cache of hash agg executor for the lookup of each key.
    lookup_miss_count: u64,
    total_lookup_count: u64,

    /// How many times have we hit the cache of hash agg executor for all the lookups generated by
    /// one StreamChunk.
    chunk_lookup_miss_count: u64,
    chunk_total_lookup_count: u64,
}

impl ExecutionStats {
    fn new() -> Self {
        Self {
            lookup_miss_count: 0,
            total_lookup_count: 0,
            chunk_lookup_miss_count: 0,
            chunk_total_lookup_count: 0,
        }
    }
}

impl<K: HashKey, S: StateStore> Executor for HashAggExecutor<K, S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        if self.inner.emit_on_window_close {
            self.execute_inner::<EmitOnWindowClose<S>>().boxed()
        } else {
            self.execute_inner::<EmitOnUpdates<S>>().boxed()
        }
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl<K: HashKey, S: StateStore> HashAggExecutor<K, S> {
    pub fn new(args: AggExecutorArgs<S, GroupAggExecutorExtraArgs>) -> StreamResult<Self> {
        let input_info = args.input.info();
        let schema = generate_agg_schema(
            args.input.as_ref(),
            &args.agg_calls,
            Some(&args.extra.group_key_indices),
        );
        Ok(Self {
            input: args.input,
            inner: ExecutorInner {
                _phantom: PhantomData,
                actor_ctx: args.actor_ctx,
                info: ExecutorInfo {
                    schema,
                    pk_indices: args.pk_indices,
                    identity: format!("HashAggExecutor {:X}", args.executor_id),
                },
                input_pk_indices: input_info.pk_indices,
                input_schema: input_info.schema,
                group_key_indices: args.extra.group_key_indices,
                agg_calls: args.agg_calls,
                row_count_index: args.row_count_index,
                storages: args.storages,
                result_table: args.result_table,
                distinct_dedup_tables: args.distinct_dedup_tables,
                watermark_epoch: args.watermark_epoch,
                extreme_cache_size: args.extreme_cache_size,
                chunk_size: args.extra.chunk_size,
                emit_on_window_close: args.extra.emit_on_window_close,
                metrics: args.extra.metrics,
            },
        })
    }

    /// Get visibilities that mask rows in the chunk for each group. The returned visibility
    /// is a `Bitmap` rather than `Option<Bitmap>` because it's likely to have multiple groups
    /// in one chunk.
    ///
    /// * `keys`: Hash Keys of rows.
    /// * `base_visibility`: Visibility of rows, `None` means all are visible.
    fn get_group_visibilities(keys: Vec<K>, base_visibility: Option<&Bitmap>) -> Vec<(K, Bitmap)> {
        let n_rows = keys.len();
        let mut vis_builders = HashMap::new();
        for (row_idx, key) in keys.into_iter().enumerate().filter(|(row_idx, _)| {
            base_visibility
                .map(|vis| vis.is_set(*row_idx))
                .unwrap_or(true)
        }) {
            vis_builders
                .entry(key)
                .or_insert_with(|| BitmapBuilder::zeroed(n_rows))
                .set(row_idx, true);
        }
        vis_builders
            .into_iter()
            .map(|(key, vis_builder)| (key, vis_builder.finish()))
            .collect()
    }

    async fn ensure_keys_in_cache(
        this: &mut ExecutorInner<K, S>,
        cache: &mut AggGroupCache<K, S>,
        keys: impl IntoIterator<Item = &K>,
        stats: &mut ExecutionStats,
    ) -> StreamExecutorResult<()> {
        let group_key_types = &this.info.schema.data_types()[..this.group_key_indices.len()];
        let futs = keys
            .into_iter()
            .filter_map(|key| {
                stats.total_lookup_count += 1;
                if cache.contains(key) {
                    None
                } else {
                    stats.lookup_miss_count += 1;
                    Some(async {
                        // Create `AggGroup` for the current group if not exists. This will
                        // fetch previous agg result from the result table.
                        let agg_group = AggGroup::create(
                            Some(key.deserialize(group_key_types)?),
                            &this.agg_calls,
                            &this.storages,
                            &this.result_table,
                            &this.input_pk_indices,
                            this.row_count_index,
                            this.extreme_cache_size,
                            &this.input_schema,
                        )
                        .await?;
                        Ok::<_, StreamExecutorError>((key.clone(), agg_group))
                    })
                }
            })
            .collect_vec(); // collect is necessary to avoid lifetime issue of `agg_group_cache`

        stats.chunk_total_lookup_count += 1;
        if !futs.is_empty() {
            // If not all the required states/keys are in the cache, it's a chunk-level cache miss.
            stats.chunk_lookup_miss_count += 1;
            let mut buffered = stream::iter(futs).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (key, agg_group) = result?;
                cache.put(key, agg_group);
            }
        }
        Ok(())
    }

    async fn apply_chunk<E: Emitter<StateStore = S>>(
        this: &mut ExecutorInner<K, S>,
        vars: &mut ExecutionVars<K, S, E>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        // Find groups in this chunk and generate visibility for each group key.
        let keys = K::build(&this.group_key_indices, chunk.data_chunk())?;
        let group_visibilities = Self::get_group_visibilities(keys, chunk.visibility());

        // Create `AggGroup` for each group if not exists.
        Self::ensure_keys_in_cache(
            this,
            &mut vars.agg_group_cache,
            group_visibilities.iter().map(|(k, _)| k),
            &mut vars.stats,
        )
        .await?;

        // Decompose the input chunk.
        let capacity = chunk.capacity();
        let (ops, columns, visibility) = chunk.into_inner();

        // Calculate the row visibility for every agg call.
        let mut call_visibilities = Vec::with_capacity(this.agg_calls.len());
        for agg_call in &this.agg_calls {
            let agg_call_filter_res = agg_call_filter_res(
                &this.actor_ctx,
                &this.info.identity,
                agg_call,
                &columns,
                visibility.as_ref(),
                capacity,
            )
            .await?;
            call_visibilities.push(agg_call_filter_res);
        }

        // Materialize input chunk if needed.
        this.storages
            .iter_mut()
            .zip_eq_fast(call_visibilities.iter().map(Option::as_ref))
            .for_each(|(storage, visibility)| {
                if let AggStateStorage::MaterializedInput { table, mapping } = storage {
                    let needed_columns = mapping
                        .upstream_columns()
                        .iter()
                        .map(|col_idx| columns[*col_idx].clone())
                        .collect();
                    table.write_chunk(StreamChunk::new(
                        ops.clone(),
                        needed_columns,
                        visibility.cloned(),
                    ));
                }
            });

        // Apply chunk to each of the state (per agg_call), for each group.
        for (key, visibility) in group_visibilities {
            let agg_group = vars.agg_group_cache.get_mut(&key).unwrap();
            let visibilities = call_visibilities
                .iter()
                .map(Option::as_ref)
                .map(|call_vis| call_vis.map_or_else(|| visibility.clone(), |v| v & &visibility))
                .map(Some)
                .collect();
            let visibilities = vars
                .distinct_dedup
                .dedup_chunk(
                    &ops,
                    &columns,
                    visibilities,
                    &mut this.distinct_dedup_tables,
                    agg_group.group_key(),
                )
                .await?;
            agg_group.apply_chunk(&mut this.storages, &ops, &columns, visibilities)?;
            // Mark the group as changed.
            vars.group_change_set.insert(key);
        }

        Ok(())
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn flush_data<'a, E: Emitter<StateStore = S>>(
        this: &'a mut ExecutorInner<K, S>,
        vars: &'a mut ExecutionVars<K, S, E>,
        epoch: EpochPair,
    ) {
        // Update metrics.
        let actor_id_str = this.actor_ctx.id.to_string();
        let table_id_str = this.result_table.table_id().to_string();
        this.metrics
            .agg_lookup_miss_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(vars.stats.lookup_miss_count);
        vars.stats.lookup_miss_count = 0;
        this.metrics
            .agg_total_lookup_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(vars.stats.total_lookup_count);
        vars.stats.total_lookup_count = 0;
        this.metrics
            .agg_cached_keys
            .with_label_values(&[&table_id_str, &actor_id_str])
            .set(vars.agg_group_cache.len() as i64);
        this.metrics
            .agg_chunk_lookup_miss_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(vars.stats.chunk_lookup_miss_count);
        vars.stats.chunk_lookup_miss_count = 0;
        this.metrics
            .agg_chunk_total_lookup_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(vars.stats.chunk_total_lookup_count);
        vars.stats.chunk_total_lookup_count = 0;

        let window_watermark = vars.window_watermark.take();
        let n_dirty_group = vars.group_change_set.len();

        // Flush agg states if needed.
        for key in &vars.group_change_set {
            let agg_group = vars
                .agg_group_cache
                .get_mut(key)
                .expect("changed group must have corresponding AggGroup");
            agg_group.flush_state_if_needed(&mut this.storages).await?;
        }

        let futs_of_all_groups = vars
            .group_change_set
            .drain()
            .map(|key| {
                // Get agg group of the key.
                let mut ptr: NonNull<_> = vars
                    .agg_group_cache
                    .get_mut(&key)
                    .expect("changed group must have corresponding AggGroup")
                    .into();
                // SAFETY: `key`s in `keys_in_batch` are unique by nature, because they're
                // from `group_change_set` which is a set.
                unsafe { ptr.as_mut() }
            })
            .map(|agg_group| async {
                // Get agg outputs and build change.
                let curr_outputs = agg_group.get_outputs(&this.storages).await?;
                let change = agg_group.build_change(curr_outputs);
                Ok::<_, StreamExecutorError>(change)
            });

        // TODO(rc): figure out a more reasonable concurrency limit.
        const MAX_CONCURRENT_TASKS: usize = 100;
        let mut futs_batches = IterChunks::chunks(futs_of_all_groups, MAX_CONCURRENT_TASKS);
        while let Some(futs) = futs_batches.next() {
            // Compute agg result changes for each group, and emit changes accordingly.
            let changes = futures::future::try_join_all(futs).await?;
            #[for_await]
            for chunk in vars.chunk_emitter.emit_from_changes(
                &mut vars.chunk_builder,
                &mut this.result_table,
                window_watermark.as_ref(),
                changes.into_iter().flatten(),
            ) {
                yield chunk?;
            }
        }

        // Emit remaining results from result table.
        #[for_await]
        for chunk in vars.chunk_emitter.emit_from_result_table(
            &mut vars.chunk_builder,
            &mut this.result_table,
            window_watermark.as_ref(),
        ) {
            yield chunk?;
        }

        // Yield the remaining rows in chunk builder.
        if let Some(chunk) = vars.chunk_builder.take() {
            yield chunk;
        }

        if n_dirty_group == 0 && window_watermark.is_none() {
            // Nothing is expected to be changed.
            this.all_state_tables_mut().for_each(|table| {
                table.commit_no_data_expected(epoch);
            });
        } else {
            if let Some(watermark) = window_watermark {
                // Update watermark of state tables, for state cleaning.
                this.all_state_tables_mut()
                    .for_each(|table| table.update_watermark(watermark.clone(), false));
            }
            // Commit all state tables.
            futures::future::try_join_all(
                this.all_state_tables_mut()
                    .map(|table| async { table.commit(epoch).await }),
            )
            .await?;
        }

        // Flush distinct dedup state.
        vars.distinct_dedup.flush(&mut this.distinct_dedup_tables)?;

        // Evict cache to target capacity.
        vars.agg_group_cache.evict();
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner<E: Emitter<StateStore = S>>(self) {
        let HashAggExecutor {
            input,
            inner: mut this,
        } = self;

        let mut vars = ExecutionVars {
            stats: ExecutionStats::new(),
            agg_group_cache: AggGroupCache::new(new_with_hasher(
                this.watermark_epoch.clone(),
                PrecomputedBuildHasher,
            )),
            group_change_set: HashSet::new(),
            distinct_dedup: DistinctDeduplicater::new(&this.agg_calls, &this.watermark_epoch),
            buffered_watermarks: vec![None; this.group_key_indices.len()],
            window_watermark: None,
            chunk_builder: ChunkBuilder::new(this.chunk_size, &this.info.schema.data_types()),
            chunk_emitter: E::new(&this.result_table),
        };

        // TODO(rc): use something like a `ColumnMapping` type
        let group_key_invert_idx = {
            let mut group_key_invert_idx = vec![None; input.info().schema.len()];
            for (group_key_seq, group_key_idx) in this.group_key_indices.iter().enumerate() {
                group_key_invert_idx[*group_key_idx] = Some(group_key_seq);
            }
            group_key_invert_idx
        };

        // First barrier
        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        this.all_state_tables_mut().for_each(|table| {
            table.init_epoch(barrier.epoch);
        });
        vars.agg_group_cache.update_epoch(barrier.epoch.curr);
        vars.distinct_dedup.dedup_caches_mut().for_each(|cache| {
            cache.update_epoch(barrier.epoch.curr);
        });

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(watermark) => {
                    let group_key_seq = group_key_invert_idx[watermark.col_idx];
                    if let Some(group_key_seq) = group_key_seq {
                        if group_key_seq == 0 {
                            vars.window_watermark = Some(watermark.val.clone());
                        }
                        vars.buffered_watermarks[group_key_seq] =
                            Some(watermark.with_idx(group_key_seq));
                    }
                }
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&mut this, &mut vars, chunk).await?;
                }
                Message::Barrier(barrier) => {
                    #[for_await]
                    for chunk in Self::flush_data(&mut this, &mut vars, barrier.epoch) {
                        yield Message::Chunk(chunk?);
                    }

                    if this.emit_on_window_close {
                        // ignore watermarks on other columns
                        if let Some(watermark) = vars.buffered_watermarks[0].take() {
                            yield Message::Watermark(watermark);
                        }
                    } else {
                        for buffered_watermark in &mut vars.buffered_watermarks {
                            if let Some(watermark) = buffered_watermark.take() {
                                yield Message::Watermark(watermark);
                            }
                        }
                    }

                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let previous_vnode_bitmap = this.result_table.vnodes().clone();
                        this.all_state_tables_mut().for_each(|table| {
                            let _ = table.update_vnode_bitmap(vnode_bitmap.clone());
                        });

                        // Manipulate the cache if necessary.
                        if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
                            vars.agg_group_cache.clear();
                            vars.distinct_dedup.dedup_caches_mut().for_each(|cache| {
                                cache.clear();
                            });
                        }
                    }

                    // Update the current epoch.
                    vars.agg_group_cache.update_epoch(barrier.epoch.curr);
                    vars.distinct_dedup.dedup_caches_mut().for_each(|cache| {
                        cache.update_epoch(barrier.epoch.curr);
                    });

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_expr::function::aggregate::{AggCall, AggKind};
    use risingwave_expr::function::args::FuncArgs;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use crate::executor::test_utils::agg_executor::new_boxed_hash_agg_executor;
    use crate::executor::test_utils::*;
    use crate::executor::{Message, PkIndices, Watermark};

    // --- Test HashAgg with in-memory StateStore ---

    #[tokio::test]
    async fn test_local_hash_aggregation_count_in_memory() {
        test_local_hash_aggregation_count(MemoryStateStore::new()).await
    }

    #[tokio::test]
    async fn test_global_hash_aggregation_count_in_memory() {
        test_global_hash_aggregation_count(MemoryStateStore::new()).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_min_in_memory() {
        test_local_hash_aggregation_min(MemoryStateStore::new()).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_min_append_only_in_memory() {
        test_local_hash_aggregation_min_append_only(MemoryStateStore::new()).await
    }

    async fn test_local_hash_aggregation_count<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            + 1
            + 2
            + 2",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            - 1
            - 2 D
            - 2",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: FuncArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Count,
                args: FuncArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Count,
                args: FuncArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            false,
            agg_calls,
            0,
            keys,
            vec![],
            1 << 10,
            false,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 1 1
                + 2 2 2 2"
            )
            .sort_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                "  I I I I
                -  1 1 1 1
                U- 2 2 2 2
                U+ 2 1 1 1"
            )
            .sort_rows(),
        );
    }

    async fn test_global_hash_aggregation_count<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I I I
            + 1 1 1
            + 2 2 2
            + 2 2 2",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I I I
            - 1 1 1
            - 2 2 2 D
            - 2 2 2
            + 3 3 3",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another sum state
        let key_indices = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: FuncArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Sum,
                args: FuncArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            // This is local hash aggregation, so we add another sum state
            AggCall {
                kind: AggKind::Sum,
                args: FuncArgs::Unary(DataType::Int64, 2),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            false,
            agg_calls,
            0,
            key_indices,
            vec![],
            1 << 10,
            false,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 1 1
                + 2 2 4 4"
            )
            .sort_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                "  I I I I
                -  1 1 1 1
                U- 2 2 4 4
                U+ 2 1 2 2
                +  3 1 3 3"
            )
            .sort_rows(),
        );
    }

    async fn test_local_hash_aggregation_min<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                // group key column
                Field::unnamed(DataType::Int64),
                // data column to get minimum
                Field::unnamed(DataType::Int64),
                // primary key column
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I     I    I
            + 1   233 1001
            + 1 23333 1002
            + 2  2333 1003",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I     I    I
            - 1   233 1001
            - 1 23333 1002 D
            - 2  2333 1003",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: FuncArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Min,
                args: FuncArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            false,
            agg_calls,
            0,
            keys,
            vec![2],
            1 << 10,
            false,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                " I I    I
                + 1 2  233
                + 2 1 2333"
            )
            .sort_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                "  I I     I
                -  2 1  2333
                U- 1 2   233
                U+ 1 1 23333"
            )
            .sort_rows(),
        );
    }

    async fn test_local_hash_aggregation_min_append_only<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                // group key column
                Field::unnamed(DataType::Int64),
                // data column to get minimum
                Field::unnamed(DataType::Int64),
                // primary key column
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I  I  I
            + 2 5  1000
            + 1 15 1001
            + 1 8  1002
            + 2 5  1003
            + 2 10 1004
            ",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I  I  I
            + 1 20 1005
            + 1 1  1006
            + 2 10 1007
            + 2 20 1008
            ",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: FuncArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Min,
                args: FuncArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            true, // is append only
            agg_calls,
            0,
            keys,
            vec![2],
            1 << 10,
            false,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                " I I    I
                + 1 2 8
                + 2 3 5"
            )
            .sort_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sort_rows(),
            StreamChunk::from_pretty(
                "  I I  I
                U- 1 2 8
                U+ 1 4 1
                U- 2 3 5
                U+ 2 5 5
                "
            )
            .sort_rows(),
        );
    }

    #[tokio::test]
    async fn test_hash_agg_emit_on_window_close_in_memory() {
        test_hash_agg_emit_on_window_close(MemoryStateStore::new()).await
    }

    async fn test_hash_agg_emit_on_window_close<S: StateStore>(store: S) {
        let input_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Varchar), // to ensure correct group key column mapping
                Field::unnamed(DataType::Int64),   // window group key column
            ],
        };
        let input_window_col = 1;
        let group_key_indices = vec![input_window_col];
        let agg_calls = vec![AggCall {
            kind: AggKind::Count, // as row count, index: 0
            args: FuncArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
        }];

        let (mut tx, source) = MockSource::channel(input_schema, PkIndices::new());
        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            false,
            agg_calls,
            0,
            group_key_indices,
            vec![],
            1 << 10,
            true, // enable emit-on-window-close
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        let mut epoch = 1;
        let mut get_epoch = || {
            let e = epoch;
            epoch += 1;
            e
        };

        {
            // init barrier
            tx.push_barrier(get_epoch(), false);
            hash_agg.expect_barrier().await;
        }

        {
            tx.push_chunk(StreamChunk::from_pretty(
                " T I
                + _ 1
                + _ 2
                + _ 3",
            ));
            tx.push_barrier(get_epoch(), false);

            // no window is closed, nothing is expected to be emitted
            hash_agg.expect_barrier().await;
        }

        {
            tx.push_chunk(StreamChunk::from_pretty(
                " T I
                - _ 2
                + _ 4",
            ));
            tx.push_int64_watermark(input_window_col, 3); // windows < 3 are closed
            tx.push_barrier(get_epoch(), false);

            let chunk = hash_agg.expect_chunk().await;
            assert_eq!(
                chunk.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    + 1 1" // 1 row for group (1,)
                )
                .sort_rows()
            );

            let wtmk = hash_agg.expect_watermark().await;
            assert_eq!(wtmk, Watermark::new(0, DataType::Int64, 3i64.into()));

            hash_agg.expect_barrier().await;
        }

        {
            tx.push_int64_watermark(input_window_col, 4); // windows < 4 are closed
            tx.push_barrier(get_epoch(), false);

            let chunk = hash_agg.expect_chunk().await;
            assert_eq!(
                chunk.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    + 3 1" // 1 rows for group (3,)
                )
                .sort_rows()
            );

            let wtmk = hash_agg.expect_watermark().await;
            assert_eq!(wtmk, Watermark::new(0, DataType::Int64, 4i64.into()));

            hash_agg.expect_barrier().await;
        }

        {
            tx.push_int64_watermark(input_window_col, 10); // windows < 10 are closed
            tx.push_barrier(get_epoch(), false);

            let chunk = hash_agg.expect_chunk().await;
            assert_eq!(
                chunk.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    + 4 1" // 1 rows for group (4,)
                )
                .sort_rows()
            );

            hash_agg.expect_watermark().await;
            hash_agg.expect_barrier().await;
        }

        {
            tx.push_int64_watermark(input_window_col, 20); // windows < 20 are closed
            tx.push_barrier(get_epoch(), false);

            hash_agg.expect_watermark().await;
            hash_agg.expect_barrier().await;
        }
    }
}
