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

use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use iter_chunks::IterChunks;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_storage::StateStore;

use super::agg_common::AggExecutorArgs;
use super::aggregation::{
    agg_call_filter_res, iter_table_storage, AggStateStorage, DistinctDeduplicater,
    OnlyOutputIfHasInput,
};
use super::{
    expect_first_barrier, ActorContextRef, Executor, ExecutorInfo, PkIndicesRef,
    StreamExecutorResult, Watermark,
};
use crate::cache::{cache_may_stale, new_with_hasher, ExecutorCache};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::aggregation::{generate_agg_schema, AggCall, AggGroup as GenericAggGroup};
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{BoxedMessageStream, Message};
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
    result_table: StateTable<S>,

    /// State tables for deduplicating rows on distinct key for distinct agg calls.
    /// One table per distinct column (may be shared by multiple agg calls).
    distinct_dedup_tables: HashMap<usize, StateTable<S>>,

    /// Watermark epoch.
    watermark_epoch: AtomicU64Ref,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// State cache size for extreme agg.
    extreme_cache_size: usize,

    metrics: Arc<StreamingMetrics>,
}

impl<K: HashKey, S: StateStore> ExecutorInner<K, S> {
    fn all_state_tables_mut(&mut self) -> impl Iterator<Item = &mut StateTable<S>> {
        iter_table_storage(&mut self.storages)
            .chain(self.distinct_dedup_tables.values_mut())
            .chain(std::iter::once(&mut self.result_table))
    }
}

struct ExecutionVars<K: HashKey, S: StateStore> {
    stats: ExecutionStats,

    /// Cache for [`AggGroup`]s. `HashKey` -> `AggGroup`.
    agg_group_cache: AggGroupCache<K, S>,

    /// Changed group keys in the current epoch (before next flush).
    group_change_set: HashSet<K>,

    /// Distinct deduplicater to deduplicate input rows for each distinct agg call.
    distinct_dedup: DistinctDeduplicater<S>,

    /// Buffer watermarks on group keys received since last barrier.
    buffered_watermarks: Vec<Option<Watermark>>,
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
        self.execute_inner().boxed()
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
    pub fn new(args: AggExecutorArgs<S>) -> StreamResult<Self> {
        let extra_args = args.extra.unwrap();

        let input_info = args.input.info();
        let schema = generate_agg_schema(
            args.input.as_ref(),
            &args.agg_calls,
            Some(&extra_args.group_key_indices),
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
                group_key_indices: extra_args.group_key_indices,
                agg_calls: args.agg_calls,
                row_count_index: args.row_count_index,
                storages: args.storages,
                result_table: args.result_table,
                distinct_dedup_tables: args.distinct_dedup_tables,
                watermark_epoch: args.watermark_epoch,
                chunk_size: extra_args.chunk_size,
                extreme_cache_size: args.extreme_cache_size,
                metrics: extra_args.metrics,
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

    async fn apply_chunk(
        this: &mut ExecutorInner<K, S>,
        vars: &mut ExecutionVars<K, S>,
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
    async fn flush_data<'a>(
        this: &'a mut ExecutorInner<K, S>,
        vars: &'a mut ExecutionVars<K, S>,
        epoch: EpochPair,
    ) {
        let state_clean_watermark = vars
            .buffered_watermarks
            .first()
            .and_then(|opt_watermark| opt_watermark.as_ref())
            .map(|watermark| watermark.val.clone());

        let actor_id_str = this.actor_ctx.id.to_string();
        this.metrics
            .agg_lookup_miss_count
            .with_label_values(&[&actor_id_str])
            .inc_by(vars.stats.lookup_miss_count);
        vars.stats.lookup_miss_count = 0;
        this.metrics
            .agg_total_lookup_count
            .with_label_values(&[&actor_id_str])
            .inc_by(vars.stats.total_lookup_count);
        vars.stats.total_lookup_count = 0;
        this.metrics
            .agg_cached_keys
            .with_label_values(&[&actor_id_str])
            .set(vars.agg_group_cache.len() as i64);
        this.metrics
            .agg_chunk_lookup_miss_count
            .with_label_values(&[&actor_id_str])
            .inc_by(vars.stats.chunk_lookup_miss_count);
        vars.stats.chunk_lookup_miss_count = 0;
        this.metrics
            .agg_chunk_total_lookup_count
            .with_label_values(&[&actor_id_str])
            .inc_by(vars.stats.chunk_total_lookup_count);
        vars.stats.chunk_total_lookup_count = 0;

        let dirty_cnt = vars.group_change_set.len();
        if dirty_cnt > 0 {
            // Produce the stream chunk
            let mut group_chunks =
                IterChunks::chunks(vars.group_change_set.drain(), this.chunk_size);
            while let Some(batch) = group_chunks.next() {
                let keys_in_batch = batch.into_iter().collect_vec();

                // Flush agg states.
                for key in &keys_in_batch {
                    let agg_group = vars
                        .agg_group_cache
                        .get_mut(key)
                        .expect("changed group must have corresponding AggGroup");
                    agg_group.flush_state_if_needed(&mut this.storages).await?;
                }

                // Create array builders.
                // As the datatype is retrieved from schema, it contains both group key and
                // aggregation state outputs.
                let mut builders = this.info.schema.create_array_builders(this.chunk_size * 2);
                let mut new_ops = Vec::with_capacity(this.chunk_size * 2);

                // Calculate current outputs, concurrently.
                let futs = keys_in_batch.into_iter().map(|key| {
                    // Get agg group of the key.
                    let agg_group = {
                        let mut ptr: NonNull<_> = vars
                            .agg_group_cache
                            .get_mut(&key)
                            .expect("changed group must have corresponding AggGroup")
                            .into();
                        // SAFETY: `key`s in `keys_in_batch` are unique by nature, because they're
                        // from `group_change_set` which is a set.
                        unsafe { ptr.as_mut() }
                    };
                    async {
                        let curr_outputs = agg_group.get_outputs(&this.storages).await?;
                        Ok::<_, StreamExecutorError>((key, agg_group, curr_outputs))
                    }
                });
                let outputs_in_batch: Vec<_> = stream::iter(futs)
                    .buffer_unordered(10)
                    .fuse()
                    .try_collect()
                    .await?;

                for (_key, agg_group, curr_outputs) in outputs_in_batch {
                    if let Some(change) = agg_group.build_change(curr_outputs) {
                        agg_group.apply_change_to_builders(&change, &mut builders, &mut new_ops);
                        agg_group.apply_change_to_result_table(&change, &mut this.result_table);
                    }
                }

                let columns = builders
                    .into_iter()
                    .map(|builder| Ok::<_, StreamExecutorError>(builder.finish().into()))
                    .try_collect()?;

                let chunk = StreamChunk::new(new_ops, columns, None);

                trace!("output_chunk: {:?}", &chunk);
                yield chunk;
            }

            // Flush distinct dedup state.
            vars.distinct_dedup.flush(&mut this.distinct_dedup_tables)?;

            // Commit all state tables.
            futures::future::try_join_all(this.all_state_tables_mut().map(|table| async {
                if let Some(watermark) = state_clean_watermark.as_ref() {
                    table.update_watermark(watermark.clone())
                };
                table.commit(epoch).await
            }))
            .await?;

            // Evict cache to target capacity.
            vars.agg_group_cache.evict();
        } else {
            // Nothing to flush.
            // Call commit on state table to increment the epoch.
            this.all_state_tables_mut().for_each(|table| {
                if let Some(watermark) = state_clean_watermark.as_ref() {
                    table.update_watermark(watermark.clone())
                };
                table.commit_no_data_expected(epoch);
            });
            return Ok(());
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let HashAggExecutor {
            input,
            inner: mut this,
            ..
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
        };

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
                Message::Watermark(mut watermark) => {
                    let group_key_seq = group_key_invert_idx[watermark.col_idx];
                    if let Some(group_key_seq) = group_key_seq {
                        watermark.col_idx = group_key_seq;
                        vars.buffered_watermarks[group_key_seq] = Some(watermark);
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

                    for buffered_watermark in &mut vars.buffered_watermarks {
                        if let Some(watermark) = buffered_watermark.take() {
                            yield Message::Watermark(watermark);
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
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::row::{AscentOwnedRow, OwnedRow, Row};
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::*;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use crate::executor::agg_common::{AggExecutorArgs, AggExecutorArgsExtra};
    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::test_utils::agg_executor::{
        create_agg_state_storage, create_result_table,
    };
    use crate::executor::test_utils::*;
    use crate::executor::{ActorContext, Executor, HashAggExecutor, Message, PkIndices};

    #[allow(clippy::too_many_arguments)]
    async fn new_boxed_hash_agg_executor<S: StateStore>(
        store: S,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        row_count_index: usize,
        group_key_indices: Vec<usize>,
        pk_indices: PkIndices,
        extreme_cache_size: usize,
        executor_id: u64,
    ) -> Box<dyn Executor> {
        let mut storages = Vec::with_capacity(agg_calls.iter().len());
        for (idx, agg_call) in agg_calls.iter().enumerate() {
            storages.push(
                create_agg_state_storage(
                    store.clone(),
                    TableId::new(idx as u32),
                    agg_call,
                    &group_key_indices,
                    &pk_indices,
                    input.as_ref(),
                )
                .await,
            )
        }

        let result_table = create_result_table(
            store,
            TableId::new(agg_calls.len() as u32),
            &agg_calls,
            &group_key_indices,
            input.as_ref(),
        )
        .await;

        HashAggExecutor::<SerializedKey, S>::new(AggExecutorArgs {
            input,
            actor_ctx: ActorContext::create(123),
            pk_indices,
            executor_id,

            extreme_cache_size,

            agg_calls,
            row_count_index,
            storages,
            result_table,
            distinct_dedup_tables: Default::default(),
            watermark_epoch: Arc::new(AtomicU64::new(0)),

            extra: Some(AggExecutorArgsExtra {
                group_key_indices,

                metrics: Arc::new(StreamingMetrics::unused()),
                chunk_size: 1024,
            }),
        })
        .unwrap()
        .boxed()
    }

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
        let append_only = false;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: AggArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            0,
            keys,
            vec![],
            1 << 10,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 1 1
                + 2 2 2 2"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I I I
                -  1 1 1 1
                U- 2 2 2 2
                U+ 2 1 1 1"
            )
            .sorted_rows(),
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
        let append_only = false;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: AggArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            // This is local hash aggregation, so we add another sum state
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 2),
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            0,
            key_indices,
            vec![],
            1 << 10,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 1 1
                + 2 2 4 4"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I I I
                -  1 1 1 1
                U- 2 2 4 4
                U+ 2 1 2 2
                +  3 1 3 3"
            )
            .sorted_rows(),
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
                args: AggArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only: false,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only: false,
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            0,
            keys,
            vec![2],
            1 << 10,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I    I
                + 1 2  233
                + 2 1 2333"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I     I
                -  2 1  2333
                U- 1 2   233
                U+ 1 1 23333"
            )
            .sorted_rows(),
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
        let append_only = true;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count, // as row count, index: 0
                args: AggArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            0,
            keys,
            vec![2],
            1 << 10,
            1,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I    I
                + 1 2 8
                + 2 3 5"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I  I
                U- 1 2 8
                U+ 1 4 1
                U- 2 3 5
                U+ 2 5 5
                "
            )
            .sorted_rows(),
        );
    }

    trait SortedRows {
        fn sorted_rows(self) -> Vec<(Op, OwnedRow)>;
    }
    impl SortedRows for StreamChunk {
        fn sorted_rows(self) -> Vec<(Op, OwnedRow)> {
            let (chunk, ops) = self.into_parts();
            ops.into_iter()
                .zip_eq_debug(
                    chunk
                        .rows()
                        .map(Row::into_owned_row)
                        .map(AscentOwnedRow::from),
                )
                .sorted()
                .map(|(op, row)| (op, row.into_inner()))
                .collect_vec()
        }
    }
}
