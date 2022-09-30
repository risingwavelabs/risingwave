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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use iter_chunks::IterChunks;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{StreamChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashCode, HashKey, PrecomputedBuildHasher};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::hash_util::Crc32FastBuilder;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::aggregation::{agg_call_filter_res, AggStateTable};
use super::{expect_first_barrier, ActorContextRef, Executor, PkIndicesRef, StreamExecutorResult};
use crate::cache::{EvictableHashMap, ExecutorCache, LruManagerRef};
use crate::error::StreamResult;
use crate::executor::aggregation::{
    generate_agg_schema, generate_managed_agg_state, AggCall, AggState,
};
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{BoxedMessageStream, Message, PkIndices, PROCESSING_WINDOW_SIZE};

type AggStateMap<K, S> = ExecutorCache<K, Option<Box<AggState<S>>>, PrecomputedBuildHasher>;

/// [`HashAggExecutor`] could process large amounts of data using a state backend. It works as
/// follows:
///
/// * The executor pulls data from the upstream, and apply the data chunks to the corresponding
///   aggregation states.
/// * While processing, it will record which keys have been modified in this epoch using
///   `modified_keys`.
/// * Upon a barrier is received, the executor will call `.flush` on the storage backend, so that
///   all modifications will be flushed to the storage backend. Meanwhile, the executor will go
///   through `modified_keys`, and produce a stream chunk based on the state changes.
pub struct HashAggExecutor<K: HashKey, S: StateStore> {
    input: Box<dyn Executor>,

    extra: HashAggExecutorExtra<K, S>,

    _phantom: PhantomData<K>,
}

struct HashAggExecutorExtra<K: HashKey, S: StateStore> {
    ctx: ActorContextRef,

    /// See [`Executor::schema`].
    schema: Schema,

    /// See [`Executor::pk_indices`].
    pk_indices: PkIndices,

    /// See [`Executor::identity`].
    identity: String,

    /// Pk indices from input
    input_pk_indices: Vec<usize>,

    /// Schema from input
    _input_schema: Schema,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// Relational state tables for each aggregation calls.
    /// `None` means the agg call need not to maintain a state table by itself.
    agg_state_tables: Vec<Option<AggStateTable<S>>>,

    /// State table for the previous result of all agg calls.
    /// The outputs of all managed agg states are collected and stored in this
    /// table when `flush_data` is called.
    result_table: StateTable<S>,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    group_key_indices: Vec<usize>,

    /// Lru manager. None if using local eviction.
    lru_manager: Option<LruManagerRef>,

    /// How many times have we hit the cache of join executor
    lookup_miss_count: AtomicU64,

    total_lookup_count: AtomicU64,

    metrics: Arc<StreamingMetrics>,

    /// Cache size (one per group by key)
    group_by_cache_size: usize,

    /// Extreme state cache size
    extreme_cache_size: usize,

    /// Changed group keys in the current epoch (before next flush).
    group_change_set: HashSet<K>,
}

impl<K: HashKey, S: StateStore> Executor for HashAggExecutor<K, S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.extra.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.extra.pk_indices
    }

    fn identity(&self) -> &str {
        &self.extra.identity
    }
}

impl<K: HashKey, S: StateStore> HashAggExecutor<K, S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        mut agg_state_tables: Vec<Option<AggStateTable<S>>>,
        mut result_table: StateTable<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        group_key_indices: Vec<usize>,
        group_by_cache_size: usize,
        extreme_cache_size: usize,
        lru_manager: Option<LruManagerRef>,
        metrics: Arc<StreamingMetrics>,
    ) -> StreamResult<Self> {
        let input_info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, Some(&group_key_indices));

        // TODO: enable sanity check for hash agg executor <https://github.com/risingwavelabs/risingwave/issues/3885>
        agg_state_tables
            .iter_mut()
            .filter_map(Option::as_mut)
            .for_each(|state_table| {
                state_table.table.disable_sanity_check();
            });
        result_table.disable_sanity_check();

        Ok(Self {
            input,
            extra: HashAggExecutorExtra {
                ctx,
                schema,
                pk_indices,
                identity: format!("HashAggExecutor {:X}", executor_id),
                input_pk_indices: input_info.pk_indices,
                _input_schema: input_info.schema,
                agg_calls,
                agg_state_tables,
                result_table,
                group_key_indices,
                group_by_cache_size,
                extreme_cache_size,
                lru_manager,
                group_change_set: HashSet::new(),
                lookup_miss_count: AtomicU64::new(0),
                total_lookup_count: AtomicU64::new(0),
                metrics,
            },
            _phantom: PhantomData,
        })
    }

    /// Get unique keys, hash codes and visibility map of each key in a batch.
    ///
    /// The returned order is the same as how we get distinct final columns from original columns.
    ///
    /// `keys` are Hash Keys of all the rows
    /// `key_hash_codes` are hash codes of the deserialized `keys`
    /// `visibility`, leave invisible ones out of aggregation
    fn get_unique_keys(
        keys: Vec<K>,
        key_hash_codes: Vec<HashCode>,
        visibility: &Option<Bitmap>,
    ) -> StreamExecutorResult<Vec<(K, HashCode, Bitmap)>> {
        let total_num_rows = keys.len();
        assert_eq!(key_hash_codes.len(), total_num_rows);
        // Each hash key, e.g. `key1` corresponds to a visibility map that not only shadows
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the
        // `input` The visibility map of each hash key will be passed into
        // `StreamingAggStateImpl`.
        let mut key_to_vis_maps = HashMap::new();

        // Give all the unique keys an order and iterate them later,
        // the order is the same as how we get distinct final columns from original columns.
        let mut unique_key_and_hash_codes = Vec::new();

        for (row_idx, (key, hash_code)) in keys.iter().zip_eq(key_hash_codes.iter()).enumerate() {
            // if the visibility map has already shadowed this row,
            // then we pass
            if let Some(vis_map) = visibility && !vis_map.is_set(row_idx) {
                continue;
            }
            let vis_map = key_to_vis_maps.entry(key).or_insert_with(|| {
                unique_key_and_hash_codes.push((key, hash_code));
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }

        let result = unique_key_and_hash_codes
            .into_iter()
            .map(|(key, hash_code)| {
                (
                    key.clone(),
                    hash_code.clone(),
                    key_to_vis_maps.remove(key).unwrap().into_iter().collect(),
                )
            })
            .collect_vec();

        Ok(result)
    }

    async fn apply_chunk(
        HashAggExecutorExtra::<K, S> {
            ref ctx,
            ref identity,
            ref group_key_indices,
            ref agg_calls,
            ref mut agg_state_tables,
            ref result_table,
            ref input_pk_indices,
            ref extreme_cache_size,
            ref mut group_change_set,
            ref schema,
            lookup_miss_count,
            total_lookup_count,
            ..
        }: &mut HashAggExecutorExtra<K, S>,
        state_map: &mut AggStateMap<K, S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        let (data_chunk, ops) = chunk.into_parts();

        // Compute hash code here before serializing keys to avoid duplicate hash code computation.
        let hash_codes = data_chunk.get_hash_values(group_key_indices, Crc32FastBuilder);
        let keys = K::build_from_hash_code(group_key_indices, &data_chunk, hash_codes.clone());
        let capacity = data_chunk.capacity();
        let (columns, vis) = data_chunk.into_parts();
        let column_refs = columns.iter().map(|col| col.array_ref()).collect_vec();
        let visibility = match vis {
            Vis::Bitmap(b) => Some(b),
            Vis::Compact(_) => None,
        };

        // --- Find unique keys in this batch and generate visibility map for each key ---
        // TODO: this might be inefficient if there are not too many duplicated keys in one batch.
        let unique_keys = Self::get_unique_keys(keys, hash_codes, &visibility)?;

        let group_key_types = &schema.data_types()[..group_key_indices.len()];
        let mut futures = vec![];
        for (key, _hash_code, _) in &unique_keys {
            // Retrieve previous state from the KeyedState.
            let states = state_map.put(key.to_owned(), None);
            total_lookup_count.fetch_add(1, Ordering::Relaxed);

            // Mark the group as changed.
            group_change_set.insert(key.clone());

            let key = key.clone();
            // To leverage more parallelism in IO operations, fetching and updating states for every
            // unique keys is created as futures and run in parallel.
            futures.push(async {
                // Create `AggState` for the current group if not exists. This will fetch
                // previous agg result from the result table.
                let agg_state = {
                    match states {
                        Some(s) => s.unwrap(),
                        None => {
                            lookup_miss_count.fetch_add(1, Ordering::Relaxed);
                            Box::new(
                                generate_managed_agg_state(
                                    Some(&key.clone().deserialize(group_key_types)?),
                                    agg_calls,
                                    agg_state_tables,
                                    result_table,
                                    input_pk_indices.clone(),
                                    *extreme_cache_size,
                                )
                                .await?,
                            )
                        }
                    }
                };

                Ok::<(_, Box<AggState<S>>), StreamExecutorError>((key, agg_state))
            });
        }

        let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();

        while let Some(result) = buffered.next().await {
            let (key, state) = result?;
            state_map.put(key, Some(state));
        }
        // Drop the stream manually to teach compiler the async closure above will not use the read
        // ref anymore.
        drop(buffered);

        // Apply batch in single-thread.
        for (key, _, vis_map) in &unique_keys {
            let agg_state = state_map.get_mut(key).unwrap().as_mut().unwrap();
            // 3. Apply batch to each of the state (per agg_call)
            for ((managed_state, agg_call), agg_state_table) in agg_state
                .managed_states()
                .iter_mut()
                .zip_eq(agg_calls.iter())
                .zip_eq(agg_state_tables.iter_mut())
            {
                let vis_map = agg_call_filter_res(
                    ctx,
                    identity,
                    agg_call,
                    &columns,
                    Some(vis_map),
                    capacity,
                )?;
                managed_state
                    .apply_chunk(
                        &ops,
                        vis_map.as_ref(),
                        &column_refs,
                        agg_state_table.as_mut(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn flush_data<'a>(
        &mut HashAggExecutorExtra::<K, S> {
            ref ctx,
            ref group_key_indices,
            ref schema,
            ref mut agg_state_tables,
            ref mut result_table,
            ref mut group_change_set,
            ref lookup_miss_count,
            ref total_lookup_count,
            ref metrics,
            ..
        }: &'a mut HashAggExecutorExtra<K, S>,
        state_map: &'a mut AggStateMap<K, S>,
        epoch: EpochPair,
    ) {
        let actor_id_str = ctx.id.to_string();
        metrics
            .agg_lookup_miss_count
            .with_label_values(&[&actor_id_str])
            .inc_by(lookup_miss_count.swap(0, Ordering::Relaxed));
        metrics
            .agg_total_lookup_count
            .with_label_values(&[&actor_id_str])
            .inc_by(total_lookup_count.swap(0, Ordering::Relaxed));
        metrics
            .agg_cached_keys
            .with_label_values(&[&actor_id_str])
            .set(state_map.values().map(|_| 1).sum());
        // --- Flush agg result to the result table and downtream ---

        let dirty_cnt = group_change_set.len();
        if dirty_cnt > 0 {
            // Batch commit data.
            futures::future::try_join_all(
                agg_state_tables
                    .iter_mut()
                    .filter_map(Option::as_mut)
                    .map(|state_table| state_table.table.commit(epoch)),
            )
            .await?;

            // --- Produce the stream chunk ---
            let group_key_data_types = &schema.data_types()[..group_key_indices.len()];
            let mut group_chunks =
                IterChunks::chunks(group_change_set.drain(), PROCESSING_WINDOW_SIZE);
            while let Some(batch) = group_chunks.next() {
                // --- Create array builders ---
                // As the datatype is retrieved from schema, it contains both group key and
                // aggregation state outputs.
                let mut builders = schema.create_array_builders(PROCESSING_WINDOW_SIZE * 2);
                let mut new_ops = Vec::with_capacity(PROCESSING_WINDOW_SIZE * 2);

                // --- Retrieve modified states and put the changes into the array builders ---
                for key in batch {
                    let agg_state = state_map
                        .get_mut(&key)
                        .expect("changed group must have corresponding AggState")
                        .as_mut()
                        .unwrap();

                    let (appended, result_row) = agg_state
                        .build_changes(
                            &mut builders[group_key_indices.len()..],
                            &mut new_ops,
                            &agg_state_tables,
                        )
                        .await?;
                    for _ in 0..appended {
                        key.clone().deserialize_to_builders(
                            &mut builders[..group_key_indices.len()],
                            group_key_data_types,
                        )?;
                    }
                    result_table.upsert(result_row);
                }

                let columns: Vec<Column> = builders
                    .into_iter()
                    .map(|builder| Ok::<_, StreamExecutorError>(builder.finish().into()))
                    .try_collect()?;

                let chunk = StreamChunk::new(new_ops, columns, None);

                trace!("output_chunk: {:?}", &chunk);
                yield chunk;
            }

            // TODO(rc): check if this need to be done earlier
            result_table.commit(epoch).await?;

            // Evict cache to target capacity
            state_map.evict();
        } else {
            // Nothing to flush.
            // Call commit on state table to increment the epoch.
            agg_state_tables
                .iter_mut()
                .filter_map(Option::as_mut)
                .for_each(|state_table| {
                    state_table.table.commit_no_data_expected(epoch);
                });
            result_table.commit_no_data_expected(epoch);
            return Ok(());
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let HashAggExecutor {
            input, mut extra, ..
        } = self;

        // The cached states. `HashKey -> (prev_value, value)`.
        let mut state_map = if let Some(lru_manager) = extra.lru_manager.clone() {
            ExecutorCache::Managed(lru_manager.create_cache_with_hasher(PrecomputedBuildHasher))
        } else {
            ExecutorCache::Local(EvictableHashMap::with_hasher(
                extra.group_by_cache_size,
                PrecomputedBuildHasher,
            ))
        };

        // First barrier
        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        extra
            .agg_state_tables
            .iter_mut()
            .filter_map(Option::as_mut)
            .for_each(|state_table| {
                state_table.table.init_epoch(barrier.epoch);
            });
        extra.result_table.init_epoch(barrier.epoch);
        state_map.update_epoch(barrier.epoch.curr);

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&mut extra, &mut state_map, chunk).await?;
                }
                Message::Barrier(barrier) => {
                    #[for_await]
                    for chunk in Self::flush_data(&mut extra, &mut state_map, barrier.epoch) {
                        yield Message::Chunk(chunk?);
                    }

                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(extra.ctx.id) {
                        extra
                            .agg_state_tables
                            .iter_mut()
                            .filter_map(Option::as_mut)
                            .for_each(|state_table| {
                                state_table.table.update_vnode_bitmap(vnode_bitmap.clone());
                            });
                        extra.result_table.update_vnode_bitmap(vnode_bitmap.clone());
                    }

                    // Update the current epoch.
                    state_map.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::data_chunk_iter::Row;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::*;
    use risingwave_storage::memory::MemoryStateStore;

    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::test_utils::agg_executor::create_state_table;
    use crate::executor::test_utils::*;
    use crate::executor::{ActorContext, Executor, HashAggExecutor, Message, PkIndices};

    #[allow(clippy::too_many_arguments)]
    fn new_boxed_hash_agg_executor(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        keyspace_gen: Vec<(MemoryStateStore, TableId)>,
        pk_indices: PkIndices,
        group_by_cache_size: usize,
        extreme_cache_size: usize,
        executor_id: u64,
    ) -> Box<dyn Executor> {
        let (state_tables, state_table_col_mappings) = keyspace_gen
            .iter()
            .zip_eq(agg_calls.iter())
            .map(|(ks, agg_call)| {
                create_state_table(
                    ks.0.clone(),
                    ks.1,
                    agg_call,
                    &key_indices,
                    &pk_indices,
                    input.as_ref(),
                )
            })
            .unzip();

        HashAggExecutor::<SerializedKey, MemoryStateStore>::new(
            ActorContext::create(123),
            input,
            agg_calls,
            pk_indices,
            executor_id,
            key_indices,
            group_by_cache_size,
            extreme_cache_size,
            state_tables,
            state_table_col_mappings,
            None,
            Arc::new(StreamingMetrics::unused()),
        )
        .unwrap()
        .boxed()
    }

    // --- Test HashAgg with in-memory KeyedState ---

    #[tokio::test]
    async fn test_local_hash_aggregation_count_in_memory() {
        test_local_hash_aggregation_count(create_in_memory_keyspace_agg(3)).await
    }

    #[tokio::test]
    async fn test_global_hash_aggregation_count_in_memory() {
        test_global_hash_aggregation_count(create_in_memory_keyspace_agg(3)).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_min_in_memory() {
        test_local_hash_aggregation_min(create_in_memory_keyspace_agg(2)).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_min_append_only_in_memory() {
        test_local_hash_aggregation_min_append_only(create_in_memory_keyspace_agg(2)).await
    }

    async fn test_local_hash_aggregation_count(keyspace: Vec<(MemoryStateStore, TableId)>) {
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
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            Box::new(source),
            agg_calls,
            keys,
            keyspace,
            vec![],
            1 << 16,
            1 << 10,
            1,
        );
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

    async fn test_global_hash_aggregation_count(keyspace: Vec<(MemoryStateStore, TableId)>) {
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
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            // This is local hash aggregation, so we add another sum state
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 2),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            Box::new(source),
            agg_calls,
            key_indices,
            keyspace,
            vec![],
            1 << 16,
            1 << 10,
            1,
        );
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

    async fn test_local_hash_aggregation_min(keyspace: Vec<(MemoryStateStore, TableId)>) {
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
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            Box::new(source),
            agg_calls,
            keys,
            keyspace,
            vec![2],
            1 << 16,
            1 << 10,
            1,
        );
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

    async fn test_local_hash_aggregation_min_append_only(
        keyspace: Vec<(MemoryStateStore, TableId)>,
    ) {
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
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            Box::new(source),
            agg_calls,
            keys,
            keyspace,
            vec![2],
            1 << 16,
            1 << 10,
            1,
        );
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
        fn sorted_rows(self) -> Vec<(Op, Row)>;
    }
    impl SortedRows for StreamChunk {
        fn sorted_rows(self) -> Vec<(Op, Row)> {
            let (chunk, ops) = self.into_parts();
            ops.into_iter()
                .zip_eq(chunk.rows().map(Row::from))
                .sorted()
                .collect_vec()
        }
    }
}
