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

use std::collections::HashMap;

use risingwave_common::array::Op;
use risingwave_common::hash::HashKey;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::{ColumnOrder, topn_watermark_forwardable_order_key};

use super::group_top_n::GroupTopNCache;
use super::top_n_cache::AppendOnlyTopNCacheTrait;
use super::utils::*;
use super::{ManagedTopNState, TopNCache};
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTablePostCommit;
use crate::executor::monitor::GroupTopNMetrics;
use crate::executor::prelude::*;
use crate::executor::top_n::top_n_cache::TopNStaging;

/// If the input is append-only, `AppendOnlyGroupTopNExecutor` does not need
/// to keep all the rows seen. As long as a record
/// is no longer in the result set, it can be deleted.
pub type AppendOnlyGroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    AppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Executor,
        ctx: ActorContextRef,
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let inner = InnerAppendOnlyGroupTopNExecutor::new(
            schema,
            storage_key,
            offset_and_limit,
            order_by,
            group_by,
            state_table,
            watermark_epoch,
            &ctx,
        )?;
        Ok(TopNExecutorWrapper { input, ctx, inner })
    }
}

pub struct InnerAppendOnlyGroupTopNExecutor<K: HashKey, S: StateStore, const WITH_TIES: bool> {
    schema: Schema,

    /// `LIMIT XXX`. None means no limit.
    limit: usize,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The storage key indices of the `AppendOnlyGroupTopNExecutor`
    storage_key_indices: Vec<usize>,

    managed_state: ManagedTopNState<S>,

    /// which column we used to group the data.
    group_by: Vec<usize>,

    /// The `ORDER BY` column whose watermarks can be forwarded, if any.
    watermark_order_key: Option<usize>,

    /// group key -> cache for this group
    caches: GroupTopNCache<K, WITH_TIES>,

    /// Used for serializing pk into `CacheKey`.
    cache_key_serde: CacheKeySerde,

    /// Minimum cache capacity per group from config
    topn_cache_min_capacity: usize,

    metrics: GroupTopNMetrics,
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        ctx: &ActorContext,
    ) -> StreamResult<Self> {
        let metrics_info = MetricsInfo::new(
            ctx.streaming_metrics.clone(),
            state_table.table_id(),
            ctx.id,
            "AppendOnlyGroupTopN",
        );
        let metrics = ctx.streaming_metrics.new_append_only_group_top_n_metrics(
            state_table.table_id(),
            ctx.id,
            ctx.fragment_id,
        );

        let cache_key_serde = create_cache_key_serde(&storage_key, &schema, &order_by, &group_by);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());

        Ok(Self {
            schema,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            watermark_order_key: topn_watermark_forwardable_order_key(&order_by),
            group_by,
            caches: GroupTopNCache::new(watermark_epoch, metrics_info),
            cache_key_serde,
            topn_cache_min_capacity: ctx.config.developer.topn_cache_min_capacity,
            metrics,
        })
    }
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
where
    TopNCache<WITH_TIES>: AppendOnlyTopNCacheTrait,
{
    type State = S;

    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let keys = K::build_many(&self.group_by, chunk.data_chunk());
        let mut stagings: HashMap<K, TopNStaging> = HashMap::new(); // K -> `TopNStaging`

        let data_types = self.schema.data_types();
        let deserializer = RowDeserializer::new(data_types.clone());
        for (r, group_cache_key) in chunk.rows_with_holes().zip_eq_debug(keys.iter()) {
            let Some((op, row_ref)) = r else {
                continue;
            };

            // The pk without group by
            let pk_row = row_ref.project(&self.storage_key_indices[self.group_by.len()..]);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);

            let group_key = row_ref.project(&self.group_by);
            self.metrics.group_top_n_total_query_cache_count.inc();
            // If 'self.caches' does not already have a cache for the current group, create a new
            // cache for it and insert it into `self.caches`
            if !self.caches.contains(group_cache_key) {
                self.metrics.group_top_n_cache_miss_count.inc();
                let mut topn_cache = TopNCache::with_min_capacity(
                    self.offset,
                    self.limit,
                    data_types.clone(),
                    self.topn_cache_min_capacity,
                );
                self.managed_state
                    .init_append_only_topn_cache(Some(group_key), &mut topn_cache)
                    .await?;
                self.caches.put(group_cache_key.clone(), topn_cache);
            }

            let mut cache = self.caches.get_mut(group_cache_key).unwrap();
            let staging = stagings.entry(group_cache_key.clone()).or_default();

            debug_assert_eq!(op, Op::Insert);
            cache.insert(
                cache_key,
                row_ref,
                staging,
                &mut self.managed_state,
                &deserializer,
            )?;
        }

        self.metrics
            .group_top_n_cached_entry_count
            .set(self.caches.len() as i64);

        let mut chunk_builder = StreamChunkBuilder::unlimited(data_types, Some(chunk.capacity()));
        for staging in stagings.into_values() {
            for res in staging.into_deserialized_changes(&deserializer) {
                let record = res?;
                let _none = chunk_builder.append_record(record);
            }
        }

        Ok(chunk_builder.take())
    }

    async fn flush_data(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.managed_state.flush(epoch).await
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        self.managed_state.try_flush().await
    }

    fn clear_cache(&mut self) {
        self.caches.clear();
    }

    fn evict(&mut self) {
        self.caches.evict()
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.init_epoch(epoch).await
    }

    async fn handle_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if watermark.col_idx == self.group_by[0] {
            // The state table is ordered by the first group key column, so the watermark on it can
            // also be used to clean up the states of the groups below it.
            self.managed_state.update_watermark(watermark.val.clone());
        }
        // A row can only change the output of its own group, so watermarks on group key columns
        // can always be forwarded. Watermarks on the first `ORDER BY` column can be forwarded if
        // it's ordered `ASC NULLS LAST`. See `topn_watermark_forwardable_order_key` for the
        // reasoning.
        (self.group_by.contains(&watermark.col_idx)
            || Some(watermark.col_idx) == self.watermark_order_key)
            .then_some(watermark)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};

    /// Same as `GroupTopNExecutor`: watermarks on group key columns are always forwarded (the
    /// first one also cleans the state), and the watermark on the first `ORDER BY` column is
    /// forwarded only when it's ordered `ASC NULLS LAST`.
    #[tokio::test]
    async fn test_watermark_forwarding() {
        let asc = OrderType::ascending();
        let desc = OrderType::descending();
        // (group_by, order_by, storage_key, forwarded watermark columns)
        let cases = [
            (
                vec![1],
                vec![ColumnOrder::new(2, asc)],
                vec![
                    ColumnOrder::new(1, asc),
                    ColumnOrder::new(2, asc),
                    ColumnOrder::new(0, asc),
                ],
                vec![1, 2],
            ),
            (
                vec![1],
                vec![ColumnOrder::new(2, desc)],
                vec![
                    ColumnOrder::new(1, asc),
                    ColumnOrder::new(2, desc),
                    ColumnOrder::new(0, asc),
                ],
                vec![1],
            ),
            (
                vec![1, 2],
                vec![ColumnOrder::new(0, asc)],
                vec![
                    ColumnOrder::new(1, asc),
                    ColumnOrder::new(2, asc),
                    ColumnOrder::new(0, asc),
                ],
                vec![0, 1, 2],
            ),
        ];
        for (group_by, order_by, storage_key, forwarded) in cases {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            let mut messages = vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(StreamChunk::from_pretty(
                    "  I I I
                    + 10 9 1
                    +  8 8 2
                    +  7 8 2
                    +  9 1 1
                    + 10 1 1
                    +  8 1 3",
                )),
            ];
            messages.extend((0..3).map(|col_idx| {
                Message::Watermark(Watermark::new(
                    col_idx,
                    DataType::Int64,
                    ScalarImpl::Int64(5),
                ))
            }));
            messages.push(Message::Barrier(Barrier::new_test_barrier(test_epoch(2))));
            let source =
                MockSource::with_messages(messages).into_executor(schema.clone(), vec![1, 2, 0]);
            let state_table = create_in_memory_state_table(
                &[DataType::Int64, DataType::Int64, DataType::Int64],
                &storage_key.iter().map(|o| o.order_type).collect::<Vec<_>>(),
                &storage_key
                    .iter()
                    .map(|o| o.column_index)
                    .collect::<Vec<_>>(),
            )
            .await;
            let top_n = AppendOnlyGroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key,
                (0, 2),
                order_by,
                group_by,
                state_table,
                Arc::new(AtomicU64::new(0)),
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            top_n.expect_barrier().await;
            top_n.expect_chunk().await;
            for col_idx in forwarded {
                let watermark = top_n.expect_watermark().await;
                assert_eq!(watermark.col_idx, col_idx);
                assert_eq!(watermark.val, ScalarImpl::Int64(5));
            }
            top_n.expect_barrier().await;
        }
    }
}
