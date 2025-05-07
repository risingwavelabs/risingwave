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

use std::collections::HashMap;

use risingwave_common::array::Op;
use risingwave_common::hash::HashKey;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::ColumnOrder;

use super::group_top_n::GroupTopNCache;
use super::top_n_cache::AppendOnlyTopNCacheTrait;
use super::utils::*;
use super::{ManagedTopNState, TopNCache};
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTablePostCommit;
use crate::executor::monitor::GroupTopNMetrics;
use crate::executor::prelude::*;

/// If the input is append-only, `AppendOnlyGroupTopNExecutor` does not need
/// to keep all the rows seen. As long as a record
/// is no longer in the result set, it can be deleted.
pub type AppendOnlyGroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    AppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[allow(clippy::too_many_arguments)]
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
    storage_key_indices: PkIndices,

    managed_state: ManagedTopNState<S>,

    /// which column we used to group the data.
    group_by: Vec<usize>,

    /// group key -> cache for this group
    caches: GroupTopNCache<K, WITH_TIES>,

    /// Used for serializing pk into `CacheKey`.
    cache_key_serde: CacheKeySerde,

    metrics: GroupTopNMetrics,
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[allow(clippy::too_many_arguments)]
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
            group_by,
            caches: GroupTopNCache::new(watermark_epoch, metrics_info),
            cache_key_serde,
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
        let mut stagings = HashMap::new(); // K -> `TopNStaging`

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
                let mut topn_cache = TopNCache::new(self.offset, self.limit, data_types.clone());
                self.managed_state
                    .init_topn_cache(Some(group_key), &mut topn_cache)
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
                let (op, row) = res?;
                let _none = chunk_builder.append_row(op, row);
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
            self.managed_state.update_watermark(watermark.val.clone());
            Some(watermark)
        } else {
            None
        }
    }
}
