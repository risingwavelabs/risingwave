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

// Copyright 2023 Singularity Data
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

use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::HashKey;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::StateStore;

use super::group_top_n::GroupTopNCache;
use super::top_n_cache::AppendOnlyTopNCacheTrait;
use super::utils::*;
use super::TopNCache;
use crate::cache::cache_may_stale;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::top_n::ManagedTopNState;
use crate::executor::{ActorContextRef, Executor, ExecutorInfo, PkIndices, Watermark};
use crate::task::AtomicU64Ref;

/// If the input contains only append, `AppendOnlyGroupTopNExecutor` does not need
/// to keep all the data records/rows that have been seen. As long as a record
/// is no longer being in the result set, it can be deleted.
pub type AppendOnlyGroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerAppendOnlyGroupTopNExecutorNew<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    AppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let info = input.info();
        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerAppendOnlyGroupTopNExecutorNew::new(
                info,
                storage_key,
                offset_and_limit,
                order_by,
                executor_id,
                group_by,
                state_table,
                watermark_epoch,
            )?,
        })
    }
}

pub struct InnerAppendOnlyGroupTopNExecutorNew<K: HashKey, S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

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

    /// Used for serializing pk into CacheKey.
    cache_key_serde: CacheKeySerde,
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    InnerAppendOnlyGroupTopNExecutorNew<K, S, WITH_TIES>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        lru_manager: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input_info;

        let cache_key_serde =
            create_cache_key_serde(&storage_key, &pk_indices, &schema, &order_by, &group_by);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());

        Ok(Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("AppendOnlyGroupTopNExecutor {:X}", executor_id),
            },
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_idx).collect(),
            group_by,
            caches: GroupTopNCache::new(lru_manager),
            cache_key_serde,
        })
    }
}
#[async_trait]
impl<K: HashKey, S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerAppendOnlyGroupTopNExecutorNew<K, S, WITH_TIES>
where
    TopNCache<WITH_TIES>: AppendOnlyTopNCacheTrait,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.limit);
        let mut res_rows = Vec::with_capacity(self.limit);
        let chunk = chunk.compact();
        let keys = K::build(&self.group_by, chunk.data_chunk())?;

        let data_types = self.schema().data_types();
        let row_deserializer = RowDeserializer::new(data_types);

        for ((op, row_ref), group_cache_key) in chunk.rows().zip_eq_debug(keys.iter()) {
            // The pk without group by
            let pk_row = row_ref.project(&self.storage_key_indices[self.group_by.len()..]);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);

            let group_key = row_ref.project(&self.group_by);

            // If 'self.caches' does not already have a cache for the current group, create a new
            // cache for it and insert it into `self.caches`
            if !self.caches.contains(group_cache_key) {
                let mut topn_cache = TopNCache::new(self.offset, self.limit);
                self.managed_state
                    .init_topn_cache(Some(group_key), &mut topn_cache)
                    .await?;
                self.caches.push(group_cache_key.clone(), topn_cache);
            }
            let cache = self.caches.get_mut(group_cache_key).unwrap();

            debug_assert_eq!(op, Op::Insert);
            cache.insert(
                cache_key,
                row_ref,
                &mut res_ops,
                &mut res_rows,
                &mut self.managed_state,
                &row_deserializer,
            )?;
        }

        generate_output(res_rows, res_ops, self.schema())
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    fn info(&self) -> &ExecutorInfo {
        &self.info
    }

    fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        let previous_vnode_bitmap = self
            .managed_state
            .state_table
            .update_vnode_bitmap(vnode_bitmap.clone());

        if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
            self.caches.clear();
        }
    }

    fn evict(&mut self) {
        self.caches.evict()
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        Ok(())
    }

    async fn handle_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if watermark.col_idx == self.group_by[0] {
            self.managed_state
                .state_table
                .update_watermark(watermark.val.clone());
            Some(watermark)
        } else {
            None
        }
    }
}
