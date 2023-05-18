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

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::RowExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::StateStore;

use super::utils::*;
use super::{ManagedTopNState, TopNCache, TopNCacheTrait};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorResult;
use crate::executor::{ActorContextRef, Executor, ExecutorInfo, PkIndices, Watermark};

/// `TopNExecutor` works with input with modification, it keeps all the data
/// records/rows that have been seen, and returns topN records overall.
pub type TopNExecutor<S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerTopNExecutor<S, WITH_TIES>>;

impl<S: StateStore, const WITH_TIES: bool> TopNExecutor<S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let info = input.info();

        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerTopNExecutor::new(
                info,
                storage_key,
                offset_and_limit,
                order_by,
                executor_id,
                state_table,
            )?,
        })
    }
}

impl<S: StateStore> TopNExecutor<S, true> {
    /// It only has 1 capacity for high cache. Used to test the case where the last element in high
    /// has ties.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub fn new_with_ties_for_test(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let info = input.info();

        let mut inner = InnerTopNExecutor::new(
            info,
            storage_key,
            offset_and_limit,
            order_by,
            executor_id,
            state_table,
        )?;

        inner.cache.high_capacity = 2;

        Ok(TopNExecutorWrapper { input, ctx, inner })
    }
}

pub struct InnerTopNExecutor<S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

    /// The storage key indices of the `TopNExecutor`
    storage_key_indices: PkIndices,

    managed_state: ManagedTopNState<S>,

    /// In-memory cache of top (N + N * `TOPN_CACHE_HIGH_CAPACITY_FACTOR`) rows
    cache: TopNCache<WITH_TIES>,

    /// Used for serializing pk into CacheKey.
    cache_key_serde: CacheKeySerde,
}

impl<S: StateStore, const WITH_TIES: bool> InnerTopNExecutor<S, WITH_TIES> {
    /// # Arguments
    ///
    /// `storage_key` -- the storage pk. It's composed of the ORDER BY columns and the missing
    /// columns of pk.
    ///
    /// `order_by_len` -- The number of fields of the ORDER BY clause, and will be used to split key
    /// into `CacheKey`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input_info;
        let num_offset = offset_and_limit.0;
        let num_limit = offset_and_limit.1;

        let cache_key_serde = create_cache_key_serde(&storage_key, &schema, &order_by, &[]);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());
        let data_types = schema.data_types();

        Ok(Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("TopNExecutor {:X}", executor_id),
            },
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            cache: TopNCache::new(num_offset, num_limit, data_types),
            cache_key_serde,
        })
    }
}

#[async_trait]
impl<S: StateStore, const WITH_TIES: bool> TopNExecutorBase for InnerTopNExecutor<S, WITH_TIES>
where
    TopNCache<WITH_TIES>: TopNCacheTrait,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.cache.limit);
        let mut res_rows = Vec::with_capacity(self.cache.limit);

        // apply the chunk to state table
        for (op, row_ref) in chunk.rows() {
            let pk_row = row_ref.project(&self.storage_key_indices);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);
            match op {
                Op::Insert | Op::UpdateInsert => {
                    // First insert input row to state store
                    self.managed_state.insert(row_ref);
                    self.cache
                        .insert(cache_key, row_ref, &mut res_ops, &mut res_rows)
                }

                Op::Delete | Op::UpdateDelete => {
                    // First remove the row from state store
                    self.managed_state.delete(row_ref);
                    self.cache
                        .delete(
                            NO_GROUP_KEY,
                            &mut self.managed_state,
                            cache_key,
                            row_ref,
                            &mut res_ops,
                            &mut res_rows,
                        )
                        .await?
                }
            }
        }
        generate_output(res_rows, res_ops, self.schema())
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    fn info(&self) -> &ExecutorInfo {
        &self.info
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        self.managed_state
            .init_topn_cache(NO_GROUP_KEY, &mut self.cache)
            .await
    }

    async fn handle_watermark(&mut self, _: Watermark) -> Option<Watermark> {
        // TODO(yuhao): handle watermark
        None
    }
}
