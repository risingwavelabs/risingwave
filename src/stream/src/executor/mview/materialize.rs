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

use std::alloc::Global;
use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::{izip, Itertools};
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use risingwave_common::array::{Row, StreamChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::row::CompactedRow;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::catalog::Table;
use risingwave_storage::table::compute_chunk_vnode;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::cache::{EvictableHashMap, ExecutorCache, LruManagerRef};
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorError;
use crate::executor::{
    expect_first_barrier, ActorContext, ActorContextRef, BoxedExecutor, BoxedMessageStream,
    Executor, ExecutorInfo, Message, PkIndicesRef,
};

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore> {
    input: BoxedExecutor,

    state_table: StateTable<S>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_columns: Vec<usize>,

    actor_context: ActorContextRef,

    info: ExecutorInfo,

    materialize_cache: MaterializeCache,
}

impl<S: StateStore> MaterializeExecutor<S> {
    /// Create a new `MaterializeExecutor` with distribution specified with `distribution_keys` and
    /// `vnodes`. For singleton distribution, `distribution_keys` should be empty and `vnodes`
    /// should be `None`.
    pub fn new(
        input: BoxedExecutor,
        store: S,
        key: Vec<OrderPair>,
        executor_id: u64,
        actor_context: ActorContextRef,
        vnodes: Option<Arc<Bitmap>>,
        table_catalog: &Table,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
    ) -> Self {
        let alloc = StatsAlloc::new(Global).shared();
        let arrange_columns: Vec<usize> = key.iter().map(|k| k.column_idx).collect();

        let schema = input.schema().clone();

        let state_table = StateTable::from_table_catalog(table_catalog, store, vnodes);

        Self {
            input,
            state_table,
            arrange_columns: arrange_columns.clone(),
            actor_context,
            info: ExecutorInfo {
                schema,
                pk_indices: arrange_columns,
                identity: format!("MaterializeExecutor {:X}", executor_id),
            },
            materialize_cache: MaterializeCache::new(lru_manager, cache_size),
        }
    }

    /// Create a new `MaterializeExecutor` without distribution info for test purpose.
    pub fn for_test(
        input: BoxedExecutor,
        store: S,
        table_id: TableId,
        keys: Vec<OrderPair>,
        column_ids: Vec<ColumnId>,
        executor_id: u64,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
    ) -> Self {
        let alloc = StatsAlloc::new(Global).shared();
        let arrange_columns: Vec<usize> = keys.iter().map(|k| k.column_idx).collect();
        let arrange_order_types = keys.iter().map(|k| k.order_type).collect();
        let schema = input.schema().clone();
        let columns = column_ids
            .into_iter()
            .zip_eq(schema.fields.iter())
            .map(|(column_id, field)| ColumnDesc::unnamed(column_id, field.data_type()))
            .collect_vec();

        let state_table = StateTable::new_without_distribution(
            store,
            table_id,
            columns,
            arrange_order_types,
            arrange_columns.clone(),
        );

        Self {
            input,
            state_table,
            arrange_columns: arrange_columns.clone(),
            actor_context: ActorContext::create(0),
            info: ExecutorInfo {
                schema,
                pk_indices: arrange_columns,
                identity: format!("MaterializeExecutor {:X}", executor_id),
            },
            materialize_cache: MaterializeCache::new(lru_manager, cache_size),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            yield match msg {
                Message::Chunk(chunk) => {
                    let (data_chunk, op) = chunk.clone().into_parts();

                    let mut pks = vec![vec![]; chunk.capacity()];
                    compute_chunk_vnode(
                        &data_chunk,
                        self.state_table.dist_key_indices(),
                        self.state_table.vnodes(),
                    )
                    .into_iter()
                    .zip_eq(pks.iter_mut())
                    .for_each(|(vnode, vnode_and_pk)| vnode_and_pk.extend(vnode.to_be_bytes()));

                    for key in pks.clone().into_iter() {
                        if self.materialize_cache.get(&key) == None {
                            if let Some(storage_value) = self
                                .state_table
                                .keyspace()
                                .get(
                                    &key,
                                    false,
                                    self.state_table.get_read_option(self.state_table.epoch()),
                                )
                                .await?
                            {
                                // update cache
                                self.materialize_cache
                                    .insert(key, CompactedRow::new(storage_value.to_vec()));
                            } else {
                                // ignore
                            }
                        }
                    }
                    let values = data_chunk.clone().serialize();
                    let (_, vis) = data_chunk.into_parts();
                    match vis {
                        Vis::Bitmap(vis) => {
                            for ((op, key, value), vis) in izip!(op, pks, values).zip_eq(vis.iter())
                            {
                                if vis {
                                    todo!()
                                }
                            }
                        }
                        Vis::Compact(_) => {
                            for (op, key, value) in izip!(op, pks, values) {
                                todo!()
                            }
                        }
                    }

                    self.state_table.write_chunk(chunk.clone());
                    Message::Chunk(chunk)
                }
                Message::Barrier(b) => {
                    self.state_table.commit(b.epoch).await?;

                    // Update the vnode bitmap for the state table if asked.
                    if let Some(vnode_bitmap) = b.as_update_vnode_bitmap(self.actor_context.id) {
                        let _ = self.state_table.update_vnode_bitmap(vnode_bitmap);
                    }

                    Message::Barrier(b)
                }
            }
        }
    }

    fn check_chunk(&self, chunk: StreamChunk) -> StreamChunk {
        let a = chunk.clone();
        a
    }
}

// for check and modify pk
impl<S: StateStore> MaterializeExecutor<S> {
    /// Make sure the key to insert should not exist in storage.
    async fn do_insert_check(&mut self, key: &[u8], value: &[u8]) -> StreamResult<()> {
        if let Some(cache_value) = self.materialize_cache.get(key) {
            if let Some(storage_value) =  cache_value {
                
                // change to update 
            } else {
            }
        }

        Ok(())
    }


    /// Make sure that the key to delete should exist in storage and the value should be matched.
    async fn do_delete_check(&mut self, key: &[u8], old_value: &[u8]) -> StreamResult<()> {
        if let Some(cache_value) = self.materialize_cache.get(key) {
            if let Some(storage_value) =  cache_value {
                // change to delete right_value 
            } else {
            }
        }

        Ok(())
    }

           /// Make sure that the key to update should exist in storage and the value should be matched
        async fn do_update_check(&mut self, key: &[u8], old_value: &[u8], new_value: &[u8]) -> StreamResult<()> {
            if let Some(cache_value) = self.materialize_cache.get(key) {
               todo!()
            }
    
            Ok(())
        }
    
}

impl<S: StateStore> Executor for MaterializeExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        self.info.identity.as_str()
    }
}

impl<S: StateStore> std::fmt::Debug for MaterializeExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            .field("input info", &self.info())
            .field("arrange_columns", &self.arrange_columns)
            .finish()
    }
}

/// A cache for materialize executors.
pub struct MaterializeCache {
    data: ExecutorCache<Vec<u8>, Option<CompactedRow>>,
}

impl MaterializeCache {
    pub fn new(lru_manager: Option<LruManagerRef>, cache_size: usize) -> Self {
        let cache = if let Some(lru_manager) = lru_manager {
            ExecutorCache::Managed(lru_manager.create_cache())
        } else {
            ExecutorCache::Local(EvictableHashMap::new(cache_size))
        };
        Self { data: cache }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Option<CompactedRow>> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: CompactedRow) {
        self.data.push(key, Some(value));
    }
}

#[cfg(test)]
mod tests {

    use futures::stream::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::storage_table::StorageTable;

    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_materialize_executor() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // Prepare source chunks.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::new_test_barrier(3)),
            ],
        );

        let order_types = vec![OrderType::Ascending];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
        );

        let mut materialize_executor = Box::new(MaterializeExecutor::for_test(
            Box::new(source),
            memory_state_store,
            table_id,
            vec![OrderPair::new(0, OrderType::Ascending)],
            column_ids,
            1,
            None,
            0,
        ))
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(3_i32.into()), Some(6_i32.into())])));
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(7_i32.into()), Some(8_i32.into())])));
            }
            _ => unreachable!(),
        }
    }
}
