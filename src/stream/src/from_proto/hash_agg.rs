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

//! Global Streaming Hash Aggregators

use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::agg_common::{build_agg_call_from_prost, build_agg_state_storages_from_proto};
use super::*;
use crate::cache::LruManagerRef;
use crate::executor::aggregation::{AggCall, AggStateStorage};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, HashAggExecutor, PkIndices};

pub struct HashAggExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    input: BoxedExecutor,
    agg_calls: Vec<AggCall>,
    storages: Vec<AggStateStorage<S>>,
    result_table: StateTable<S>,
    group_key_indices: Vec<usize>,
    group_key_types: Vec<DataType>,
    pk_indices: PkIndices,
    group_by_cache_size: usize,
    extreme_cache_size: usize,
    executor_id: u64,
    lru_manager: Option<LruManagerRef>,
    metrics: Arc<StreamingMetrics>,
    chunk_size: usize,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcherArgs<S> {
    type Output = StreamResult<BoxedExecutor>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new(
            self.ctx,
            self.input,
            self.agg_calls,
            self.storages,
            self.result_table,
            self.pk_indices,
            self.executor_id,
            self.group_key_indices,
            self.group_by_cache_size,
            self.extreme_cache_size,
            self.lru_manager,
            self.metrics,
            self.chunk_size,
        )?
        .boxed())
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder;

impl ExecutorBuilder for HashAggExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::HashAgg)?;
        let group_key_indices = node
            .get_group_key()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let group_key_types = group_key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].data_type())
            .collect_vec();

        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.is_append_only, agg_call))
            .try_collect()?;

        let vnodes = Some(Arc::new(
            params.vnode_bitmap.expect("vnodes not set for hash agg"),
        ));
        let storages = build_agg_state_storages_from_proto(
            node.get_agg_call_states(),
            store.clone(),
            vnodes.clone(),
        );
        let result_table =
            StateTable::from_table_catalog(node.get_result_table().unwrap(), store, vnodes);

        let args = HashAggExecutorDispatcherArgs {
            ctx: params.actor_context,
            input,
            agg_calls,
            storages,
            result_table,
            group_key_indices,
            group_key_types,
            pk_indices: params.pk_indices,
            group_by_cache_size: stream.config.developer.unsafe_stream_hash_agg_cache_size,
            extreme_cache_size: stream.config.developer.unsafe_stream_extreme_cache_size,
            executor_id: params.executor_id,
            lru_manager: stream.context.lru_manager.clone(),
            metrics: params.executor_stats,
            chunk_size: params.env.config().developer.stream_chunk_size,
        };
        args.dispatch()
    }
}
