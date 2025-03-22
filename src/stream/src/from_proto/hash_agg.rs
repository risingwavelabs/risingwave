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

//! Streaming Hash Aggregator

use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::AggCall;
use risingwave_pb::stream_plan::HashAggNode;

use super::agg_common::{
    build_agg_state_storages_from_proto, build_distinct_dedup_table_from_proto,
};
use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::aggregate::{AggExecutorArgs, HashAggExecutor, HashAggExecutorExtraArgs};

pub struct HashAggExecutorDispatcherArgs<S: StateStore> {
    args: AggExecutorArgs<S, HashAggExecutorExtraArgs>,
    group_key_types: Vec<DataType>,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new(self.args)?.boxed())
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder;

impl ExecutorBuilder for HashAggExecutorBuilder {
    type Node = HashAggNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
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
            .map(AggCall::from_protobuf)
            .try_collect()?;

        let vnodes = Some(Arc::new(
            params.vnode_bitmap.expect("vnodes not set for hash agg"),
        ));
        let storages = build_agg_state_storages_from_proto(
            node.get_agg_call_states(),
            store.clone(),
            vnodes.clone(),
        )
        .await;
        // disable sanity check so that old value is not required when updating states
        let intermediate_state_table = StateTable::from_table_catalog(
            node.get_intermediate_state_table().unwrap(),
            store.clone(),
            vnodes.clone(),
        )
        .await;
        let distinct_dedup_tables =
            build_distinct_dedup_table_from_proto(node.get_distinct_dedup_tables(), store, vnodes)
                .await;

        let exec = HashAggExecutorDispatcherArgs {
            args: AggExecutorArgs {
                version: node.version(),

                input,
                actor_ctx: params.actor_context,
                info: params.info.clone(),

                extreme_cache_size: params.env.config().developer.unsafe_extreme_cache_size,

                agg_calls,
                row_count_index: node.get_row_count_index() as usize,
                storages,
                intermediate_state_table,
                distinct_dedup_tables,
                watermark_epoch: params.watermark_epoch,
                extra: HashAggExecutorExtraArgs {
                    group_key_indices,
                    chunk_size: params.env.config().developer.chunk_size,
                    max_dirty_groups_heap_size: params
                        .env
                        .config()
                        .developer
                        .hash_agg_max_dirty_groups_heap_size,
                    emit_on_window_close: node.get_emit_on_window_close(),
                },
            },
            group_key_types,
        }
        .dispatch()?;
        Ok((params.info, exec).into())
    }
}
