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

//! Global Streaming Hash Aggregators

use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::function::aggregate::AggCall;
use risingwave_pb::stream_plan::HashAggNode;

use super::agg_common::{
    build_agg_state_storages_from_proto, build_distinct_dedup_table_from_proto,
};
use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::agg_common::{AggExecutorArgs, GroupAggExecutorExtraArgs};
use crate::executor::HashAggExecutor;

pub struct HashAggExecutorDispatcherArgs<S: StateStore> {
    args: AggExecutorArgs<S, GroupAggExecutorExtraArgs>,
    group_key_types: Vec<DataType>,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcherArgs<S> {
    type Output = StreamResult<BoxedExecutor>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new(self.args)?.boxed())
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for HashAggExecutorBuilder {
    type Node = HashAggNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
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
        let result_table = StateTable::from_table_catalog(
            node.get_result_table().unwrap(),
            store.clone(),
            vnodes.clone(),
        )
        .await;
        let distinct_dedup_tables =
            build_distinct_dedup_table_from_proto(node.get_distinct_dedup_tables(), store, vnodes)
                .await;

        HashAggExecutorDispatcherArgs {
            args: AggExecutorArgs {
                input,
                actor_ctx: params.actor_context,
                pk_indices: params.pk_indices,
                executor_id: params.executor_id,

                extreme_cache_size: stream.config.developer.unsafe_extreme_cache_size,

                agg_calls,
                row_count_index: node.get_row_count_index() as usize,
                storages,
                result_table,
                distinct_dedup_tables,
                watermark_epoch: stream.get_watermark_epoch(),
                metrics: params.executor_stats,
                extra: GroupAggExecutorExtraArgs {
                    group_key_indices,
                    chunk_size: params.env.config().developer.chunk_size,
                    emit_on_window_close: false,
                },
            },
            group_key_types,
        }
        .dispatch()
    }
}
