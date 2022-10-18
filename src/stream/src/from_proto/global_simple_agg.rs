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

//! Streaming Aggregators

use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::agg_common::{build_agg_call_from_prost, build_agg_state_storages_from_proto};
use super::*;
use crate::executor::aggregation::AggCall;
use crate::executor::GlobalSimpleAggExecutor;

pub struct GlobalSimpleAggExecutorBuilder;

impl ExecutorBuilder for GlobalSimpleAggExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::GlobalSimpleAgg)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.is_append_only, agg_call))
            .try_collect()?;
        let storages =
            build_agg_state_storages_from_proto(node.get_agg_call_states(), store.clone(), None);
        let result_table =
            StateTable::from_table_catalog(node.get_result_table().unwrap(), store, None);

        Ok(GlobalSimpleAggExecutor::new(
            params.actor_context,
            input,
            agg_calls,
            storages,
            result_table,
            params.pk_indices,
            params.executor_id,
            stream.config.developer.unsafe_stream_extreme_cache_size,
        )?
        .boxed())
    }
}
