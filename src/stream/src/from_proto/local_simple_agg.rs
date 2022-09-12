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

use super::agg_call::build_agg_call_from_prost;
use super::*;
use crate::executor::aggregation::AggCall;
use crate::executor::LocalSimpleAggExecutor;

pub struct LocalSimpleAggExecutorBuilder;

impl ExecutorBuilder for LocalSimpleAggExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::LocalSimpleAgg)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.is_append_only, agg_call))
            .try_collect()?;

        Ok(LocalSimpleAggExecutor::new(
            params.actor_context,
            input,
            agg_calls,
            params.pk_indices,
            params.executor_id,
        )?
        .boxed())
    }
}
