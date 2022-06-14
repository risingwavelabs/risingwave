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

use risingwave_common::catalog::TableId;

use super::*;
use crate::executor::aggregation::AggCall;
use crate::executor::SimpleAggExecutor;

pub struct SimpleAggExecutorBuilder;

impl ExecutorBuilder for SimpleAggExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::GlobalSimpleAgg)?;
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.append_only, agg_call))
            .try_collect()?;
        // Build vector of keyspace via table ids.
        // One keyspace for one agg call.
        let keyspace = node
            .internal_tables
            .iter()
            .map(|table| {
                Keyspace::table_root_with_default_vnodes(store.clone(), &TableId::new(table.id))
            })
            .collect();
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();

        Ok(SimpleAggExecutor::new(
            params.input.remove(0),
            agg_calls,
            keyspace,
            params.pk_indices,
            params.executor_id,
            key_indices,
        )?
        .boxed())
    }
}
