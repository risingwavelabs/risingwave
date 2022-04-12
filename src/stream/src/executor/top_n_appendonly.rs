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

use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan::OrderType as ProstOrderType;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{Executor, ExecutorBuilder};
use crate::executor_v2::{
    AppendOnlyTopNExecutor as AppendOnlyTopNExecutorV2, Executor as ExecutorV2,
};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct AppendOnlyTopNExecutorBuilder {}

impl ExecutorBuilder for AppendOnlyTopNExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::AppendOnlyTopNNode)?;
        let order_types: Vec<_> = node
            .get_order_types()
            .iter()
            .map(|v| ProstOrderType::from_i32(*v).unwrap())
            .map(|v| OrderType::from_prost(&v))
            .collect();
        assert_eq!(order_types.len(), params.pk_indices.len());
        let limit = if node.limit == 0 {
            None
        } else {
            Some(node.limit as usize)
        };
        let cache_size = Some(1024);
        let total_count = (0, 0);
        let keyspace = Keyspace::executor_root(store, params.executor_id);
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        Ok(Box::new(
            Box::new(AppendOnlyTopNExecutorV2::new_from_v1(
                params.input.remove(0),
                order_types,
                (node.offset as usize, limit),
                params.pk_indices,
                keyspace,
                cache_size,
                total_count,
                params.executor_id,
                params.op_info,
                key_indices,
            )?)
            .v1(),
        ))
    }
}
