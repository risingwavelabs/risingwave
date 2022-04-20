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

use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::try_match_expand;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{Executor, ExecutorBuilder, Result};
use crate::executor_v2::{
    BoxedExecutor, Executor as ExecutorV2, MaterializeExecutor as MaterializeExecutorV2,
};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct MaterializeExecutorBuilder;

impl ExecutorBuilder for MaterializeExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::MaterializeNode)?;

        let table_id = TableId::from(&node.table_ref_id);
        let keys = node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        let column_ids = node
            .column_ids
            .iter()
            .map(|id| ColumnId::from(*id))
            .collect();

        let keyspace = Keyspace::table_root(store, &table_id);

        let v2 = MaterializeExecutorV2::new_from_v1(
            params.input.remove(0),
            keyspace,
            keys,
            column_ids,
            params.executor_id,
            params.op_info,
        );

        Ok(v2.boxed())
    }
}

pub struct ArrangeExecutorBuilder;

impl ExecutorBuilder for ArrangeExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let arrange_node = try_match_expand!(node.get_node().unwrap(), Node::ArrangeNode)?;

        let keyspace = Keyspace::shared_executor_root(store, params.operator_id);

        // Set materialize keys as arrange key + pk
        let keys = arrange_node
            .arrange_key_indexes
            .iter()
            .map(|x| OrderPair::new(*x as usize, OrderType::Ascending))
            .chain(
                node.pk_indices
                    .iter()
                    .map(|x| OrderPair::new(*x as usize, OrderType::Ascending)),
            )
            .collect();

        // Simply generate column id 0..schema_len
        let column_ids = node
            .fields
            .iter()
            .enumerate()
            .map(|(idx, _)| ColumnId::from(idx as i32))
            .collect();

        let v2 = MaterializeExecutorV2::new_from_v1(
            params.input.remove(0),
            keyspace,
            keys,
            column_ids,
            params.executor_id,
            params.op_info,
        );

        Ok(v2.boxed())
    }
}
