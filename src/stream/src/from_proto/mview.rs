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
use risingwave_common::util::sort_util::OrderPair;

use super::*;
use crate::executor::MaterializeExecutor;

pub struct MaterializeExecutorBuilder;

impl ExecutorBuilder for MaterializeExecutorBuilder {
    fn new_boxed_executor<S: StateStoreProxy>(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: S,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Materialize)?;

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

        let keyspace =
            Keyspace::table_root_with_vnodes(store, &table_id, (*params.vnode_bitmap).clone());

        let distribution_keys = node
            .distribution_keys
            .iter()
            .map(|key| *key as usize)
            .collect();

        let executor = MaterializeExecutor::new(
            params.input.remove(0),
            keyspace,
            keys,
            column_ids,
            params.executor_id,
            distribution_keys,
        );

        Ok(executor.boxed())
    }
}

pub struct ArrangeExecutorBuilder;

impl ExecutorBuilder for ArrangeExecutorBuilder {
    fn new_boxed_executor<S: StateStoreProxy>(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: S,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let arrange_node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Arrange)?;

        let keyspace = Keyspace::table_root_with_vnodes(
            store,
            &TableId::from(arrange_node.table_id),
            (*params.vnode_bitmap).clone(),
        );

        let keys = arrange_node
            .get_table_info()?
            .arrange_key_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();

        let column_ids = arrange_node
            .get_table_info()?
            .column_descs
            .iter()
            .map(|x| ColumnId::from(x.column_id))
            .collect();

        let distribution_keys = arrange_node
            .distribution_keys
            .iter()
            .map(|key| *key as usize)
            .collect();

        let executor = MaterializeExecutor::new(
            params.input.remove(0),
            keyspace,
            keys,
            column_ids,
            params.executor_id,
            distribution_keys,
        );

        Ok(executor.boxed())
    }
}
