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

use std::sync::Arc;

use risingwave_common::util::sort_util::OrderPair;

use super::*;
use crate::executor::MaterializeExecutor;

pub struct MaterializeExecutorBuilder;

impl ExecutorBuilder for MaterializeExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Materialize)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let order_key = node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();

        let table = node.get_table()?;
        let executor = MaterializeExecutor::new(
            input,
            store,
            order_key,
            params.executor_id,
            params.actor_context,
            params.vnode_bitmap.map(Arc::new),
            table,
        );

        Ok(executor.boxed())
    }
}

pub struct ArrangeExecutorBuilder;

impl ExecutorBuilder for ArrangeExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let arrange_node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Arrange)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let keys = arrange_node
            .get_table_info()?
            .arrange_key_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();

        let table = arrange_node.get_table()?;

        // FIXME: Lookup is now implemented without cell-based table API and relies on all vnodes
        // being `DEFAULT_VNODE`, so we need to make the Arrange a singleton.
        let vnodes = params.vnode_bitmap.map(Arc::new);

        let executor = MaterializeExecutor::new(
            input,
            store,
            keys,
            params.executor_id,
            params.actor_context,
            vnodes,
            table,
        );

        Ok(executor.boxed())
    }
}
