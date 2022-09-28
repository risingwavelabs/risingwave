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
use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::executor::AppendOnlyTopNExecutor;

pub struct AppendOnlyTopNExecutorBuilder;

impl ExecutorBuilder for AppendOnlyTopNExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::AppendOnlyTopN)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let table = node.get_table()?;
        let vnodes = params.vnode_bitmap.map(Arc::new);
        let state_table = StateTable::from_table_catalog(table, store, vnodes);
        let order_pairs = table.get_pk().iter().map(OrderPair::from_prost).collect();
        Ok(AppendOnlyTopNExecutor::new(
            input,
            order_pairs,
            (node.offset as usize, node.limit as usize),
            node.order_by_len as usize,
            params.pk_indices,
            params.executor_id,
            state_table,
        )?
        .boxed())
    }
}
