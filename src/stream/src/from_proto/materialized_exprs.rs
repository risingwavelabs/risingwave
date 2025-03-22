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

use std::sync::Arc;

use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::MaterializedExprsNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::project::{MaterializedExprsArgs, MaterializedExprsExecutor};

pub struct MaterializedExprsExecutorBuilder;

impl ExecutorBuilder for MaterializedExprsExecutorBuilder {
    type Node = MaterializedExprsNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let exprs: Vec<_> = node
            .get_exprs()
            .iter()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .try_collect()?;

        let vnodes = params.vnode_bitmap.map(Arc::new);
        let state_table =
            StateTable::from_table_catalog(node.get_state_table()?, store, vnodes).await;

        let exec = MaterializedExprsExecutor::new(MaterializedExprsArgs {
            actor_ctx: params.actor_context,
            input,
            exprs,
            state_table,
            state_clean_col_idx: node.state_clean_col_idx.map(|i| i as _),
        });
        Ok((params.info, exec).into())
    }
}
