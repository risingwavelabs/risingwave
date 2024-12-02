// Copyright 2024 RisingWave Labs
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

use itertools::Itertools;
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::ValuesNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::{Executor, ValuesExecutor};
use crate::task::ExecutorParams;

/// Build a `ValuesExecutor` for stream. As is a leaf, current workaround registers a `sender` for
/// this executor. May refractor with `BarrierRecvExecutor` in the near future.
pub struct ValuesExecutorBuilder;

impl ExecutorBuilder for ValuesExecutorBuilder {
    type Node = ValuesNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &ValuesNode,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let barrier_receiver = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);
        let progress = params
            .local_barrier_manager
            .register_create_mview_progress(params.actor_context.id);
        let rows = node
            .get_tuples()
            .iter()
            .map(|tuple| {
                tuple
                    .get_cells()
                    .iter()
                    .map(|node| {
                        build_non_strict_from_prost(node, params.eval_error_report.clone()).unwrap()
                    })
                    .collect_vec()
            })
            .collect_vec();
        let exec = ValuesExecutor::new(
            params.actor_context,
            params.info.schema.clone(),
            progress,
            rows,
            barrier_receiver,
        );
        Ok((params.info, exec).into())
    }
}
