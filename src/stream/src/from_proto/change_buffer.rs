// Copyright 2026 RisingWave Labs
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

use risingwave_pb::stream_plan::ChangeBufferNode;
use risingwave_storage::StateStore;

use super::*;
use crate::executor::ChangeBufferExecutor;

pub struct ChangeBufferExecutorBuilder;

impl ExecutorBuilder for ChangeBufferExecutorBuilder {
    type Node = ChangeBufferNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        build_change_buffer_executor(params, node, store).await
    }
}

async fn build_change_buffer_executor<S: StateStore>(
    params: ExecutorParams,
    node: &ChangeBufferNode,
    store: S,
) -> StreamResult<Executor> {
    let [input]: [_; 1] = params.input.try_into().unwrap();
    let vnodes = Arc::new(
        params
            .vnode_bitmap
            .expect("vnodes not set for change buffer"),
    );

    let exec = ChangeBufferExecutor::new(
        params.actor_context,
        input,
        store,
        node.get_state_table()?.clone(),
        vnodes,
        params.config.developer.sync_log_store_buffer_size,
    )?;
    Ok((params.info, exec).into())
}
