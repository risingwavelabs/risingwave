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

use risingwave_pb::stream_plan::RowIdGenNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::Executor;
use crate::executor::row_id_gen::RowIdGenExecutor;
use crate::task::ExecutorParams;

pub struct RowIdGenExecutorBuilder;

impl ExecutorBuilder for RowIdGenExecutorBuilder {
    type Node = RowIdGenNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();
        tracing::debug!("row id gen executor: {:?}", params.vnode_bitmap);
        let vnodes = params
            .vnode_bitmap
            .expect("vnodes not set for row id gen executor");
        let exec = RowIdGenExecutor::new(
            params.actor_context,
            upstream,
            node.row_id_index as _,
            vnodes,
        );
        Ok((params.info, exec).into())
    }
}
