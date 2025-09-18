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

use risingwave_pb::stream_plan::ChangeLogNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::{ChangeLogExecutor, Executor};
use crate::task::ExecutorParams;

pub struct ChangeLogExecutorBuilder;

impl ExecutorBuilder for ChangeLogExecutorBuilder {
    type Node = ChangeLogNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let vnodes = params
            .vnode_bitmap
            .expect("vnodes not set for row id gen executor");
        let vnode_count = node.vnode_count as usize;
        let exec = ChangeLogExecutor::new(
            params.actor_context,
            input,
            node.need_op,
            vnode_count,
            vnodes,
        );
        Ok((params.info, exec).into())
    }
}
