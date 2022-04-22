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

use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::Result;
use crate::executor::ExecutorBuilder;
use crate::executor_v2::BoxedExecutor;
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct MergeExecutorBuilder {}

impl ExecutorBuilder for MergeExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::MergeNode)?;
        stream.create_merge_node(params, node)
    }
}
