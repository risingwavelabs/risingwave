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

use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::stream_plan::RowMergeNode;

use crate::executor::RowMergeExecutor;
use crate::from_proto::*;

pub struct RowMergeExecutorBuilder;

impl ExecutorBuilder for RowMergeExecutorBuilder {
    type Node = RowMergeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [lhs_input, rhs_input]: [_; 2] = params.input.try_into().unwrap();
        let lhs_mapping = ColIndexMapping::from_protobuf(node.lhs_mapping.as_ref().unwrap());
        let rhs_mapping = ColIndexMapping::from_protobuf(node.rhs_mapping.as_ref().unwrap());

        let exec = RowMergeExecutor::new(
            params.actor_context,
            lhs_input,
            rhs_input,
            lhs_mapping,
            rhs_mapping,
            params.info.schema.clone(),
        );
        Ok(Executor::new(params.info, exec.boxed()))
    }
}
