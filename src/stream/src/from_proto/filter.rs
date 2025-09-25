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

use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::FilterNode;
use risingwave_pb::stream_plan::stream_node::PbStreamKind;

use super::*;
use crate::executor::{FilterExecutor, UpsertFilterExecutor};

pub struct FilterExecutorBuilder;

impl ExecutorBuilder for FilterExecutorBuilder {
    type Node = FilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let search_condition =
            build_non_strict_from_prost(node.get_search_condition()?, params.eval_error_report)?;

        let exec = if let PbStreamKind::Upsert = input.stream_kind() {
            UpsertFilterExecutor::new(params.actor_context, input, search_condition).boxed()
        } else {
            FilterExecutor::new(params.actor_context, input, search_condition).boxed()
        };
        Ok((params.info, exec).into())
    }
}
