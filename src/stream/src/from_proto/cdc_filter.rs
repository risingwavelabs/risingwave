// Copyright 2023 RisingWave Labs
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
use risingwave_pb::stream_plan::CdcFilterNode;

use super::*;
use crate::executor::FilterExecutor;

pub struct CdcFilterExecutorBuilder;

/// `CdcFilter` is an extension to the Filter executor
impl ExecutorBuilder for CdcFilterExecutorBuilder {
    type Node = CdcFilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let search_condition =
            build_non_strict_from_prost(node.get_search_condition()?, params.eval_error_report)?;

        Ok(FilterExecutor::new(params.actor_context, params.info, input, search_condition).boxed())
    }
}
