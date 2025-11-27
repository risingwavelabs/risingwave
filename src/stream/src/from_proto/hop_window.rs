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
use risingwave_pb::stream_plan::HopWindowNode;

use super::*;
use crate::executor::HopWindowExecutor;

pub struct HopWindowExecutorBuilder;

impl ExecutorBuilder for HopWindowExecutorBuilder {
    type Node = HopWindowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let input = params.input.into_iter().next().unwrap();
        // TODO: reuse the schema derivation with frontend.
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let window_start_exprs: Vec<_> = node
            .get_window_start_exprs()
            .iter()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .try_collect()?;
        let window_end_exprs: Vec<_> = node
            .get_window_end_exprs()
            .iter()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .try_collect()?;

        let time_col = node.get_time_col() as usize;
        let window_slide = node.get_window_slide()?.into();
        let window_size = node.get_window_size()?.into();

        let chunk_size = params.config.developer.chunk_size;

        let exec = HopWindowExecutor::new(
            params.actor_context,
            input,
            time_col,
            window_slide,
            window_size,
            window_start_exprs,
            window_end_exprs,
            output_indices,
            chunk_size,
        );
        Ok((params.info, exec).into())
    }
}
