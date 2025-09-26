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

use risingwave_pb::stream_plan::UpstreamSinkUnionNode;

use super::*;
use crate::executor::{UpstreamFragmentInfo, UpstreamSinkUnionExecutor};

pub struct UpstreamSinkUnionExecutorBuilder;

impl ExecutorBuilder for UpstreamSinkUnionExecutorBuilder {
    type Node = UpstreamSinkUnionNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let init_upstreams = node
            .get_init_upstreams()
            .iter()
            .map(|init_upstream| {
                let upstream_fragment_id = init_upstream.get_upstream_fragment_id();
                UpstreamFragmentInfo::new(
                    upstream_fragment_id,
                    &params.actor_context.initial_upstream_actors,
                    init_upstream.get_sink_output_schema(),
                    init_upstream.get_project_exprs(),
                    params.eval_error_report.clone(),
                )
            })
            .try_collect()?;

        let executor = UpstreamSinkUnionExecutor::new(
            params.actor_context,
            params.local_barrier_manager,
            params.executor_stats,
            params.env.config().developer.chunk_size,
            init_upstreams,
            params.eval_error_report,
        )
        .await?;

        Ok((params.info, executor).into())
    }
}
