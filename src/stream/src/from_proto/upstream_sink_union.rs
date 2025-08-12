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

use risingwave_common::catalog::Field;
use risingwave_expr::expr::build_non_strict_from_prost;
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
        let mut upstreams = Vec::with_capacity(node.get_init_upstreams().len());
        for init_upstream in node.get_init_upstreams() {
            let upstream_fragment_id = init_upstream.get_upstream_fragment_id();
            let actors = params
                .actor_context
                .initial_upstream_actors
                .get(&upstream_fragment_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "upstream fragment {} not found in initial upstream actors",
                        upstream_fragment_id
                    )
                })?;
            let merge_schema = init_upstream
                .get_sink_output_schema()
                .iter()
                .map(Field::from)
                .collect();
            let project_exprs = init_upstream
                .get_project_exprs()
                .iter()
                .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| anyhow::anyhow!(err))?;
            upstreams.push(UpstreamFragmentInfo {
                upstream_fragment_id,
                upstream_actors: actors.actors.clone(),
                merge_schema,
                project_exprs,
            });
        }

        Ok((
            params.info,
            UpstreamSinkUnionExecutor::new(
                params.actor_context,
                params.local_barrier_manager,
                params.executor_stats,
                params.env.config().developer.chunk_size,
                upstreams,
                params.eval_error_report,
            ),
        )
            .into())
    }
}
