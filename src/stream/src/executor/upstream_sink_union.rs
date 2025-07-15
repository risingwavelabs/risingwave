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

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::try_join_all;
use risingwave_expr::expr::NonStrictExpression;

use crate::executor::MergeExecutor;
use crate::executor::exchange::input::{Input, new_input};
use crate::executor::merge::DynamicReceivers;
use crate::executor::prelude::*;
use crate::executor::project::apply_project_exprs;
use crate::task::{FragmentId, LocalBarrierManager};

/// `MergeProjectExecutor` applies a projection to the output of a merge executor.
struct MergeProjectExecutor {
    /// Expressions of the current projection.
    project_exprs: Vec<NonStrictExpression>,

    /// The stream of messages after handled by merge executor.
    merge: BoxedMessageStream,
}

impl MergeProjectExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Self) {
        while let Some(msg) = self.merge.next().await {
            let msg = msg?;
            if let Message::Chunk(chunk) = msg {
                // Apply the projection expressions to the chunk.
                let new_chunk = apply_project_exprs(&self.project_exprs, chunk).await?;
                yield Message::Chunk(new_chunk);
            } else {
                yield msg;
            }
        }
    }
}

pub struct SinkHandlerInput {
    /// The ID of the upstream fragment that this input is associated with.
    upstream_fragment_id: FragmentId,

    /// The stream of messages from the upstream fragment.
    executor_stream: BoxedMessageStream,
}

impl SinkHandlerInput {
    pub fn new(
        upstream_fragment_id: FragmentId,
        merge: Box<MergeExecutor>,
        project_exprs: Vec<NonStrictExpression>,
    ) -> Self {
        let merge_stream = merge.execute();
        let executor = MergeProjectExecutor {
            project_exprs,
            merge: merge_stream,
        };
        let executor_stream = executor.execute_inner().boxed();
        Self {
            upstream_fragment_id,
            executor_stream,
        }
    }
}

impl Input for SinkHandlerInput {
    type InputId = FragmentId;

    fn id(&self) -> Self::InputId {
        // Return a unique identifier for this input, e.g., based on the upstream fragment ID
        self.upstream_fragment_id
    }
}

impl Stream for SinkHandlerInput {
    type Item = MessageStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.executor_stream.poll_next_unpin(cx)
    }
}

#[derive(Debug)]
pub struct UpstreamInfo {
    pub upstream_fragment_id: FragmentId,
    pub merge_schema: Schema,
    pub project_exprs: Vec<NonStrictExpression>,
}

pub struct UpstreamSinkUnionExecutor {
    /// The context of the actor.
    actor_context: ActorContextRef,

    /// Used to create merge executors.
    local_barrier_manager: LocalBarrierManager,

    /// Streaming metrics.
    executor_stats: Arc<StreamingMetrics>,

    /// The size of the chunks to be processed.
    chunk_size: usize,

    /// The merge infos for upstream fragments.
    upstream_infos: Vec<UpstreamInfo>,
}

impl Debug for UpstreamSinkUnionExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamSinkUnionExecutor")
            .field("upstream_infos", &self.upstream_infos)
            .finish()
    }
}

impl Execute for UpstreamSinkUnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl UpstreamSinkUnionExecutor {
    pub fn new(
        ctx: ActorContextRef,
        local_barrier_manager: LocalBarrierManager,
        executor_stats: Arc<StreamingMetrics>,
        chunk_size: usize,
        upstream_infos: Vec<(FragmentId, Schema, Vec<NonStrictExpression>)>,
    ) -> Self {
        Self {
            actor_context: ctx,
            local_barrier_manager,
            executor_stats,
            chunk_size,
            upstream_infos: upstream_infos
                .into_iter()
                .map(
                    |(upstream_fragment_id, merge_schema, project_exprs)| UpstreamInfo {
                        upstream_fragment_id,
                        merge_schema,
                        project_exprs,
                    },
                )
                .collect(),
        }
    }

    async fn new_merge_executor(
        &self,
        upstream_fragment_id: FragmentId,
        schema: Schema,
    ) -> StreamExecutorResult<MergeExecutor> {
        let barrier_rx = self
            .local_barrier_manager
            .subscribe_barrier(self.actor_context.id);

        let inputs: Vec<_> = try_join_all(
            self.actor_context
                .initial_upstream_actors
                .get(&upstream_fragment_id)
                .map(|actors| actors.actors.iter())
                .into_iter()
                .flatten()
                .map(|upstream_actor| {
                    new_input(
                        &self.local_barrier_manager,
                        self.executor_stats.clone(),
                        self.actor_context.id,
                        self.actor_context.fragment_id,
                        upstream_actor,
                        upstream_fragment_id,
                    )
                }),
        )
        .await?;

        let upstreams =
            MergeExecutor::new_select_receiver(inputs, &self.executor_stats, &self.actor_context);

        Ok(MergeExecutor::new(
            self.actor_context.clone(),
            self.actor_context.fragment_id,
            upstream_fragment_id,
            upstreams,
            self.local_barrier_manager.clone(),
            self.executor_stats.clone(),
            barrier_rx,
            self.chunk_size,
            schema,
        ))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        let actor_id = self.actor_context.id;
        let fragment_id = self.actor_context.fragment_id;

        let barrier_align = self
            .executor_stats
            .barrier_align_duration
            .with_guarded_label_values(&[
                actor_id.to_string().as_str(),
                fragment_id.to_string().as_str(),
                "",
                "UpstreamSinkUnion",
            ]);

        let inputs: Vec<_> = {
            let upstream_infos = std::mem::replace(&mut self.upstream_infos, Vec::new());
            let mut inputs = Vec::with_capacity(upstream_infos.len());
            for UpstreamInfo {
                upstream_fragment_id,
                merge_schema,
                project_exprs,
            } in upstream_infos
            {
                let merge_executor = self
                    .new_merge_executor(upstream_fragment_id, merge_schema)
                    .await?;

                let input = SinkHandlerInput::new(
                    upstream_fragment_id,
                    Box::new(merge_executor),
                    project_exprs,
                )
                .boxed_input();

                inputs.push(input);
            }
            inputs
        };

        let upstreams = DynamicReceivers::new(inputs, Some(barrier_align), None);
        pin_mut!(upstreams);

        // let mut start_time = Instant::now();

        while let Some(msg) = upstreams.next().await {
            yield msg?;
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use risingwave_storage::hummock::compactor::await_tree_key::Compaction::MergingTask;

//     use super::*;

//     #[tokio::test]
//     async fn test_static() {
//         let merger = MergeExecutor::for_test(actor_id, inputs, local_barrier_manager, Schema::new(vec![]));
//     }
// }
