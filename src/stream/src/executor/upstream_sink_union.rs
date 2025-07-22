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
use pin_project::pin_project;
use risingwave_expr::expr::NonStrictExpression;

use crate::executor::exchange::input::{Input, new_input};
use crate::executor::prelude::*;
use crate::executor::project::apply_project_exprs;
use crate::executor::{BarrierMutationType, BoxedMessageInput, DynamicReceivers, MergeExecutor};
use crate::task::{FragmentId, LocalBarrierManager};

type MergeStream = impl Stream<Item = MessageStreamItem>;
type ProcessedMessageStream = impl Stream<Item = MessageStreamItem>;

#[pin_project]
pub struct SinkHandlerInput {
    /// The ID of the upstream fragment that this input is associated with.
    upstream_fragment_id: FragmentId,

    /// The stream of messages from the upstream fragment.
    #[pin]
    processed_stream: ProcessedMessageStream,
}

impl SinkHandlerInput {
    pub fn new(
        upstream_fragment_id: FragmentId,
        merge: Box<MergeExecutor>,
        project_exprs: Vec<NonStrictExpression>,
    ) -> Self {
        let merge_stream = Self::generate_stream_from_merge(merge);
        let processed_stream = Self::apply_project_exprs_stream(merge_stream, project_exprs);
        Self {
            upstream_fragment_id,
            processed_stream,
        }
    }

    #[define_opaque(MergeStream)]
    fn generate_stream_from_merge(merge: Box<MergeExecutor>) -> MergeStream {
        merge.execute_inner()
    }

    #[define_opaque(ProcessedMessageStream)]
    fn apply_project_exprs_stream(
        merge_stream: MergeStream,
        project_exprs: Vec<NonStrictExpression>,
    ) -> ProcessedMessageStream {
        Self::apply_project_exprs_stream_impl(merge_stream, project_exprs)
    }

    /// Applies a projection to the output of a merge executor.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn apply_project_exprs_stream_impl(
        merge_stream: MergeStream,
        project_exprs: Vec<NonStrictExpression>,
    ) {
        pin_mut!(merge_stream);
        while let Some(msg) = merge_stream.next().await {
            let msg = msg?;
            if let Message::Chunk(chunk) = msg {
                // Apply the projection expressions to the chunk.
                let new_chunk = apply_project_exprs(&project_exprs, chunk).await?;
                yield Message::Chunk(new_chunk);
            } else {
                yield msg;
            }
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().processed_stream.poll_next(cx)
    }
}

#[derive(Debug)]
pub struct UpstreamInfo {
    pub upstream_fragment_id: FragmentId,
    pub merge_schema: Schema,
    pub project_exprs: Vec<NonStrictExpression>,
}

type BoxedSinkInput = BoxedMessageInput<FragmentId, BarrierMutationType>;

pub struct UpstreamSinkUnionExecutor {
    /// The context of the actor.
    actor_context: ActorContextRef,

    /// Used to create merge executors.
    local_barrier_manager: LocalBarrierManager,

    /// Streaming metrics.
    executor_stats: Arc<StreamingMetrics>,

    /// The size of the chunks to be processed.
    chunk_size: usize,

    /// The initial inputs to the executor.
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

    #[cfg(test)]
    pub fn for_test(
        actor_id: ActorId,
        local_barrier_manager: LocalBarrierManager,
        chunk_size: usize,
    ) -> Self {
        let metrics = StreamingMetrics::unused();
        let actor_ctx = ActorContext::for_test(actor_id);
        Self {
            actor_context: actor_ctx,
            local_barrier_manager,
            executor_stats: metrics.into(),
            chunk_size,
            upstream_infos: vec![],
        }
    }

    #[allow(dead_code)]
    async fn new_sink_input(
        &self,
        upstream_info: UpstreamInfo,
    ) -> StreamExecutorResult<BoxedSinkInput> {
        let (upstream_fragment_id, merge_schema, project_exprs) = (
            upstream_info.upstream_fragment_id,
            upstream_info.merge_schema,
            upstream_info.project_exprs,
        );

        let merge_executor = self
            .new_merge_executor(upstream_fragment_id, merge_schema)
            .await?;

        Ok(SinkHandlerInput::new(
            upstream_fragment_id,
            Box::new(merge_executor),
            project_exprs,
        )
        .boxed_input())
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
        let inputs: Vec<_> = {
            let upstream_infos = std::mem::take(&mut self.upstream_infos);
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

        let execution_stream = self.execute_with_inputs(inputs);
        pin_mut!(execution_stream);
        while let Some(msg) = execution_stream.next().await {
            yield msg?;
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_with_inputs(self: Box<Self>, inputs: Vec<BoxedSinkInput>) {
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

        let upstreams = DynamicReceivers::new(inputs, Some(barrier_align), None);
        pin_mut!(upstreams);

        // let mut start_time = Instant::now();

        while let Some(msg) = upstreams.next().await {
            yield msg?;
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Op, StreamChunkTestExt};
    use risingwave_common::catalog::Field;

    use super::*;
    use crate::executor::MessageInner;
    use crate::executor::exchange::permit::{Sender, channel_for_test};
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;

    #[tokio::test]
    async fn test_sink_input() {
        let test_env = LocalBarrierTestEnv::for_test().await;

        let actor_id = 2;

        let b1 = Barrier::with_prev_epoch_for_test(2, 1);

        test_env.inject_barrier(&b1, [actor_id]);
        test_env.flush_all_events().await;

        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (tx1, rx1) = channel_for_test();
        let (tx2, rx2) = channel_for_test();

        let merge = MergeExecutor::for_test(
            actor_id,
            vec![rx1, rx2],
            test_env.local_barrier_manager.clone(),
            schema.clone(),
            5,
        );

        let test_expr = build_from_pretty("$1:int8");

        let mut input = SinkHandlerInput::new(
            1919, // from MergeExecutor::for_test()
            Box::new(merge),
            vec![test_expr],
        )
        .boxed_input();

        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I
            + 7 8
            - 3 6",
        );

        tx1.send(MessageInner::Chunk(chunk1).into()).await.unwrap();
        tx2.send(MessageInner::Chunk(chunk2).into()).await.unwrap();

        let msg = input.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 4
                + 5
                + 6
                + 8
                - 6"
            )
        );
    }

    fn new_input_for_test(
        actor_id: ActorId,
        local_barrier_manager: LocalBarrierManager,
    ) -> (BoxedSinkInput, Sender) {
        let (tx, rx) = channel_for_test();
        let merge = MergeExecutor::for_test(
            actor_id,
            vec![rx],
            local_barrier_manager,
            Schema::new(vec![]),
            10,
        );
        let input = SinkHandlerInput::new(actor_id, Box::new(merge), vec![]).boxed_input();
        (input, tx)
    }

    fn build_test_chunk(size: u64) -> StreamChunk {
        let ops = vec![Op::Insert; size as usize];
        StreamChunk::new(ops, vec![])
    }

    #[tokio::test]
    async fn test_fixed_upstreams() {
        let test_env = LocalBarrierTestEnv::for_test().await;

        let actor_id = 2;

        let b1 = Barrier::with_prev_epoch_for_test(2, 1);

        test_env.inject_barrier(&b1, [actor_id]);
        test_env.flush_all_events().await;

        let mut inputs = Vec::with_capacity(3);
        let mut txs = Vec::with_capacity(3);
        for _ in 0..3 {
            let (input, tx) = new_input_for_test(actor_id, test_env.local_barrier_manager.clone());
            inputs.push(input);
            txs.push(tx);
        }

        let sink_union = UpstreamSinkUnionExecutor::for_test(
            actor_id,
            test_env.local_barrier_manager.clone(),
            10,
        );
        let mut sink_union = Box::new(sink_union).execute_with_inputs(inputs).boxed();

        for tx in txs {
            tx.send(MessageInner::Chunk(build_test_chunk(10)).into())
                .await
                .unwrap();
            tx.send(MessageInner::Chunk(build_test_chunk(10)).into())
                .await
                .unwrap();
            tx.send(MessageInner::Barrier(b1.clone().into_dispatcher()).into())
                .await
                .unwrap();
        }

        for _ in 0..6 {
            let msg = sink_union.next().await.unwrap().unwrap();
            assert!(msg.is_chunk());
            assert_eq!(msg.as_chunk().unwrap().ops().len(), 10);
        }

        let msg = sink_union.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());
        let barrier = msg.as_barrier().unwrap();
        assert_eq!(barrier.epoch, b1.epoch);
    }
}
