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

use std::collections::HashMap;
use std::iter;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Context as _;
use futures::future::try_join_all;
use pin_project::pin_project;
use risingwave_common::catalog::Field;
use risingwave_expr::expr::{EvalErrorReport, NonStrictExpression, build_non_strict_from_prost};
use risingwave_pb::common::PbActorInfo;
use risingwave_pb::expr::PbExprNode;
use risingwave_pb::plan_common::PbField;
use risingwave_pb::stream_service::inject_barrier_request::build_actor_info::UpstreamActors;
use rw_futures_util::pending_on_none;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::executor::exchange::input::{Input, assert_equal_dispatcher_barrier, new_input};
use crate::executor::prelude::*;
use crate::executor::project::apply_project_exprs;
use crate::executor::{
    BarrierMutationType, BoxedMessageInput, DynamicReceivers, MergeExecutor, Message,
};
use crate::task::{ActorEvalErrorReport, FragmentId, LocalBarrierManager};

type ProcessedMessageStream = impl Stream<Item = MessageStreamItem>;

/// A wrapper that merges data from a single upstream fragment and applies projection expressions.
/// Each `SinkHandlerInput` represents one upstream fragment with its own merge executor and projection logic.
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
        let processed_stream = Self::apply_project_exprs_stream(merge, project_exprs);
        Self {
            upstream_fragment_id,
            processed_stream,
        }
    }

    #[define_opaque(ProcessedMessageStream)]
    fn apply_project_exprs_stream(
        merge: Box<MergeExecutor>,
        project_exprs: Vec<NonStrictExpression>,
    ) -> ProcessedMessageStream {
        // Apply the projection expressions to the output of the merge executor.
        Self::apply_project_exprs_stream_impl(merge, project_exprs)
    }

    /// Applies a projection to the output of a merge executor.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn apply_project_exprs_stream_impl(
        merge: Box<MergeExecutor>,
        project_exprs: Vec<NonStrictExpression>,
    ) {
        let merge_stream = merge.execute_inner();
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

/// Information about an upstream fragment including its schema and projection expressions.
#[derive(Debug)]
pub struct UpstreamFragmentInfo {
    pub upstream_fragment_id: FragmentId,
    pub upstream_actors: Vec<PbActorInfo>,
    pub merge_schema: Schema,
    pub project_exprs: Vec<NonStrictExpression>,
}

impl UpstreamFragmentInfo {
    pub fn new(
        upstream_fragment_id: FragmentId,
        initial_upstream_actors: &HashMap<FragmentId, UpstreamActors>,
        sink_output_schema: &[PbField],
        project_exprs: &[PbExprNode],
        error_report: impl EvalErrorReport + 'static,
    ) -> StreamResult<Self> {
        let actors = initial_upstream_actors
            .get(&upstream_fragment_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "upstream fragment {} not found in initial upstream actors",
                    upstream_fragment_id
                )
            })?;
        let merge_schema = sink_output_schema.iter().map(Field::from).collect();
        let project_exprs = project_exprs
            .iter()
            .map(|e| build_non_strict_from_prost(e, error_report.clone()))
            .try_collect()
            .map_err(|err| anyhow::anyhow!(err))?;
        Ok(Self {
            upstream_fragment_id,
            upstream_actors: actors.actors.clone(),
            merge_schema,
            project_exprs,
        })
    }
}

type BoxedSinkInput = BoxedMessageInput<FragmentId, BarrierMutationType>;

/// `UpstreamSinkUnionExecutor` merges data from multiple upstream fragments, where each fragment
/// has its own merge logic and projection expressions. This executor is specifically designed for
/// sink operations that need to union data from different upstream sources.
///
/// Unlike a simple union that just merges streams, this executor:
/// 1. Creates a separate `MergeExecutor` for each upstream fragment
/// 2. Applies fragment-specific projection expressions to each stream
/// 3. Unions all the processed streams into a single output stream
///
/// This is useful for sink operators that need to collect data from multiple upstream fragments
/// with potentially different schemas or processing requirements.
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
    initial_upstream_infos: Vec<UpstreamFragmentInfo>,

    /// The error report for evaluation errors.
    eval_error_report: ActorEvalErrorReport,

    /// Used to get barriers directly from the `BarrierManager`.
    barrier_rx: UnboundedReceiver<Barrier>,

    /// Used to send barriers to the merge executors of upstream fragments.
    barrier_tx_map: HashMap<FragmentId, UnboundedSender<Barrier>>,
}

impl Debug for UpstreamSinkUnionExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamSinkUnionExecutor")
            .field("initial_upstream_infos", &self.initial_upstream_infos)
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
        initial_upstream_infos: Vec<UpstreamFragmentInfo>,
        eval_error_report: ActorEvalErrorReport,
    ) -> Self {
        let barrier_rx = local_barrier_manager.subscribe_barrier(ctx.id);
        Self {
            actor_context: ctx,
            local_barrier_manager,
            executor_stats,
            chunk_size,
            initial_upstream_infos,
            eval_error_report,
            barrier_rx,
            barrier_tx_map: Default::default(),
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
        let barrier_rx = local_barrier_manager.subscribe_barrier(actor_id);
        Self {
            actor_context: actor_ctx.clone(),
            local_barrier_manager,
            executor_stats: metrics.into(),
            chunk_size,
            initial_upstream_infos: vec![],
            eval_error_report: ActorEvalErrorReport {
                actor_context: actor_ctx,
                identity: format!("UpstreamSinkUnionExecutor-{}", actor_id).into(),
            },
            barrier_rx,
            barrier_tx_map: Default::default(),
        }
    }

    fn subscribe_local_barrier(&mut self, fragment_id: FragmentId) -> UnboundedReceiver<Barrier> {
        let (tx, rx) = unbounded_channel();
        self.barrier_tx_map
            .try_insert(fragment_id, tx)
            .expect("non-duplicate");
        rx
    }

    async fn new_sink_input(
        &mut self,
        UpstreamFragmentInfo {
            upstream_fragment_id,
            upstream_actors,
            merge_schema,
            project_exprs,
        }: UpstreamFragmentInfo,
    ) -> StreamExecutorResult<BoxedSinkInput> {
        let merge_executor = self
            .new_merge_executor(upstream_fragment_id, upstream_actors, merge_schema)
            .await?;

        Ok(SinkHandlerInput::new(
            upstream_fragment_id,
            Box::new(merge_executor),
            project_exprs,
        )
        .boxed_input())
    }

    async fn new_merge_executor(
        &mut self,
        upstream_fragment_id: FragmentId,
        upstream_actors: Vec<PbActorInfo>,
        schema: Schema,
    ) -> StreamExecutorResult<MergeExecutor> {
        let barrier_rx = self.subscribe_local_barrier(upstream_fragment_id);

        let inputs = try_join_all(upstream_actors.iter().map(|actor| {
            new_input(
                &self.local_barrier_manager,
                self.executor_stats.clone(),
                self.actor_context.id,
                self.actor_context.fragment_id,
                actor,
                upstream_fragment_id,
            )
        }))
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
            let initial_upstream_infos = std::mem::take(&mut self.initial_upstream_infos);
            let mut inputs = Vec::with_capacity(initial_upstream_infos.len());
            for UpstreamFragmentInfo {
                upstream_fragment_id,
                upstream_actors,
                merge_schema,
                project_exprs,
            } in initial_upstream_infos
            {
                let merge_executor = self
                    .new_merge_executor(upstream_fragment_id, upstream_actors, merge_schema)
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

    async fn handle_update(
        &mut self,
        upstreams: &mut DynamicReceivers<FragmentId, BarrierMutationType>,
        barrier: &Barrier,
    ) -> StreamExecutorResult<()> {
        let fragment_id = self.actor_context.fragment_id;
        if let Some(new_upstream_sink) = barrier.as_new_upstream_sink(fragment_id) {
            // Create new inputs for the newly added upstream sinks.
            let info = new_upstream_sink.get_info().unwrap();
            let merge_schema = info
                .get_sink_output_schema()
                .iter()
                .map(Field::from)
                .collect();
            let project_exprs = info
                .get_project_exprs()
                .iter()
                .map(|e| build_non_strict_from_prost(e, self.eval_error_report.clone()))
                .try_collect()
                .map_err(|err| anyhow::anyhow!(err))?;
            let mut new_input = self
                .new_sink_input(UpstreamFragmentInfo {
                    upstream_fragment_id: info.get_upstream_fragment_id(),
                    upstream_actors: new_upstream_sink.get_upstream_actors().clone(),
                    merge_schema,
                    project_exprs,
                })
                .await?;
            self.barrier_tx_map
                .get(&info.get_upstream_fragment_id())
                .unwrap()
                .send(barrier.clone())
                .map_err(|e| StreamExecutorError::from(anyhow::anyhow!(e)))?;

            let new_barrier = expect_first_barrier(&mut new_input).await?;
            assert_equal_dispatcher_barrier(barrier, &new_barrier);

            upstreams.add_upstreams_from(iter::once(new_input));
        }

        if let Some(dropped_upstream_sinks) = barrier.as_dropped_upstream_sinks()
            && !dropped_upstream_sinks.is_empty()
        {
            // Remove the upstream sinks that are no longer needed.
            upstreams.remove_upstreams(dropped_upstream_sinks);
            for upstream_fragment_id in dropped_upstream_sinks {
                self.barrier_tx_map.remove(upstream_fragment_id);
            }
        }

        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_with_inputs(mut self: Box<Self>, inputs: Vec<BoxedSinkInput>) {
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

        let upstreams = DynamicReceivers::new(inputs, Some(barrier_align.clone()), None);
        pin_mut!(upstreams);

        let mut current_barrier = None;

        // Here, `tokio::select` cannot be used directly in `try_stream` function.
        // Err for breaking the loop. Ok(None) for continuing the loop.
        let mut select_once = async || -> StreamExecutorResult<Message> {
            loop {
                tokio::select! {
                    biased;

                    // If None is returned, it means upstreams is empty, which
                    // means we should continue pending and wait on the second branch.
                    msg = pending_on_none(upstreams.next()) => {
                        let msg = msg.context("UpstreamSinkUnionExecutor pull upstream failed")?;
                        if let Message::Barrier(barrier) = &msg {
                            let current_barrier = current_barrier.take().unwrap();
                            assert_equal_dispatcher_barrier(&current_barrier, barrier);
                            self.handle_update(&mut upstreams, barrier).await?;
                        }
                        return Ok(msg);
                    }

                    barrier = self.barrier_rx.recv(), if current_barrier.is_none() => {
                        let barrier = barrier.context("Failed to receive barrier from barrier_rx")?;
                        // Here, if there's no upstream, we should process the barrier directly and send it out.
                        // Otherwise, we need to forward the barrier to the upstream and then wait in the first branch
                        // until the upstreams have processed the barrier.
                        if upstreams.is_empty() {
                            self.handle_update(&mut upstreams, &barrier).await?;
                            return Ok(Message::Barrier(barrier.clone()));
                        } else {
                            for tx in self.barrier_tx_map.values() {
                                tx.send(barrier.clone())
                                    .map_err(|e| StreamExecutorError::from(anyhow::anyhow!(e)))?;
                            }
                            current_barrier = Some(barrier);
                            continue;
                        }
                    }
                }
            }
        };

        loop {
            yield select_once().await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use futures::FutureExt;
    use risingwave_common::array::{Op, StreamChunkTestExt};
    use risingwave_common::catalog::Field;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::PbUpstreamSinkInfo;
    use risingwave_pb::stream_plan::add_mutation::PbNewUpstreamSink;

    use super::*;
    use crate::executor::exchange::permit::{Sender, channel_for_test};
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::{AddMutation, MessageInner, StopMutation};
    use crate::task::NewOutputRequest;
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;
    use crate::task::test_utils::helper_make_local_actor;

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
            None,
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
    ) -> (BoxedSinkInput, Sender, UnboundedSender<Barrier>) {
        let (tx, rx) = channel_for_test();
        let (barrier_tx, barrier_rx) = unbounded_channel();
        let merge = MergeExecutor::for_test(
            actor_id,
            vec![rx],
            local_barrier_manager,
            Schema::new(vec![]),
            10,
            Some(barrier_rx),
        );
        let input = SinkHandlerInput::new(actor_id, Box::new(merge), vec![]).boxed_input();
        (input, tx, barrier_tx)
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
        let mut barrier_txs = Vec::with_capacity(3);
        for _ in 0..3 {
            let (input, tx, barrier_tx) =
                new_input_for_test(actor_id, test_env.local_barrier_manager.clone());
            inputs.push(input);
            txs.push(tx);
            barrier_txs.push(barrier_tx);
        }

        let sink_union = UpstreamSinkUnionExecutor::for_test(
            actor_id,
            test_env.local_barrier_manager.clone(),
            10,
        );
        // Flush subscribe_barrier events to ensure the executor is ready.
        test_env.flush_all_events().await;
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

        // Because the barrier has not been emitted yet, it should not be received.
        assert!(sink_union.next().now_or_never().is_none());

        for barrier_tx in barrier_txs {
            barrier_tx.send(b1.clone()).unwrap();
        }

        let msg = sink_union.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());
        let barrier = msg.as_barrier().unwrap();
        assert_eq!(barrier.epoch, b1.epoch);
    }

    #[tokio::test]
    async fn test_dynamic_upstreams() {
        let test_env = LocalBarrierTestEnv::for_test().await;

        let actor_id = 2;
        let fragment_id = 0; // from ActorContext::for_test
        let upstream_fragment_id = 11;
        let upstream_actor_id = 101;

        let upstream_actor = helper_make_local_actor(upstream_actor_id);

        let add_upstream = PbNewUpstreamSink {
            info: Some(PbUpstreamSinkInfo {
                upstream_fragment_id,
                sink_output_schema: vec![],
                project_exprs: vec![],
            }),
            upstream_actors: vec![upstream_actor],
        };

        let b1 = Barrier::new_test_barrier(test_epoch(1));
        let b2 =
            Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Add(AddMutation {
                new_upstream_sinks: HashMap::from([(fragment_id, add_upstream)]),
                ..Default::default()
            }));
        let b3 = Barrier::new_test_barrier(test_epoch(3));
        let b4 =
            Barrier::new_test_barrier(test_epoch(4)).with_mutation(Mutation::Stop(StopMutation {
                dropped_sink_fragments: HashSet::from([upstream_fragment_id]),
                ..Default::default()
            }));
        for barrier in [&b1, &b2, &b3, &b4] {
            test_env.inject_barrier(barrier, [actor_id]);
        }
        test_env.flush_all_events().await;

        let executor = UpstreamSinkUnionExecutor::for_test(
            actor_id,
            test_env.local_barrier_manager.clone(),
            10,
        );
        // Flush subscribe_barrier events to ensure the executor is ready.
        test_env.flush_all_events().await;

        // No upstream, but should still forward the barrier.
        let mut exec_stream = Box::new(executor).execute_inner().boxed();
        let msg = exec_stream.next().await.unwrap().unwrap();
        assert_eq!(msg.as_barrier().unwrap().epoch, b1.epoch);

        // Add new upstream.
        // The barrier should not be emitted because the executor is waiting for new upstream.
        assert!(exec_stream.next().now_or_never().is_none());

        let mut output_req = test_env
            .take_pending_new_output_requests(upstream_actor_id)
            .await;
        let (_, req) = output_req.pop().unwrap();
        let tx = match req {
            NewOutputRequest::Local(tx) => tx,
            NewOutputRequest::Remote(_) => unreachable!(),
        };

        tx.send(MessageInner::Barrier(b2.clone().into_dispatcher()).into())
            .await
            .unwrap();
        // Now the executor should emit the barrier.
        let msg = exec_stream.next().await.unwrap().unwrap();
        assert_eq!(msg.as_barrier().unwrap().epoch, b2.epoch);

        tx.send(MessageInner::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        let msg = exec_stream.next().await.unwrap().unwrap();
        assert!(msg.is_chunk());

        tx.send(MessageInner::Barrier(b3.clone().into_dispatcher()).into())
            .await
            .unwrap();
        let msg = exec_stream.next().await.unwrap().unwrap();
        assert_eq!(msg.as_barrier().unwrap().epoch, b3.epoch);

        // Remove upstream.
        tx.send(MessageInner::Barrier(b4.clone().into_dispatcher()).into())
            .await
            .unwrap();
        // The executor should emit the barrier with the removal update.
        let msg = exec_stream.next().await.unwrap().unwrap();
        assert_eq!(msg.as_barrier().unwrap().epoch, b4.epoch);
    }
}
