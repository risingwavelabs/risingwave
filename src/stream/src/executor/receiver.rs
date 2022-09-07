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
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::Schema;

use super::exchange::input::BoxedInput;
use super::ActorContextRef;
use crate::executor::exchange::input::new_input;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, BoxedMessageStream, Executor, ExecutorInfo, Message, PkIndices,
    PkIndicesRef,
};
use crate::task::{FragmentId, SharedContext};
/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    /// Input from upstream.
    input: BoxedInput,

    /// Logical Operator Info
    info: ExecutorInfo,

    /// The context of the actor.
    actor_context: ActorContextRef,

    /// Belonged fragment id.
    fragment_id: FragmentId,

    /// Upstream fragment id.
    upstream_fragment_id: FragmentId,

    /// Shared context of the stream manager.
    context: Arc<SharedContext>,

    /// Metrics
    metrics: Arc<StreamingMetrics>,
}

impl std::fmt::Debug for ReceiverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiverExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl ReceiverExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        ctx: ActorContextRef,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
        input: BoxedInput,
        context: Arc<SharedContext>,
        _receiver_id: u64,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            input,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "ReceiverExecutor".to_string(),
            },
            actor_context: ctx,
            upstream_fragment_id,
            metrics,
            fragment_id,
            context,
        }
    }

    #[cfg(test)]
    pub fn for_test(input: tokio::sync::mpsc::Receiver<Message>) -> Self {
        use super::exchange::input::LocalInput;
        use crate::executor::ActorContext;

        Self::new(
            Schema::default(),
            vec![],
            ActorContext::create(233),
            514,
            1919,
            LocalInput::for_test(input),
            SharedContext::for_test().into(),
            810,
            StreamingMetrics::unused().into(),
        )
    }
}

impl Executor for ReceiverExecutor {
    fn execute(mut self: Box<Self>) -> BoxedMessageStream {
        let actor_id = self.actor_context.id;
        let actor_id_str = actor_id.to_string();
        let upstream_fragment_id_str = self.upstream_fragment_id.to_string();

        let stream = #[try_stream]
        async move {
            let mut start_time = minstant::Instant::now();
            while let Some(msg) = self.input.next().await {
                self.metrics
                    .actor_input_buffer_blocking_duration_ns
                    .with_label_values(&[&actor_id_str, &upstream_fragment_id_str])
                    .inc_by(start_time.elapsed().as_nanos() as u64);
                let mut msg: Message = msg?;

                match &mut msg {
                    Message::Chunk(chunk) => {
                        self.metrics
                            .actor_in_record_cnt
                            .with_label_values(&[&actor_id_str])
                            .inc_by(chunk.cardinality() as _);
                    }
                    Message::Barrier(barrier) => {
                        tracing::trace!(
                            target: "events::barrier::path",
                            actor_id = actor_id,
                            "receiver receives barrier from path: {:?}",
                            barrier.passed_actors
                        );
                        barrier.passed_actors.push(actor_id);

                        if let Some(update) = barrier.as_update_merge(self.actor_context.id) {
                            assert_eq!(
                                update.removed_upstream_actor_id,
                                vec![self.input.actor_id()],
                                "the removed upstream actor should be the same as the current input"
                            );
                            let upstream_actor_id = *update
                                .added_upstream_actor_id
                                .iter()
                                .exactly_one()
                                .expect("receiver should have exactly one upstream");

                            // Create new upstream receiver.
                            let mut new_upstream = new_input(
                                &self.context,
                                self.metrics.clone(),
                                self.actor_context.id,
                                self.fragment_id,
                                upstream_actor_id,
                                self.upstream_fragment_id,
                            )
                            .context("failed to create upstream input")?;

                            // Poll the first barrier from the new upstream. It must be the same as
                            // the one we polled from original upstream.
                            let new_barrier = expect_first_barrier(&mut new_upstream).await?;
                            assert_eq!(barrier, &new_barrier);

                            // Replace the input.
                            self.input = new_upstream;
                        }
                    }
                };

                yield msg;
                start_time = minstant::Instant::now();
            }
        };

        stream.boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
