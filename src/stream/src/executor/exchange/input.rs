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

use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::anyhow;
use local_input::LocalInputStreamInner;
use pin_project::pin_project;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_rpc_client::ComputeClientPool;
use tokio::sync::mpsc;

use super::permit::Receiver;
use crate::executor::prelude::*;
use crate::executor::{DispatcherBarrier, DispatcherMessage};
use crate::task::{
    FragmentId, LocalBarrierManager, SharedContext, UpDownActorIds, UpDownFragmentIds,
};

/// `Input` provides an interface for [`MergeExecutor`](crate::executor::MergeExecutor) and
/// [`ReceiverExecutor`](crate::executor::ReceiverExecutor) to receive data from upstream actors.
pub trait Input: MessageStream {
    /// The upstream actor id.
    fn actor_id(&self) -> ActorId;

    fn boxed_input(self) -> BoxedInput
    where
        Self: Sized + 'static,
    {
        Box::pin(self)
    }
}

pub type BoxedInput = Pin<Box<dyn Input>>;

impl std::fmt::Debug for dyn Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Input")
            .field("actor_id", &self.actor_id())
            .finish_non_exhaustive()
    }
}

/// `LocalInput` receives data from a local channel.
#[pin_project]
pub struct LocalInput {
    #[pin]
    inner: LocalInputStreamInner,

    actor_id: ActorId,
}

async fn process_msg<'a>(
    msg: DispatcherMessage,
    get_mutation_subscriber: impl for<'b> FnOnce(
            &'b DispatcherBarrier,
        )
            -> &'a mut mpsc::UnboundedReceiver<crate::task::SubscribeMutationItem>
        + 'a,
) -> StreamExecutorResult<Message> {
    let barrier = match msg {
        DispatcherMessage::Chunk(c) => {
            return Ok(Message::Chunk(c));
        }
        DispatcherMessage::Barrier(b) => b,
        DispatcherMessage::Watermark(watermark) => {
            return Ok(Message::Watermark(watermark));
        }
    };
    let mutation_subscriber = get_mutation_subscriber(&barrier);

    let mutation = mutation_subscriber
        .recv()
        .await
        .ok_or_else(|| anyhow!("failed to receive mutation of barrier {:?}", barrier))
        .map(|(prev_epoch, mutation)| {
            assert_eq!(prev_epoch, barrier.epoch.prev);
            mutation
        })?;
    Ok(Message::Barrier(Barrier {
        epoch: barrier.epoch,
        mutation,
        kind: barrier.kind,
        tracing_context: barrier.tracing_context,
        passed_actors: barrier.passed_actors,
    }))
}

impl LocalInput {
    pub fn new(
        channel: Receiver,
        upstream_actor_id: ActorId,
        self_actor_id: ActorId,
        local_barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            inner: local_input::run(
                channel,
                upstream_actor_id,
                self_actor_id,
                local_barrier_manager,
            ),
            actor_id: upstream_actor_id,
        }
    }
}

mod local_input {
    use await_tree::InstrumentAwait;

    use crate::executor::exchange::error::ExchangeChannelClosed;
    use crate::executor::exchange::input::process_msg;
    use crate::executor::exchange::permit::Receiver;
    use crate::executor::prelude::try_stream;
    use crate::executor::{Message, StreamExecutorError};
    use crate::task::{ActorId, LocalBarrierManager};

    pub(super) type LocalInputStreamInner = impl crate::executor::MessageStream;

    pub(super) fn run(
        channel: Receiver,
        upstream_actor_id: ActorId,
        self_actor_id: ActorId,
        local_barrier_manager: LocalBarrierManager,
    ) -> LocalInputStreamInner {
        run_inner(
            channel,
            upstream_actor_id,
            self_actor_id,
            local_barrier_manager,
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn run_inner(
        mut channel: Receiver,
        upstream_actor_id: ActorId,
        self_actor_id: ActorId,
        local_barrier_manager: LocalBarrierManager,
    ) {
        let span: await_tree::Span = format!("LocalInput (actor {upstream_actor_id})").into();
        let mut mutation_subscriber = None;
        while let Some(msg) = channel.recv().verbose_instrument_await(span.clone()).await {
            yield process_msg(msg, |barrier| {
                mutation_subscriber.get_or_insert_with(|| {
                    local_barrier_manager.subscribe_barrier_mutation(self_actor_id, barrier)
                })
            })
            .await?;
        }
        // Always emit an error outside the loop. This is because we use barrier as the control
        // message to stop the stream. Reaching here means the channel is closed unexpectedly.
        Err(ExchangeChannelClosed::local_input(upstream_actor_id))?
    }
}

impl Stream for LocalInput {
    type Item = MessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO: shall we pass the error with local exchange?
        self.project().inner.poll_next(cx)
    }
}

impl Input for LocalInput {
    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// `RemoteInput` connects to the upstream exchange server and receives data with `gRPC`.
#[pin_project]
pub struct RemoteInput {
    #[pin]
    inner: RemoteInputStreamInner,

    actor_id: ActorId,
}

use remote_input::RemoteInputStreamInner;

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub fn new(
        local_barrier_manager: LocalBarrierManager,
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
        batched_permits: usize,
    ) -> Self {
        let actor_id = up_down_ids.0;

        Self {
            actor_id,
            inner: remote_input::run(
                local_barrier_manager,
                client_pool,
                upstream_addr,
                up_down_ids,
                up_down_frag,
                metrics,
                batched_permits,
            ),
        }
    }
}

mod remote_input {
    use std::sync::Arc;

    use anyhow::Context;
    use await_tree::InstrumentAwait;
    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::task_service::{permits, GetStreamResponse};
    use risingwave_rpc_client::ComputeClientPool;

    use crate::executor::exchange::error::ExchangeChannelClosed;
    use crate::executor::exchange::input::process_msg;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::prelude::{pin_mut, try_stream, StreamExt};
    use crate::executor::{DispatcherMessage, Message, StreamExecutorError};
    use crate::task::{LocalBarrierManager, UpDownActorIds, UpDownFragmentIds};

    pub(super) type RemoteInputStreamInner = impl crate::executor::MessageStream;

    pub(super) fn run(
        local_barrier_manager: LocalBarrierManager,
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
        batched_permits_limit: usize,
    ) -> RemoteInputStreamInner {
        run_inner(
            local_barrier_manager,
            client_pool,
            upstream_addr,
            up_down_ids,
            up_down_frag,
            metrics,
            batched_permits_limit,
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn run_inner(
        local_barrier_manager: LocalBarrierManager,
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
        batched_permits_limit: usize,
    ) {
        let self_actor_id = up_down_ids.1;
        let client = client_pool.get_by_addr(upstream_addr).await?;
        let (stream, permits_tx) = client
            .get_stream(up_down_ids.0, up_down_ids.1, up_down_frag.0, up_down_frag.1)
            .await?;

        let up_actor_id = up_down_ids.0.to_string();
        let up_fragment_id = up_down_frag.0.to_string();
        let down_fragment_id = up_down_frag.1.to_string();
        let exchange_frag_recv_size_metrics = metrics
            .exchange_frag_recv_size
            .with_guarded_label_values(&[&up_fragment_id, &down_fragment_id]);

        let span: await_tree::Span = format!("RemoteInput (actor {up_actor_id})").into();

        let mut batched_permits_accumulated = 0;
        let mut mutation_subscriber = None;

        pin_mut!(stream);
        while let Some(data_res) = stream.next().verbose_instrument_await(span.clone()).await {
            match data_res {
                Ok(GetStreamResponse { message, permits }) => {
                    let msg = message.unwrap();
                    let bytes = DispatcherMessage::get_encoded_len(&msg);

                    exchange_frag_recv_size_metrics.inc_by(bytes as u64);

                    let msg_res = DispatcherMessage::from_protobuf(&msg);
                    if let Some(add_back_permits) = match permits.unwrap().value {
                        // For records, batch the permits we received to reduce the backward
                        // `AddPermits` messages.
                        Some(permits::Value::Record(p)) => {
                            batched_permits_accumulated += p;
                            if batched_permits_accumulated >= batched_permits_limit as u32 {
                                let permits = std::mem::take(&mut batched_permits_accumulated);
                                Some(permits::Value::Record(permits))
                            } else {
                                None
                            }
                        }
                        // For barriers, always send it back immediately.
                        Some(permits::Value::Barrier(p)) => Some(permits::Value::Barrier(p)),
                        None => None,
                    } {
                        permits_tx
                            .send(add_back_permits)
                            .context("RemoteInput backward permits channel closed.")?;
                    }

                    let msg = msg_res.context("RemoteInput decode message error")?;

                    yield process_msg(msg, |barrier| {
                        mutation_subscriber.get_or_insert_with(|| {
                            local_barrier_manager.subscribe_barrier_mutation(self_actor_id, barrier)
                        })
                    })
                    .await?;
                }

                Err(e) => Err(ExchangeChannelClosed::remote_input(up_down_ids.0, Some(e)))?,
            }
        }

        // Always emit an error outside the loop. This is because we use barrier as the control
        // message to stop the stream. Reaching here means the channel is closed unexpectedly.
        Err(ExchangeChannelClosed::remote_input(up_down_ids.0, None))?
    }
}

impl Stream for RemoteInput {
    type Item = MessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl Input for RemoteInput {
    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Create a [`LocalInput`] or [`RemoteInput`] instance with given info. Used by merge executors and
/// receiver executors.
pub(crate) fn new_input(
    context: &SharedContext,
    metrics: Arc<StreamingMetrics>,
    actor_id: ActorId,
    fragment_id: FragmentId,
    upstream_actor_id: ActorId,
    upstream_fragment_id: FragmentId,
) -> StreamResult<BoxedInput> {
    let upstream_addr = context
        .get_actor_info(&upstream_actor_id)?
        .get_host()?
        .into();

    let input = if is_local_address(&context.addr, &upstream_addr) {
        LocalInput::new(
            context.take_receiver((upstream_actor_id, actor_id))?,
            upstream_actor_id,
            actor_id,
            context.local_barrier_manager.clone(),
        )
        .boxed_input()
    } else {
        RemoteInput::new(
            context.local_barrier_manager.clone(),
            context.compute_client_pool.as_ref().to_owned(),
            upstream_addr,
            (upstream_actor_id, actor_id),
            (upstream_fragment_id, fragment_id),
            metrics,
            context.config.developer.exchange_batched_permits,
        )
        .boxed_input()
    };

    Ok(input)
}
