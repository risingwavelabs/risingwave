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

use anyhow::anyhow;
use either::Either;
use local_input::LocalInputStreamInner;
use pin_project::pin_project;
use risingwave_common::util::addr::{HostAddr, is_local_address};
use tokio::sync::mpsc;

use super::permit::Receiver;
use crate::executor::prelude::*;
use crate::executor::{
    BarrierInner, DispatcherBarrier, DispatcherMessage, DispatcherMessageBatch,
    DispatcherMessageStreamItem, MessageStreamItemInner,
};
use crate::task::{FragmentId, InputId, LocalBarrierManager, UpDownActorIds, UpDownFragmentIds};

/// `Input` is a more abstract upstream input type, used for `DynamicReceivers` type
/// handling of multiple upstream inputs
pub trait Input: Stream + Send {
    /// The upstream input id.
    fn id(&self) -> InputId;
}

pub type BoxedInput<Message> = Pin<Box<dyn Input<Item = Message>>>;
pub type BoxedMessageInput<M> = BoxedInput<MessageStreamItemInner<M>>;

/// `ActorInput` provides an interface for [`MergeExecutor`](crate::executor::MergeExecutor) and
/// [`ReceiverExecutor`](crate::executor::ReceiverExecutor) to receive data from upstream actors.
/// Only used for actor inputs.
pub trait ActorInput: Input<Item = DispatcherMessageStreamItem> {
    /// The upstream actor id.
    fn actor_id(&self) -> ActorId;

    fn boxed_input(self) -> BoxedActorInput
    where
        Self: Sized + 'static,
    {
        Box::pin(self)
    }
}

pub type BoxedActorInput = Pin<Box<dyn ActorInput>>;

impl std::fmt::Debug for dyn ActorInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Input")
            .field("actor_id", &self.actor_id())
            .finish_non_exhaustive()
    }
}

impl<T: ActorInput> Input for T {
    fn id(&self) -> InputId {
        self.actor_id() as InputId
    }
}

/// `LocalInput` receives data from a local channel.
#[pin_project]
pub struct LocalInput {
    #[pin]
    inner: LocalInputStreamInner,

    actor_id: ActorId,
}

pub(crate) fn assert_equal_dispatcher_barrier<M1, M2>(
    first: &BarrierInner<M1>,
    second: &BarrierInner<M2>,
) {
    assert_eq!(first.epoch, second.epoch);
    assert_eq!(first.kind, second.kind);
}

pub(crate) fn apply_dispatcher_barrier(
    recv_barrier: &mut Barrier,
    dispatcher_barrier: DispatcherBarrier,
) {
    assert_equal_dispatcher_barrier(recv_barrier, &dispatcher_barrier);
    recv_barrier
        .passed_actors
        .extend(dispatcher_barrier.passed_actors);
}

pub(crate) async fn process_dispatcher_msg(
    dispatcher_msg: DispatcherMessage,
    barrier_rx: &mut mpsc::UnboundedReceiver<Barrier>,
) -> StreamExecutorResult<Message> {
    let msg = match dispatcher_msg {
        DispatcherMessage::Chunk(chunk) => Message::Chunk(chunk),
        DispatcherMessage::Barrier(barrier) => {
            let mut recv_barrier = barrier_rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("end of barrier recv"))?;
            apply_dispatcher_barrier(&mut recv_barrier, barrier);
            Message::Barrier(recv_barrier)
        }
        DispatcherMessage::Watermark(watermark) => Message::Watermark(watermark),
    };
    Ok(msg)
}

impl LocalInput {
    pub fn new(channel: Receiver, upstream_actor_id: ActorId) -> Self {
        Self {
            inner: local_input::run(channel, upstream_actor_id),
            actor_id: upstream_actor_id,
        }
    }
}

mod local_input {
    use await_tree::InstrumentAwait;
    use either::Either;

    use crate::executor::exchange::error::ExchangeChannelClosed;
    use crate::executor::exchange::permit::Receiver;
    use crate::executor::prelude::try_stream;
    use crate::executor::{DispatcherMessage, StreamExecutorError};
    use crate::task::ActorId;

    pub(super) type LocalInputStreamInner = impl crate::executor::DispatcherMessageStream;

    #[define_opaque(LocalInputStreamInner)]
    pub(super) fn run(channel: Receiver, upstream_actor_id: ActorId) -> LocalInputStreamInner {
        run_inner(channel, upstream_actor_id)
    }

    #[try_stream(ok = DispatcherMessage, error = StreamExecutorError)]
    async fn run_inner(mut channel: Receiver, upstream_actor_id: ActorId) {
        let span = await_tree::span!("LocalInput (actor {upstream_actor_id})").verbose();
        while let Some(msg) = channel.recv().instrument_await(span.clone()).await {
            match msg.into_messages() {
                Either::Left(barriers) => {
                    for b in barriers {
                        yield b;
                    }
                }
                Either::Right(m) => {
                    yield m;
                }
            }
        }
        // Always emit an error outside the loop. This is because we use barrier as the control
        // message to stop the stream. Reaching here means the channel is closed unexpectedly.
        Err(ExchangeChannelClosed::local_input(upstream_actor_id))?
    }
}

impl Stream for LocalInput {
    type Item = DispatcherMessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO: shall we pass the error with local exchange?
        self.project().inner.poll_next(cx)
    }
}

impl ActorInput for LocalInput {
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
use risingwave_pb::common::ActorInfo;

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn new(
        local_barrier_manager: &LocalBarrierManager,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
    ) -> StreamExecutorResult<Self> {
        let actor_id = up_down_ids.0;

        let client = local_barrier_manager
            .env
            .client_pool()
            .get_by_addr(upstream_addr)
            .await?;
        let (stream, permits_tx) = client
            .get_stream(
                up_down_ids.0,
                up_down_ids.1,
                up_down_frag.0,
                up_down_frag.1,
                local_barrier_manager.database_id,
                local_barrier_manager.term_id.clone(),
            )
            .await?;

        Ok(Self {
            actor_id,
            inner: remote_input::run(
                stream,
                permits_tx,
                up_down_ids,
                up_down_frag,
                metrics,
                local_barrier_manager
                    .env
                    .config()
                    .developer
                    .exchange_batched_permits,
            ),
        })
    }
}

mod remote_input {
    use std::sync::Arc;

    use anyhow::Context;
    use await_tree::InstrumentAwait;
    use either::Either;
    use risingwave_pb::task_service::{GetStreamResponse, permits};
    use tokio::sync::mpsc;
    use tonic::Streaming;

    use crate::executor::exchange::error::ExchangeChannelClosed;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::prelude::{StreamExt, pin_mut, try_stream};
    use crate::executor::{DispatcherMessage, StreamExecutorError};
    use crate::task::{UpDownActorIds, UpDownFragmentIds};

    pub(super) type RemoteInputStreamInner = impl crate::executor::DispatcherMessageStream;

    #[define_opaque(RemoteInputStreamInner)]
    pub(super) fn run(
        stream: Streaming<GetStreamResponse>,
        permits_tx: mpsc::UnboundedSender<permits::Value>,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
        batched_permits_limit: usize,
    ) -> RemoteInputStreamInner {
        run_inner(
            stream,
            permits_tx,
            up_down_ids,
            up_down_frag,
            metrics,
            batched_permits_limit,
        )
    }

    #[try_stream(ok = DispatcherMessage, error = StreamExecutorError)]
    async fn run_inner(
        stream: Streaming<GetStreamResponse>,
        permits_tx: mpsc::UnboundedSender<permits::Value>,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
        batched_permits_limit: usize,
    ) {
        let up_actor_id = up_down_ids.0.to_string();
        let up_fragment_id = up_down_frag.0.to_string();
        let down_fragment_id = up_down_frag.1.to_string();
        let exchange_frag_recv_size_metrics = metrics
            .exchange_frag_recv_size
            .with_guarded_label_values(&[&up_fragment_id, &down_fragment_id]);

        let span = await_tree::span!("RemoteInput (actor {up_actor_id})").verbose();

        let mut batched_permits_accumulated = 0;

        pin_mut!(stream);
        while let Some(data_res) = stream.next().instrument_await(span.clone()).await {
            match data_res {
                Ok(GetStreamResponse { message, permits }) => {
                    use crate::executor::DispatcherMessageBatch;
                    let msg = message.unwrap();
                    let bytes = DispatcherMessageBatch::get_encoded_len(&msg);

                    exchange_frag_recv_size_metrics.inc_by(bytes as u64);

                    let msg_res = DispatcherMessageBatch::from_protobuf(&msg);
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
                    match msg.into_messages() {
                        Either::Left(barriers) => {
                            for b in barriers {
                                yield b;
                            }
                        }
                        Either::Right(m) => {
                            yield m;
                        }
                    }
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
    type Item = DispatcherMessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl ActorInput for RemoteInput {
    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Create a [`LocalInput`] or [`RemoteInput`] instance with given info. Used by merge executors and
/// receiver executors.
pub(crate) async fn new_input(
    local_barrier_manager: &LocalBarrierManager,
    metrics: Arc<StreamingMetrics>,
    actor_id: ActorId,
    fragment_id: FragmentId,
    upstream_actor_info: &ActorInfo,
    upstream_fragment_id: FragmentId,
) -> StreamExecutorResult<BoxedActorInput> {
    let upstream_actor_id = upstream_actor_info.actor_id;
    let upstream_addr = upstream_actor_info.get_host()?.into();

    let input = if is_local_address(local_barrier_manager.env.server_address(), &upstream_addr) {
        LocalInput::new(
            local_barrier_manager.register_local_upstream_output(actor_id, upstream_actor_id),
            upstream_actor_id,
        )
        .boxed_input()
    } else {
        RemoteInput::new(
            local_barrier_manager,
            upstream_addr,
            (upstream_actor_id, actor_id),
            (upstream_fragment_id, fragment_id),
            metrics,
        )
        .await?
        .boxed_input()
    };

    Ok(input)
}

impl DispatcherMessageBatch {
    fn into_messages(self) -> Either<impl Iterator<Item = DispatcherMessage>, DispatcherMessage> {
        match self {
            DispatcherMessageBatch::BarrierBatch(barriers) => {
                Either::Left(barriers.into_iter().map(DispatcherMessage::Barrier))
            }
            DispatcherMessageBatch::Chunk(c) => Either::Right(DispatcherMessage::Chunk(c)),
            DispatcherMessageBatch::Watermark(w) => Either::Right(DispatcherMessage::Watermark(w)),
        }
    }
}
