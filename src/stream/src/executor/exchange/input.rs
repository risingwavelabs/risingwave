// Copyright 2022 RisingWave Labs
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
use std::pin::Pin;
use std::task::{Context, Poll};

use either::Either;
use futures::future::try_join_all;
use local_input::LocalInputStreamInner;
use pin_project::pin_project;
use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::{HostAddr, is_local_address};

use super::multiplexed::{LogicalInput, run_multiplexed_remote_input};
use super::permit::Receiver;
use crate::executor::prelude::*;
use crate::executor::{
    BarrierInner, DispatcherMessage, DispatcherMessageBatch, DispatcherMessageStreamItem,
};
use crate::task::{FragmentId, LocalBarrierManager, UpDownActorIds, UpDownFragmentIds};

/// `Input` is a more abstract upstream input type, used for `DynamicReceivers` type
/// handling of multiple upstream inputs
pub trait Input: Stream + Send {
    type InputId;
    /// The upstream input id.
    fn id(&self) -> Self::InputId;

    fn boxed_input(self) -> BoxedInput<Self::InputId, Self::Item>
    where
        Self: Sized + 'static,
    {
        Box::pin(self)
    }
}

pub type BoxedInput<InputId, Item> = Pin<Box<dyn Input<InputId = InputId, Item = Item>>>;

/// `ActorInput` provides an interface for [`MergeExecutor`](crate::executor::MergeExecutor) and
/// [`ReceiverExecutor`](crate::executor::ReceiverExecutor) to receive data from upstream actors.
/// Only used for actor inputs.
pub trait ActorInput = Input<Item = DispatcherMessageStreamItem, InputId = ActorId>;

pub type BoxedActorInput = Pin<Box<dyn ActorInput>>;

impl std::fmt::Debug for dyn ActorInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Input")
            .field("actor_id", &self.id())
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

pub(crate) fn assert_equal_dispatcher_barrier<M1, M2>(
    first: &BarrierInner<M1>,
    second: &BarrierInner<M2>,
) {
    assert_eq!(first.epoch, second.epoch);
    assert_eq!(first.kind, second.kind);
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

impl Input for LocalInput {
    type InputId = ActorId;

    fn id(&self) -> Self::InputId {
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
use risingwave_pb::id::PartialGraphId;

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn new(
        local_barrier_manager: &LocalBarrierManager,
        upstream_addr: HostAddr,
        upstream_partial_graph_id: PartialGraphId,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
        actor_config: Arc<StreamingConfig>,
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
                upstream_partial_graph_id,
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
                actor_config.developer.exchange_batched_permits,
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

impl Input for RemoteInput {
    type InputId = ActorId;

    fn id(&self) -> Self::InputId {
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
    actor_config: Arc<StreamingConfig>,
) -> StreamExecutorResult<BoxedActorInput> {
    let upstream_actor_id = upstream_actor_info.actor_id;
    let upstream_addr = upstream_actor_info.get_host()?.into();

    let input = if is_local_address(local_barrier_manager.env.server_address(), &upstream_addr) {
        LocalInput::new(
            local_barrier_manager.register_local_upstream_output(
                actor_id,
                upstream_actor_id,
                upstream_actor_info.partial_graph_id,
            ),
            upstream_actor_id,
        )
        .boxed_input()
    } else {
        RemoteInput::new(
            local_barrier_manager,
            upstream_addr,
            upstream_actor_info.partial_graph_id,
            (upstream_actor_id, actor_id),
            (upstream_fragment_id, fragment_id),
            metrics,
            actor_config,
        )
        .await?
        .boxed_input()
    };

    Ok(input)
}

/// Create inputs for a set of upstream actors, with optional multiplexing for remote actors
/// on the same node. When multiplexing is enabled, remote actors co-located on the same
/// upstream node share a single gRPC stream with barrier coalescing, reducing the number
/// of barrier messages from N×M to M per exchange per epoch.
///
/// Local inputs are always created individually (no change from the existing behavior).
/// Remote inputs are grouped by upstream host address and multiplexed when the group has
/// more than one actor.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn new_inputs(
    local_barrier_manager: &LocalBarrierManager,
    metrics: Arc<StreamingMetrics>,
    actor_id: ActorId,
    fragment_id: FragmentId,
    upstream_actors: &[ActorInfo],
    upstream_fragment_id: FragmentId,
    actor_config: Arc<StreamingConfig>,
    enable_multiplexing: bool,
) -> StreamExecutorResult<Vec<BoxedActorInput>> {
    if !enable_multiplexing || upstream_actors.len() <= 1 {
        // Fall back to per-actor inputs when multiplexing is disabled or there's only one actor.
        let inputs = try_join_all(upstream_actors.iter().map(|upstream_actor| {
            new_input(
                local_barrier_manager,
                metrics.clone(),
                actor_id,
                fragment_id,
                upstream_actor,
                upstream_fragment_id,
                actor_config.clone(),
            )
        }))
        .await?;
        return Ok(inputs);
    }

    // Separate local and remote actors, grouping remote actors by node address.
    let local_addr = local_barrier_manager.env.server_address();
    let mut local_actors = Vec::new();
    let mut remote_groups: HashMap<HostAddr, Vec<&ActorInfo>> = HashMap::new();

    for actor in upstream_actors {
        let addr: HostAddr = actor.get_host()?.into();
        if is_local_address(local_addr, &addr) {
            local_actors.push(actor);
        } else {
            remote_groups.entry(addr).or_default().push(actor);
        }
    }

    let mut inputs: Vec<BoxedActorInput> = Vec::with_capacity(upstream_actors.len());

    // Create local inputs (unchanged — each local actor gets its own LocalInput).
    for actor in &local_actors {
        let input = new_input(
            local_barrier_manager,
            metrics.clone(),
            actor_id,
            fragment_id,
            actor,
            upstream_fragment_id,
            actor_config.clone(),
        )
        .await?;
        inputs.push(input);
    }

    // Create remote inputs, multiplexed per node.
    for (addr, actors) in remote_groups {
        if actors.len() == 1 {
            // Single remote actor on this node — use regular RemoteInput.
            let input = new_input(
                local_barrier_manager,
                metrics.clone(),
                actor_id,
                fragment_id,
                actors[0],
                upstream_fragment_id,
                actor_config.clone(),
            )
            .await?;
            inputs.push(input);
        } else {
            // Multiple remote actors on this node — create a multiplexed stream.
            let up_actor_ids: Vec<ActorId> = actors.iter().map(|a| a.actor_id).collect();
            let partial_graph_id = actors[0].partial_graph_id;

            let client = local_barrier_manager
                .env
                .client_pool()
                .get_by_addr(addr)
                .await?;

            let (stream, permits_tx) = client
                .get_stream_multiplexed(
                    up_actor_ids.clone(),
                    actor_id,
                    upstream_fragment_id,
                    fragment_id,
                    partial_graph_id,
                    local_barrier_manager.term_id.clone(),
                )
                .await?;

            // Create per-actor logical inputs.
            let mut logical_outputs = HashMap::new();
            for &up_actor_id in &up_actor_ids {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                logical_outputs.insert(up_actor_id, tx);
                let logical_input = LogicalInput::new(up_actor_id, rx);
                inputs.push(logical_input.boxed_input());
            }

            // Spawn the demuxer task that reads from the multiplexed gRPC stream
            // and dispatches to the per-actor logical inputs.
            let batched_permits_limit = actor_config.developer.exchange_batched_permits;
            tokio::spawn(async move {
                if let Err(e) = run_multiplexed_remote_input(
                    stream,
                    permits_tx,
                    logical_outputs,
                    batched_permits_limit,
                )
                .await
                {
                    tracing::error!("multiplexed remote input error: {}", e);
                }
            });
        }
    }

    Ok(inputs)
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
