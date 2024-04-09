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

use anyhow::Context as _;
use futures::Stream;
use futures_async_stream::try_stream;
use pin_project::pin_project;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_pb::task_service::{GetStreamResponse, PbPermits};
use risingwave_rpc_client::error::TonicStatusWrapper;
use risingwave_rpc_client::ComputeClientPool;
use thiserror_ext::AsReport;

use super::permit::Receiver;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::*;
use crate::task::{
    FragmentId, LocalBarrierManager, SharedContext, UpDownActorIds, UpDownFragmentIds,
};

/// `Input` provides an interface for [`MergeExecutor`] and [`ReceiverExecutor`] to receive data
/// from upstream actors.
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
type LocalInputStreamInner = impl MessageStream;

impl LocalInput {
    pub fn new(channel: Receiver, actor_id: ActorId) -> Self {
        Self {
            inner: Self::run(channel, actor_id),
            actor_id,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn run(mut channel: Receiver, actor_id: ActorId) {
        let span: await_tree::Span = format!("LocalInput (actor {actor_id})").into();
        while let Some(msg) = channel.recv().verbose_instrument_await(span.clone()).await {
            yield msg;
        }
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
type RemoteInputStreamInner = impl MessageStream;

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
    ) -> Self {
        let actor_id = up_down_ids.0;

        Self {
            actor_id,
            inner: Self::run(
                local_barrier_manager,
                client_pool,
                upstream_addr,
                up_down_ids,
                up_down_frag,
                metrics,
            ),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn run(
        local_barrier_manager: LocalBarrierManager,
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
    ) {
        let client = client_pool.get_by_addr(upstream_addr).await?;
        let (stream, permits_tx) = client
            .get_stream(up_down_ids.0, up_down_ids.1, up_down_frag.0, up_down_frag.1)
            .await?;

        let up_actor_id = up_down_ids.0.to_string();
        let up_fragment_id = up_down_frag.0.to_string();
        let down_fragment_id = up_down_frag.1.to_string();

        let span: await_tree::Span = format!("RemoteInput (actor {up_actor_id})").into();

        // process messages and ack permits in batch
        const BATCH_SIZE: usize = 8; // arbitrary number
        let mut stream = stream.ready_chunks(BATCH_SIZE);
        while let Some(inputs) = stream.next().verbose_instrument_await(span.clone()).await {
            let mut ack_permits = PbPermits::default();
            let mut messages = Vec::with_capacity(BATCH_SIZE);

            for result in inputs {
                let GetStreamResponse { message, permits } = result.map_err(|e| {
                    // TODO(error-handling): maintain the source chain
                    StreamExecutorError::channel_closed(format!(
                        "RemoteInput tonic error: {}",
                        TonicStatusWrapper::from(e).as_report()
                    ))
                })?;
                let msg = Message::from_protobuf(&message.unwrap())
                    .context("RemoteInput decode message error")?;
                messages.push(msg);

                let permits = permits.unwrap();
                ack_permits.records += permits.records;
                ack_permits.bytes += permits.bytes;
                ack_permits.barriers += permits.barriers;
            }

            metrics
                .exchange_frag_recv_size
                .with_label_values(&[&up_fragment_id, &down_fragment_id])
                .inc_by(ack_permits.bytes);

            permits_tx
                .send(ack_permits)
                .context("RemoteInput backward permits channel closed.")?;

            for mut msg in messages {
                // Read barrier mutation from local barrier manager and attach it to the barrier message.
                if cfg!(not(test)) {
                    if let Message::Barrier(barrier) = &mut msg {
                        assert!(
                            barrier.mutation.is_none(),
                            "Mutation should be erased in remote side"
                        );
                        let mutation = local_barrier_manager
                            .read_barrier_mutation(barrier)
                            .await
                            .context("Read barrier mutation error")?;
                        barrier.mutation = mutation;
                    }
                }
                yield msg;
            }
        }
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
            context.take_receiver(
                (upstream_actor_id, actor_id),
                (upstream_fragment_id, fragment_id),
            )?,
            upstream_actor_id,
        )
        .boxed_input()
    } else {
        RemoteInput::new(
            context.local_barrier_manager.clone(),
            context.compute_client_pool.clone(),
            upstream_addr,
            (upstream_actor_id, actor_id),
            (upstream_fragment_id, fragment_id),
            metrics,
        )
        .boxed_input()
    };

    Ok(input)
}
