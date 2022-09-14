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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use async_stack_trace::{SpanValue, StackTrace};
use futures::{pin_mut, Stream};
use futures_async_stream::try_stream;
use pin_project::pin_project;
use risingwave_common::bail;
use risingwave_common::error::Result;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_rpc_client::ComputeClientPool;
use tokio::sync::mpsc::Receiver;

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::*;
use crate::task::{FragmentId, SharedContext, UpDownActorIds, UpDownFragmentIds};

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
    fn new(channel: Receiver<Message>, actor_id: ActorId) -> Self {
        Self {
            inner: Self::run(channel, actor_id),
            actor_id,
        }
    }

    #[cfg(test)]
    pub fn for_test(channel: Receiver<Message>) -> BoxedInput {
        // `actor_id` is currently only used by configuration change, use a dummy value.
        Self::new(channel, 0).boxed_input()
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn run(mut channel: Receiver<Message>, actor_id: ActorId) {
        let span: SpanValue = format!("LocalInput (actor {actor_id})").into();
        while let Some(msg) = channel.recv().stack_trace(span.clone()).await {
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
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        metrics: Arc<StreamingMetrics>,
    ) {
        let client = client_pool.get_by_addr(upstream_addr).await?;
        let stream = client
            .get_stream(up_down_ids.0, up_down_ids.1, up_down_frag.0, up_down_frag.1)
            .await?;

        let up_actor_id = up_down_ids.0.to_string();
        let down_actor_id = up_down_ids.1.to_string();
        let up_fragment_id = up_down_frag.0.to_string();
        let down_fragment_id = up_down_frag.1.to_string();

        let mut rr = 0;
        const SAMPLING_FREQUENCY: u64 = 100;

        let span: SpanValue = format!("RemoteInput (actor {up_actor_id})").into();

        pin_mut!(stream);
        while let Some(data_res) = stream.next().stack_trace(span.clone()).await {
            match data_res {
                Ok(stream_msg) => {
                    let bytes = Message::get_encoded_len(&stream_msg);
                    let msg = stream_msg.get_message().expect("no message");

                    metrics
                        .exchange_recv_size
                        .with_label_values(&[&up_actor_id, &down_actor_id])
                        .inc_by(bytes as u64);

                    metrics
                        .exchange_frag_recv_size
                        .with_label_values(&[&up_fragment_id, &down_fragment_id])
                        .inc_by(bytes as u64);

                    // add deserialization duration metric with given sampling frequency
                    let msg_res = if rr % SAMPLING_FREQUENCY == 0 {
                        let start_time = Instant::now();
                        let msg_res = Message::from_protobuf(msg);
                        metrics
                            .actor_sampled_deserialize_duration_ns
                            .with_label_values(&[&down_actor_id])
                            .inc_by(start_time.elapsed().as_nanos() as u64);
                        msg_res
                    } else {
                        Message::from_protobuf(msg)
                    };
                    rr += 1;

                    match msg_res {
                        Ok(msg) => yield msg,
                        Err(e) => bail!("RemoteInput decode message error: {}", e),
                    }
                }
                Err(e) => bail!("RemoteInput tonic error: {}", e),
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
) -> Result<BoxedInput> {
    let upstream_addr = context
        .get_actor_info(&upstream_actor_id)?
        .get_host()?
        .into();

    let input = if is_local_address(&context.addr, &upstream_addr) {
        LocalInput::new(
            context.take_receiver(&(upstream_actor_id, actor_id))?,
            upstream_actor_id,
        )
        .boxed_input()
    } else {
        RemoteInput::new(
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
