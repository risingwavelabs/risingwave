use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures_async_stream::try_stream;
use madsim::time::Instant;
use pin_project::pin_project;
use risingwave_common::error::Result;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_rpc_client::ComputeClientPool;
use tokio::sync::mpsc::Receiver;

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::*;
use crate::task::{FragmentId, SharedContext, UpDownActorIds, UpDownFragmentIds};

pub trait Input: MessageStream {
    fn actor_id(&self) -> ActorId;

    fn boxed_input(self) -> BoxedInput
    where
        Self: Sized + 'static,
    {
        Box::pin(self)
    }
}

pub type BoxedInput = Pin<Box<dyn Input>>;

type RemoteInputStreamInner = impl MessageStream;

#[pin_project]
pub struct RemoteInput {
    #[pin]
    inner: RemoteInputStreamInner,

    actor_id: ActorId,
}

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
        let client = client_pool.get_client_for_addr(upstream_addr).await?;
        let stream = client
            .get_stream(up_down_ids.0, up_down_ids.1, up_down_frag.0, up_down_frag.1)
            .await?;

        let up_actor_id = up_down_ids.0.to_string();
        let down_actor_id = up_down_ids.1.to_string();
        let up_fragment_id = up_down_frag.0.to_string();
        let down_fragment_id = up_down_frag.1.to_string();

        let mut rr = 0;
        const SAMPLING_FREQUENCY: u64 = 100;

        #[for_await]
        for data_res in stream {
            match data_res {
                Ok(stream_msg) => {
                    let bytes = Message::get_encoded_len(&stream_msg);
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
                        let msg_res = Message::from_protobuf(
                            stream_msg
                                .get_message()
                                .expect("no message in stream response!"),
                        );
                        metrics
                            .actor_sampled_deserialize_duration_ns
                            .with_label_values(&[&down_actor_id])
                            .inc_by(start_time.elapsed().as_nanos() as u64);
                        msg_res
                    } else {
                        Message::from_protobuf(
                            stream_msg
                                .get_message()
                                .expect("no message in stream response!"),
                        )
                    };
                    rr += 1;

                    match msg_res {
                        Ok(msg) => {
                            yield msg;
                        }
                        Err(e) => {
                            error!("RemoteInput forward message error:{}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("RemoteInput tonic error status:{}", e);
                    break;
                }
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

pub struct LocalInput {
    channel: Receiver<Message>,

    actor_id: ActorId,
}

impl LocalInput {
    pub fn new(channel: Receiver<Message>, actor_id: ActorId) -> Self {
        Self { channel, actor_id }
    }
}

impl Stream for LocalInput {
    type Item = MessageStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.channel.poll_recv(cx).map(|m| m.map(Ok))
    }
}

impl Input for LocalInput {
    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Create an input for merge and receiver executor. For local upstream actor, this will be simply a
/// channel receiver. For remote upstream actor, this will spawn a long running [`RemoteInput`] task
/// to receive messages from `gRPC` exchange service and return the receiver.
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
