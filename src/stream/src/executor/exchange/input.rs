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
use std::future::pending;
use std::mem::replace;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::anyhow;
use either::Either;
use futures::stream::FuturesUnordered;
use local_input::LocalInputStreamInner;
use pin_project::pin_project;
use risingwave_common::util::addr::{HostAddr, is_local_address};
use risingwave_rpc_client::ComputeClientPool;
use tokio::select;
use tokio::sync::mpsc;

use super::permit::Receiver;
use crate::executor::prelude::*;
use crate::executor::{
    BarrierInner, DispatcherBarrier, DispatcherMessage, DispatcherMessageBatch,
    DispatcherMessageStream, DispatcherMessageStreamItem,
};
use crate::task::{FragmentId, LocalBarrierManager, UpDownActorIds, UpDownFragmentIds};

mod new_input_future {
    use anyhow::Context;
    use risingwave_pb::common::ActorInfo;

    use crate::executor::exchange::input::{BarrierReceiver, BoxedInput, new_input};
    use crate::executor::{DispatcherBarrier, StreamExecutorResult, expect_first_barrier};

    pub(super) type NewInputFuture =
        impl Future<Output = StreamExecutorResult<(BoxedInput, DispatcherBarrier)>> + 'static;

    impl BarrierReceiver {
        pub(super) fn new_input_future(&self, upstream_actor: &ActorInfo) -> NewInputFuture {
            let input = new_input(
                &self.local_barrier_manager,
                self.metrics.clone(),
                self.actor_id,
                self.fragment_id,
                upstream_actor,
                self.upstream_fragment_id,
            );
            async move {
                let mut input = input.context("failed to create upstream receivers")?;
                let barrier = expect_first_barrier(&mut input).await?;
                Ok((input, barrier))
            }
        }
    }
}

use new_input_future::*;

enum BarrierReceiverState {
    WaitingBarrier,
    ReceivingNewInputFirstBarrier(
        Barrier,
        HashMap<ActorId, BoxedInput>,
        FuturesUnordered<NewInputFuture>,
    ),
    Ready(Barrier, Option<HashMap<ActorId, BoxedInput>>),
    Err,
}

pub(crate) struct BarrierReceiver {
    barrier_rx: mpsc::UnboundedReceiver<Barrier>,
    actor_id: ActorId,
    fragment_id: FragmentId,
    pub(crate) upstream_fragment_id: FragmentId,
    local_barrier_manager: LocalBarrierManager,
    metrics: Arc<StreamingMetrics>,
    state: BarrierReceiverState,
}

impl BarrierReceiver {
    pub(crate) fn new(
        barrier_rx: mpsc::UnboundedReceiver<Barrier>,
        actor_id: ActorId,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
        local_barrier_manager: LocalBarrierManager,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            barrier_rx,
            actor_id,
            fragment_id,
            upstream_fragment_id,
            local_barrier_manager,
            metrics,
            state: BarrierReceiverState::WaitingBarrier,
        }
    }

    async fn wait_ready(&mut self) -> StreamExecutorResult<()> {
        match &mut self.state {
            BarrierReceiverState::WaitingBarrier => {
                let Some(barrier) = self.barrier_rx.recv().await else {
                    self.state = BarrierReceiverState::Err;
                    return Err(anyhow!("end of barrier stream").into());
                };
                if let Some(update) =
                    barrier.as_update_merge(self.actor_id, self.upstream_fragment_id)
                {
                    if update.added_upstream_actors.is_empty() {
                        self.state = BarrierReceiverState::Ready(barrier, Some(HashMap::new()));
                        return Ok(());
                    } else {
                        let futures = FuturesUnordered::from_iter(
                            update
                                .added_upstream_actors
                                .iter()
                                .map(|upstream_actor| self.new_input_future(upstream_actor)),
                        );
                        self.state = BarrierReceiverState::ReceivingNewInputFirstBarrier(
                            barrier,
                            HashMap::new(),
                            futures,
                        );
                    }
                } else {
                    self.state = BarrierReceiverState::Ready(barrier, None);
                    return Ok(());
                }
            }
            BarrierReceiverState::ReceivingNewInputFirstBarrier(_, _, _) => {}
            BarrierReceiverState::Ready(_, _) => {
                return Ok(());
            }
            BarrierReceiverState::Err => {
                unreachable!("should not poll after err")
            }
        };
        let BarrierReceiverState::ReceivingNewInputFirstBarrier(
            barrier,
            received_inputs,
            receiving_inputs,
        ) = &mut self.state
        else {
            unreachable!("should be ReceivingNewInputFirstBarrier");
        };
        while let Some(result) = receiving_inputs.next().await {
            let (input, first_barrier) = match result {
                Ok(item) => item,
                Err(e) => {
                    self.state = BarrierReceiverState::Err;
                    return Err(e);
                }
            };
            assert_equal_dispatcher_barrier(&first_barrier, &*barrier);
            let downstream_actor_id = input.actor_id();
            assert!(
                received_inputs.insert(downstream_actor_id, input).is_none(),
                "non-duplicated but get {}",
                downstream_actor_id
            );
        }
        self.state = {
            let BarrierReceiverState::ReceivingNewInputFirstBarrier(
                barrier,
                received_inputs,
                receiving_inputs,
            ) = replace(&mut self.state, BarrierReceiverState::Err)
            else {
                unreachable!()
            };
            assert!(receiving_inputs.is_empty());
            BarrierReceiverState::Ready(barrier, Some(received_inputs))
        };
        Ok(())
    }

    pub(crate) async fn process_dispatcher_barrier(
        &mut self,
        dispatcher_barrier: DispatcherBarrier,
    ) -> StreamExecutorResult<(Barrier, Option<HashMap<ActorId, BoxedInput>>)> {
        self.wait_ready().await?;
        let BarrierReceiverState::Ready(mut recv_barrier, new_inputs) =
            replace(&mut self.state, BarrierReceiverState::WaitingBarrier)
        else {
            unreachable!("should be at Ready when wait_ready returns")
        };
        assert_equal_dispatcher_barrier(&recv_barrier, &dispatcher_barrier);
        recv_barrier.passed_actors.extend(
            dispatcher_barrier
                .passed_actors
                .into_iter()
                .chain([self.actor_id]),
        );
        Ok((recv_barrier, new_inputs))
    }

    pub(crate) async fn run_future<O>(
        &mut self,
        fut: impl Future<Output = O> + Unpin,
    ) -> StreamExecutorResult<O> {
        select! {
            biased;
            msg = fut => {
                Ok(msg)
            }
            e = async {
                let result = self.wait_ready().await;
                if let Err(e) = result {
                    e
                } else {
                    pending().await
                }
            } => {
                Err(e)
            }
        }
    }
}

/// `Input` provides an interface for [`MergeExecutor`](crate::executor::MergeExecutor) and
/// [`ReceiverExecutor`](crate::executor::ReceiverExecutor) to receive data from upstream actors.
pub trait Input: DispatcherMessageStream {
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
use risingwave_common::catalog::DatabaseId;
use risingwave_pb::common::ActorInfo;

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        database_id: DatabaseId,
        metrics: Arc<StreamingMetrics>,
        batched_permits: usize,
        term_id: String,
    ) -> Self {
        let actor_id = up_down_ids.0;

        Self {
            actor_id,
            inner: remote_input::run(
                client_pool,
                upstream_addr,
                up_down_ids,
                up_down_frag,
                database_id,
                metrics,
                batched_permits,
                term_id,
            ),
        }
    }
}

mod remote_input {
    use std::sync::Arc;

    use anyhow::Context;
    use await_tree::InstrumentAwait;
    use either::Either;
    use risingwave_common::catalog::DatabaseId;
    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::task_service::{GetStreamResponse, permits};
    use risingwave_rpc_client::ComputeClientPool;

    use crate::executor::exchange::error::ExchangeChannelClosed;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::prelude::{StreamExt, pin_mut, try_stream};
    use crate::executor::{DispatcherMessage, StreamExecutorError};
    use crate::task::{UpDownActorIds, UpDownFragmentIds};

    pub(super) type RemoteInputStreamInner = impl crate::executor::DispatcherMessageStream;

    #[expect(clippy::too_many_arguments)]
    pub(super) fn run(
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        database_id: DatabaseId,
        metrics: Arc<StreamingMetrics>,
        batched_permits_limit: usize,
        term_id: String,
    ) -> RemoteInputStreamInner {
        run_inner(
            client_pool,
            upstream_addr,
            up_down_ids,
            up_down_frag,
            database_id,
            metrics,
            batched_permits_limit,
            term_id,
        )
    }

    #[expect(clippy::too_many_arguments)]
    #[try_stream(ok = DispatcherMessage, error = StreamExecutorError)]
    async fn run_inner(
        client_pool: ComputeClientPool,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        database_id: DatabaseId,
        metrics: Arc<StreamingMetrics>,
        batched_permits_limit: usize,
        term_id: String,
    ) {
        let client = client_pool.get_by_addr(upstream_addr).await?;
        let (stream, permits_tx) = client
            .get_stream(
                up_down_ids.0,
                up_down_ids.1,
                up_down_frag.0,
                up_down_frag.1,
                database_id,
                term_id,
            )
            .await?;

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
    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Create a [`LocalInput`] or [`RemoteInput`] instance with given info. Used by merge executors and
/// receiver executors.
pub(crate) fn new_input(
    local_barrier_manager: &LocalBarrierManager,
    metrics: Arc<StreamingMetrics>,
    actor_id: ActorId,
    fragment_id: FragmentId,
    upstream_actor_info: &ActorInfo,
    upstream_fragment_id: FragmentId,
) -> StreamResult<BoxedInput> {
    let context = &local_barrier_manager.shared_context;
    let upstream_actor_id = upstream_actor_info.actor_id;
    let upstream_addr = upstream_actor_info.get_host()?.into();

    let input = if is_local_address(&context.addr, &upstream_addr) {
        LocalInput::new(
            context.take_receiver((upstream_actor_id, actor_id))?,
            upstream_actor_id,
        )
        .boxed_input()
    } else {
        RemoteInput::new(
            context.compute_client_pool.as_ref().to_owned(),
            upstream_addr,
            (upstream_actor_id, actor_id),
            (upstream_fragment_id, fragment_id),
            context.database_id,
            metrics,
            context.config.developer.exchange_batched_permits,
            context.term_id(),
        )
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
