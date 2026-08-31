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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use either::Either;
use local_input::LocalInputStreamInner;
use pin_project::pin_project;
use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::{HostAddr, is_local_address};
use risingwave_common::util::retry::exponential_backoff;
use risingwave_rpc_client::error::RpcError;
use thiserror_ext::AsReport;
use tokio_retry::strategy::jitter;

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

const REMOTE_INPUT_CONNECT_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const REMOTE_INPUT_CONNECT_RETRY_MAX_DELAY: Duration = Duration::from_secs(10);

fn remote_input_connect_retry_backoff() -> impl Iterator<Item = Duration> {
    exponential_backoff(
        REMOTE_INPUT_CONNECT_RETRY_BASE_DELAY,
        2,
        REMOTE_INPUT_CONNECT_RETRY_MAX_DELAY,
    )
    .map(jitter)
}

/// Aggregates connection-failure logging per upstream peer. During a recovery storm, thousands of
/// remote inputs may fail against the same unreachable peer at once; a warning per input per
/// attempt would flood the logs. Instead, individual failures are logged at debug level, and once
/// a peer keeps failing, a single warning per peer is emitted at most once per `WARN_INTERVAL`,
/// carrying the number of failures observed since the last warning.
mod connect_failure_log {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    use risingwave_common::util::addr::HostAddr;
    use tokio::time::Instant;

    /// Escalate to a warning only after a peer fails this many times in a row, so that a
    /// transient blip during startup stays at debug level.
    const WARN_THRESHOLD: u64 = 3;
    /// Emit at most one warning per peer per this interval.
    const WARN_INTERVAL: Duration = Duration::from_secs(5);

    #[derive(Default)]
    struct PeerFailures {
        consecutive: u64,
        since_last_warn: u64,
        last_warn: Option<Instant>,
    }

    static PEER_FAILURES: LazyLock<Mutex<HashMap<HostAddr, PeerFailures>>> =
        LazyLock::new(Default::default);

    /// Records a connection failure against the peer. Returns
    /// `Some((consecutive_failures, failures_since_last_warn))` if the caller should log a
    /// warning on behalf of this peer, or `None` to stay at debug level.
    pub(super) fn on_failure(peer: &HostAddr) -> Option<(u64, u64)> {
        let mut peers = PEER_FAILURES.lock().unwrap();
        let state = peers.entry(peer.clone()).or_default();
        state.consecutive += 1;
        state.since_last_warn += 1;
        if state.consecutive < WARN_THRESHOLD {
            return None;
        }
        let now = Instant::now();
        if state
            .last_warn
            .is_some_and(|last| now.duration_since(last) < WARN_INTERVAL)
        {
            return None;
        }
        state.last_warn = Some(now);
        Some((
            state.consecutive,
            std::mem::take(&mut state.since_last_warn),
        ))
    }

    /// Clears the failure state of the peer after a successful connection.
    pub(super) fn on_success(peer: &HostAddr) {
        PEER_FAILURES.lock().unwrap().remove(peer);
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn test_warn_escalation_is_aggregated_per_peer() {
            let peer: HostAddr = "test-warn-escalation:5688".parse().unwrap();

            // Below the threshold: stay at debug.
            assert_eq!(on_failure(&peer), None);
            assert_eq!(on_failure(&peer), None);
            // Reaching the threshold: warn once with the aggregated count.
            assert_eq!(on_failure(&peer), Some((3, 3)));
            // Within the interval: suppressed again.
            assert_eq!(on_failure(&peer), None);
            // After the interval: warn again, reporting only the failures since the last warning.
            #[cfg(madsim)]
            tokio::time::advance(WARN_INTERVAL);
            #[cfg(not(madsim))]
            tokio::time::advance(WARN_INTERVAL).await;
            assert_eq!(on_failure(&peer), Some((5, 2)));

            // A successful connection resets the state.
            on_success(&peer);
            assert_eq!(on_failure(&peer), None);
        }
    }
}

async fn retry_connection_errors<T, F, Fut>(
    peer: &HostAddr,
    mut operation: F,
    mut retry_backoff: impl Iterator<Item = Duration>,
) -> Result<T, RpcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RpcError>>,
{
    loop {
        match operation().await {
            Ok(value) => {
                connect_failure_log::on_success(peer);
                return Ok(value);
            }
            Err(err) if err.is_connection_error() => {
                let retry_delay = retry_backoff.next().expect("retry strategy is infinite");
                if let Some((consecutive_failures, failures_since_last_warn)) =
                    connect_failure_log::on_failure(peer)
                {
                    tracing::warn!(
                        %peer,
                        consecutive_failures,
                        failures_since_last_warn,
                        error = %err.as_report(),
                        ?retry_delay,
                        "RPC connection to upstream peer keeps failing; retrying"
                    );
                } else {
                    tracing::debug!(
                        %peer,
                        error = %err.as_report(),
                        ?retry_delay,
                        "transient RPC connection failure; retrying"
                    );
                }
                tokio::time::sleep(retry_delay).await;
            }
            Err(err) => return Err(err),
        }
    }
}

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

        // Establishing the compute client has no server-side subscription state, so it is safe to
        // retry transient failures in place. Aborting the actor on a term reset cancels this loop.
        let client_pool = local_barrier_manager.env.client_pool();
        let client = retry_connection_errors(
            &upstream_addr,
            || client_pool.get_by_addr(upstream_addr.clone()),
            remote_input_connect_retry_backoff(),
        )
        .await?;

        // Limit only the rate of starting subscriptions. Holding a concurrency permit until
        // `get_stream` returns can deadlock when upstream actors are waiting on their own remote
        // inputs before they can provide this actor's output receiver.
        local_barrier_manager
            .env
            .wait_remote_input_subscription()
            .await;
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
        let rx = local_barrier_manager.register_local_upstream_output(
            actor_id,
            upstream_actor_id,
            upstream_fragment_id,
            upstream_actor_info.partial_graph_id,
            metrics,
        );
        LocalInput::new(rx, upstream_actor_id).boxed_input()
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tonic::Status;

    use super::*;

    #[tokio::test]
    async fn retry_transient_connection_errors() {
        let attempts = AtomicUsize::new(0);
        retry_connection_errors(
            &"test-retry-transient:5688".parse().unwrap(),
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                std::future::ready(if attempt < 2 {
                    Err(RpcError::from_compute_status(Status::unavailable(
                        "compute starting",
                    )))
                } else {
                    Ok(())
                })
            },
            std::iter::repeat(Duration::ZERO),
        )
        .await
        .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn do_not_retry_non_connection_errors() {
        let attempts = AtomicUsize::new(0);
        let result = retry_connection_errors(
            &"test-no-retry:5688".parse().unwrap(),
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(RpcError::from_compute_status(
                    Status::invalid_argument("invalid subscription"),
                )))
            },
            std::iter::repeat(Duration::ZERO),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
