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

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use either::Either;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_pb::id::FragmentId;
use risingwave_pb::task_service::stream_exchange_service_server::StreamExchangeService;
use risingwave_pb::task_service::{GetStreamRequest, GetStreamResponse, PbPermits, permits};
use risingwave_stream::executor::DispatcherMessageBatch;
use risingwave_stream::executor::exchange::multiplexed::BarrierCoalescer;
use risingwave_stream::executor::exchange::permit::{MessageWithPermits, Permits, Receiver};
use risingwave_stream::task::{ActorId, LocalStreamManager};
use tonic::{Request, Response, Status, Streaming};

pub mod metrics;
pub use metrics::{GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS, StreamExchangeServiceMetrics};

pub type StreamDataStream = impl Stream<Item = std::result::Result<GetStreamResponse, Status>>;

#[derive(Clone)]
pub struct StreamExchangeServiceImpl {
    stream_mgr: LocalStreamManager,
    metrics: Arc<StreamExchangeServiceMetrics>,
}

#[async_trait::async_trait]
impl StreamExchangeService for StreamExchangeServiceImpl {
    type GetStreamStream = StreamDataStream;

    #[define_opaque(StreamDataStream)]
    async fn get_stream(
        &self,
        request: Request<Streaming<GetStreamRequest>>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        use risingwave_pb::task_service::get_stream_request::*;

        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;

        let mut request_stream: Streaming<GetStreamRequest> = request.into_inner();

        // Extract the first `Get` request from the stream.
        let Get {
            up_actor_id,
            down_actor_id,
            up_fragment_id,
            down_fragment_id,
            up_partial_graph_id,
            term_id,
            up_actor_ids,
        } = {
            let req = request_stream
                .next()
                .await
                .ok_or_else(|| Status::invalid_argument("get_stream request is empty"))??;
            match req.value.unwrap() {
                Value::Get(get) => get,
                Value::AddPermits(_) => unreachable!("the first message must be `Get`"),
            }
        };

        // Map the remaining stream to add-permits.
        let add_permits_stream = request_stream.map_ok(|req| match req.value.unwrap() {
            Value::Get(_) => unreachable!("the following messages must be `AddPermits`"),
            Value::AddPermits(add_permits) => add_permits.value.unwrap(),
        });

        if !up_actor_ids.is_empty() {
            // Multiplexed mode: take N receivers (one per upstream actor) and merge
            // them into a single gRPC response stream with barrier coalescing.
            let mut receivers = Vec::with_capacity(up_actor_ids.len());
            for up_id in &up_actor_ids {
                let receiver = self
                    .stream_mgr
                    .take_receiver(
                        up_partial_graph_id,
                        term_id.clone(),
                        (*up_id, down_actor_id),
                    )
                    .await?;
                receivers.push((*up_id, receiver));
            }

            let stream: Pin<
                Box<dyn Stream<Item = std::result::Result<GetStreamResponse, Status>> + Send>,
            > = Box::pin(Self::get_stream_multiplexed_impl(
                self.metrics.clone(),
                peer_addr,
                receivers,
                add_permits_stream,
                (up_fragment_id, down_fragment_id),
            ));
            Ok(Response::new(stream))
        } else {
            // Legacy single-actor mode.
            let receiver = self
                .stream_mgr
                .take_receiver(up_partial_graph_id, term_id, (up_actor_id, down_actor_id))
                .await?;

            let stream: Pin<
                Box<dyn Stream<Item = std::result::Result<GetStreamResponse, Status>> + Send>,
            > = Box::pin(Self::get_stream_impl(
                self.metrics.clone(),
                peer_addr,
                receiver,
                add_permits_stream,
                (up_fragment_id, down_fragment_id),
            ));
            Ok(Response::new(stream))
        }
    }
}

impl StreamExchangeServiceImpl {
    pub fn new(stream_mgr: LocalStreamManager, metrics: Arc<StreamExchangeServiceMetrics>) -> Self {
        Self {
            stream_mgr,
            metrics,
        }
    }

    #[try_stream(ok = GetStreamResponse, error = Status)]
    async fn get_stream_impl(
        metrics: Arc<StreamExchangeServiceMetrics>,
        peer_addr: SocketAddr,
        mut receiver: Receiver,
        add_permits_stream: impl Stream<Item = std::result::Result<permits::Value, tonic::Status>>,
        up_down_fragment_ids: (FragmentId, FragmentId),
    ) {
        tracing::debug!(target: "events::compute::exchange", peer_addr = %peer_addr, "serve stream exchange RPC");
        let up_fragment_id = up_down_fragment_ids.0.to_string();
        let down_fragment_id = up_down_fragment_ids.1.to_string();

        let permits = receiver.permits();

        // Select from the permits back from the downstream and the upstream receiver.
        let select_stream = futures::stream::select(
            add_permits_stream.map_ok(Either::Left),
            #[try_stream]
            async move {
                while let Some(m) = receiver.recv_raw().await {
                    yield Either::Right(m);
                }
            },
        );
        pin_mut!(select_stream);

        let exchange_frag_send_size_metrics = metrics
            .stream_fragment_exchange_bytes
            .with_label_values(&[&up_fragment_id, &down_fragment_id]);

        while let Some(r) = select_stream.try_next().await? {
            match r {
                Either::Left(permits_to_add) => {
                    permits.add_permits(permits_to_add);
                }
                Either::Right(MessageWithPermits { message, permits }) => {
                    let message = match message {
                        DispatcherMessageBatch::Chunk(chunk) => {
                            DispatcherMessageBatch::Chunk(chunk.compact_vis())
                        }
                        msg @ (DispatcherMessageBatch::Watermark(_)
                        | DispatcherMessageBatch::BarrierBatch(_)) => msg,
                    };
                    let proto = message.to_protobuf();
                    // forward the acquired permit to the downstream
                    let response = GetStreamResponse {
                        message: Some(proto),
                        permits: Some(PbPermits { value: permits }),
                    };
                    let bytes = DispatcherMessageBatch::get_encoded_len(&response);

                    yield response;

                    exchange_frag_send_size_metrics.inc_by(bytes as u64);
                }
            }
        }
    }

    /// Multiplexed stream exchange: merges N upstream actor receivers into a single gRPC
    /// response stream with barrier coalescing.
    ///
    /// Data chunks and watermarks are forwarded immediately, tagged with `source_actor_id`.
    /// Barriers from all N actors are coalesced into one, with `coalesced_actor_ids` set.
    ///
    /// Permits handling:
    /// - Record permits: tracked per-message in a FIFO queue, returned to the originating
    ///   receiver's semaphore when the downstream sends them back.
    /// - Barrier permits: returned to ALL receiver semaphores immediately after coalescing
    ///   (not forwarded to downstream).
    #[try_stream(ok = GetStreamResponse, error = Status)]
    async fn get_stream_multiplexed_impl(
        metrics: Arc<StreamExchangeServiceMetrics>,
        peer_addr: SocketAddr,
        receivers: Vec<(ActorId, Receiver)>,
        add_permits_stream: impl Stream<Item = std::result::Result<permits::Value, tonic::Status>>,
        up_down_fragment_ids: (FragmentId, FragmentId),
    ) {
        tracing::debug!(
            target: "events::compute::exchange",
            peer_addr = %peer_addr,
            num_upstream = receivers.len(),
            "serve multiplexed stream exchange RPC"
        );
        let up_fragment_id = up_down_fragment_ids.0.to_string();
        let down_fragment_id = up_down_fragment_ids.1.to_string();

        // Barrier coalescer to merge N barriers into 1 per epoch.
        let actor_ids: HashSet<ActorId> = receivers.iter().map(|(id, _)| *id).collect();
        let mut coalescer = BarrierCoalescer::new(actor_ids);

        // Collect permit handles for returning barrier permits after coalescing.
        let all_permits: Vec<Arc<Permits>> =
            receivers.iter().map(|(_, r)| r.permits()).collect();

        // FIFO queue tracking which receiver's semaphore to credit when the downstream
        // returns record permits. Each entry is (permits_handle, num_permits).
        let mut record_permits_queue: VecDeque<(Arc<Permits>, u32)> = VecDeque::new();

        // Create per-receiver streams and merge them with select_all.
        // Each stream yields (actor_id, permits_handle, MessageWithPermits).
        let receiver_streams = receivers.into_iter().map(|(actor_id, receiver)| {
            let permits_handle = receiver.permits();
            futures::stream::unfold(
                (permits_handle, receiver, false),
                move |(permits_handle, mut receiver, done)| async move {
                    if done {
                        return None;
                    }
                    match receiver.recv_raw().await {
                        Some(msg) => Some((
                            Ok::<_, Status>((actor_id, permits_handle.clone(), msg)),
                            (permits_handle, receiver, false),
                        )),
                        None => Some((
                            Err(Status::internal(format!(
                                "multiplexed exchange receiver for actor {} closed unexpectedly",
                                actor_id
                            ))),
                            (permits_handle, receiver, true),
                        )),
                    }
                },
            )
            .boxed()
        });
        let merged_receivers = futures::stream::select_all(receiver_streams);

        // Select between permits-back from downstream and merged upstream messages.
        let select_stream = futures::stream::select(
            add_permits_stream.map_ok(Either::Left),
            merged_receivers.map(|item| item.map(Either::Right)),
        );
        pin_mut!(select_stream);

        let exchange_frag_send_size_metrics = metrics
            .stream_fragment_exchange_bytes
            .with_label_values(&[&up_fragment_id, &down_fragment_id]);

        while let Some(r) = select_stream.try_next().await? {
            match r {
                Either::Left(permits_to_add) => {
                    // Return permits from downstream to the correct upstream receivers.
                    match permits_to_add {
                        permits::Value::Record(mut remaining) => {
                            // FIFO credit-back: return record permits to the receivers
                            // that originally consumed them, in order.
                            while remaining > 0 {
                                if let Some((permits_handle, k)) =
                                    record_permits_queue.front_mut()
                                {
                                    if *k <= remaining {
                                        let k_val = *k;
                                        remaining -= k_val;
                                        let (p, _) = record_permits_queue.pop_front().unwrap();
                                        p.add_permits(permits::Value::Record(k_val));
                                    } else {
                                        *k -= remaining;
                                        permits_handle
                                            .add_permits(permits::Value::Record(remaining));
                                        remaining = 0;
                                    }
                                } else {
                                    // Queue is empty but we still have permits to return.
                                    // Distribute evenly as fallback (shouldn't happen normally).
                                    let per_channel = remaining / all_permits.len() as u32;
                                    let extra = remaining % all_permits.len() as u32;
                                    for (i, p) in all_permits.iter().enumerate() {
                                        let amount =
                                            per_channel + if (i as u32) < extra { 1 } else { 0 };
                                        if amount > 0 {
                                            p.add_permits(permits::Value::Record(amount));
                                        }
                                    }
                                    remaining = 0;
                                }
                            }
                        }
                        permits::Value::Barrier(_) => {
                            // Barrier permits are handled at coalesce time (returned
                            // immediately to all channels). The downstream sends
                            // permits: None for coalesced barriers, so this branch
                            // should not be reached for multiplexed barriers.
                        }
                    }
                }
                Either::Right((actor_id, permits_handle, msg_with_permits)) => {
                    let MessageWithPermits {
                        message,
                        permits: msg_permits,
                    } = msg_with_permits;

                    match message {
                        DispatcherMessageBatch::Chunk(chunk) => {
                            let chunk = chunk.compact_vis();

                            // Track record permits for FIFO credit-back.
                            if let Some(permits::Value::Record(k)) = &msg_permits {
                                record_permits_queue
                                    .push_back((permits_handle, *k));
                            }

                            let mut proto =
                                DispatcherMessageBatch::Chunk(chunk).to_protobuf();
                            proto.source_actor_id = actor_id;

                            let response = GetStreamResponse {
                                message: Some(proto),
                                permits: Some(PbPermits { value: msg_permits }),
                            };
                            let bytes =
                                DispatcherMessageBatch::get_encoded_len(&response);
                            yield response;
                            exchange_frag_send_size_metrics.inc_by(bytes as u64);
                        }
                        DispatcherMessageBatch::Watermark(watermark) => {
                            let mut proto =
                                DispatcherMessageBatch::Watermark(watermark).to_protobuf();
                            proto.source_actor_id = actor_id;

                            let response = GetStreamResponse {
                                message: Some(proto),
                                permits: Some(PbPermits { value: msg_permits }),
                            };
                            let bytes =
                                DispatcherMessageBatch::get_encoded_len(&response);
                            yield response;
                            exchange_frag_send_size_metrics.inc_by(bytes as u64);
                        }
                        DispatcherMessageBatch::BarrierBatch(barriers) => {
                            for barrier in barriers {
                                if let Some((coalesced_barrier, coalesced_ids)) =
                                    coalescer.collect(actor_id, barrier)
                                {
                                    // All actors reached this epoch — return barrier
                                    // permits to ALL channels immediately so senders
                                    // can proceed to the next epoch.
                                    for p in &all_permits {
                                        p.add_permits(permits::Value::Barrier(1));
                                    }

                                    // Build the coalesced barrier response.
                                    let coalesced_msg = DispatcherMessageBatch::BarrierBatch(
                                        vec![coalesced_barrier],
                                    );
                                    let mut proto = coalesced_msg.to_protobuf();

                                    // Set coalesced_actor_ids in the BarrierBatch.
                                    if let Some(ref mut smb) = proto.stream_message_batch {
                                        use risingwave_pb::stream_plan::stream_message_batch::StreamMessageBatch as SmBatch;
                                        if let SmBatch::BarrierBatch(bb) = smb {
                                            bb.coalesced_actor_ids = coalesced_ids;
                                        }
                                    }

                                    // No permits forwarded for coalesced barriers;
                                    // barrier backpressure is handled server-side.
                                    let response = GetStreamResponse {
                                        message: Some(proto),
                                        permits: Some(PbPermits { value: None }),
                                    };
                                    let bytes =
                                        DispatcherMessageBatch::get_encoded_len(&response);
                                    yield response;
                                    exchange_frag_send_size_metrics.inc_by(bytes as u64);
                                }
                                // If not yet coalesced, wait for more actors.
                            }
                        }
                    }
                }
            }
        }
    }
}
