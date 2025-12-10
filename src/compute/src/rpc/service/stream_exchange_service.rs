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
use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use futures::stream::SelectAll;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_pb::id::{ActorId, FragmentId};
use risingwave_pb::task_service::stream_exchange_service_server::StreamExchangeService;
use risingwave_pb::task_service::{
    GetMuxStreamRequest, GetMuxStreamResponse, GetStreamRequest, GetStreamResponse, PbPermits,
    permits,
};
use risingwave_stream::executor::DispatcherMessageBatch;
use risingwave_stream::executor::exchange::permit::{MessageWithPermits, Receiver};
use risingwave_stream::task::LocalStreamManager;
use tonic::{Request, Response, Status, Streaming};

pub mod metrics;
pub use metrics::{GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS, StreamExchangeServiceMetrics};

pub type StreamDataStream = impl Stream<Item = std::result::Result<GetStreamResponse, Status>>;
pub type NewStreamDataStream =
    impl Stream<Item = std::result::Result<GetMuxStreamResponse, Status>>;

#[derive(Clone)]
pub struct StreamExchangeServiceImpl {
    stream_mgr: LocalStreamManager,
    metrics: Arc<StreamExchangeServiceMetrics>,
}

#[async_trait::async_trait]
impl StreamExchangeService for StreamExchangeServiceImpl {
    type GetMuxStreamStream = NewStreamDataStream;
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
            database_id,
            term_id,
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

        let receiver = self
            .stream_mgr
            .take_receiver(database_id, term_id, (up_actor_id, down_actor_id))
            .await?;

        // Map the remaining stream to add-permits.
        let add_permits_stream = request_stream.map_ok(|req| match req.value.unwrap() {
            Value::Get(_) => unreachable!("the following messages must be `AddPermits`"),
            Value::AddPermits(add_permits) => add_permits.value.unwrap(),
        });

        Ok(Response::new(Self::get_stream_impl(
            self.metrics.clone(),
            peer_addr,
            receiver,
            add_permits_stream,
            (up_fragment_id, down_fragment_id),
        )))
    }

    #[define_opaque(NewStreamDataStream)]
    async fn get_mux_stream(
        &self,
        request: Request<Streaming<GetMuxStreamRequest>>,
    ) -> std::result::Result<Response<Self::GetMuxStreamStream>, Status> {
        let request_stream = request.into_inner();

        Ok(Response::new(Self::get_mux_stream_impl(
            self.stream_mgr.clone(),
            request_stream,
        )))
    }
}

impl StreamExchangeServiceImpl {
    #[try_stream(ok = GetMuxStreamResponse, error = Status)]
    async fn get_mux_stream_impl(
        stream_mgr: LocalStreamManager,
        mut request_stream: Streaming<GetMuxStreamRequest>,
    ) {
        use risingwave_pb::task_service::get_mux_stream_request::*;

        // Extract the first `Init` request from the stream.
        let Init {
            up_fragment_id: _,
            down_fragment_id: _,
            database_id,
            term_id,
        } = {
            let req = request_stream
                .next()
                .await
                .ok_or_else(|| Status::invalid_argument("get_mux_stream request is empty"))??;
            match req.value.unwrap() {
                Value::Init(init) => init,
                Value::Register(_) | Value::AddPermits(_) => {
                    unreachable!("the first message must be `Init`")
                }
            }
        };

        enum Req {
            Request(Result<GetMuxStreamRequest, Status>),
            Message {
                up_actor_id: ActorId,
                down_actor_id: ActorId,
                message: MessageWithPermits,
            },
        }

        let mut select_all = SelectAll::new();
        select_all.push(request_stream.map(Req::Request).boxed());

        let mut all_permits = HashMap::new();

        while let Some(r) = select_all.next().await {
            match r {
                Req::Request(req) => match req?.value.unwrap() {
                    Value::Init(_) => unreachable!("the stream has already been initialized"),
                    Value::Register(Register {
                        up_actor_id,
                        down_actor_id,
                    }) => {
                        let receiver = stream_mgr
                            .take_receiver(
                                database_id,
                                term_id.clone(),
                                (up_actor_id, down_actor_id),
                            )
                            .await?;
                        let permits = Arc::downgrade(&receiver.permits());
                        all_permits.insert((up_actor_id, down_actor_id), permits);
                        select_all.push(
                            receiver
                                .into_raw_stream()
                                .map(move |message| Req::Message {
                                    up_actor_id,
                                    down_actor_id,
                                    message,
                                })
                                .boxed(),
                        );
                    }
                    Value::AddPermits(AddPermits {
                        up_actor_id,
                        down_actor_id,
                        permits,
                    }) => {
                        if let Some(to_add) = permits.unwrap().value
                            && let Some(permits) = all_permits
                                .get(&(up_actor_id, down_actor_id))
                                .and_then(|p| p.upgrade())
                        {
                            permits.add_permits(to_add);
                        }
                    }
                },

                Req::Message {
                    up_actor_id,
                    down_actor_id,
                    message: MessageWithPermits { message, permits },
                } => {
                    let message = match message {
                        DispatcherMessageBatch::Chunk(chunk) => {
                            DispatcherMessageBatch::Chunk(chunk.compact_vis())
                        }
                        msg @ (DispatcherMessageBatch::Watermark(_)
                        | DispatcherMessageBatch::BarrierBatch(_)) => msg,
                    };
                    let proto = message.to_protobuf();
                    // forward the acquired permit to the downstream
                    let response = GetMuxStreamResponse {
                        message: Some(proto),
                        permits: Some(PbPermits { value: permits }),
                        up_actor_id,
                        down_actor_id,
                    };

                    yield response;
                }
            }
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

                    metrics
                        .stream_fragment_exchange_bytes
                        .with_label_values(&[&up_fragment_id, &down_fragment_id])
                        .inc_by(bytes as u64);
                }
            }
        }
    }
}
