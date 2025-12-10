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
use std::sync::{Arc, Weak};

use futures::StreamExt;
use futures::stream::SelectAll;
use futures_async_stream::try_stream;
use risingwave_pb::id::ActorId;
use risingwave_pb::task_service::{GetMuxStreamRequest, GetMuxStreamResponse, PbPermits};
use risingwave_stream::executor::DispatcherMessageBatch;
use risingwave_stream::executor::exchange::permit::{MessageWithPermits, Permits};
use risingwave_stream::task::LocalStreamManager;
use tonic::{Status, Streaming};

use crate::rpc::service::stream_exchange_service::StreamExchangeServiceImpl;

impl StreamExchangeServiceImpl {
    #[try_stream(ok = GetMuxStreamResponse, error = Status)]
    pub(super) async fn get_mux_stream_impl(
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

        enum Event {
            Request(Result<GetMuxStreamRequest, Status>),
            ExchangeMessage {
                up_actor_id: ActorId,
                down_actor_id: ActorId,
                message: MessageWithPermits,
            },
        }

        // Merge events from the downstream client and all upstream actors.
        let mut select_all = SelectAll::new();
        select_all.push(request_stream.map(Event::Request).left_stream());

        // Weak permit handles of all registered actor pairs.
        let mut all_permit_handles: HashMap<(ActorId, ActorId), Weak<Permits>> = HashMap::new();

        while let Some(event) = select_all.next().await {
            match event {
                // Request from the downstream client.
                Event::Request(req) => match req?.value.unwrap() {
                    Value::Init(_) => unreachable!("the stream has already been initialized"),

                    // Register a new actor pair to this multiplexed stream.
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
                        all_permit_handles.insert(
                            (up_actor_id, down_actor_id),
                            Arc::downgrade(&receiver.permits()),
                        );
                        select_all.push(
                            Box::pin(receiver.into_raw_stream())
                                .map(move |message| Event::ExchangeMessage {
                                    up_actor_id,
                                    down_actor_id,
                                    message,
                                })
                                .right_stream(),
                        );
                    }

                    // Add permits back to the upstream.
                    Value::AddPermits(AddPermits {
                        up_actor_id,
                        down_actor_id,
                        permits,
                    }) => {
                        if let Some(to_add) = permits.unwrap().value {
                            if let Some(permits) = all_permit_handles
                                .get(&(up_actor_id, down_actor_id))
                                .and_then(|p| p.upgrade())
                            {
                                permits.add_permits(to_add);
                            } else {
                                tracing::warn!(
                                    %up_actor_id,
                                    %down_actor_id,
                                    ?to_add,
                                    "failed to add permits to non-existing actor pair",
                                );
                            }
                        }
                    }
                },

                // Exchange message from the upstream.
                Event::ExchangeMessage {
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
