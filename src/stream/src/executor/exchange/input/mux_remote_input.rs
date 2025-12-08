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
use std::sync::{Arc, LazyLock};

use anyhow::Context as _;
use futures::StreamExt;
use futures::future::Either;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::task_service::get_mux_stream_request::{self, AddPermits, Value};
use risingwave_pb::task_service::{GetMuxStreamRequest, GetMuxStreamResponse};
use risingwave_rpc_client::ComputeClient;
use risingwave_rpc_client::error::RpcError;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::executor::exchange::error::ExchangeChannelClosed;
use crate::executor::exchange::input::RemoteInput;
use crate::executor::prelude::StreamingMetrics;
use crate::executor::{DispatcherMessage, StreamExecutorError, StreamExecutorResult};
use crate::task::{LocalBarrierManager, StreamEnvironment, UpDownActorIds, UpDownFragmentIds};

struct RegisterReq {
    get: get_mux_stream_request::Get,
    msg_tx: mpsc::UnboundedSender<StreamExecutorResult<DispatcherMessage>>,
}

#[derive(Clone)]
struct Worker {
    register_tx: mpsc::UnboundedSender<RegisterReq>,
    join_handle: Arc<JoinHandle<StreamExecutorResult<()>>>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct WorkerKey {
    upstream_addr: HostAddr,
    init: get_mux_stream_request::Init,
}

impl Worker {
    async fn new(
        client: ComputeClient,
        init: get_mux_stream_request::Init,
    ) -> Result<Self, RpcError> {
        let (stream, req_tx) = client.get_mux_stream(init).await?;

        let (register_tx, register_rx) = mpsc::unbounded_channel();

        let task = async move {
            enum Event {
                Register(RegisterReq),
                Response(Result<GetMuxStreamResponse, tonic::Status>),
            }

            let mut stream = futures::stream_select!(
                tokio_stream::wrappers::UnboundedReceiverStream::new(register_rx)
                    .map(Event::Register),
                stream.map(Event::Response),
            );

            let mut msg_txs = HashMap::new();

            while let Some(event) = stream.next().await {
                match event {
                    Event::Register(RegisterReq { get, msg_tx }) => {
                        req_tx
                            .send(GetMuxStreamRequest {
                                value: Some(Value::Get(get)),
                            })
                            .map_err(|_| {
                                ExchangeChannelClosed::remote_input(114514.into(), None)
                            })?;

                        msg_txs.insert((get.up_actor_id, get.down_actor_id), msg_tx);
                    }
                    Event::Response(res) => {
                        let GetMuxStreamResponse {
                            message,
                            permits,
                            up_actor_id,
                            down_actor_id,
                        } = res.map_err(|e| {
                            ExchangeChannelClosed::remote_input(114514.into(), Some(e))
                        })?;

                        let actor_pair = (up_actor_id, down_actor_id);

                        if let Some(msg_tx) = msg_txs.get(&actor_pair) {
                            use crate::executor::DispatcherMessageBatch;
                            let msg = message.unwrap();

                            let msg_res = DispatcherMessageBatch::from_protobuf(&msg);

                            let send_result: Result<(), ()> = try {
                                // immediately put back permits
                                req_tx
                                    .send(GetMuxStreamRequest {
                                        value: Some(Value::AddPermits(AddPermits {
                                            up_actor_id,
                                            down_actor_id,
                                            permits,
                                        })),
                                    })
                                    .map_err(|_| ())?;

                                match msg_res {
                                    Ok(msg) => {
                                        for msg in msg.into_messages() {
                                            msg_tx.send(Ok(msg)).map_err(|_| ())?;
                                        }
                                    }
                                    Err(e) => {
                                        msg_tx.send(Err(e)).map_err(|_| ())?;
                                    }
                                }
                            };

                            if send_result.is_err() {
                                msg_txs.remove(&actor_pair);
                            }
                        }
                    }
                }
            }

            Ok::<_, StreamExecutorError>(())
        };

        // TODO: handler
        let join_handle = tokio::spawn(task);

        Ok(Self {
            register_tx,
            join_handle: Arc::new(join_handle),
        })
    }
}

struct Mux {
    cache: moka::future::Cache<WorkerKey, Result<Worker, Arc<RpcError>>>,
}

impl Mux {
    pub fn new() -> Self {
        Self {
            cache: moka::future::Cache::new(u64::MAX),
        }
    }

    async fn get(
        &self,
        init: get_mux_stream_request::Init,
        upstream_addr: HostAddr,
        env: &StreamEnvironment,
    ) -> StreamExecutorResult<Worker> {
        self.cache
            .entry(WorkerKey {
                upstream_addr: upstream_addr.clone(),
                init: init.clone(),
            })
            .or_insert_with_if(
                async move {
                    let client = env.client_pool().get_by_addr(upstream_addr).await?;
                    let worker = Worker::new(client, init).await?;
                    Ok(worker)
                },
                |w| match w {
                    Ok(w) => w.join_handle.is_finished(),
                    Err(_) => true,
                },
            )
            .await
            .into_value()
            .context("failed to connect to upstream")
            .map_err(Into::into)
    }
}

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn new_mux(
        local_barrier_manager: &LocalBarrierManager,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        _metrics: Arc<StreamingMetrics>,
    ) -> StreamExecutorResult<Self> {
        let actor_id = up_down_ids.0;

        static MUX: LazyLock<Mux> = LazyLock::new(Mux::new);

        let init = get_mux_stream_request::Init {
            up_fragment_id: up_down_frag.0,
            down_fragment_id: up_down_frag.1,
            database_id: local_barrier_manager.database_id,
            term_id: local_barrier_manager.term_id.clone(),
        };

        let worker = MUX
            .get(init, upstream_addr, &local_barrier_manager.env)
            .await?;

        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        worker
            .register_tx
            .send(RegisterReq {
                get: get_mux_stream_request::Get {
                    up_actor_id: up_down_ids.0,
                    down_actor_id: up_down_ids.1,
                },
                msg_tx,
            })
            .ok()
            .context("failed to connect to upstream")?;

        Ok(Self {
            actor_id,
            inner: Either::Right(make_input_stream(msg_rx)),
        })
    }
}

pub(super) type MuxRemoteInputStream = impl crate::executor::DispatcherMessageStream;

#[define_opaque(MuxRemoteInputStream)]
fn make_input_stream(
    msg_rx: mpsc::UnboundedReceiver<StreamExecutorResult<DispatcherMessage>>,
) -> MuxRemoteInputStream {
    tokio_stream::wrappers::UnboundedReceiverStream::new(msg_rx).chain(futures::stream::once(
        async {
            // Always emit an error outside the loop. This is because we use barrier as the control
            // message to stop the stream. Reaching here means the channel is closed unexpectedly.
            Err(ExchangeChannelClosed::remote_input(1919810.into(), None).into())
        },
    ))
}
