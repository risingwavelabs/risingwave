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
use std::sync::Arc;

use anyhow::Context as _;
use futures::StreamExt;
use futures::future::Either;
use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::id::{ActorId, FragmentId};
use risingwave_pb::task_service::get_mux_stream_request::{self, AddPermits, Value};
use risingwave_pb::task_service::{GetMuxStreamRequest, GetMuxStreamResponse, PbPermits, permits};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::{ComputeClient, ComputeClientPoolRef};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;

use crate::executor::exchange::error::ExchangeChannelClosed;
use crate::executor::exchange::input::RemoteInput;
use crate::executor::prelude::StreamingMetrics;
use crate::executor::{DispatcherMessage, DispatcherMessageBatch, StreamExecutorResult};
use crate::task::{LocalBarrierManager, UpDownActorIds, UpDownFragmentIds};

struct RegisterReq {
    register: get_mux_stream_request::Register,
    msg_tx: mpsc::UnboundedSender<StreamExecutorResult<DispatcherMessage>>,
}

#[derive(Clone)]
struct Worker {
    register_tx: mpsc::UnboundedSender<RegisterReq>,
    join_handle: Arc<JoinHandle<()>>,
}

impl Worker {
    /// Create a new worker by calling `get_mux_stream` to the upstream and running the loop.
    async fn new(
        client: ComputeClient,
        init: get_mux_stream_request::Init,
        config: &StreamingConfig,
    ) -> Result<Self, RpcError> {
        let up_fragment_id = init.up_fragment_id;
        let batched_permits_limit = config.developer.exchange_batched_permits as u32;
        let (stream, req_tx) = client.get_mux_stream(init).await?;
        let (register_tx, register_rx) = mpsc::unbounded_channel();

        let join_handle = tokio::spawn(Self::run(
            up_fragment_id,
            stream,
            req_tx,
            register_rx,
            batched_permits_limit,
        ));

        Ok(Self {
            register_tx,
            join_handle: Arc::new(join_handle),
        })
    }

    async fn run(
        up_fragment_id: FragmentId,
        stream: Streaming<GetMuxStreamResponse>,
        req_tx: mpsc::UnboundedSender<GetMuxStreamRequest>,
        register_rx: mpsc::UnboundedReceiver<RegisterReq>,
        batched_permits_limit: u32,
    ) {
        enum Event {
            Register(RegisterReq),
            Response(Result<GetMuxStreamResponse, tonic::Status>),
        }

        // Merge events from the register channel and the upstream response stream.
        let mut stream = futures::stream_select!(
            UnboundedReceiverStream::new(register_rx).map(Event::Register),
            stream.map(Event::Response),
        );

        type MsgTx = mpsc::UnboundedSender<StreamExecutorResult<DispatcherMessage>>;
        struct ActorEntry {
            msg_tx: MsgTx,
            batched_record_permits: u32,
        }

        impl ActorEntry {
            fn new(msg_tx: MsgTx) -> Self {
                Self {
                    msg_tx,
                    batched_record_permits: 0,
                }
            }

            fn accumulate_permits(
                &mut self,
                permits: permits::Value,
                batched_permits_limit: u32,
            ) -> Option<permits::Value> {
                match permits {
                    // For records, batch the permits we received to reduce the backward
                    // `AddPermits` messages.
                    permits::Value::Record(p) => {
                        self.batched_record_permits += p;
                        if self.batched_record_permits >= batched_permits_limit {
                            let permits = std::mem::take(&mut self.batched_record_permits);
                            Some(permits::Value::Record(permits))
                        } else {
                            None
                        }
                    }
                    // For barriers, always send it back immediately.
                    permits::Value::Barrier(p) => Some(permits::Value::Barrier(p)),
                }
            }
        }

        // All registered actor pairs.
        let mut entries: HashMap<(ActorId, ActorId), ActorEntry> = HashMap::new();

        let result: Result<(), ExchangeChannelClosed> = try {
            while let Some(event) = stream.next().await {
                match event {
                    // Register a new actor pair.
                    Event::Register(RegisterReq { register, msg_tx }) => {
                        req_tx
                            .send(GetMuxStreamRequest {
                                value: Some(Value::Register(register)),
                            })
                            .map_err(|_| {
                                ExchangeChannelClosed::remote_input_fragment(up_fragment_id, None)
                            })?;

                        entries.insert(
                            (register.up_actor_id, register.down_actor_id),
                            ActorEntry::new(msg_tx),
                        );
                    }

                    // Exchange message from the upstream.
                    Event::Response(res) => {
                        let GetMuxStreamResponse {
                            message,
                            permits,
                            up_actor_id,
                            down_actor_id,
                        } = res.map_err(|e| {
                            ExchangeChannelClosed::remote_input_fragment(up_fragment_id, Some(e))
                        })?;

                        let actor_pair = (up_actor_id, down_actor_id);
                        let Some(entry) = entries.get_mut(&actor_pair) else {
                            tracing::warn!(
                                %up_actor_id,
                                %down_actor_id,
                                "received message for unregistered actor pair"
                            );
                            continue;
                        };

                        let add_permits = permits.and_then(|p| p.value).and_then(|permits| {
                            entry.accumulate_permits(permits, batched_permits_limit)
                        });
                        let msg_tx = &entry.msg_tx;

                        // Put permits back to the upstream if accumulated enough.
                        if let Some(permits) = add_permits {
                            req_tx
                                .send(GetMuxStreamRequest {
                                    value: Some(Value::AddPermits(AddPermits {
                                        up_actor_id,
                                        down_actor_id,
                                        permits: Some(PbPermits {
                                            value: Some(permits),
                                        }),
                                    })),
                                })
                                .map_err(|_| {
                                    ExchangeChannelClosed::remote_input_fragment(
                                        up_fragment_id,
                                        None,
                                    )
                                })?;
                        }

                        // Any error occurred during sending the message to the specific actor should be
                        // treated as the actor disconnected. We should gracefully remove the actor from
                        // the worker, instead of failing the whole worker.
                        let send_result: Result<(), ()> = try {
                            let msg = message.unwrap();

                            match DispatcherMessageBatch::from_protobuf(&msg) {
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
                            tracing::debug!(
                                %up_actor_id,
                                %down_actor_id,
                                "downstream actor disconnected, removing from mux worker",
                            );
                            entries.remove(&actor_pair);
                        }
                    }
                }
            }
        };

        // Forward worker error to all registered actors.
        // Although we always emit an error in `MuxRemoteInputStream`, this can be more accurate.
        if let Err(e) = result {
            for (_, entry) in entries {
                entry.msg_tx.send(Err(e.clone().into())).ok();
            }
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct WorkerKey {
    upstream_addr: HostAddr,
    init: get_mux_stream_request::Init,
}

/// Workers for multiplexed exchange.
#[derive(Clone)]
pub struct MuxExchangeWorkers {
    // Note: we store `Result` in the value in order to use moka's `or_insert_with_if`.
    cache: moka::future::Cache<WorkerKey, Result<Worker, Arc<RpcError>>>,
}

impl std::fmt::Debug for MuxExchangeWorkers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MuxExchangeWorkers").finish_non_exhaustive()
    }
}

impl MuxExchangeWorkers {
    /// Create a new `MuxExchangeWorkers`.
    pub(crate) fn new() -> Self {
        Self {
            cache: moka::future::Cache::new(u64::MAX),
        }
    }

    /// Return a worker for the given remote exchange information.
    ///
    /// A worker will be reused if the `init` and `upstream_addr` of the remote exchange match, i.e.,
    /// the exchange data of actor pairs in the same fragment pair, upstream worker node, database
    /// and term will be multiplexed.
    ///
    /// If the worker doesn't exist or the previous one has disconnected, a new one will be created
    /// using the given `client_pool`.
    async fn get(
        &self,
        init: get_mux_stream_request::Init,
        upstream_addr: HostAddr,
        client_pool: ComputeClientPoolRef,
        config: &StreamingConfig,
    ) -> StreamExecutorResult<Worker> {
        self.cache
            .entry(WorkerKey {
                upstream_addr: upstream_addr.clone(),
                init: init.clone(),
            })
            .or_insert_with_if(
                async move {
                    let client = client_pool.get_by_addr(upstream_addr).await?;
                    let worker = Worker::new(client, init, config).await?;
                    Ok(worker)
                },
                |w| match w {
                    // Create a new one if the previous worker exited.
                    Ok(w) => w.join_handle.is_finished(),
                    // Create a new one if previous connection failed.
                    Err(_) => true,
                },
            )
            .await
            .into_value()
            .context("failed to create a mux exchange worker to upstream")
            .map_err(Into::into)
    }
}

impl RemoteInput {
    /// Create a remote input with the experimental multiplexing implementation.
    pub(super) async fn new_mux(
        local_barrier_manager: &LocalBarrierManager,
        upstream_addr: HostAddr,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        _metrics: Arc<StreamingMetrics>,
    ) -> StreamExecutorResult<Self> {
        let env = &local_barrier_manager.env;
        let actor_id = up_down_ids.0;

        let init = get_mux_stream_request::Init {
            up_fragment_id: up_down_frag.0,
            down_fragment_id: up_down_frag.1,
            database_id: local_barrier_manager.database_id,
            term_id: local_barrier_manager.term_id.clone(),
        };

        let worker = env
            .mux_exchange_workers()
            .get(init, upstream_addr, env.client_pool(), env.global_config())
            .await?;

        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        worker
            .register_tx
            .send(RegisterReq {
                register: get_mux_stream_request::Register {
                    up_actor_id: up_down_ids.0,
                    down_actor_id: up_down_ids.1,
                },
                msg_tx,
            })
            .ok()
            // Worker exited immediately after we got it, could be connection error.
            .context("failed to connect to upstream")?;

        Ok(Self {
            actor_id,
            inner: Either::Right(make_input_stream(actor_id, msg_rx)),
        })
    }
}

pub(super) type MuxRemoteInputStream = impl crate::executor::DispatcherMessageStream;

#[define_opaque(MuxRemoteInputStream)]
fn make_input_stream(
    actor_id: ActorId,
    msg_rx: mpsc::UnboundedReceiver<StreamExecutorResult<DispatcherMessage>>,
) -> MuxRemoteInputStream {
    UnboundedReceiverStream::new(msg_rx).chain(futures::stream::once(async move {
        // Always emit an error after worker exited. This is because we use barrier as the control
        // message to stop the stream. Reaching here means the channel is closed unexpectedly.
        Err(ExchangeChannelClosed::remote_input(actor_id, None).into())
    }))
}
