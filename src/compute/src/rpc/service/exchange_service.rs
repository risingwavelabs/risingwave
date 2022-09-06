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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::Stream;
use futures_async_stream::try_stream;
use risingwave_batch::task::BatchManager;
use risingwave_pb::task_service::exchange_service_server::ExchangeService;
use risingwave_pb::task_service::{
    GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
};
use risingwave_stream::executor::Message;
use risingwave_stream::task::LocalStreamManager;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::rpc::service::exchange_metrics::ExchangeServiceMetrics;

/// Buffer size of the receiver of the remote channel.
const BATCH_EXCHANGE_BUFFER_SIZE: usize = 1024;

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    batch_mgr: Arc<BatchManager>,
    stream_mgr: Arc<LocalStreamManager>,
    metrics: Arc<ExchangeServiceMetrics>,
}

type BatchDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;
type StreamDataStream = impl Stream<Item = std::result::Result<GetStreamResponse, Status>>;

#[async_trait::async_trait]
impl ExchangeService for ExchangeServiceImpl {
    type GetDataStream = BatchDataStream;
    type GetStreamStream = StreamDataStream;

    #[cfg_attr(coverage, no_coverage)]
    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let pb_task_output_id = request
            .into_inner()
            .task_output_id
            .expect("Failed to get task output id.");
        let (tx, rx) = tokio::sync::mpsc::channel(BATCH_EXCHANGE_BUFFER_SIZE);
        if let Err(e) = self.batch_mgr.get_data(tx, peer_addr, &pb_task_output_id) {
            error!("Failed to serve exchange RPC from {}: {}", peer_addr, e);
            return Err(e.into());
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream(
        &self,
        request: Request<GetStreamRequest>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;
        let req = request.into_inner();
        let up_down_actor_ids = (req.up_actor_id, req.down_actor_id);
        let up_down_fragment_ids = (req.up_fragment_id, req.down_fragment_id);
        let receiver = self.stream_mgr.take_receiver(up_down_actor_ids)?;

        Ok(Response::new(Self::get_stream_impl(
            self.metrics.clone(),
            peer_addr,
            receiver,
            up_down_actor_ids,
            up_down_fragment_ids,
        )))
    }
}

impl ExchangeServiceImpl {
    pub fn new(
        mgr: Arc<BatchManager>,
        stream_mgr: Arc<LocalStreamManager>,
        metrics: Arc<ExchangeServiceMetrics>,
    ) -> Self {
        ExchangeServiceImpl {
            batch_mgr: mgr,
            stream_mgr,
            metrics,
        }
    }

    #[try_stream(ok = GetStreamResponse, error = Status)]
    async fn get_stream_impl(
        metrics: Arc<ExchangeServiceMetrics>,
        peer_addr: SocketAddr,
        mut receiver: Receiver<Message>,
        up_down_actor_ids: (u32, u32),
        up_down_fragment_ids: (u32, u32),
    ) {
        tracing::trace!(target: "events::compute::exchange", peer_addr = %peer_addr, "serve stream exchange RPC");
        let up_actor_id = up_down_actor_ids.0.to_string();
        let down_actor_id = up_down_actor_ids.1.to_string();
        let up_fragment_id = up_down_fragment_ids.0.to_string();
        let down_fragment_id = up_down_fragment_ids.1.to_string();

        let mut rr = 0;
        const SAMPLING_FREQUENCY: u64 = 100;

        while let Some(msg) = receiver.recv().await {
            // add serialization duration metric with given sampling frequency
            let proto = if rr % SAMPLING_FREQUENCY == 0 {
                let start_time = Instant::now();
                let proto = msg.to_protobuf();
                metrics
                    .actor_sampled_serialize_duration_ns
                    .with_label_values(&[&up_actor_id])
                    .inc_by(start_time.elapsed().as_nanos() as u64);
                proto
            } else {
                msg.to_protobuf()
            }?;
            rr += 1;

            let message = GetStreamResponse {
                message: Some(proto),
            };
            let bytes = Message::get_encoded_len(&message);

            yield message;

            metrics
                .stream_exchange_bytes
                .with_label_values(&[&up_actor_id, &down_actor_id])
                .inc_by(bytes as u64);
            metrics
                .stream_fragment_exchange_bytes
                .with_label_values(&[&up_fragment_id, &down_fragment_id])
                .inc_by(bytes as u64);
        }
    }
}
