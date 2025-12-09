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

use std::sync::Arc;

use risingwave_batch::task::BatchManager;
use risingwave_pb::task_service::batch_exchange_service_server::BatchExchangeService;
use risingwave_pb::task_service::{GetDataRequest, GetDataResponse};
use thiserror_ext::AsReport;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub type BatchDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;

#[derive(Clone)]
pub struct BatchExchangeServiceImpl {
    batch_mgr: Arc<BatchManager>,
}

impl BatchExchangeServiceImpl {
    pub fn new(batch_mgr: Arc<BatchManager>) -> Self {
        Self { batch_mgr }
    }
}

#[async_trait::async_trait]
impl BatchExchangeService for BatchExchangeServiceImpl {
    type GetDataStream = BatchDataStream;

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
        let (tx, rx) =
            tokio::sync::mpsc::channel(self.batch_mgr.config().developer.receiver_channel_size);
        if let Err(e) = self.batch_mgr.get_data(tx, peer_addr, &pb_task_output_id) {
            error!(
                %peer_addr,
                error = %e.as_report(),
                "Failed to serve exchange RPC"
            );
            return Err(e.into());
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
