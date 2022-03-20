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
//
use risingwave_common::error::tonic_err;
use risingwave_common::try_match_expand;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::SubscribeRequest;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::manager::{Notification, NotificationManagerRef};

pub struct NotificationServiceImpl {
    nm: NotificationManagerRef,
}

impl NotificationServiceImpl {
    pub fn new(nm: NotificationManagerRef) -> Self {
        Self { nm }
    }
}

#[async_trait::async_trait]
impl NotificationService for NotificationServiceImpl {
    type SubscribeStream = ReceiverStream<Notification>;

    #[cfg_attr(coverage, no_coverage)]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let worker_type = req.get_worker_type().map_err(tonic_err)?;
        let host_address = try_match_expand!(req.host, Some, "SubscribeRequest::host is empty")
            .map_err(|e| e.to_grpc_status())?;

        let rx = self
            .nm
            .subscribe(host_address, worker_type)
            .await
            .map_err(|e| e.to_grpc_status())?;

        Ok(Response::new(rx))
    }
}
