// Copyright 2023 RisingWave Labs
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

use std::time::{Duration, Instant};

use risingwave_meta::manager::MetadataManager;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
use thiserror_ext::AsReport;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

const HEARTBEAT_RPC_STUCK_WARN_AFTER: Duration = Duration::from_secs(5);
const HEARTBEAT_RPC_SLOW_LOG_AFTER: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct HeartbeatServiceImpl {
    metadata_manager: MetadataManager,
}

impl HeartbeatServiceImpl {
    pub fn new(metadata_manager: MetadataManager) -> Self {
        HeartbeatServiceImpl { metadata_manager }
    }
}

#[async_trait::async_trait]
impl HeartbeatService for HeartbeatServiceImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id;
        let resource = req.resource.clone();
        let started_at = Instant::now();
        let (done_tx, done_rx) = oneshot::channel();

        tokio::spawn(async move {
            if tokio::time::timeout(HEARTBEAT_RPC_STUCK_WARN_AFTER, done_rx)
                .await
                .is_err()
            {
                tracing::warn!(
                    %node_id,
                    resource = ?resource,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "heartbeat RPC is still running in meta service"
                );
            }
        });

        let result = self
            .metadata_manager
            .cluster_controller
            .heartbeat(node_id, req.resource)
            .await;
        let elapsed = started_at.elapsed();
        let _ = done_tx.send(());

        if elapsed >= HEARTBEAT_RPC_SLOW_LOG_AFTER {
            tracing::warn!(
                %node_id,
                elapsed_ms = elapsed.as_millis(),
                is_ok = result.is_ok(),
                "heartbeat RPC finished slowly in meta service"
            );
        }

        match result {
            Ok(_) => Ok(Response::new(HeartbeatResponse { status: None })),
            Err(e) => {
                if e.is_invalid_worker() {
                    return Ok(Response::new(HeartbeatResponse {
                        status: Some(risingwave_pb::common::Status {
                            code: risingwave_pb::common::status::Code::UnknownWorker as i32,
                            message: e.to_report_string(),
                        }),
                    }));
                }
                Err(e.into())
            }
        }
    }
}
