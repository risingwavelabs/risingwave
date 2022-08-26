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

use itertools::Itertools;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
use tonic::{Request, Response, Status};

use crate::manager::ClusterManagerRef;
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    cluster_manager: ClusterManagerRef<S>,
}

impl<S> HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(cluster_manager: ClusterManagerRef<S>) -> Self {
        HeartbeatServiceImpl { cluster_manager }
    }
}

#[async_trait::async_trait]
impl<S> HeartbeatService for HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .cluster_manager
            .heartbeat(
                req.node_id,
                req.info
                    .into_iter()
                    .filter_map(|node_info| node_info.info)
                    .collect_vec(),
            )
            .await;
        match result {
            Ok(_) => Ok(Response::new(HeartbeatResponse { status: None })),
            Err(e) => {
                if e.is_invalid_worker() {
                    return Ok(Response::new(HeartbeatResponse {
                        status: Some(risingwave_pb::common::Status {
                            code: risingwave_pb::common::status::Code::UnknownWorker as i32,
                            message: format!("{}", e),
                        }),
                    }));
                }
                Err(e.into())
            }
        }
    }
}
