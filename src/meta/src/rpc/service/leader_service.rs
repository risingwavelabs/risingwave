// Copyright 2023 Singularity Data
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

use risingwave_common::util::addr::leader_info_to_host_addr;
use risingwave_pb::common::HostAddress;
use risingwave_pb::leader::leader_service_server::LeaderService;
use risingwave_pb::leader::{LeaderRequest, LeaderResponse};
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::watch::Receiver;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct LeaderServiceImpl {
    leader_rx: Receiver<(MetaLeaderInfo, bool)>,
}

impl LeaderServiceImpl {
    pub fn new(leader_rx: Receiver<(MetaLeaderInfo, bool)>) -> Self {
        LeaderServiceImpl { leader_rx }
    }
}

#[async_trait::async_trait]
impl LeaderService for LeaderServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn leader(
        &self,
        _request: Request<LeaderRequest>,
    ) -> Result<Response<LeaderResponse>, Status> {
        let leader_info = self.leader_rx.borrow().0.clone();
        let leader_addr = leader_info_to_host_addr(leader_info);
        let leader_address = HostAddress {
            host: leader_addr.host,
            port: leader_addr.port.into(),
        };
        Ok(Response::new(LeaderResponse {
            leader_addr: Some(leader_address),
        }))
    }
}
