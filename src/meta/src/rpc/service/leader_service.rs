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

use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::leader_service_server::LeaderService;
use risingwave_pb::meta::{LeaderRequest, LeaderResponse};
use tokio::sync::watch::Receiver;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct LeaderServiceImpl {
    leader_rx: Receiver<(HostAddr, bool)>,
}

impl LeaderServiceImpl {
    pub fn new(leader_rx: Receiver<(HostAddr, bool)>) -> Self {
        LeaderServiceImpl { leader_rx }
    }
}

//  expected struct `risingwave_pb::meta::LeaderRequest`, found struct
// `risingwave_pb::leader::LeaderRequest`

#[async_trait::async_trait]
impl LeaderService for LeaderServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn leader(
        &self,
        _request: Request<LeaderRequest>,
    ) -> Result<Response<LeaderResponse>, Status> {
        // service never called

        // TODO: change request. Need only a simple ping
        // let req = request.into_inner();
        let leader_addr = self.leader_rx.borrow().0.clone();
        let leader_address = HostAddress {
            host: leader_addr.host,
            port: leader_addr.port.into(),
        };
        Ok(Response::new(LeaderResponse {
            leader_addr: Some(leader_address),
        }))
    }
}
