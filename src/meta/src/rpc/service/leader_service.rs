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

use std::borrow::Borrow;

use either::Either;
use risingwave_common::util::addr::leader_info_to_host_addr;
use risingwave_pb::common::HostAddress;
use risingwave_pb::leader::leader_service_server::LeaderService;
use risingwave_pb::leader::{LeaderRequest, LeaderResponse};
use risingwave_pb::meta::MetaLeaderInfo;
use tonic::{Request, Response, Status};

use crate::rpc::server::ElectionClientRef;

#[derive(Clone)]
pub struct LeaderServiceImpl {
    election_client: Either<ElectionClientRef, MetaLeaderInfo>,
}

impl LeaderServiceImpl {
    pub fn new(election_client: Either<ElectionClientRef, MetaLeaderInfo>) -> Self {
        LeaderServiceImpl { election_client }
    }
}

#[async_trait::async_trait]
impl LeaderService for LeaderServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn leader(
        &self,
        _request: Request<LeaderRequest>,
    ) -> Result<Response<LeaderResponse>, Status> {
        let leader = match self.election_client.borrow() {
            Either::Left(election_client) => election_client.leader().await,
            Either::Right(leader) => Ok(Some(leader.clone())),
        }?;

        let leader_address = leader
            .map(leader_info_to_host_addr)
            .map(|leader_addr| HostAddress {
                host: leader_addr.host,
                port: leader_addr.port.into(),
            });

        Ok(Response::new(LeaderResponse {
            leader_addr: leader_address,
        }))
    }
}
