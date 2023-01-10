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

use risingwave_common::util::addr::{leader_info_to_host_addr, HostAddr};
use risingwave_pb::common::HostAddress;
use risingwave_pb::leader::leader_service_server::LeaderService;
use risingwave_pb::leader::{LeaderRequest, LeaderResponse, Member, MemberRequest, MemberResponse};
use risingwave_pb::meta::MetaLeaderInfo;
use tonic::{Request, Response, Status};

use crate::rpc::server::ElectionClientRef;

#[derive(Clone)]
pub struct LeaderServiceImpl {
    election_client: Option<ElectionClientRef>,
    current_leader: MetaLeaderInfo,
}

impl LeaderServiceImpl {
    pub fn new(election_client: Option<ElectionClientRef>, current_leader: MetaLeaderInfo) -> Self {
        LeaderServiceImpl {
            election_client,
            current_leader,
        }
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
            None => Ok(Some(self.current_leader.clone())),
            Some(client) => client.leader().await.map(|member| member.map(Into::into)),
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

    async fn members(
        &self,
        _request: Request<MemberRequest>,
    ) -> Result<Response<MemberResponse>, Status> {
        let members = if let Some(election_client) = self.election_client.borrow() {
            let mut members = vec![];
            for member in election_client.get_members().await? {
                let host_addr = member.id.parse::<HostAddr>()?;
                members.push(Member {
                    member_addr: Some(HostAddress {
                        host: host_addr.host,
                        port: host_addr.port.into(),
                    }),
                    lease_id: member.lease,
                })
            }

            members
        } else {
            let host_addr = self.current_leader.node_address.parse::<HostAddr>()?;
            vec![Member {
                member_addr: Some(HostAddress {
                    host: host_addr.host,
                    port: host_addr.port.into(),
                }),
                lease_id: self.current_leader.lease_id as i64,
            }]
        };

        Ok(Response::new(MemberResponse { members }))
    }
}
