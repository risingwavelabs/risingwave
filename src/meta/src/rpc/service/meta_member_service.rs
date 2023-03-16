// Copyright 2023 RisingWave Labs
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
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::meta_member_service_server::MetaMemberService;
use risingwave_pb::meta::{MembersRequest, MembersResponse, MetaMember};
use tonic::{Request, Response, Status};

use crate::rpc::server::{AddressInfo, ElectionClientRef};

#[derive(Clone)]
pub struct MetaMemberServiceImpl {
    election_client_or_self: Either<ElectionClientRef, AddressInfo>,
}

impl MetaMemberServiceImpl {
    pub fn new(election_client_or_self: Either<ElectionClientRef, AddressInfo>) -> Self {
        MetaMemberServiceImpl {
            election_client_or_self,
        }
    }
}

#[async_trait::async_trait]
impl MetaMemberService for MetaMemberServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn members(
        &self,
        _request: Request<MembersRequest>,
    ) -> Result<Response<MembersResponse>, Status> {
        let members = match self.election_client_or_self.borrow() {
            Either::Left(election_client) => {
                let mut members = vec![];
                for member in election_client.get_members().await? {
                    let host_addr = member
                        .id
                        .parse::<HostAddr>()
                        .map_err(|err| Status::from_error(err.into()))?;
                    members.push(MetaMember {
                        address: Some(HostAddress {
                            host: host_addr.host,
                            port: host_addr.port.into(),
                        }),
                        is_leader: member.is_leader,
                    })
                }

                members
            }
            Either::Right(self_as_leader) => {
                let host_addr = self_as_leader
                    .advertise_addr
                    .parse::<HostAddr>()
                    .map_err(|err| Status::from_error(err.into()))?;
                vec![MetaMember {
                    address: Some(HostAddress {
                        host: host_addr.host,
                        port: host_addr.port.into(),
                    }),
                    is_leader: true,
                }]
            }
        };

        Ok(Response::new(MembersResponse { members }))
    }
}
