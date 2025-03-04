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

use risingwave_pb::catalog::connection::Info;
use risingwave_pb::catalog::{PbConnection, connection};

use crate::catalog::{ConnectionId, OwnedByUserCatalog};
use crate::user::UserId;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionCatalog {
    pub id: ConnectionId,
    pub name: String,
    pub info: connection::Info,
    pub owner: UserId,
}

impl ConnectionCatalog {
    pub fn connection_type(&self) -> &str {
        match &self.info {
            Info::PrivateLinkService(srv) => srv.get_provider().unwrap().as_str_name(),
            Info::ConnectionParams(params) => params.get_connection_type().unwrap().as_str_name(),
        }
    }

    pub fn provider(&self) -> &str {
        match &self.info {
            Info::PrivateLinkService(_) => "PRIVATELINK",
            Info::ConnectionParams(_) => panic!("ConnectionParams is not supported as provider."),
        }
    }
}

impl From<&PbConnection> for ConnectionCatalog {
    fn from(prost: &PbConnection) -> Self {
        Self {
            id: prost.id,
            name: prost.name.clone(),
            info: prost.info.clone().unwrap(),
            owner: prost.owner,
        }
    }
}

impl OwnedByUserCatalog for ConnectionCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
