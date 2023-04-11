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

use risingwave_pb::catalog::{connection, PbConnection};

use super::ConnectionId;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionCatalog {
    pub id: ConnectionId,
    pub name: String,
    pub info: connection::Info,
}

impl From<&PbConnection> for ConnectionCatalog {
    fn from(prost: &PbConnection) -> Self {
        Self {
            id: prost.id,
            name: prost.name.clone(),
            info: prost.info.clone().unwrap(),
        }
    }
}
