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

use std::collections::{BTreeMap, HashMap};

use risingwave_pb::catalog::Connection;

use crate::manager::{ConnectionId, MetaSrvEnv};
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::{MetaError, MetaResult};

pub struct ConnectionManager {
    pub connections: BTreeMap<ConnectionId, Connection>,

    // index by service name
    pub connection_by_name: HashMap<String, ConnectionId>,
}

impl ConnectionManager {
    pub async fn new<S: MetaStore>(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        // load connections from meta store
        let connections = Connection::list(env.meta_store()).await?;
        let connections = BTreeMap::from_iter(connections.into_iter().map(|conn| (conn.id, conn)));

        let connection_by_name = connections
            .values()
            .map(|conn| (conn.name.clone(), conn.id))
            .collect();

        Ok(Self {
            connections,
            connection_by_name,
        })
    }

    pub(crate) fn check_connection_duplicated(&self, conn_name: &str) -> MetaResult<()> {
        if self
            .connections
            .values()
            .any(|conn| conn.name.eq(conn_name))
        {
            Err(MetaError::catalog_duplicated("connection", conn_name))
        } else {
            Ok(())
        }
    }

    pub fn get_connection_by_name(&self, name: &str) -> Option<&Connection> {
        self.connection_by_name
            .get(name)
            .and_then(|id| self.connections.get(id))
    }

    pub fn list_connections(&self) -> Vec<Connection> {
        self.connections.values().cloned().collect()
    }
}
