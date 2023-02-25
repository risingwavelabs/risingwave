use std::collections::{BTreeMap, HashMap};

use risingwave_pb::catalog::Connection;

use crate::manager::{ConnectionId, MetaSrvEnv};
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

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

    pub fn get_connection_by_name(&self, name: &str) -> Option<&Connection> {
        self.connection_by_name
            .get(name)
            .and_then(|id| self.connections.get(id))
    }

    pub fn list_connections(&self) -> Vec<Connection> {
        self.connections.values().cloned().collect()
    }
}
