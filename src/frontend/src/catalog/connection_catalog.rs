// use risingwave_pb::catalog::{PbSource, StreamSourceInfo, WatermarkDesc};

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
