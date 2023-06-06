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

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::error::{Result, RwError};
use risingwave_connector::source::kafka::private_link::insert_privatelink_broker_rewrite_map;
use risingwave_connector::source::KAFKA_CONNECTOR;
use risingwave_pb::catalog::connection::private_link_service::PrivateLinkProvider;
use risingwave_pb::catalog::{connection, PbConnection};

use crate::catalog::{ConnectionId, OwnedByUserCatalog};
use crate::user::UserId;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionCatalog {
    pub id: ConnectionId,
    pub name: String,
    pub info: connection::Info,
    owner: UserId,
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

#[inline(always)]
fn is_kafka_connector(with_properties: &BTreeMap<String, String>) -> bool {
    const UPSTREAM_SOURCE_KEY: &str = "connector";
    with_properties
        .get(UPSTREAM_SOURCE_KEY)
        .unwrap_or(&"".to_string())
        .to_lowercase()
        .eq_ignore_ascii_case(KAFKA_CONNECTOR)
}

pub(crate) fn resolve_private_link_connection(
    connection: &Arc<ConnectionCatalog>,
    properties: &mut BTreeMap<String, String>,
) -> Result<()> {
    #[allow(irrefutable_let_patterns)]
    if let connection::Info::PrivateLinkService(svc) = &connection.info {
        if !is_kafka_connector(properties) {
            return Err(RwError::from(anyhow!(
                "Private link is only supported for Kafka connector"
            )));
        }
        // skip all checks for mock connection
        if svc.get_provider()? == PrivateLinkProvider::Mock {
            return Ok(());
        }
        insert_privatelink_broker_rewrite_map(svc, properties).map_err(RwError::from)?;
    }
    Ok(())
}
