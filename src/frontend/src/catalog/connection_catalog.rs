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
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::common::AwsPrivateLinkItem;
use risingwave_connector::source::kafka::{KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS};
use risingwave_connector::source::KAFKA_CONNECTOR;
use risingwave_pb::catalog::connection::private_link_service::PrivateLinkProvider;
use risingwave_pb::catalog::{connection, PbConnection};

use crate::catalog::ConnectionId;

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

#[inline(always)]
fn kafka_props_broker_key(with_properties: &BTreeMap<String, String>) -> &str {
    if with_properties.contains_key(KAFKA_PROPS_BROKER_KEY) {
        KAFKA_PROPS_BROKER_KEY
    } else {
        KAFKA_PROPS_BROKER_KEY_ALIAS
    }
}

#[inline(always)]
fn get_property_required(
    with_properties: &BTreeMap<String, String>,
    property: &str,
) -> Result<String> {
    with_properties
        .get(property)
        .map(|s| s.to_lowercase())
        .ok_or(RwError::from(anyhow!(
            "Required property \"{property}\" is not provided"
        )))
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
    let mut broker_rewrite_map = HashMap::new();
    const PRIVATE_LINK_TARGETS_KEY: &str = "privatelink.targets";

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
        let link_target_value = get_property_required(properties, PRIVATE_LINK_TARGETS_KEY)?;
        let servers = get_property_required(properties, kafka_props_broker_key(properties))?;
        let broker_addrs = servers.split(',').collect_vec();
        let link_targets: Vec<AwsPrivateLinkItem> =
            serde_json::from_str(link_target_value.as_str()).map_err(|e| anyhow!(e))?;
        if broker_addrs.len() != link_targets.len() {
            return Err(RwError::from(anyhow!(
                    "The number of broker addrs {} does not match the number of private link targets {}",
                    broker_addrs.len(),
                    link_targets.len()
                )));
        }

        for (link, broker) in link_targets.iter().zip_eq_fast(broker_addrs.into_iter()) {
            if let connection::Info::PrivateLinkService(svc) = &connection.info {
                if svc.dns_entries.is_empty() {
                    return Err(RwError::from(anyhow!(
                        "No available private link endpoints for Kafka broker {}",
                        broker
                    )));
                }
                // rewrite the broker address to the dns name w/o az
                // requires the NLB has enabled the cross-zone load balancing
                broker_rewrite_map.insert(
                    broker.to_string(),
                    format!("{}:{}", &svc.endpoint_dns_name, link.port),
                );
            }
        }

        // save private link dns names into source properties, which
        // will be extracted into KafkaProperties
        let json = serde_json::to_string(&broker_rewrite_map).map_err(|e| anyhow!(e))?;
        const BROKER_REWRITE_MAP_KEY: &str = "broker.rewrite.endpoints";
        properties.insert(BROKER_REWRITE_MAP_KEY.to_string(), json);
    }
    Ok(())
}
