// Copyright 2024 RisingWave Labs
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
use std::str::FromStr;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use rdkafka::client::BrokerAddr;
use risingwave_common::bail;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::connection::PrivateLinkService;

use crate::connector_common::{
    AwsPrivateLinkItem, PRIVATE_LINK_BROKER_REWRITE_MAP_KEY, PRIVATE_LINK_TARGETS_KEY,
};
use crate::error::ConnectorResult;
use crate::source::kafka::{KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS};

pub const PRIVATELINK_ENDPOINT_KEY: &str = "privatelink.endpoint";

#[derive(Debug)]
pub(super) enum PrivateLinkContextRole {
    Consumer,
    #[expect(dead_code)]
    Producer,
}

impl std::fmt::Display for PrivateLinkContextRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrivateLinkContextRole::Consumer => write!(f, "consumer"),
            PrivateLinkContextRole::Producer => write!(f, "producer"),
        }
    }
}

pub(super) struct BrokerAddrRewriter {
    #[expect(dead_code)]
    role: PrivateLinkContextRole,
    rewrite_map: BTreeMap<BrokerAddr, BrokerAddr>,
}

impl BrokerAddrRewriter {
    pub(super) fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        let rewrote_addr = match self.rewrite_map.get(&addr) {
            None => addr,
            Some(new_addr) => new_addr.clone(),
        };
        rewrote_addr
    }

    pub fn new(
        role: PrivateLinkContextRole,
        broker_rewrite_map: Option<BTreeMap<String, String>>,
    ) -> ConnectorResult<Self> {
        let rewrite_map: ConnectorResult<BTreeMap<BrokerAddr, BrokerAddr>> = broker_rewrite_map
            .map_or(Ok(BTreeMap::new()), |addr_map| {
                tracing::info!("[{}] rewrite map {:?}", role, addr_map);
                addr_map
                    .into_iter()
                    .map(|(old_addr, new_addr)| {
                        let old_addr = HostAddr::from_str(&old_addr)?;
                        let new_addr = HostAddr::from_str(&new_addr)?;
                        let old_addr = BrokerAddr {
                            host: old_addr.host,
                            port: old_addr.port.to_string(),
                        };
                        let new_addr = BrokerAddr {
                            host: new_addr.host,
                            port: new_addr.port.to_string(),
                        };
                        Ok((old_addr, new_addr))
                    })
                    .collect()
            });
        let rewrite_map = rewrite_map?;
        Ok(Self { role, rewrite_map })
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
) -> ConnectorResult<String> {
    with_properties
        .get(property)
        .map(|s| s.to_lowercase())
        .with_context(|| format!("Required property \"{property}\" is not provided"))
        .map_err(Into::into)
}

pub fn insert_privatelink_broker_rewrite_map(
    with_options: &mut BTreeMap<String, String>,
    svc: Option<&PrivateLinkService>,
    privatelink_endpoint: Option<String>,
) -> ConnectorResult<()> {
    let mut broker_rewrite_map = HashMap::new();
    let servers = get_property_required(with_options, kafka_props_broker_key(with_options))?;
    let broker_addrs = servers.split(',').collect_vec();
    let link_target_value = get_property_required(with_options, PRIVATE_LINK_TARGETS_KEY)?;
    let link_targets: Vec<AwsPrivateLinkItem> =
        serde_json::from_str(link_target_value.as_str()).map_err(|e| anyhow!(e))?;
    // remove the private link targets from WITH options, as they are useless after we constructed the rewrite mapping
    with_options.remove(PRIVATE_LINK_TARGETS_KEY);

    if broker_addrs.len() != link_targets.len() {
        bail!(
            "The number of broker addrs {} does not match the number of private link targets {}",
            broker_addrs.len(),
            link_targets.len()
        );
    }

    if let Some(endpoint) = privatelink_endpoint {
        for (link, broker) in link_targets.iter().zip_eq_fast(broker_addrs.into_iter()) {
            // rewrite the broker address to endpoint:port
            broker_rewrite_map.insert(broker.to_string(), format!("{}:{}", &endpoint, link.port));
        }
    } else {
        if svc.is_none() {
            bail!("Privatelink endpoint not found.");
        }
        let svc = svc.unwrap();
        for (link, broker) in link_targets.iter().zip_eq_fast(broker_addrs.into_iter()) {
            if svc.dns_entries.is_empty() {
                bail!(
                    "No available private link endpoints for Kafka broker {}",
                    broker
                );
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
    with_options.insert(PRIVATE_LINK_BROKER_REWRITE_MAP_KEY.to_string(), json);
    Ok(())
}
