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
use std::str::FromStr;

use anyhow::anyhow;
use itertools::Itertools;
use rdkafka::client::BrokerAddr;
use rdkafka::consumer::ConsumerContext;
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::connection::PrivateLinkService;

use crate::common::AwsPrivateLinkItem;
use crate::source::kafka::{
    BROKER_REWRITE_MAP_KEY, KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS,
    PRIVATE_LINK_TARGETS_KEY,
};
use crate::source::KAFKA_CONNECTOR;

#[derive(Debug)]
enum PrivateLinkContextRole {
    Consumer,
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

struct BrokerAddrRewriter {
    role: PrivateLinkContextRole,
    rewrite_map: BTreeMap<BrokerAddr, BrokerAddr>,
}

impl BrokerAddrRewriter {
    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        let rewrote_addr = match self.rewrite_map.get(&addr) {
            None => addr,
            Some(new_addr) => new_addr.clone(),
        };
        rewrote_addr
    }

    pub fn new(
        role: PrivateLinkContextRole,
        broker_rewrite_map: Option<HashMap<String, String>>,
    ) -> anyhow::Result<Self> {
        tracing::info!("[{}] rewrite map {:?}", role, broker_rewrite_map);
        let rewrite_map: anyhow::Result<BTreeMap<BrokerAddr, BrokerAddr>> = broker_rewrite_map
            .map_or(Ok(BTreeMap::new()), |addr_map| {
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

pub struct PrivateLinkConsumerContext {
    inner: BrokerAddrRewriter,
}

impl PrivateLinkConsumerContext {
    pub fn new(broker_rewrite_map: Option<HashMap<String, String>>) -> anyhow::Result<Self> {
        let inner = BrokerAddrRewriter::new(PrivateLinkContextRole::Consumer, broker_rewrite_map)?;
        Ok(Self { inner })
    }
}

impl ClientContext for PrivateLinkConsumerContext {
    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        self.inner.rewrite_broker_addr(addr)
    }
}

// required by the trait bound of BaseConsumer
impl ConsumerContext for PrivateLinkConsumerContext {}

pub struct PrivateLinkProducerContext {
    inner: BrokerAddrRewriter,
}

impl PrivateLinkProducerContext {
    pub fn new(broker_rewrite_map: Option<HashMap<String, String>>) -> anyhow::Result<Self> {
        let inner = BrokerAddrRewriter::new(PrivateLinkContextRole::Producer, broker_rewrite_map)?;
        Ok(Self { inner })
    }
}

impl ClientContext for PrivateLinkProducerContext {
    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        self.inner.rewrite_broker_addr(addr)
    }
}

impl ProducerContext for PrivateLinkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
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
) -> anyhow::Result<String> {
    with_properties
        .get(property)
        .map(|s| s.to_lowercase())
        .ok_or(anyhow!("Required property \"{property}\" is not provided"))
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

pub fn insert_privatelink_broker_rewrite_map(
    svc: &PrivateLinkService,
    properties: &mut BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let mut broker_rewrite_map = HashMap::new();

    let link_target_value = get_property_required(properties, PRIVATE_LINK_TARGETS_KEY)?;
    let servers = get_property_required(properties, kafka_props_broker_key(properties))?;
    let broker_addrs = servers.split(',').collect_vec();
    let link_targets: Vec<AwsPrivateLinkItem> =
        serde_json::from_str(link_target_value.as_str()).map_err(|e| anyhow!(e))?;
    if broker_addrs.len() != link_targets.len() {
        return Err(anyhow!(
            "The number of broker addrs {} does not match the number of private link targets {}",
            broker_addrs.len(),
            link_targets.len()
        ));
    }

    for (link, broker) in link_targets.iter().zip_eq_fast(broker_addrs.into_iter()) {
        if svc.dns_entries.is_empty() {
            return Err(anyhow!(
                "No available private link endpoints for Kafka broker {}",
                broker
            ));
        }
        // rewrite the broker address to the dns name w/o az
        // requires the NLB has enabled the cross-zone load balancing
        broker_rewrite_map.insert(
            broker.to_string(),
            format!("{}:{}", &svc.endpoint_dns_name, link.port),
        );
    }

    // save private link dns names into source properties, which
    // will be extracted into KafkaProperties
    let json = serde_json::to_string(&broker_rewrite_map).map_err(|e| anyhow!(e))?;
    properties.insert(BROKER_REWRITE_MAP_KEY.to_string(), json);
    Ok(())
}
