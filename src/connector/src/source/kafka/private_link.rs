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
use std::str::FromStr;

use itertools::Itertools;
use rdkafka::client::BrokerAddr;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::iter_util::ZipEqFast;

pub struct PrivateLinkConsumerContext {
    rewrite_map: BTreeMap<BrokerAddr, BrokerAddr>,
}

impl PrivateLinkConsumerContext {
    pub fn new(brokers: &str, private_links: &Option<String>) -> anyhow::Result<Self> {
        let mut rewrite_map = BTreeMap::new();
        if let Some(private_links) = private_links {
            let dns_names = private_links.split(',').collect_vec();
            let broker_adds = brokers.split(',').collect_vec();

            for (broker_addr, dns_name) in
                broker_adds.into_iter().zip_eq_fast(dns_names.into_iter())
            {
                let broker_addr = HostAddr::from_str(broker_addr)?;
                let dns_name = HostAddr::from_str(dns_name)?;
                let old_addr = BrokerAddr {
                    host: broker_addr.host,
                    port: broker_addr.port.to_string(),
                };
                let new_addr = BrokerAddr {
                    host: dns_name.host,
                    port: dns_name.port.to_string(),
                };
                rewrite_map.insert(old_addr, new_addr);
            }
        }
        tracing::info!("broker addr rewrite map {:?}", rewrite_map);
        Ok(Self { rewrite_map })
    }
}

impl ClientContext for PrivateLinkConsumerContext {
    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        match self.rewrite_map.get(&addr) {
            None => addr,
            Some(new_addr) => {
                tracing::debug!("broker addr {:?} rewrote to {:?}", addr, new_addr);
                new_addr.clone()
            }
        }
    }
}

// required by the trait bound of BaseConsumer
impl ConsumerContext for PrivateLinkConsumerContext {}
