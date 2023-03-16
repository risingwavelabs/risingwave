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

use rdkafka::client::BrokerAddr;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
use risingwave_common::util::addr::HostAddr;

pub struct PrivateLinkConsumerContext {
    rewrite_map: BTreeMap<BrokerAddr, BrokerAddr>,
}

impl PrivateLinkConsumerContext {
    pub fn new(broker_rewrite_map: Option<HashMap<String, String>>) -> anyhow::Result<Self> {
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
