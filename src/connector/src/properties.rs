// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub enum ConnectorProperties {
    Kafka(KafkaProperties),
    Pulsar(PulsarProperties),
    Kinesis(KinesisProperties),
    Nexmark(Box<NexmarkProperties>),
    S3(S3Properties),
}

impl ConnectorProperties {
    pub fn new(mut props: HashMap<String, String>) -> Result<Self> {
        const UPSTREAM_SOURCE_KEY: &str = "connector";

        if let Some(connector) = props.get(UPSTREAM_SOURCE_KEY).cloned() {
            props.remove(UPSTREAM_SOURCE_KEY);
            let json_value = serde_json::to_value(props).unwrap();
            match connector.as_str() {
                "kafka" => Ok(Self::Kafka(
                    KafkaProperties::deserialize(json_value).unwrap(),
                )),
                "pulsar" => Ok(Self::Pulsar(
                    PulsarProperties::deserialize(json_value).unwrap(),
                )),
                "kinesis" => Ok(Self::Kinesis(
                    KinesisProperties::deserialize(json_value).unwrap(),
                )),
                "nexmark" => Ok(Self::Nexmark(Box::new(
                    NexmarkProperties::deserialize(json_value).unwrap(),
                ))),
                _ => Err(RwError::from(ProtocolError(format!(
                    "connector '{}' is not supported",
                    connector,
                )))),
            }
        } else {
            Err(RwError::from(ProtocolError(
                "Must specify 'connector' in WITH clause".to_string(),
            )))
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct KafkaProperties {
    #[serde(rename = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "kafka.topic")]
    pub topic: String,

    #[serde(rename = "kafka.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "kafka.time.offset")]
    pub time_offset: Option<String>,

    #[serde(rename = "kafka.consumer.group")]
    pub consumer_group: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarProperties {
    #[serde(rename = "pulsar.topic")]
    pub topic: String,

    #[serde(rename = "pulsar.admin.url")]
    pub admin_url: String,

    #[serde(rename = "pulsar.service.url")]
    pub service_url: String,

    #[serde(rename = "pulsar.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "pulsar.time.offset")]
    pub time_offset: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NexmarkProperties {
    #[serde(rename = "nexmark.split.num")]
    pub split_num: Option<i32>,

    /// The total event count of Bid + Auction + Person
    #[serde(rename = "nexmark.event.num", default = "default_event_num")]
    pub event_num: i64,

    #[serde(rename = "nexmark.table.type", default)]
    pub table_type: String,

    #[serde(rename = "nexmark.max.chunk.size", default = "default_max_chunk_size")]
    pub max_chunk_size: u64,

    /// The event time gap will be like the time gap in the generated data, default false
    #[serde(rename = "nexmark.use.real.time", default)]
    pub use_real_time: bool,

    /// Minimal gap between two events, default 100000, so that the default max throughput is 10000
    #[serde(
        rename = "nexmark.min.event.gap.in.ns",
        default = "default_min_event_gap_in_ns"
    )]
    pub min_event_gap_in_ns: u64,

    #[serde(rename = "nexmark.active.people")]
    pub active_people: Option<usize>,
    #[serde(rename = "nexmark.in.flight.auctions")]
    pub in_flight_auctions: Option<usize>,
    #[serde(rename = "nexmark.out.of.order.group.size")]
    pub out_of_order_group_size: Option<usize>,
    #[serde(rename = "nexmark.hot.seller.ratio")]
    pub hot_seller_ratio: Option<usize>,
    #[serde(rename = "nexmark.hot.auction.ratio")]
    pub hot_auction_ratio: Option<usize>,
    #[serde(rename = "nexmark.hot.bidder.ratio")]
    pub hot_bidder_ratio: Option<usize>,
    #[serde(rename = "nexmark.first.event.id")]
    pub hot_first_event_id: Option<usize>,
    #[serde(rename = "nexmark.first.event.number")]
    pub first_event_number: Option<usize>,
    #[serde(rename = "nexmark.num.categories")]
    pub num_categories: Option<usize>,
    #[serde(rename = "nexmark.auction.id.lead")]
    pub auction_id_lead: Option<usize>,
    #[serde(rename = "nexmark.hot.seller.ratio.2")]
    pub hot_seller_ratio_2: Option<usize>,
    #[serde(rename = "nexmark.hot.auction.ratio.2")]
    pub hot_auction_ratio_2: Option<usize>,
    #[serde(rename = "nexmark.hot.bidder.ratio.2")]
    pub hot_bidder_ratio_2: Option<usize>,
    #[serde(rename = "nexmark.person.proportion")]
    pub person_proportion: Option<usize>,
    #[serde(rename = "nexmark.auction.proportion")]
    pub auction_proportion: Option<usize>,
    #[serde(rename = "nexmark.bid.proportion")]
    pub bid_proportion: Option<usize>,
    #[serde(rename = "nexmark.first.auction.id")]
    pub first_auction_id: Option<usize>,
    #[serde(rename = "nexmark.first.person.id")]
    pub first_person_id: Option<usize>,
    #[serde(rename = "nexmark.first.category.id")]
    pub first_category_id: Option<usize>,
    #[serde(rename = "nexmark.person.id.lead")]
    pub person_id_lead: Option<usize>,
    #[serde(rename = "nexmark.sine.approx.steps")]
    pub sine_approx_steps: Option<usize>,
    #[serde(rename = "nexmark.base.time")]
    pub base_time: Option<usize>,
    #[serde(rename = "nexmark.us.states")]
    pub us_states: Option<String>,
    #[serde(rename = "nexmark.us.cities")]
    pub us_cities: Option<String>,
    #[serde(rename = "nexmark.first.names")]
    pub first_names: Option<String>,
    #[serde(rename = "nexmark.last.names")]
    pub last_names: Option<String>,
    #[serde(rename = "nexmark.rate.shape")]
    pub rate_shape: Option<String>,
    #[serde(rename = "nexmark.rate.period")]
    pub rate_period: Option<usize>,
    #[serde(rename = "nexmark.first.event.rate")]
    pub first_event_rate: Option<usize>,
    #[serde(rename = "nexmark.events.per.sec")]
    pub events_per_sec: Option<usize>,
    #[serde(rename = "nexmark.next.event.rate")]
    pub next_event_rate: Option<usize>,
    #[serde(rename = "nexmark.us.per.unit")]
    pub us_per_unit: Option<usize>,
    #[serde(rename = "nexmark.threads")]
    pub threads: Option<usize>,
}

fn default_event_num() -> i64 {
    -1
}

fn default_min_event_gap_in_ns() -> u64 {
    100000
}

fn default_max_chunk_size() -> u64 {
    1024
}

impl Default for NexmarkProperties {
    fn default() -> Self {
        let v = serde_json::to_value(HashMap::<String, String>::new()).unwrap();
        NexmarkProperties::deserialize(v).unwrap()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct KinesisProperties {
    #[serde(rename = "kinesis.stream.name")]
    pub stream_name: String,
    #[serde(rename = "kinesis.stream.region")]
    pub stream_region: String,
    #[serde(rename = "kinesis.endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "kinesis.credentials.access")]
    pub credentials_access_key: Option<String>,
    #[serde(rename = "kinesis.credentials.secret")]
    pub credentials_secret_access_key: Option<String>,
    #[serde(rename = "kinesis.credentials.session_token")]
    pub session_token: Option<String>,
    #[serde(rename = "kinesis.assumerole.arn")]
    pub assume_role_arn: Option<String>,
    #[serde(rename = "kinesis.assumerole.external_id")]
    pub assume_role_externeal_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct S3Properties {
    #[serde(rename = "s3.region_name")]
    pub region_name: String,
    #[serde(rename = "s3.bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "sqs_queue_name")]
    pub sqs_queue_name: String,
    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,
    #[serde(rename = "s3.credentials.access", default)]
    pub access: String,
    #[serde(rename = "s3.credentials.secret", default)]
    pub secret: String,
}
