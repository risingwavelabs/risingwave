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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

pub use enumerator::*;
use nexmark::config::{NexmarkConfig, RateShape};
use nexmark::event::EventType;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
pub use split::*;

pub const NEXMARK_CONNECTOR: &str = "nexmark";

const fn identity_i32<const V: i32>() -> i32 {
    V
}

const fn identity_u64<const V: u64>() -> u64 {
    V
}

const fn none<T>() -> Option<T> {
    None
}

pub type NexmarkProperties = Box<NexmarkPropertiesInner>;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct NexmarkPropertiesInner {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "nexmark.split.num", default = "identity_i32::<1>")]
    pub split_num: i32,

    /// The total event count of Bid + Auction + Person
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "nexmark.event.num", default = "default_event_num")]
    pub event_num: u64,

    #[serde(rename = "nexmark.table.type", default = "none")]
    pub table_type: Option<EventType>,

    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "nexmark.max.chunk.size", default = "identity_u64::<1024>")]
    pub max_chunk_size: u64,

    #[serde_as(as = "DisplayFromStr")]
    /// The event time gap will be like the time gap in the generated data, default false
    #[serde(rename = "nexmark.use.real.time", default)]
    pub use_real_time: bool,

    #[serde_as(as = "DisplayFromStr")]
    /// Minimal gap between two events, default 100000, so that the default max throughput is 10000
    #[serde(
        rename = "nexmark.min.event.gap.in.ns",
        default = "identity_u64::<100_000>"
    )]
    pub min_event_gap_in_ns: u64,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.active.people", default = "none")]
    pub active_people: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.in.flight.auctions", default = "none")]
    pub in_flight_auctions: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.out.of.order.group.size", default = "none")]
    pub out_of_order_group_size: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.avg.person.byte.size", default = "none")]
    pub avg_person_byte_size: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.avg.auction.byte.size", default = "none")]
    pub avg_auction_byte_size: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.avg.bid.byte.size", default = "none")]
    pub avg_bid_byte_size: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.seller.ratio", default = "none")]
    pub hot_seller_ratio: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.auction.ratio", default = "none")]
    pub hot_auction_ratio: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.bidder.ratio", default = "none")]
    pub hot_bidder_ratio: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.channel.ratio", default = "none")]
    pub hot_channel_ratio: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.first.event.id", default = "none")]
    pub first_event_id: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.first.event.number", default = "none")]
    pub first_event_number: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.num.categories", default = "none")]
    pub num_categories: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.auction.id.lead", default = "none")]
    pub auction_id_lead: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.seller.ratio.2", default = "none")]
    pub hot_seller_ratio_2: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.auction.ratio.2", default = "none")]
    pub hot_auction_ratio_2: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.hot.bidder.ratio.2", default = "none")]
    pub hot_bidder_ratio_2: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.person.proportion", default = "none")]
    pub person_proportion: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.auction.proportion", default = "none")]
    pub auction_proportion: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.bid.proportion", default = "none")]
    pub bid_proportion: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.first.auction.id", default = "none")]
    pub first_auction_id: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.first.person.id", default = "none")]
    pub first_person_id: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.first.category.id", default = "none")]
    pub first_category_id: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.person.id.lead", default = "none")]
    pub person_id_lead: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.sine.approx.steps", default = "none")]
    pub sine_approx_steps: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.base.time", default = "none")]
    pub base_time: Option<u64>,

    #[serde(rename = "nexmark.us.states")]
    pub us_states: Option<String>,

    #[serde(rename = "nexmark.us.cities")]
    pub us_cities: Option<String>,

    #[serde(rename = "nexmark.first.names")]
    pub first_names: Option<String>,

    #[serde(rename = "nexmark.last.names")]
    pub last_names: Option<String>,

    #[serde(rename = "nexmark.rate.shape")]
    pub rate_shape: Option<RateShape>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.rate.period", default = "none")]
    pub rate_period: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.first.event.rate", default = "none")]
    pub first_event_rate: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.events.per.sec", default = "none")]
    pub events_per_sec: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.next.event.rate", default = "none")]
    pub next_event_rate: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.us.per.unit", default = "none")]
    pub us_per_unit: Option<usize>,

    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "nexmark.threads", default = "none")]
    pub threads: Option<usize>,
}

fn default_event_num() -> u64 {
    u64::MAX
}

impl Default for NexmarkPropertiesInner {
    fn default() -> Self {
        let v = serde_json::to_value(HashMap::<String, String>::new()).unwrap();
        NexmarkPropertiesInner::deserialize(v).unwrap()
    }
}

impl From<&NexmarkPropertiesInner> for NexmarkConfig {
    fn from(value: &NexmarkPropertiesInner) -> Self {
        // 2015-07-15 00:00:00
        pub const BASE_TIME: u64 = 1_436_918_400_000;

        let mut cfg = match value.table_type {
            // This is the old way
            Some(_) => NexmarkConfig {
                base_time: BASE_TIME,
                ..Default::default()
            },
            // By using default, it will choose the default proportion of three different events.
            None => NexmarkConfig::default(),
        };

        macro_rules! set {
            ($name:ident) => {
                set!($name, $name);
            };
            ($cfg_name:ident, $prop_name:ident) => {
                if let Some(v) = value.$prop_name {
                    cfg.$cfg_name = v;
                }
            };
            ($name:ident @ $map:ident) => {
                if let Some(v) = &value.$name {
                    cfg.$name = $map(v);
                }
            };
        }
        set!(active_people);
        set!(in_flight_auctions);
        set!(out_of_order_group_size);
        set!(avg_person_byte_size);
        set!(avg_auction_byte_size);
        set!(avg_bid_byte_size);
        set!(hot_seller_ratio);
        set!(hot_auction_ratio);
        set!(hot_bidder_ratio);
        set!(hot_channel_ratio);
        set!(first_event_id);
        set!(first_event_number);
        set!(base_time);
        set!(num_categories);
        set!(auction_id_lead);
        set!(person_proportion);
        set!(auction_proportion);
        set!(bid_proportion);
        set!(first_auction_id);
        set!(first_person_id);
        set!(first_category_id);
        set!(person_id_lead);
        set!(sine_approx_steps);
        set!(us_states @ split_str);
        set!(us_cities @ split_str);
        set!(first_names @ split_str);
        set!(last_names @ split_str);
        set!(num_event_generators, threads);
        set!(rate_shape);
        set!(rate_period);
        set!(first_rate, first_event_rate);
        set!(next_rate, first_event_rate);
        set!(us_per_unit);
        cfg
    }
}

fn split_str(string: &str) -> Vec<String> {
    string.split(',').map(String::from).collect()
}
