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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::f64::consts::PI;

use crate::source::nexmark::utils::{build_channel_url_map, get_base_url};
use crate::source::nexmark::{NexmarkProperties, NEXMARK_BASE_TIME};

pub const CHANNEL_NUMBER: usize = 10_000;

#[derive(PartialEq)]
enum RateShape {
    Square,
    Sine,
}

/// Nexmark Configuration
#[derive(Clone, Debug)]
pub struct NexmarkConfig {
    /// Maximum number of people to consider as active for placing auctions or
    /// bids.
    pub active_people: usize,
    /// Average number of auction which should be inflight at any time, per
    /// generator.
    pub in_flight_auctions: usize,
    /// Number of events in out-of-order groups.
    /// 1 implies no out-of-order events. 1000 implies every 1000 events per
    /// generator are emitted in pseudo-random order.
    pub out_of_order_group_size: usize,
    /// Average idealized size of a 'new person' event, in bytes.
    pub avg_person_byte_size: usize,
    /// Average idealized size of a 'new auction' event, in bytes.
    pub avg_auction_byte_size: usize,
    /// Average idealized size of a 'bid' event, in bytes.
    pub avg_bid_byte_size: usize,
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio: usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio: usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio: usize,
    /// Ratio of bids for 'hot' channels compared to all other channels.
    pub hot_channel_ratio: usize,
    /// Event id of first event to be generated.
    /// Event ids are unique over all generators, and are used as a seed to
    /// generate each event's data.
    pub first_event_id: usize,
    /// First event number.
    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp.
    pub first_event_number: usize,
    /// Time for first event (ms since epoch).
    pub base_time: usize,
    /// Delay before changing the current inter-event delay.
    pub step_length: usize,
    /// Number of events per epoch.
    /// Derived from above. (Ie number of events to run through cycle for all
    /// interEventDelayUs entries).
    pub events_per_epoch: usize,
    /// True period of epoch in milliseconds. Derived from above. (Ie time to
    /// run through cycle for all interEventDelayUs entries).
    pub epoch_period: f32,
    /// Delay between events, in microseconds.
    /// If the array has more than one entry then the rate is changed every
    /// step_length, and wraps around.
    pub inter_event_delays: Vec<f32>,
    // Originally constants
    /// Auction categories.
    pub num_categories: usize,
    /// Use to calculate the next auction id.
    pub auction_id_lead: usize,
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio_2: usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio_2: usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio_2: usize,
    /// Person Proportion.
    pub person_proportion: usize,
    /// Auction Proportion.
    pub auction_proportion: usize,
    /// Bid Proportion.
    pub bid_proportion: usize,
    /// Proportion Denominator.
    pub proportion_denominator: usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_auction_id: usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_person_id: usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_category_id: usize,
    /// Use to calculate the next id.
    pub person_id_lead: usize,
    /// Use to calculate inter_event_delays for rate-shape sine.
    pub sine_approx_steps: usize,
    /// The collection of U.S. statees
    pub us_states: Vec<String>,
    /// The collection of U.S. cities.
    pub us_cities: Vec<String>,
    /// The collection of hot_channels.
    pub hot_channels: Vec<String>,
    /// The collection of hot urls.
    pub hot_urls: Vec<String>,
    /// The collection of first names.
    pub first_names: Vec<String>,
    /// The collection of last names.
    pub last_names: Vec<String>,
    /// The collection of channels and urls
    pub channel_url_map: HashMap<usize, (String, String)>,
    /// Number of event generators to use. Each generates events in its own
    /// timeline.
    pub num_event_generators: usize,
}

impl NexmarkConfig {
    pub fn from(properties: NexmarkProperties) -> anyhow::Result<Self> {
        let active_people = properties.active_people.unwrap_or(1000);
        let in_flight_auctions = properties.in_flight_auctions.unwrap_or(100);
        let out_of_order_group_size = properties.out_of_order_group_size.unwrap_or(1);
        let avg_person_byte_size = properties.avg_person_byte_size.unwrap_or(200);
        let avg_auction_byte_size = properties.avg_auction_byte_size.unwrap_or(500);
        let avg_bid_byte_size = properties.avg_bid_byte_size.unwrap_or(100);
        let hot_seller_ratio = properties.hot_seller_ratio.unwrap_or(4);
        let hot_auction_ratio = properties.hot_auction_ratio.unwrap_or(2);
        let hot_bidder_ratio = properties.hot_bidder_ratio.unwrap_or(4);
        let hot_channel_ratio = properties.hot_channel_ratio.unwrap_or(2);
        let first_event_id = properties.hot_first_event_id.unwrap_or(0);
        let first_event_number = properties.first_event_number.unwrap_or(0);
        let num_categories = properties.num_categories.unwrap_or(5);
        let auction_id_lead = properties.auction_id_lead.unwrap_or(10);
        let hot_seller_ratio_2 = properties.hot_seller_ratio_2.unwrap_or(100);
        let hot_auction_ratio_2 = properties.hot_auction_ratio_2.unwrap_or(100);
        let hot_bidder_ratio_2 = properties.hot_bidder_ratio_2.unwrap_or(100);
        let person_proportion = properties.person_proportion.unwrap_or(1);
        let auction_proportion = properties.auction_proportion.unwrap_or(3);
        let bid_proportion = properties.bid_proportion.unwrap_or(46);
        let proportion_denominator = person_proportion + auction_proportion + bid_proportion;
        let first_auction_id = properties.first_auction_id.unwrap_or(1000);
        let first_person_id = properties.first_person_id.unwrap_or(1000);
        let first_category_id = properties.first_category_id.unwrap_or(10);
        let person_id_lead = properties.person_id_lead.unwrap_or(10);
        let sine_approx_steps = properties.sine_approx_steps.unwrap_or(10);
        let base_time = properties.base_time.unwrap_or(NEXMARK_BASE_TIME);
        let us_states = split_string_arg(
            properties
                .us_states
                .unwrap_or_else(|| "az,ca,id,or,wa,wy".to_string()),
        );
        let us_cities = split_string_arg(properties.us_cities.unwrap_or_else(|| {
            "phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne"
                .to_string()
        }));
        let first_names = split_string_arg(properties.first_names.unwrap_or_else(|| {
            "peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter".to_string()
        }));
        let last_names = split_string_arg(properties.last_names.unwrap_or_else(|| {
            "shultz,abrams,spencer,white,bartels,walton,smith,jones,noris".to_string()
        }));
        let hot_channels = split_string_arg("Google,Facebook,Baidu,Apple".to_string());
        let hot_urls = (0..4).map(get_base_url).collect();
        let rate_shape = if properties.rate_shape.unwrap_or_else(|| "sine".to_string()) == "sine" {
            RateShape::Sine
        } else {
            RateShape::Square
        };
        let rate_period = properties.rate_period.unwrap_or(600);
        let first_rate = properties.first_event_rate.unwrap_or(10_000);
        let next_rate = properties.next_event_rate.unwrap_or(first_rate);
        let us_per_unit = properties.us_per_unit.unwrap_or(1_000_000); // Rate is in μs
        let generators = properties.threads.unwrap_or(1) as f32;

        // Calculate inter event delays array.
        let mut inter_event_delays = Vec::new();
        let rate_to_period = |r| (us_per_unit) as f32 / r as f32;
        if first_rate == next_rate {
            inter_event_delays.push(rate_to_period(first_rate) * generators);
        } else {
            match rate_shape {
                RateShape::Square => {
                    inter_event_delays.push(rate_to_period(first_rate) * generators);
                    inter_event_delays.push(rate_to_period(next_rate) * generators);
                }
                RateShape::Sine => {
                    let mid = (first_rate + next_rate) as f64 / 2.0;
                    let amp = (first_rate - next_rate) as f64 / 2.0;
                    for i in 0..sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays.push(rate_to_period(rate.round() as usize) * generators);
                    }
                }
            }
        }
        // Calculate events per epoch and epoch period.
        let n = if rate_shape == RateShape::Square {
            2
        } else {
            sine_approx_steps
        };
        let step_length = (rate_period + n - 1) / n;
        let mut events_per_epoch = 0;
        let mut epoch_period = 0.0;
        if inter_event_delays.len() > 1 {
            for inter_event_delay in &inter_event_delays {
                let num_events_for_this_cycle =
                    (step_length * 1_000_000) as f32 / inter_event_delay;
                events_per_epoch += num_events_for_this_cycle.round() as usize;
                epoch_period += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
            }
        }

        let channel_url_map = build_channel_url_map(CHANNEL_NUMBER);

        Ok(NexmarkConfig {
            active_people,
            in_flight_auctions,
            out_of_order_group_size,
            avg_person_byte_size,
            avg_auction_byte_size,
            avg_bid_byte_size,
            hot_seller_ratio,
            hot_auction_ratio,
            hot_bidder_ratio,
            hot_channel_ratio,
            first_event_id,
            first_event_number,
            base_time,
            step_length,
            events_per_epoch,
            epoch_period,
            inter_event_delays,
            // Originally constants
            num_categories,
            auction_id_lead,
            hot_seller_ratio_2,
            hot_auction_ratio_2,
            hot_bidder_ratio_2,
            person_proportion,
            auction_proportion,
            bid_proportion,
            proportion_denominator,
            first_auction_id,
            first_person_id,
            first_category_id,
            person_id_lead,
            sine_approx_steps,
            us_states,
            us_cities,
            hot_channels,
            hot_urls,
            first_names,
            last_names,
            channel_url_map,
            num_event_generators: generators as usize,
        })
    }

    /// Returns a new event timestamp.
    pub fn event_timestamp(&self, event_number: usize) -> usize {
        if self.inter_event_delays.len() == 1 {
            return self.base_time
                + ((event_number as f32 * self.inter_event_delays[0]) / 1000.0).round() as usize;
        }

        let epoch = event_number / self.events_per_epoch;
        let mut event_i = event_number % self.events_per_epoch;
        let mut offset_in_epoch = 0.0;
        for inter_event_delay in &self.inter_event_delays {
            let num_events_for_this_cycle =
                (self.step_length * 1_000_000) as f32 / inter_event_delay;
            if self.out_of_order_group_size < num_events_for_this_cycle.round() as usize {
                let offset_in_cycle = event_i as f32 * inter_event_delay;
                return self.base_time
                    + (epoch as f32 * self.epoch_period
                        + offset_in_epoch
                        + offset_in_cycle / 1000.0)
                        .round() as usize;
            }
            event_i -= num_events_for_this_cycle.round() as usize;
            offset_in_epoch += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
        }
        0
    }

    /// Returns the next adjusted event.
    pub fn next_adjusted_event(&self, events_so_far: usize) -> usize {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953) % n
    }
}

fn split_string_arg(string: String) -> Vec<String> {
    string.split(',').map(String::from).collect::<Vec<String>>()
}

#[cfg(test)]
mod tests {
    use std::io::Result;

    use super::*;

    #[test]
    fn test_config() -> Result<()> {
        let properties = NexmarkProperties::default();
        let res = NexmarkConfig::from(properties);
        assert!(res.is_ok());
        let config = res.unwrap();
        println!("config {:?}", config);

        assert_eq!(config.active_people, 1000);
        Ok(())
    }
}
