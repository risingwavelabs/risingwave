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

//! Nexmark configurations.

use std::f64::consts::PI;
use std::ops::Deref;
use std::time::SystemTime;

use crate::event::EventType;
use crate::utils::get_base_url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RateShape {
    #[cfg_attr(feature = "serde", serde(rename = "square"))]
    Square,
    #[cfg_attr(feature = "serde", serde(rename = "sine"))]
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
    ///
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
    ///
    /// Event ids are unique over all generators, and are used as a seed to
    /// generate each event's data.
    pub first_event_id: usize,
    /// First event number.
    ///
    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp.
    pub first_event_number: usize,
    /// Time for first event (ms since epoch).
    pub base_time: u64,

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
    /// Number of event generators to use. Each generates events in its own
    /// timeline.
    pub num_event_generators: usize,
    pub rate_shape: RateShape,
    pub rate_period: usize,
    pub first_rate: usize,
    pub next_rate: usize,
    pub us_per_unit: usize,
}

/// Default configuration.
impl Default for NexmarkConfig {
    fn default() -> Self {
        Self {
            active_people: 1000,
            in_flight_auctions: 100,
            out_of_order_group_size: 1,
            avg_person_byte_size: 200,
            avg_auction_byte_size: 500,
            avg_bid_byte_size: 100,
            hot_seller_ratio: 4,
            hot_auction_ratio: 2,
            hot_bidder_ratio: 4,
            hot_channel_ratio: 2,
            first_event_id: 0,
            first_event_number: 0,
            num_categories: 5,
            auction_id_lead: 10,
            hot_seller_ratio_2: 100,
            hot_auction_ratio_2: 100,
            hot_bidder_ratio_2: 100,
            person_proportion: 1,
            auction_proportion: 3,
            bid_proportion: 46,
            first_auction_id: 1000,
            first_person_id: 1000,
            first_category_id: 10,
            person_id_lead: 10,
            sine_approx_steps: 10,
            base_time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            us_states: split_str("az,ca,id,or,wa,wy"),
            us_cities: split_str("phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne"),
            hot_channels: split_str("Google,Facebook,Baidu,Apple"),
            hot_urls: (0..4).map(get_base_url).collect(),
            first_names: split_str("peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter"),
            last_names: split_str("shultz,abrams,spencer,white,bartels,walton,smith,jones,noris"),
            num_event_generators: 1,
            rate_shape: RateShape::Sine,
            rate_period: 600,
            first_rate: 10_000,
            next_rate: 10_000,
            us_per_unit: 1_000_000,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GeneratorConfig {
    pub config: NexmarkConfig,

    // The following are derived from config thus should not be changed.
    /// The proportion denominator.
    pub proportion_denominator: usize,
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
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        NexmarkConfig::default().into()
    }
}

impl Deref for GeneratorConfig {
    type Target = NexmarkConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl From<NexmarkConfig> for GeneratorConfig {
    fn from(cfg: NexmarkConfig) -> Self {
        let proportion_denominator =
            cfg.person_proportion + cfg.auction_proportion + cfg.bid_proportion;
        let generators = cfg.num_event_generators as f32;

        // Calculate inter event delays array.
        let mut inter_event_delays = Vec::new();
        let rate_to_period = |r| cfg.us_per_unit as f32 / r as f32;
        if cfg.first_rate == cfg.next_rate {
            inter_event_delays.push(rate_to_period(cfg.first_rate) * generators);
        } else {
            match cfg.rate_shape {
                RateShape::Square => {
                    inter_event_delays.push(rate_to_period(cfg.first_rate) * generators);
                    inter_event_delays.push(rate_to_period(cfg.next_rate) * generators);
                }
                RateShape::Sine => {
                    let mid = (cfg.first_rate + cfg.next_rate) as f64 / 2.0;
                    let amp = (cfg.first_rate - cfg.next_rate) as f64 / 2.0;
                    for i in 0..cfg.sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / cfg.sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays.push(rate_to_period(rate.round() as usize) * generators);
                    }
                }
            }
        }
        // Calculate events per epoch and epoch period.
        let n = if cfg.rate_shape == RateShape::Square {
            2
        } else {
            cfg.sine_approx_steps
        };
        let step_length = (cfg.rate_period + n - 1) / n;
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

        Self {
            config: cfg,
            proportion_denominator,
            step_length,
            events_per_epoch,
            epoch_period,
            inter_event_delays,
        }
    }
}

impl GeneratorConfig {
    /// Returns the timestamp of event.
    pub fn event_timestamp(&self, event_number: usize) -> u64 {
        if self.inter_event_delays.len() == 1 {
            return self.base_time
                + ((event_number as f32 * self.inter_event_delays[0]) / 1000.0).round() as u64;
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
                        .round() as u64;
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

    /// Returns the event type.
    pub fn event_type(&self, event_number: usize) -> EventType {
        let rem = event_number % self.proportion_denominator;
        if rem < self.person_proportion {
            EventType::Person
        } else if rem < self.person_proportion + self.auction_proportion {
            EventType::Auction
        } else {
            EventType::Bid
        }
    }
}

fn split_str(string: &str) -> Vec<String> {
    string.split(',').map(String::from).collect()
}
