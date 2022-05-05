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

use std::f64::consts::PI;

use super::NEXMARK_BASE_TIME;
use crate::Properties;

const NEXMARK_CONFIG_ACTIVE_PEOPLE: &str = "nexmark.active.people";
const NEXMARK_CONFIG_IN_FLIGHT_AUCTIONS: &str = "nexmark.in.flight.auctions";
const NEXMARK_CONFIG_OUT_OF_ORDER_GROUP_SIZE: &str = "nexmark.out.of.order.group.size";
const NEXMARK_CONFIG_HOT_SELLER_RATIO: &str = "nexmark.hot.seller.ratio";
const NEXMARK_CONFIG_HOT_AUCTION_RATIO: &str = "nexmark.hot.auction.ratio";
const NEXMARK_CONFIG_HOT_BIDDER_RATIO: &str = "nexmark.hot.bidder.ratio";
const NEXMARK_CONFIG_FIRST_EVENT_ID: &str = "nexmark.first.event.id";
const NEXMARK_CONFIG_FIRST_EVENT_NUMBER: &str = "nexmark.first.event.number";
const NEXMARK_CONFIG_NUM_CATEGORIES: &str = "nexmark.num.categories";
const NEXMARK_CONFIG_AUCTION_ID_LEAD: &str = "nexmark.auction.id.lead";
const NEXMARK_CONFIG_HOT_SELLER_RATIO_2: &str = "nexmark.hot.seller.ratio.2";
const NEXMARK_CONFIG_HOT_AUCTION_RATIO_2: &str = "nexmark.hot.auction.ratio.2";
const NEXMARK_CONFIG_HOT_BIDDER_RATIO_2: &str = "nexmark.hot.bidder.ratio.2";
const NEXMARK_CONFIG_PERSON_PROPORTION: &str = "nexmark.person.proportion";
const NEXMARK_CONFIG_AUCTION_PROPORTION: &str = "nexmark.auction.proportion";
const NEXMARK_CONFIG_BID_PROPORTION: &str = "nexmark.bid.proportion";
const NEXMARK_CONFIG_FIRST_AUCTION_ID: &str = "nexmark.first.auction.id";
const NEXMARK_CONFIG_FIRST_PERSON_ID: &str = "nexmark.first.person.id";
const NEXMARK_CONFIG_FIRST_CATEGORY_ID: &str = "nexmark.first.category.id";
const NEXMARK_CONFIG_PERSON_ID_LEAD: &str = "nexmark.person.id.lead";
const NEXMARK_CONFIG_SINE_APPROX_STEPS: &str = "nexmark.sine.approx.steps";
const NEXMARK_CONFIG_BASE_TIME: &str = "nexmark.base.time";
const NEXMARK_CONFIG_US_STATES: &str = "nexmark.us.states";
const NEXMARK_CONFIG_US_CITIES: &str = "nexmark.us.cities";
const NEXMARK_CONFIG_FIRST_NAMES: &str = "nexmark.first.names";
const NEXMARK_CONFIG_LAST_NAMES: &str = "nexmark.last.names";
const NEXMARK_CONFIG_RATE_SHAPE: &str = "nexmark.rate.shape";
const NEXMARK_CONFIG_RATE_PERIOD: &str = "nexmark.rate.period";
const NEXMARK_CONFIG_FIRST_EVENT_RATE: &str = "nexmark.first.event.rate";
const NEXMARK_CONFIG_EVENTS_PER_SECOND: &str = "nexmark.events.per.second";
const NEXMARK_CONFIG_NEXT_EVENT_RATE: &str = "nexmark.next.event.rate";
const NEXMARK_CONFIG_US_PER_UNIT: &str = "nexmark.us.per.unit";
const NEXMARK_CONFIG_THREADS: &str = "nexmark.threads";

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
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio: usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio: usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio: usize,
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
    /// The collection of first names.
    pub first_names: Vec<String>,
    /// The collection of last names.
    pub last_names: Vec<String>,
    /// Number of event generators to use. Each generates events in its own
    /// timeline.
    pub num_event_generators: usize,
}

impl NexmarkConfig {
    pub fn from(properties: &Properties) -> anyhow::Result<Self> {
        let active_people = properties.get_as_or(NEXMARK_CONFIG_ACTIVE_PEOPLE, 1000)?;
        let in_flight_auctions = properties.get_as_or(NEXMARK_CONFIG_IN_FLIGHT_AUCTIONS, 100)?;
        let out_of_order_group_size =
            properties.get_as_or(NEXMARK_CONFIG_OUT_OF_ORDER_GROUP_SIZE, 1)?;
        let hot_seller_ratio = properties.get_as_or(NEXMARK_CONFIG_HOT_SELLER_RATIO, 4)?;
        let hot_auction_ratio = properties.get_as_or(NEXMARK_CONFIG_HOT_AUCTION_RATIO, 2)?;
        let hot_bidder_ratio = properties.get_as_or(NEXMARK_CONFIG_HOT_BIDDER_RATIO, 4)?;
        let first_event_id = properties.get_as_or(NEXMARK_CONFIG_FIRST_EVENT_ID, 0)?;
        let first_event_number = properties.get_as_or(NEXMARK_CONFIG_FIRST_EVENT_NUMBER, 0)?;
        let num_categories = properties.get_as_or(NEXMARK_CONFIG_NUM_CATEGORIES, 5)?;
        let auction_id_lead = properties.get_as_or(NEXMARK_CONFIG_AUCTION_ID_LEAD, 10)?;
        let hot_seller_ratio_2 = properties.get_as_or(NEXMARK_CONFIG_HOT_SELLER_RATIO_2, 100)?;
        let hot_auction_ratio_2 = properties.get_as_or(NEXMARK_CONFIG_HOT_AUCTION_RATIO_2, 100)?;
        let hot_bidder_ratio_2 = properties.get_as_or(NEXMARK_CONFIG_HOT_BIDDER_RATIO_2, 100)?;
        let person_proportion = properties.get_as_or(NEXMARK_CONFIG_PERSON_PROPORTION, 1)?;
        let auction_proportion = properties.get_as_or(NEXMARK_CONFIG_AUCTION_PROPORTION, 3)?;
        let bid_proportion = properties.get_as_or(NEXMARK_CONFIG_BID_PROPORTION, 46)?;
        let proportion_denominator = person_proportion + auction_proportion + bid_proportion;
        let first_auction_id = properties.get_as_or(NEXMARK_CONFIG_FIRST_AUCTION_ID, 1000)?;
        let first_person_id = properties.get_as_or(NEXMARK_CONFIG_FIRST_PERSON_ID, 1000)?;
        let first_category_id = properties.get_as_or(NEXMARK_CONFIG_FIRST_CATEGORY_ID, 10)?;
        let person_id_lead = properties.get_as_or(NEXMARK_CONFIG_PERSON_ID_LEAD, 10)?;
        let sine_approx_steps = properties.get_as_or(NEXMARK_CONFIG_SINE_APPROX_STEPS, 10)?;
        let base_time = properties.get_as_or(NEXMARK_CONFIG_BASE_TIME, NEXMARK_BASE_TIME)?;
        let us_states =
            split_string_arg(properties.get_or(NEXMARK_CONFIG_US_STATES, "az,ca,id,or,wa,wy"));
        let us_cities = split_string_arg(properties.get_or(
            NEXMARK_CONFIG_US_CITIES,
            "phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne",
        ));
        let first_names = split_string_arg(properties.get_or(
            NEXMARK_CONFIG_FIRST_NAMES,
            "peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter",
        ));
        let last_names = split_string_arg(properties.get_or(
            NEXMARK_CONFIG_LAST_NAMES,
            "shultz,abrams,spencer,white,bartels,walton,smith,jones,noris",
        ));
        let rate_shape = if properties.get_or(NEXMARK_CONFIG_RATE_SHAPE, "sine") == "sine" {
            RateShape::Sine
        } else {
            RateShape::Square
        };
        let rate_period = properties.get_as_or(NEXMARK_CONFIG_RATE_PERIOD, 600)?;
        let first_rate = properties.get_as_or(
            NEXMARK_CONFIG_FIRST_EVENT_RATE,
            properties.get_as_or(NEXMARK_CONFIG_EVENTS_PER_SECOND, 10_000)?,
        )?;
        let next_rate = properties.get_as_or(NEXMARK_CONFIG_NEXT_EVENT_RATE, first_rate)?;
        let us_per_unit = properties.get_as_or(NEXMARK_CONFIG_US_PER_UNIT, 1_000_000)?; // Rate is in μs
        let generators = properties.get_as_or(NEXMARK_CONFIG_THREADS, 1)? as f32;

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

        Ok(NexmarkConfig {
            active_people,
            in_flight_auctions,
            out_of_order_group_size,
            hot_seller_ratio,
            hot_auction_ratio,
            hot_bidder_ratio,
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
            first_names,
            last_names,
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

    use maplit::hashmap;

    use super::*;

    #[test]
    fn test_config() -> Result<()> {
        let properties = Properties::new(hashmap! {});
        let res = NexmarkConfig::from(&properties);
        assert!(res.is_ok());
        let config = res.unwrap();
        println!("config {:?}", config);

        assert_eq!(config.active_people, 1000);
        Ok(())
    }
}
