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

use std::cmp::{max, min};

use chrono::NaiveDateTime;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::source::nexmark::config::NexmarkConfig;

const MIN_STRING_LENGTH: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventType {
    Person,
    Auction,
    Bid,
}

trait NexmarkRng {
    fn gen_string(&mut self, max: usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NexmarkRng for SmallRng {
    fn gen_string(&mut self, max: usize) -> String {
        let len = self.gen_range(MIN_STRING_LENGTH..max);
        String::from(
            (0..len)
                .map(|_| {
                    if self.gen_range(0..13) == 0 {
                        String::from(" ")
                    } else {
                        ::std::char::from_u32('a' as u32 + self.gen_range(0..26))
                            .unwrap()
                            .to_string()
                    }
                })
                .collect::<Vec<String>>()
                .join("")
                .trim(),
        )
    }

    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf((*self).gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

type Id = usize;

/// The `Nexmark` Event, including `Person`, `Auction`, and `Bid`.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    /// The Person event.
    Person(Person),
    /// The Auction event.
    Auction(Auction),
    /// The Bid event.
    Bid(Bid),
}

impl Event {
    /// Creates a new event randomly.
    pub fn new(
        events_so_far: usize,
        nex: &NexmarkConfig,
        wall_clock_base_time: usize,
    ) -> (Event, usize) {
        let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
        let timestamp = nex.event_timestamp(nex.next_adjusted_event(events_so_far));
        let new_wall_clock_base_time = timestamp - nex.base_time + wall_clock_base_time;
        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);
        let mut rng = SmallRng::seed_from_u64(id as u64);
        if rem < nex.person_proportion {
            (
                Event::Person(Person::new(id, timestamp, &mut rng, nex)),
                new_wall_clock_base_time,
            )
        } else if rem < nex.person_proportion + nex.auction_proportion {
            (
                Event::Auction(Auction::new(events_so_far, id, timestamp, &mut rng, nex)),
                new_wall_clock_base_time,
            )
        } else {
            (
                Event::Bid(Bid::new(id, timestamp, &mut rng, nex)),
                new_wall_clock_base_time,
            )
        }
    }

    pub fn to_json(&self) -> String {
        match self {
            Event::Person(p) => serde_json::to_string(p).unwrap(),
            Event::Auction(a) => serde_json::to_string(a).unwrap(),
            Event::Bid(b) => serde_json::to_string(b).unwrap(),
        }
    }

    pub fn event_type(&self) -> EventType {
        match self {
            Event::Person(_) => EventType::Person,
            Event::Auction(_) => EventType::Auction,
            Event::Bid(_) => EventType::Bid,
        }
    }
}

/// Person represents a person submitting an item for auction and/or making a
/// bid on an auction.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Person {
    /// A person-unique integer ID.
    pub p_id: Id,
    /// A string for the person’s full name.
    pub name: String,
    /// The person’s email address as a string.
    pub email_address: String,
    /// The credit card number as a 19-letter string.
    pub credit_card: String,
    /// One of several US city names as a string.
    pub city: String,
    /// One of several US states as a two-letter string.
    pub state: String,
    /// A millisecond timestamp for the event origin.
    pub p_date_time: String,
}

impl Person {
    /// Creates a new `Person` event.
    fn new(id: usize, time: usize, rng: &mut SmallRng, nex: &NexmarkConfig) -> Self {
        Self {
            p_id: Self::last_id(id, nex) + nex.first_person_id,
            name: format!(
                "{} {}",
                nex.first_names.choose(rng).unwrap(),
                nex.last_names.choose(rng).unwrap(),
            ),
            email_address: format!("{}@{}.com", rng.gen_string(7), rng.gen_string(5)),
            credit_card: (0..4)
                .map(|_| format!("{:04}", rng.gen_range(0..10000)))
                .collect::<Vec<String>>()
                .join(" "),
            city: nex.us_cities.choose(rng).unwrap().clone(),
            state: nex.us_states.choose(rng).unwrap().clone(),
            p_date_time: milli_ts_to_timestamp_string(time),
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NexmarkConfig) -> Id {
        let people = Self::last_id(id, nex) + 1;
        let active = min(people, nex.active_people);
        people - active + rng.gen_range(0..active + nex.person_id_lead)
    }

    fn last_id(id: usize, nex: &NexmarkConfig) -> Id {
        let epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if nex.person_proportion <= offset {
            offset = nex.person_proportion - 1;
        }
        epoch * nex.person_proportion + offset
    }
}

/// Auction represents an item under auction.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Auction {
    /// An auction-unique integer ID.
    pub a_id: Id,
    /// The name of the item being auctioned.
    pub item_name: String,
    /// A short description of the item.
    pub description: String,
    /// The initial bid price in cents.
    pub initial_bid: usize,
    /// The minimum price for the auction to succeed.
    pub reserve: usize,
    /// A millisecond timestamp for the event origin.
    pub a_date_time: String,
    /// A UNIX epoch timestamp for the expiration date of the auction.
    pub expires: String,
    /// The ID of the person that created this auction.
    pub seller: Id,
    /// The ID of the category this auction belongs to.
    pub category: Id,
}

impl Auction {
    fn new(
        events_so_far: usize,
        id: usize,
        time: usize,
        rng: &mut SmallRng,
        nex: &NexmarkConfig,
    ) -> Self {
        let initial_bid = rng.gen_price();
        let seller = if rng.gen_range(0..nex.hot_seller_ratio) > 0 {
            (Person::last_id(id, nex) / nex.hot_seller_ratio_2) * nex.hot_seller_ratio_2
        } else {
            Person::next_id(id, rng, nex)
        };
        Auction {
            a_id: Self::last_id(id, nex) + nex.first_auction_id,
            item_name: rng.gen_string(20),
            description: rng.gen_string(100),
            initial_bid,
            reserve: initial_bid + rng.gen_price(),
            a_date_time: milli_ts_to_timestamp_string(time),
            expires: milli_ts_to_timestamp_string(
                time + Self::next_length(events_so_far, rng, time, nex),
            ),
            seller: seller + nex.first_person_id,
            category: nex.first_category_id + rng.gen_range(0..nex.num_categories),
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NexmarkConfig) -> Id {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions {
            0
        } else {
            max_auction - nex.in_flight_auctions
        };
        min_auction + rng.gen_range(0..max_auction - min_auction + 1 + nex.auction_id_lead)
    }

    fn last_id(id: usize, nex: &NexmarkConfig) -> Id {
        let mut epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if offset < nex.person_proportion {
            epoch -= 1;
            offset = nex.auction_proportion - 1;
        } else if nex.person_proportion + nex.auction_proportion <= offset {
            offset = nex.auction_proportion - 1;
        } else {
            offset -= nex.person_proportion;
        }
        epoch * nex.auction_proportion + offset
    }

    fn next_length(
        events_so_far: usize,
        rng: &mut SmallRng,
        time: usize,
        nex: &NexmarkConfig,
    ) -> usize {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions =
            (nex.in_flight_auctions * nex.proportion_denominator) / nex.auction_proportion;
        let future_auction = nex.event_timestamp(current_event + events_for_auctions);

        let horizon = future_auction - time;
        1 + rng.gen_range(0..max(horizon * 2, 1))
    }
}

/// Bid represents a bid for an item under auction.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Bid {
    /// The ID of the auction this bid is for.
    pub auction: Id,
    /// The ID of the person that placed this bid.
    pub bidder: Id,
    /// The price in cents that the person bid for.
    pub price: usize,
    /// A millisecond timestamp for the event origin.
    pub b_date_time: String,
}

impl Bid {
    fn new(id: usize, time: usize, rng: &mut SmallRng, nex: &NexmarkConfig) -> Self {
        let auction = if 0 < rng.gen_range(0..nex.hot_auction_ratio) {
            (Auction::last_id(id, nex) / nex.hot_auction_ratio_2) * nex.hot_auction_ratio_2
        } else {
            Auction::next_id(id, rng, nex)
        };
        let bidder = if 0 < rng.gen_range(0..nex.hot_bidder_ratio) {
            (Person::last_id(id, nex) / nex.hot_bidder_ratio_2) * nex.hot_bidder_ratio_2 + 1
        } else {
            Person::next_id(id, rng, nex)
        };
        Bid {
            auction: auction + nex.first_auction_id,
            bidder: bidder + nex.first_person_id,
            price: rng.gen_price(),
            b_date_time: milli_ts_to_timestamp_string(time),
        }
    }
}

fn milli_ts_to_timestamp_string(milli_ts: usize) -> String {
    NaiveDateTime::from_timestamp(
        milli_ts as i64 / 1000,
        (milli_ts % (1000_usize)) as u32 * 1000000,
    )
    .format("%Y-%m-%d %H:%M:%S%.f")
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::io::Result;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::source::nexmark::{NexmarkProperties, NEXMARK_BASE_TIME};

    #[test]
    fn test_milli_ts_to_timestamp_string() -> Result<()> {
        let mut init_ts = milli_ts_to_timestamp_string(0);
        assert_eq!(init_ts, "1970-01-01 00:00:00");
        init_ts = milli_ts_to_timestamp_string(1);
        assert_eq!(init_ts, "1970-01-01 00:00:00.001");
        init_ts = milli_ts_to_timestamp_string(1000);
        assert_eq!(init_ts, "1970-01-01 00:00:01");
        Ok(())
    }

    #[test]
    fn test_event() -> Result<()> {
        let properties = NexmarkProperties::default();
        let res = NexmarkConfig::from(properties);
        assert!(res.is_ok());
        let config = res.unwrap();

        let wall_clock_base_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as usize;

        let (event_1, _) = Event::new(0, &config, wall_clock_base_time);
        let (event_2, _) = Event::new(0, &config, NEXMARK_BASE_TIME);
        assert_eq!(event_1, event_2);

        let event_1_payload = r#"{"p_id":1000,"name":"vicky noris","email_address":"vzbhp@wxv.com","credit_card":"4355 0142 3460 9324","city":"boise","state":"ca","p_date_time":"2015-07-15 00:00:00"}"#.to_string();
        assert_eq!(event_1.to_json(), event_1_payload);
        Ok(())
    }
}
