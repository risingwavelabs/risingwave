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

//! Nexmark events.

use std::cmp::{max, min};

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::config::{GeneratorConfig, CHANNEL_NUMBER};
use crate::utils::{milli_ts_to_timestamp_string, NexmarkRng};

type Id = usize;

/// The type of a Nexmark event.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum EventType {
    Person,
    Auction,
    Bid,
}

/// The Nexmark Event, including [`Person`], [`Auction`], and [`Bid`].
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    pub(crate) fn new(
        events_so_far: usize,
        cfg: &GeneratorConfig,
        wall_clock_base_time: u64,
    ) -> (Event, u64) {
        let rem = cfg.next_adjusted_event(events_so_far) % cfg.proportion_denominator;
        let timestamp = cfg.event_timestamp(cfg.next_adjusted_event(events_so_far));
        let new_wall_clock_base_time = timestamp - cfg.base_time + wall_clock_base_time;
        let id = cfg.first_event_id + cfg.next_adjusted_event(events_so_far);
        let mut rng = SmallRng::seed_from_u64(id as u64);
        let event = if rem < cfg.person_proportion {
            Event::Person(Person::new(id, timestamp, &mut rng, cfg))
        } else if rem < cfg.person_proportion + cfg.auction_proportion {
            Event::Auction(Auction::new(events_so_far, id, timestamp, &mut rng, cfg))
        } else {
            Event::Bid(Bid::new(id, timestamp, &mut rng, cfg))
        };
        (event, new_wall_clock_base_time)
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
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Person {
    /// A person-unique integer ID.
    pub id: Id,
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
    pub date_time: String,
    /// Extra information
    pub extra: String,
}

impl Person {
    /// Creates a new `Person` event.
    fn new(id: usize, time: u64, rng: &mut SmallRng, cfg: &GeneratorConfig) -> Self {
        let id = Self::last_id(id, cfg) + cfg.first_person_id;
        let name = format!(
            "{} {}",
            cfg.first_names.choose(rng).unwrap(),
            cfg.last_names.choose(rng).unwrap(),
        );
        let email_address = format!("{}@{}.com", rng.gen_string(7), rng.gen_string(5));
        let credit_card = (0..4)
            .map(|_| format!("{:04}", rng.gen_range(0..10000)))
            .collect::<Vec<String>>()
            .join(" ");
        let city = cfg.us_cities.choose(rng).unwrap().clone();
        let state = cfg.us_states.choose(rng).unwrap().clone();

        let current_size =
            8 + name.len() + email_address.len() + credit_card.len() + city.len() + state.len();
        let extra = rng.gen_next_extra(current_size, cfg.avg_person_byte_size);

        Self {
            id,
            name,
            email_address,
            credit_card,
            city,
            state,
            date_time: milli_ts_to_timestamp_string(time),
            extra,
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &GeneratorConfig) -> Id {
        let people = Self::last_id(id, nex) + 1;
        let active = min(people, nex.active_people);
        people - active + rng.gen_range(0..active + nex.person_id_lead)
    }

    fn last_id(id: usize, nex: &GeneratorConfig) -> Id {
        let epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if nex.person_proportion <= offset {
            offset = nex.person_proportion - 1;
        }
        epoch * nex.person_proportion + offset
    }
}

/// Auction represents an item under auction.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Auction {
    /// An auction-unique integer ID.
    pub id: Id,
    /// The name of the item being auctioned.
    pub item_name: String,
    /// A short description of the item.
    pub description: String,
    /// The initial bid price in cents.
    pub initial_bid: usize,
    /// The minimum price for the auction to succeed.
    pub reserve: usize,
    /// A millisecond timestamp for the event origin.
    pub date_time: String,
    /// A UNIX epoch timestamp for the expiration date of the auction.
    pub expires: String,
    /// The ID of the person that created this auction.
    pub seller: Id,
    /// The ID of the category this auction belongs to.
    pub category: Id,
    /// Extra information
    pub extra: String,
}

impl Auction {
    fn new(
        events_so_far: usize,
        id: usize,
        time: u64,
        rng: &mut SmallRng,
        cfg: &GeneratorConfig,
    ) -> Self {
        let id = Self::last_id(id, cfg) + cfg.first_auction_id;
        let item_name = rng.gen_string(20);
        let description = rng.gen_string(100);
        let initial_bid = rng.gen_price();

        let reserve = initial_bid + rng.gen_price();
        let date_time = milli_ts_to_timestamp_string(time);
        let expires =
            milli_ts_to_timestamp_string(time + Self::next_length(events_so_far, rng, time, cfg));
        let mut seller = if rng.gen_range(0..cfg.hot_seller_ratio) > 0 {
            (Person::last_id(id, cfg) / cfg.hot_seller_ratio_2) * cfg.hot_seller_ratio_2
        } else {
            Person::next_id(id, rng, cfg)
        };
        seller += cfg.first_person_id;
        let category = cfg.first_category_id + rng.gen_range(0..cfg.num_categories);

        let current_size = 8 + item_name.len() + description.len() + 8 + 8 + 8 + 8 + 8;
        let extra = rng.gen_next_extra(current_size, cfg.avg_auction_byte_size);

        Auction {
            id,
            item_name,
            description,
            initial_bid,
            reserve,
            date_time,
            expires,
            seller,
            category,
            extra,
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &GeneratorConfig) -> Id {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions {
            0
        } else {
            max_auction - nex.in_flight_auctions
        };
        min_auction + rng.gen_range(0..max_auction - min_auction + 1 + nex.auction_id_lead)
    }

    fn last_id(id: usize, nex: &GeneratorConfig) -> Id {
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
        time: u64,
        cfg: &GeneratorConfig,
    ) -> u64 {
        let current_event = cfg.next_adjusted_event(events_so_far);
        let events_for_auctions =
            (cfg.in_flight_auctions * cfg.proportion_denominator) / cfg.auction_proportion;
        let future_auction = cfg.event_timestamp(current_event + events_for_auctions);

        let horizon = future_auction - time;
        1 + rng.gen_range(0..max(horizon * 2, 1))
    }
}

/// Bid represents a bid for an item under auction.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Bid {
    /// The ID of the auction this bid is for.
    pub auction: Id,
    /// The ID of the person that placed this bid.
    pub bidder: Id,
    /// The price in cents that the person bid for.
    pub price: usize,
    /// The channel of this bid
    pub channel: String,
    /// The url of this bid
    pub url: String,
    /// A millisecond timestamp for the event origin.
    pub date_time: String,
    /// Extra information
    pub extra: String,
}

impl Bid {
    fn new(id: usize, time: u64, rng: &mut SmallRng, nex: &GeneratorConfig) -> Self {
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

        let price = rng.gen_price();

        let (channel, url) = if rng.gen_range(0..nex.hot_channel_ratio) > 0 {
            let index = rng.gen_range(0..nex.hot_channels.len());
            (nex.hot_channels[index].clone(), nex.hot_urls[index].clone())
        } else {
            nex.channel_url_map
                .get(&rng.gen_range(0..CHANNEL_NUMBER))
                .unwrap()
                .clone()
        };

        let current_size = 8 + 8 + 8 + 8;
        let extra = rng.gen_next_extra(current_size, nex.avg_bid_byte_size);

        Bid {
            auction: auction + nex.first_auction_id,
            bidder: bidder + nex.first_person_id,
            price,
            date_time: milli_ts_to_timestamp_string(time),
            channel,
            url,
            extra,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::config::{NexmarkConfig, NEXMARK_BASE_TIME};

    #[test]
    fn test_event() {
        let config = GeneratorConfig::from(NexmarkConfig::default());

        let wall_clock_base_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let (event_1, _) = Event::new(0, &config, wall_clock_base_time);
        let (event_2, _) = Event::new(0, &config, NEXMARK_BASE_TIME);
        assert_eq!(event_1, event_2);

        assert_eq!(
            event_1,
            Event::Person(Person {
                id: 1000,
                name: "vicky noris".into(),
                email_address: "vzbhp@wxv.com".into(),
                credit_card: "4355 0142 3460 9324".into(),
                city: "boise".into(),
                state: "ca".into(),
                date_time: "2015-07-15 00:00:00".into(),
                extra: "cllnesmssnthtljklifqbqcyhcjwiuoaudxxwcnnwgmsmwgqelplzyckqzuoaitfpxubgpkjtqjhktelmbskvjkxrhziyowxibbgnqneuaiazqduhkynvgeisbxtknbxmqmzbgnptlrcyigjginataks".into(),
            })
        );
    }
}
