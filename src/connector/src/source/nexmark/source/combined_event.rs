// Copyright 2023 Singularity Data
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

use nexmark::event::{Auction, Bid, Person};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct CombinedEvent {
    event_type: u64,
    /// The Person event
    person: Option<Person>,
    /// The Auction event.
    auction: Option<Auction>,
    /// The Bid event.
    bid: Option<Bid>,
}

impl CombinedEvent {
    fn new(
        event_type: u64,
        person: Option<Person>,
        auction: Option<Auction>,
        bid: Option<Bid>,
    ) -> Self {
        Self {
            event_type,
            person,
            auction,
            bid,
        }
    }

    pub fn person(person: Person) -> Self {
        Self::new(0, Some(person), None, None)
    }

    pub fn auction(auction: Auction) -> Self {
        Self::new(1, None, Some(auction), None)
    }

    pub fn bid(bid: Bid) -> Self {
        Self::new(2, None, None, Some(bid))
    }
}
