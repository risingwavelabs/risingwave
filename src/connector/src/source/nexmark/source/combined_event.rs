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

use itertools::Itertools;
pub use nexmark::event::EventType;
use nexmark::event::{Auction, Bid, Event, Person};
use risingwave_common::array::StructValue;
use risingwave_common::catalog::row_id_column_name;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::{DataType, Datum, ScalarImpl, Timestamp};
use risingwave_common::util::iter_util::ZipEqFast;
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

pub fn new_combined_event(event: Event) -> CombinedEvent {
    match event {
        Event::Person(p) => CombinedEvent::person(p),
        Event::Auction(a) => CombinedEvent::auction(a),
        Event::Bid(b) => CombinedEvent::bid(b),
    }
}

pub fn get_event_data_types_with_names(
    event_type: Option<EventType>,
    row_id_index: Option<usize>,
) -> Vec<(String, DataType)> {
    let mut fields = match event_type {
        None => {
            vec![
                ("event_type".to_owned(), DataType::Int64),
                (
                    "person".to_owned(),
                    DataType::Struct(get_person_struct_type().into()),
                ),
                (
                    "auction".to_owned(),
                    DataType::Struct(get_auction_struct_type().into()),
                ),
                (
                    "bid".to_owned(),
                    DataType::Struct(get_bid_struct_type().into()),
                ),
            ]
        }
        Some(EventType::Person) => {
            let struct_type = get_person_struct_type();
            struct_type
                .field_names
                .into_iter()
                .zip_eq_fast(struct_type.fields)
                .collect_vec()
        }
        Some(EventType::Auction) => {
            let struct_type = get_auction_struct_type();
            struct_type
                .field_names
                .into_iter()
                .zip_eq_fast(struct_type.fields)
                .collect_vec()
        }
        Some(EventType::Bid) => {
            let struct_type = get_bid_struct_type();
            struct_type
                .field_names
                .into_iter()
                .zip_eq_fast(struct_type.fields)
                .collect_vec()
        }
    };

    if let Some(row_id_index) = row_id_index {
        // _row_id
        fields.insert(row_id_index, (row_id_column_name(), DataType::Serial));
    }

    fields
}

pub(crate) fn get_event_data_types(
    event_type: Option<EventType>,
    row_id_index: Option<usize>,
) -> Vec<DataType> {
    let mut fields = match event_type {
        None => {
            vec![
                DataType::Int64,
                DataType::Struct(get_person_struct_type().into()),
                DataType::Struct(get_auction_struct_type().into()),
                DataType::Struct(get_bid_struct_type().into()),
            ]
        }
        Some(EventType::Person) => get_person_struct_type().fields,
        Some(EventType::Auction) => get_auction_struct_type().fields,
        Some(EventType::Bid) => get_bid_struct_type().fields,
    };

    if let Some(row_id_index) = row_id_index {
        // _row_id
        fields.insert(row_id_index, DataType::Serial);
    }

    fields
}

pub(crate) fn get_person_struct_type() -> StructType {
    let fields = vec![
        DataType::Int64,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Timestamp,
        DataType::Varchar,
    ];
    let field_names = vec![
        "id",
        "name",
        "email_address",
        "credit_card",
        "city",
        "state",
        "date_time",
        "extra",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect();
    StructType {
        fields,
        field_names,
    }
}

pub(crate) fn get_auction_struct_type() -> StructType {
    let fields = vec![
        DataType::Int64,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int64,
        DataType::Int64,
        DataType::Timestamp,
        DataType::Timestamp,
        DataType::Int64,
        DataType::Int64,
        DataType::Varchar,
    ];
    let field_names = vec![
        "id",
        "item_name",
        "description",
        "initial_bid",
        "reserve",
        "date_time",
        "expires",
        "seller",
        "category",
        "extra",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect();

    StructType {
        fields,
        field_names,
    }
}

pub(crate) fn get_bid_struct_type() -> StructType {
    let fields = vec![
        DataType::Int64,
        DataType::Int64,
        DataType::Int64,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Timestamp,
        DataType::Varchar,
    ];
    let field_names = vec![
        "auction",
        "bidder",
        "price",
        "channel",
        "url",
        "date_time",
        "extra",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect();

    StructType {
        fields,
        field_names,
    }
}

pub(crate) fn combined_event_to_row(e: CombinedEvent, row_id_index: Option<usize>) -> OwnedRow {
    let mut fields = vec![
        Some(ScalarImpl::Int64(e.event_type as i64)),
        e.person
            .map(person_to_datum)
            .map(|fields| StructValue::new(fields).into()),
        e.auction
            .map(auction_to_datum)
            .map(|fields| StructValue::new(fields).into()),
        e.bid
            .map(bid_to_datum)
            .map(|fields| StructValue::new(fields).into()),
    ];

    if let Some(row_id_index) = row_id_index {
        // _row_id
        fields.insert(row_id_index, None);
    }

    OwnedRow::new(fields)
}

pub(crate) fn event_to_row(e: Event, row_id_index: Option<usize>) -> OwnedRow {
    let mut fields = match e {
        Event::Person(p) => person_to_datum(p),
        Event::Auction(a) => auction_to_datum(a),
        Event::Bid(b) => bid_to_datum(b),
    };
    if let Some(row_id_index) = row_id_index {
        // _row_id
        fields.insert(row_id_index, None);
    }
    OwnedRow::new(fields)
}

fn person_to_datum(p: Person) -> Vec<Datum> {
    let fields = vec![
        Some(ScalarImpl::Int64(p.id as i64)),
        Some(ScalarImpl::Utf8(p.name.into())),
        Some(ScalarImpl::Utf8(p.email_address.into())),
        Some(ScalarImpl::Utf8(p.credit_card.into())),
        Some(ScalarImpl::Utf8(p.city.into())),
        Some(ScalarImpl::Utf8(p.state.into())),
        Some(ScalarImpl::Timestamp(
            Timestamp::with_secs_nsecs(
                (p.date_time / 1_000) as i64,
                (p.date_time % 1_000) as u32 * 1_000_000,
            )
            .unwrap(),
        )),
        Some(ScalarImpl::Utf8(p.extra.into())),
    ];
    fields
}

fn auction_to_datum(a: Auction) -> Vec<Datum> {
    let fields = vec![
        Some(ScalarImpl::Int64(a.id as i64)),
        Some(ScalarImpl::Utf8(a.item_name.into())),
        Some(ScalarImpl::Utf8(a.description.into())),
        Some(ScalarImpl::Int64(a.initial_bid as i64)),
        Some(ScalarImpl::Int64(a.reserve as i64)),
        Some(ScalarImpl::Timestamp(
            Timestamp::with_secs_nsecs(
                (a.date_time / 1_000) as i64,
                (a.date_time % 1_000) as u32 * 1_000_000,
            )
            .unwrap(),
        )),
        Some(ScalarImpl::Timestamp(
            Timestamp::with_secs_nsecs(
                (a.expires / 1_000) as i64,
                (a.expires % 1_000) as u32 * 1_000_000,
            )
            .unwrap(),
        )),
        Some(ScalarImpl::Int64(a.seller as i64)),
        Some(ScalarImpl::Int64(a.category as i64)),
        Some(ScalarImpl::Utf8(a.extra.into())),
    ];

    fields
}

fn bid_to_datum(b: Bid) -> Vec<Datum> {
    let fields = vec![
        Some(ScalarImpl::Int64(b.auction as i64)),
        Some(ScalarImpl::Int64(b.bidder as i64)),
        Some(ScalarImpl::Int64(b.price as i64)),
        Some(ScalarImpl::Utf8(b.channel.into())),
        Some(ScalarImpl::Utf8(b.url.into())),
        Some(ScalarImpl::Timestamp(
            Timestamp::with_secs_nsecs(
                (b.date_time / 1_000) as i64,
                (b.date_time % 1_000) as u32 * 1_000_000,
            )
            .unwrap(),
        )),
        Some(ScalarImpl::Utf8(b.extra.into())),
    ];

    fields
}
