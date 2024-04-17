// Copyright 2024 RisingWave Labs
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

use std::sync::LazyLock;

use chrono::{NaiveTime, Timelike as _};

use super::schema::{ConnectSchema, PrimitiveSchema, SchemaType};
use crate::schema::kafka_connect::error::DataException;

pub const LOGICAL_NAME: &str = "org.apache.kafka.connect.data.Time";

pub static SCHEMA: LazyLock<ConnectSchema> = LazyLock::new(|| {
    let mut schema = ConnectSchema::Primitive(PrimitiveSchema::new(SchemaType::Int32));
    let base_mut = schema.base_mut();
    base_mut.name = Some(LOGICAL_NAME.into());
    base_mut.version = Some(1);
    schema
});

pub fn from_logical(schema: &ConnectSchema, value: NaiveTime) -> Result<i32, DataException> {
    if Some(LOGICAL_NAME) != schema.base().name.as_deref() {
        return Err(DataException::new(
            "Requested conversion of Time object but the schema does not match.",
        ));
    }
    let nano = value.nanosecond();
    let milli = nano / 1_000_000;
    Ok((value.num_seconds_from_midnight() * 1_000 + milli)
        .try_into()
        .unwrap())
}

pub fn to_logical(schema: &ConnectSchema, value: i32) -> Result<NaiveTime, DataException> {
    if Some(LOGICAL_NAME) != schema.base().name.as_deref() {
        return Err(DataException::new(
            "Requested conversion of Time object but the schema does not match.",
        ));
    }
    value
        .try_into()
        .ok()
        .and_then(|milli: u32| {
            let nano = milli % 1_000 * 1_000_000;
            NaiveTime::from_num_seconds_from_midnight_opt(milli / 1_000, nano)
        })
        .ok_or_else(|| {
            DataException::new(
                "Time values must use number of milliseconds greater than 0 and less than 86400000",
            )
        })
}
