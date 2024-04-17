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

use chrono::{Datelike as _, NaiveDate};

use super::schema::{ConnectSchema, PrimitiveSchema, SchemaType};
use crate::schema::kafka_connect::error::DataException;

pub const LOGICAL_NAME: &str = "org.apache.kafka.connect.data.Date";
pub const DAYS_UNIX_EPOCH_FROM_CE: i32 = 719_163;

pub static SCHEMA: LazyLock<ConnectSchema> = LazyLock::new(|| {
    let mut schema = ConnectSchema::Primitive(PrimitiveSchema::new(SchemaType::Int32));
    let base_mut = schema.base_mut();
    base_mut.name = Some(LOGICAL_NAME.into());
    base_mut.version = Some(1);
    schema
});

/// What about the following?
/// ```
/// impl Logical for Date {
///     type Physical = i32;
///     const SCHEMA_TYPE = SchemaType::Int32;
/// }
/// trait LogicalConvert<Class>: Logical {
///     fn from_logical(schema: &ConnectSchema, value: Class) -> Result<Self::Physical, DataException>;
///     fn to_logical(schema: &ConnectSchema, value: Self::Physical) -> Result<Class, DataException>;
/// }
/// ````
pub fn from_logical(schema: &ConnectSchema, value: NaiveDate) -> Result<i32, DataException> {
    if Some(LOGICAL_NAME) != schema.base().name.as_deref() {
        return Err(DataException::new(
            "Requested conversion of Date object but the schema does not match.",
        ));
    }
    Ok(value.num_days_from_ce() - DAYS_UNIX_EPOCH_FROM_CE)
}

pub fn to_logical(schema: &ConnectSchema, value: i32) -> Result<NaiveDate, DataException> {
    if Some(LOGICAL_NAME) != schema.base().name.as_deref() {
        return Err(DataException::new(
            "Requested conversion of Date object but the schema does not match.",
        ));
    }
    NaiveDate::from_num_days_from_ce_opt(value + DAYS_UNIX_EPOCH_FROM_CE)
        .ok_or_else(|| DataException::new("Date overflow"))
}
