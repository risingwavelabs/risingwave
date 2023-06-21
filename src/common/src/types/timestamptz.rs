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

use std::io::Write;

use bytes::{Bytes, BytesMut};
use chrono::{TimeZone, Utc};
use postgres_types::ToSql;
use serde::{Deserialize, Serialize};

use super::to_binary::ToBinary;
use super::to_text::ToText;
use super::DataType;
use crate::array::ArrayResult;
use crate::error::Result;
use crate::estimate_size::ZeroHeapSize;

#[derive(
    Default,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    parse_display::Display,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct Timestamptz(pub i64);

impl ZeroHeapSize for Timestamptz {}

impl ToBinary for Timestamptz {
    fn to_binary_with_type(&self, _ty: &DataType) -> Result<Option<Bytes>> {
        let instant = Utc.timestamp_nanos(self.0 * 1000);
        let mut out = BytesMut::new();
        // postgres_types::Type::ANY is only used as a placeholder.
        instant
            .to_sql(&postgres_types::Type::ANY, &mut out)
            .unwrap();
        Ok(Some(out.freeze()))
    }
}

impl ToText for Timestamptz {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        // Just a meaningful representation as placeholder. The real implementation depends
        // on TimeZone from session. See #3552.
        let instant = Utc.timestamp_nanos(self.0 * 1000);
        // PostgreSQL uses a space rather than `T` to separate the date and time.
        // https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT
        // same as `instant.format("%Y-%m-%d %H:%M:%S%.f%:z")` but faster
        write!(f, "{}+00:00", instant.naive_local())
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        assert_eq!(ty, &DataType::Timestamptz);
        self.write(f)
    }
}

impl Timestamptz {
    pub const MIN: Self = Self(i64::MIN);

    pub fn from_protobuf(timestamp_micros: i64) -> ArrayResult<Self> {
        Ok(Self(timestamp_micros))
    }

    pub fn to_protobuf(self, output: &mut impl Write) -> ArrayResult<usize> {
        output.write(&self.0.to_be_bytes()).map_err(Into::into)
    }
}
