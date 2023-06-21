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
use std::str::FromStr;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, TimeZone, Utc};
use postgres_types::ToSql;
use serde::{Deserialize, Serialize};

use super::to_binary::ToBinary;
use super::to_text::ToText;
use super::DataType;
use crate::array::ArrayResult;
use crate::error::Result;
use crate::estimate_size::ZeroHeapSize;

#[derive(
    Default, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct Timestamptz(i64);

impl ZeroHeapSize for Timestamptz {}

impl ToBinary for Timestamptz {
    fn to_binary_with_type(&self, _ty: &DataType) -> Result<Option<Bytes>> {
        let instant = self.to_datetime_utc();
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
        let instant = self.to_datetime_utc();
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

    pub fn from_secs(timestamp_secs: i64) -> Self {
        Self(timestamp_secs * 1_000_000)
    }

    pub fn from_micros(timestamp_micros: i64) -> Self {
        Self(timestamp_micros)
    }

    pub fn from_millis(timestamp_millis: i64) -> Self {
        Self(timestamp_millis * 1000)
    }

    /// Returns the number of non-leap-microseconds since January 1, 1970 UTC.
    pub fn timestamp_micros(&self) -> i64 {
        self.0
    }

    /// Returns the number of non-leap seconds since January 1, 1970 0:00:00 UTC (aka "UNIX
    /// timestamp").
    pub fn timestamp(&self) -> i64 {
        self.0.div_euclid(1_000_000)
    }

    /// Returns the number of nanoseconds since the last second boundary.
    pub fn timestamp_subsec_nanos(&self) -> u32 {
        self.0.rem_euclid(1_000_000) as u32 * 1000
    }

    pub fn to_datetime_utc(self) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(self.timestamp(), self.timestamp_subsec_nanos())
            .unwrap()
    }

    pub fn from_protobuf(timestamp_micros: i64) -> ArrayResult<Self> {
        Ok(Self(timestamp_micros))
    }

    pub fn to_protobuf(self, output: &mut impl Write) -> ArrayResult<usize> {
        output.write(&self.0.to_be_bytes()).map_err(Into::into)
    }
}

impl From<chrono::DateTime<Utc>> for Timestamptz {
    fn from(dt: chrono::DateTime<Utc>) -> Self {
        Self(dt.timestamp_micros())
    }
}

impl FromStr for Timestamptz {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        pub const ERROR_MSG: &str = concat!(
            "Can't cast string to timestamp with time zone (expected format is YYYY-MM-DD HH:MM:SS[.D+{up to 6 digits}] followed by +hh:mm or literal Z)"
            , "\nFor example: '2021-04-01 00:00:00+00:00'"
        );
        let ret = s.parse::<DateTime<Utc>>().map_err(|_| ERROR_MSG)?;
        Ok(Timestamptz(ret.timestamp_micros()))
    }
}

impl std::fmt::Display for Timestamptz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse() {
        assert!("1999-01-08 04:05:06".parse::<Timestamptz>().is_err());
        assert_eq!(
            "2022-08-03 10:34:02Z".parse::<Timestamptz>().unwrap(),
            "2022-08-03 02:34:02-08:00".parse::<Timestamptz>().unwrap()
        );
    }
}
