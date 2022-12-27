use std::io::Write;

use bytes::{Bytes, BytesMut};
use chrono::{TimeZone, Utc};
use postgres_types::ToSql;
use serde::{Deserialize, Serialize};

use super::to_binary::ToBinary;
use super::to_text::ToText;
use crate::array::ArrayResult;
use crate::error::Result;

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
pub struct Timestampz(pub i64);

impl ToBinary for Timestampz {
    fn to_binary(&self) -> Result<Option<Bytes>> {
        let instant = Utc.timestamp_nanos(self.0 * 1000);
        let mut out = BytesMut::new();
        // postgres_types::Type::ANY is only used as a placeholder.
        instant
            .to_sql(&postgres_types::Type::ANY, &mut out)
            .unwrap();
        Ok(Some(out.freeze()))
    }
}

impl ToText for Timestampz {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        // Just a meaningful representation as placeholder. The real implementation depends
        // on TimeZone from session. See #3552.
        let instant = Utc.timestamp_nanos(self.0 * 1000);
        // PostgreSQL uses a space rather than `T` to separate the date and time.
        // https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT
        // same as `instant.format("%Y-%m-%d %H:%M:%S%.f%:z")` but faster
        write!(f, "{}+00:00", instant.naive_local())
    }
}

impl Timestampz {
    pub const MIN: Self = Self(i64::MIN);

    pub fn from_protobuf(timestamp_micros: i64) -> ArrayResult<Self> {
        Ok(Self(timestamp_micros))
    }

    pub fn to_protobuf(self, output: &mut impl Write) -> ArrayResult<usize> {
        output.write(&self.0.to_be_bytes()).map_err(Into::into)
    }
}
