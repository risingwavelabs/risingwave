// Copyright 2025 RisingWave Labs
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

use std::error::Error;
use std::hash::Hash;

use bytes::BytesMut;
use postgres_types::{IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::ZeroHeapSize;
use serde::{Serialize, Serializer};

use crate::util::row_id::RowId;

// Serial is an alias for i64
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Serial(pub(crate) i64);

impl From<Serial> for i64 {
    fn from(value: Serial) -> i64 {
        value.0
    }
}

impl From<i64> for Serial {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl ZeroHeapSize for Serial {}

impl Serial {
    #[inline]
    pub fn into_inner(self) -> i64 {
        self.0
    }

    #[inline]
    pub fn as_row_id(self) -> RowId {
        self.0 as RowId
    }
}

impl Serialize for Serial {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl crate::types::to_text::ToText for Serial {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: std::fmt::Write>(
        &self,
        _ty: &crate::types::DataType,
        f: &mut W,
    ) -> std::fmt::Result {
        self.write(f)
    }
}

impl ToSql for Serial {
    accepts!(INT8);

    to_sql_checked!();

    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }
}
