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

use std::hash::Hash;

use postgres_types::{ToSql as _, Type};
use serde::{Serialize, Serializer};

use crate::estimate_size::EstimateSize;
use crate::util::row_id::RowId;

// Serial is an alias for i64
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Serial(i64);

impl From<i64> for Serial {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl EstimateSize for Serial {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

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

impl crate::types::to_binary::ToBinary for Serial {
    fn to_binary_with_type(
        &self,
        _ty: &crate::types::DataType,
    ) -> crate::error::Result<Option<bytes::Bytes>> {
        let mut output = bytes::BytesMut::new();
        self.0.to_sql(&Type::ANY, &mut output).unwrap();
        Ok(Some(output.freeze()))
    }
}
