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

use postgres_types::{ToSql as _, Type};
use serde_json::Value;

use crate::types::{Scalar, ScalarImpl, ScalarRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonbVal(Box<Value>);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct JsonbRef<'a>(&'a Value);

impl Scalar for JsonbVal {
    type ScalarRefType<'a> = JsonbRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        JsonbRef(self.0.as_ref())
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Jsonb(self)
    }
}

impl<'a> ScalarRef<'a> for JsonbRef<'a> {
    type ScalarType = JsonbVal;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        JsonbVal(self.0.clone().into())
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::hash::Hash as _;
        self.0.to_string().hash(state)
    }
}

impl PartialOrd for JsonbVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_scalar_ref().cmp(&other.as_scalar_ref())
    }
}

impl PartialOrd for JsonbRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_string().cmp(&other.0.to_string())
    }
}

impl crate::types::to_text::ToText for JsonbRef<'_> {
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

impl crate::types::to_binary::ToBinary for JsonbRef<'_> {
    fn to_binary_with_type(
        &self,
        _ty: &crate::types::DataType,
    ) -> crate::error::Result<Option<bytes::Bytes>> {
        let mut output = bytes::BytesMut::new();
        self.0.to_sql(&Type::JSONB, &mut output).unwrap();
        Ok(Some(output.freeze()))
    }
}

impl JsonbRef<'_> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        let s = self.0.to_string();
        serde::Serialize::serialize(&s, serializer)
    }

    pub fn value_serialize(&self) -> Vec<u8> {
        let mut output = bytes::BytesMut::new();
        self.0.to_sql(&Type::JSONB, &mut output).unwrap();
        output.freeze().into()
    }
}
