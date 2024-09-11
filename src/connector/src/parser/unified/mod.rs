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

//! Unified parsers for both normal events or CDC events of multiple message formats

use auto_impl::auto_impl;
use risingwave_common::types::{DataType, DatumCow};
use risingwave_connector_codec::decoder::avro::AvroAccess;
pub use risingwave_connector_codec::decoder::{uncategorized, Access, AccessError, AccessResult};

use self::bytes::BytesAccess;
use self::json::JsonAccess;
use self::protobuf::ProtobufAccess;
use crate::parser::unified::debezium::MongoJsonAccess;
use crate::source::SourceColumnDesc;

pub mod bytes;
pub mod debezium;
pub mod json;
pub mod kv_event;
pub mod maxwell;
pub mod protobuf;
pub mod util;

pub enum AccessImpl<'a> {
    Avro(AvroAccess<'a>),
    Bytes(BytesAccess<'a>),
    Protobuf(ProtobufAccess),
    Json(JsonAccess<'a>),
    MongoJson(MongoJsonAccess<JsonAccess<'a>>),
}

impl Access for AccessImpl<'_> {
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        match self {
            Self::Avro(accessor) => accessor.access(path, type_expected),
            Self::Bytes(accessor) => accessor.access(path, type_expected),
            Self::Protobuf(accessor) => accessor.access(path, type_expected),
            Self::Json(accessor) => accessor.access(path, type_expected),
            Self::MongoJson(accessor) => accessor.access(path, type_expected),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChangeEventOperation {
    Upsert, // Insert or Update
    Delete,
}

/// Methods to access a CDC event.
#[auto_impl(&)]
pub trait ChangeEvent {
    /// Access the operation type.
    fn op(&self) -> AccessResult<ChangeEventOperation>;
    /// Access the field.
    fn access_field(&self, desc: &SourceColumnDesc) -> AccessResult<DatumCow<'_>>;
}

impl<A> ChangeEvent for (ChangeEventOperation, A)
where
    A: Access,
{
    fn op(&self) -> AccessResult<ChangeEventOperation> {
        Ok(self.0)
    }

    fn access_field(&self, desc: &SourceColumnDesc) -> AccessResult<DatumCow<'_>> {
        self.1.access(&[desc.name.as_str()], &desc.data_type)
    }
}
