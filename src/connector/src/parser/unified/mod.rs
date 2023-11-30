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

//! Unified parsers for both normal events or CDC events of multiple message formats

use auto_impl::auto_impl;
use risingwave_common::types::{DataType, Datum};
use thiserror::Error;

use self::avro::AvroAccess;
use self::bytes::BytesAccess;
use self::json::JsonAccess;
use self::protobuf::ProtobufAccess;

pub mod avro;
pub mod bytes;
pub mod debezium;
pub mod json;
pub mod maxwell;
pub mod protobuf;
pub mod upsert;
pub mod util;

pub type AccessResult<T = Datum> = std::result::Result<T, AccessError>;

/// Access a certain field in an object according to the path
pub trait Access {
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult;
}

pub enum AccessImpl<'a, 'b> {
    Avro(AvroAccess<'a, 'b>),
    Bytes(BytesAccess<'a>),
    Protobuf(ProtobufAccess),
    Json(JsonAccess<'a, 'b>),
}

impl Access for AccessImpl<'_, '_> {
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult {
        match self {
            Self::Avro(accessor) => accessor.access(path, type_expected),
            Self::Bytes(accessor) => accessor.access(path, type_expected),
            Self::Protobuf(accessor) => accessor.access(path, type_expected),
            Self::Json(accessor) => accessor.access(path, type_expected),
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
    fn op(&self) -> std::result::Result<ChangeEventOperation, AccessError>;
    /// Access the field after the operation.
    fn access_field(&self, name: &str, type_expected: &DataType) -> AccessResult;
}

impl<A> ChangeEvent for (ChangeEventOperation, A)
where
    A: Access,
{
    fn op(&self) -> std::result::Result<ChangeEventOperation, AccessError> {
        Ok(self.0)
    }

    fn access_field(&self, name: &str, type_expected: &DataType) -> AccessResult {
        self.1.access(&[name], Some(type_expected))
    }
}

#[derive(Error, Debug)]
pub enum AccessError {
    #[error("Undefined field `{name}` at `{path}`")]
    Undefined { name: String, path: String },
    #[error("Expected type `{expected}` but got `{got}` for `{value}`")]
    TypeError {
        expected: String,
        got: String,
        value: String,
    },
    #[error("Unsupported data type `{ty}`")]
    UnsupportedType { ty: String },
    #[error(transparent)]
    Other(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}
