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

use risingwave_common::types::{DataType, Datum};
use thiserror::Error;

pub mod avro;
pub mod debezium;
pub mod json;
pub mod maxwell;
pub mod upsert;
pub mod util;

pub type AccessResult = std::result::Result<Datum, AccessError>;

/// Access a certain field in an object according to the path
pub trait Access {
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult;
}

#[derive(Debug, Clone, Copy)]
pub enum ChangeEventOperation {
    Upsert, // Insert or Update
    Delete,
}

/// Methods to access a CDC event.
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
    #[error("Undefined {name} at {path}")]
    Undefined { name: String, path: String },
    #[error("TypeError {expected} expected, got {got} {value}")]
    TypeError {
        expected: String,
        got: String,
        value: String,
    },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
