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

use std::collections::HashMap;

use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;

use crate::sink::Result;

mod avro;
mod json;
mod proto;

pub use avro::AvroEncoder;
pub use json::JsonEncoder;
pub use proto::ProtoEncoder;

/// Encode a row of a relation into
/// * an object in json
/// * a message in protobuf
/// * a record in avro
pub trait RowEncoder {
    type Output: SerTo<Vec<u8>>;

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output>;
    fn schema(&self) -> &Schema;
    fn col_indices(&self) -> Option<&[usize]>;

    fn encode(&self, row: impl Row) -> Result<Self::Output> {
        assert_eq!(row.len(), self.schema().len());
        match self.col_indices() {
            Some(col_indices) => self.encode_cols(row, col_indices.iter().copied()),
            None => self.encode_cols(row, 0..self.schema().len()),
        }
    }
}

/// Do the actual encoding from
/// * an json object
/// * a protobuf message
/// * an avro record
/// into
/// * string (required by kinesis key)
/// * bytes
///
/// This is like `TryInto` but allows us to `impl<T: SerTo<String>> SerTo<Vec<u8>> for T`.
///
/// Note that `serde` does not fit here because its data model does not contain logical types.
/// For example, although `chrono::DateTime` implements `Serialize`,
/// it produces avro String rather than avro `TimestampMicros`.
pub trait SerTo<T> {
    fn ser_to(self) -> Result<T>;
}

impl<T: SerTo<String>> SerTo<Vec<u8>> for T {
    fn ser_to(self) -> Result<Vec<u8>> {
        self.ser_to().map(|s: String| s.into_bytes())
    }
}

impl<T> SerTo<T> for T {
    fn ser_to(self) -> Result<T> {
        Ok(self)
    }
}

/// Useful for both json and protobuf
#[derive(Clone, Copy)]
pub enum TimestampHandlingMode {
    Milli,
    String,
}

#[derive(Clone)]
pub enum CustomJsonType {
    Doris(HashMap<String, (u8, u8)>),
    None,
}

#[derive(Debug)]
struct FieldEncodeError {
    message: String,
    rev_path: Vec<String>,
}

impl FieldEncodeError {
    fn new(message: impl std::fmt::Display) -> Self {
        Self {
            message: message.to_string(),
            rev_path: vec![],
        }
    }

    fn with_name(mut self, name: &str) -> Self {
        self.rev_path.push(name.into());
        self
    }
}

impl std::fmt::Display for FieldEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use itertools::Itertools;

        write!(
            f,
            "encode {} error: {}",
            self.rev_path.iter().rev().join("."),
            self.message
        )
    }
}

impl From<FieldEncodeError> for super::SinkError {
    fn from(value: FieldEncodeError) -> Self {
        Self::Encode(value.to_string())
    }
}
