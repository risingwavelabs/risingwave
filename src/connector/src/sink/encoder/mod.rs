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

use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;

use crate::sink::Result;

mod json;

pub use json::JsonEncoder;

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
/// Shall we consider `impl serde::Serialize` in the future?
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
