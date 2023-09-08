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

use risingwave_common::array::RowRef;
use risingwave_common::catalog::Field;
use risingwave_common::row::Row;

use crate::sink::Result;

mod empty;
mod json;

pub use empty::EmptyEncoder;
pub use json::JsonEncoder;

pub trait RowEncoder {
    type Output: SerToBytes;

    fn encode(
        &self,
        row: RowRef<'_>,
        schema: &[Field],
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output>;

    fn encode_all(&self, row: RowRef<'_>, schema: &[Field]) -> Result<Self::Output> {
        assert_eq!(row.len(), schema.len());
        self.encode(row, schema, 0..schema.len())
    }
}

pub trait SerToBytes {
    fn ser_to_bytes(&self) -> Result<Vec<u8>>;
}

pub trait SerToString {
    fn ser_to_string(&self) -> Result<String>;
}

impl<T: SerToString> SerToBytes for T {
    fn ser_to_bytes(&self) -> Result<Vec<u8>> {
        self.ser_to_string().map(|s| s.into_bytes())
    }
}
