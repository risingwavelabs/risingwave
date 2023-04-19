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

use bytes::Bytes;

use super::{OwnedRow, Row, RowDeserializer};
use crate::estimate_size::EstimateSize;
use crate::types::DataType;
use crate::util::value_encoding;

/// `CompactedRow` is used in streaming executors' cache, which takes less memory than `Vec<Datum>`.
/// Executors need to serialize Row into `CompactedRow` before writing into cache.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct CompactedRow {
    pub row: Bytes,
}

impl CompactedRow {
    /// Create a new [`CompactedRow`] from given bytes. Caller must ensure the bytes are in valid
    /// value-encoding row format.
    pub fn new(value_encoding_bytes: Bytes) -> Self {
        Self {
            row: value_encoding_bytes,
        }
    }

    /// Deserialize [`CompactedRow`] into [`OwnedRow`] with given types.
    pub fn deserialize(&self, data_types: &[DataType]) -> value_encoding::Result<OwnedRow> {
        RowDeserializer::new(data_types).deserialize(self.row.as_ref())
    }
}

impl<R: Row> From<R> for CompactedRow {
    fn from(row: R) -> Self {
        Self {
            row: row.value_serialize_bytes(),
        }
    }
}

impl EstimateSize for CompactedRow {
    fn estimated_heap_size(&self) -> usize {
        self.row.estimated_heap_size()
    }
}
