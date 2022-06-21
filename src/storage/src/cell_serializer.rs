// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::Row;
use risingwave_common::error::Result;

pub type KeyBytes = Vec<u8>;
pub type ValueBytes = Vec<u8>;

pub trait CellSerializer {
    /// Serialize key and value.
    fn serialize(&mut self, pk: &[u8], row: Row) -> Result<Vec<(KeyBytes, ValueBytes)>>;

    /// Serialize key and value. Each column id will occupy a position in Vec. For `column_ids` that
    /// doesn't correspond to a cell, the position will be None. Aparts from user-specified
    /// `column_ids`, there will also be a `SENTINEL_CELL_ID` at the end.
    fn serialize_without_filter(
        &mut self,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<Option<(KeyBytes, ValueBytes)>>>;

    /// Different from [`CellSerializer::serialize`], only serialize key into cell key (With
    /// column id appended).
    fn serialize_cell_key(&mut self, pk: &[u8], row: &Row) -> Result<Vec<KeyBytes>>;
}
