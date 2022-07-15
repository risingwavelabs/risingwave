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
use risingwave_common::util::value_encoding::serialize_datum;
type ValueBytes = Vec<u8>;

#[derive(Clone)]
pub struct RowBasedSerializer {}

impl RowBasedSerializer {
    pub fn new() -> Self {
        Self {}
    }

    /// Serialize the row into a value encode bytes.
    /// All values are nullable. Each value will have 1 extra byte to indicate whether it is null.
    pub fn serialize(&mut self, row: &Row) -> Result<ValueBytes> {
        let mut res = vec![];
        for cell in &row.0 {
            res.extend(serialize_datum(cell)?);
        }
        Ok(res)
    }
}

impl Default for RowBasedSerializer {
    fn default() -> Self {
        Self::new()
    }
}
