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
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::serialize_pk_and_row;

type KeyBytes = Vec<u8>;
type ValueBytes = Vec<u8>;
#[derive(Clone)]
pub struct CellBasedRowSerializer {}
impl Default for CellBasedRowSerializer {
    fn default() -> Self {
        Self::new()
    }
}
impl CellBasedRowSerializer {
    pub fn new() -> Self {
        Self {}
    }
    pub fn serialize(
        &mut self,
        pk: &[u8],
        row: Option<Row>,
        column_ids: Vec<ColumnId>,
    ) -> Result<Vec<(KeyBytes, Option<ValueBytes>)>> {
        serialize_pk_and_row(pk, &row, &column_ids)
    }
}
