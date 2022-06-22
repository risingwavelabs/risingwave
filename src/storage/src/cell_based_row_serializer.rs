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

use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::types::VirtualNode;
use risingwave_common::util::ordered::serialize_pk_and_row;

type KeyBytes = Vec<u8>;
type ValueBytes = Vec<u8>;

#[derive(Clone)]
pub struct CellBasedRowSerializer {
    pub column_ids: Vec<ColumnId>,
}

impl CellBasedRowSerializer {
    pub fn new(column_ids: Vec<ColumnId>) -> Self {
        Self { column_ids }
    }

    /// Serialize key and value. The `row` must be in the same order with the column ids in this
    /// serializer.
    pub fn serialize(
        &mut self,
        vnode: VirtualNode,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<(KeyBytes, ValueBytes)>> {
        // TODO: avoid this allocation
        let key = [vnode.to_be_bytes().as_slice(), pk].concat();
        let res = serialize_pk_and_row(&key, &row, &self.column_ids)?
            .into_iter()
            .flatten()
            .collect_vec();
        Ok(res)
    }

    /// Serialize key and value. Each column id will occupy a position in Vec. For `column_ids` that
    /// doesn't correspond to a cell, the position will be None. Aparts from user-specified
    /// `column_ids`, there will also be a `SENTINEL_CELL_ID` at the end.
    pub fn serialize_without_filter(
        &mut self,
        vnode: VirtualNode,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<Option<(KeyBytes, ValueBytes)>>> {
        // TODO: avoid this allocation
        let key = [vnode.to_be_bytes().as_slice(), pk].concat();
        let res = serialize_pk_and_row(&key, &row, &self.column_ids)?;
        Ok(res)
    }
}
