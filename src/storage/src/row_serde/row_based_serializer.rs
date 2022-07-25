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
use risingwave_common::types::VirtualNode;
use risingwave_common::util::value_encoding::serialize_datum;

use super::RowSerialize;

#[derive(Clone)]
pub struct RowBasedSerializer {}

impl RowSerialize for RowBasedSerializer {
    /// Serialize the row into a value encode bytes.
    /// All values are nullable. Each value will have 1 extra byte to indicate whether it is null.
    fn serialize(
        &mut self,
        vnode: VirtualNode,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut value_bytes = vec![];
        let key = [vnode.to_be_bytes().as_slice(), pk].concat();
        for cell in &row.0 {
            value_bytes.extend(serialize_datum(cell)?);
        }
        let res = vec![(key, value_bytes)];
        Ok(res)
    }

    fn create_row_serializer(
        _pk_indices: &[usize],
        _column_descs: &[risingwave_common::catalog::ColumnDesc],
        _column_ids: &[risingwave_common::catalog::ColumnId],
    ) -> Self {
        // todo: these parameters may be used in row-based pk dudup later.
        Self {}
    }

    fn serialize_for_update(
        &mut self,
        vnode: risingwave_common::types::VirtualNode,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<Option<(super::KeyBytes, super::ValueBytes)>>> {
        let mut value_bytes = vec![];
        let key = [vnode.to_be_bytes().as_slice(), pk].concat();
        for cell in &row.0 {
            value_bytes.extend(serialize_datum(cell)?);
        }
        let res = vec![Some((key, value_bytes))];
        Ok(res)
    }

    fn serialize_sentinel_cell(
        _pk_buf: &[u8],
        _col_id: &risingwave_common::catalog::ColumnId,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
