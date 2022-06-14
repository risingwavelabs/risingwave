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

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::deserialize_column_id;
use risingwave_common::util::value_encoding::deserialize_cell;

pub type ColumnDescMapping = HashMap<ColumnId, (ColumnDesc, usize)>;
pub type GeneralCellBasedRowDeserializer = CellBasedRowDeserializer<Arc<ColumnDescMapping>>;

#[derive(Clone)]
pub struct CellBasedRowDeserializer<Desc: Deref<Target = ColumnDescMapping>> {
    /// A mapping from column id to its desc and the index in the row.
    columns: Desc,

    data: Vec<Datum>,

    /// `CellBasedRowDeserializer` does not deserialize pk itself. We need to take the key in as
    /// we have to know the cell id of each datum. So `pk_bytes` serves as an additional check
    /// which should also be done on the caller side.
    pk_bytes: Option<Vec<u8>>,
}

pub fn make_cell_based_row_deserializer(
    table_column_descs: Vec<ColumnDesc>,
) -> GeneralCellBasedRowDeserializer {
    GeneralCellBasedRowDeserializer::new(Arc::new(make_column_desc_index(table_column_descs)))
}

pub fn make_column_desc_index(table_column_descs: Vec<ColumnDesc>) -> ColumnDescMapping {
    table_column_descs
        .into_iter()
        .enumerate()
        .map(|(index, d)| (d.column_id, (d, index)))
        .collect()
}

impl<Desc: Deref<Target = ColumnDescMapping>> CellBasedRowDeserializer<Desc> {
    pub fn new(column_mapping: Desc) -> Self {
        let num_cells = column_mapping.len();
        Self {
            columns: column_mapping,
            data: vec![None; num_cells],
            pk_bytes: None,
        }
    }

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    pub fn deserialize(
        &mut self,
        pk_with_cell_id: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<u8>, Row)>> {
        let pk_with_cell_id = pk_with_cell_id.as_ref();
        let pk_vec_len = pk_with_cell_id.len();
        if pk_vec_len < 4 {
            return Err(ErrorCode::InternalError(format!(
                "corrupted key: {:?}",
                Bytes::copy_from_slice(pk_with_cell_id)
            ))
            .into());
        }
        let (cur_pk_bytes, cell_id_bytes) = pk_with_cell_id.split_at(pk_vec_len - 4);
        let result;

        let cell_id = deserialize_column_id(cell_id_bytes)?;
        if let Some(prev_pk_bytes) = &self.pk_bytes && prev_pk_bytes != cur_pk_bytes  {
            result = self.take();
            self.pk_bytes = Some(cur_pk_bytes.to_vec());
        } else if self.pk_bytes.is_none() {
            self.pk_bytes = Some(cur_pk_bytes.to_vec());
            result = None;
        } else {
            result = None;
        }
        let mut cell = cell.as_ref();
        if let Some((column_desc, index)) = self.columns.get(&cell_id) {
            if let Some(datum) = deserialize_cell(&mut cell, &column_desc.data_type)? {
                let old = self.data.get_mut(*index).unwrap().replace(datum);
                assert!(old.is_none());
            }
        } else {
            // TODO: enable this check after we migrate all executors to use cell-based table

            // return Err(ErrorCode::InternalError(format!(
            //     "found null value in storage: {:?}",
            //     Bytes::copy_from_slice(pk_with_cell_id)
            // ))
            // .into());
        }

        Ok(result)
    }

    /// Take the remaining data out of the deserializer.
    pub fn take(&mut self) -> Option<(Vec<u8>, Row)> {
        let cur_pk_bytes = self.pk_bytes.take();
        cur_pk_bytes.map(|bytes| {
            let ret = std::mem::replace(&mut self.data, vec![None; self.columns.len()]);
            (bytes, Row(ret))
        })
    }

    /// Since [`CellBasedRowDeserializer`] can be repetitively used with different inputs,
    /// it needs to be reset so that pk and data are both cleared for the next use.
    pub fn reset(&mut self) {
        self.data.iter_mut().for_each(|datum| {
            datum.take();
        });
        self.pk_bytes.take();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::ordered::serialize_pk_and_row_state;

    use super::make_cell_based_row_deserializer;

    #[test]
    fn test_cell_based_deserializer() {
        let column_ids = vec![
            ColumnId::from(5),
            ColumnId::from(3),
            ColumnId::from(7),
            ColumnId::from(1),
        ];
        let table_column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
            ColumnDesc::unnamed(column_ids[2], DataType::Int64),
            ColumnDesc::unnamed(column_ids[3], DataType::Float64),
        ];
        let pk1 = vec![0u8, 0u8, 0u8, 0u8];
        let pk2 = vec![0u8, 0u8, 0u8, 1u8];
        let pk3 = vec![0u8, 0u8, 0u8, 2u8];
        let row1 = Row(vec![
            Some(ScalarImpl::Utf8("abc".to_string())),
            None,
            Some(ScalarImpl::Int64(1500)),
            Some(ScalarImpl::Float64(233.3f64.into())),
        ]);
        let row2 = Row(vec![None, None, None, None]);
        let row3 = Row(vec![
            None,
            Some(ScalarImpl::Int32(2020)),
            Some(ScalarImpl::Int64(2021)),
            Some(ScalarImpl::Float64(666.6f64.into())),
        ]);
        let bytes1 = serialize_pk_and_row_state(&pk1, &Some(row1.clone()), &column_ids).unwrap();
        let bytes2 = serialize_pk_and_row_state(&pk2, &Some(row2.clone()), &column_ids).unwrap();
        let bytes3 = serialize_pk_and_row_state(&pk3, &Some(row3.clone()), &column_ids).unwrap();
        let bytes = [bytes1, bytes2, bytes3].concat();
        let partial_table_column_descs =
            table_column_descs.into_iter().skip(1).take(3).collect_vec();
        let mut result = vec![];
        let mut deserializer = make_cell_based_row_deserializer(partial_table_column_descs);
        for (key_bytes, value_bytes) in bytes {
            let pk_and_row = deserializer
                .deserialize(&Bytes::from(key_bytes), &Bytes::from(value_bytes.unwrap()))
                .unwrap();
            if let Some(pk_and_row) = pk_and_row {
                result.push(pk_and_row.1);
            }
        }
        let pk_and_row = deserializer.take();

        result.push(pk_and_row.unwrap().1);

        for (expected, result) in [row1, row2, row3].into_iter().zip_eq(result.into_iter()) {
            assert_eq!(
                expected.0.into_iter().skip(1).take(3).collect_vec(),
                result.0
            );
        }
    }
}
