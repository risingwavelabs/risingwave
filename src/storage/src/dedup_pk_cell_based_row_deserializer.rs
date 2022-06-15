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

use itertools::Itertools;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::deserialize_column_id;
use risingwave_common::util::value_encoding::deserialize_cell;
use risingwave_common::types::DataType;

use risingwave_common::util::ordered::OrderedRowDeserializer;
use risingwave_common::util::sort_util::OrderType;

use crate::error::{StorageError, StorageResult};

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

    pk_decoder: OrderedRowDeserializer,

    pk_to_row_mapping: Vec<Option<usize>>,
}

pub fn make_cell_based_row_deserializer(
    table_column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    data_types: Vec<DataType>,
    pk_indices: Vec<usize>,
) -> GeneralCellBasedRowDeserializer {
    // let pk_indices = vec![2];
    // println!("pk_indices: {:#?}", pk_indices);
    let pk_descs = table_column_descs
        .iter()
        .enumerate()
        .filter(|(i, d)| pk_indices.contains(i))
        .map(|(i, d)| d)
        .cloned()
        .collect_vec();

    let data_types = data_types
        .into_iter()
        .enumerate()
        .filter(|(i, d)| pk_indices.contains(i))
        .map(|(i, d)| d)
        .collect_vec();
    println!("data_types: {:#?}", data_types);
    let order_types = data_types
        .iter()
        .map(|_| OrderType::Ascending) // FIXME: THIS IS A HACK
        .collect();

    // let order_types = order_types
        // .into_iter()
        // .enumerate()
        // .filter(|(i, d)| pk_indices.contains(i))
        // .map(|(i, d)| d)
        // .collect();

    let col_id_to_row_idx: HashMap<ColumnId, usize> = table_column_descs
        .iter()
        .enumerate()
        .map(|(i, d)| (d.column_id, i))
        .collect();

    let pk_to_row_mapping = pk_descs
            .iter()
            .map(|d| {
                if d.data_type.mem_cmp_eq_value_enc() {
                    col_id_to_row_idx.get(&d.column_id).copied()
                } else {
                    None
                }
            })
            .collect();

    let pk_decoder = OrderedRowDeserializer::new(data_types, order_types);
    GeneralCellBasedRowDeserializer::new(
        Arc::new(make_column_desc_index(table_column_descs)),
        pk_decoder,
        pk_to_row_mapping,
    )
}

pub fn make_column_desc_index(table_column_descs: Vec<ColumnDesc>) -> ColumnDescMapping {
    table_column_descs
        .into_iter()
        .enumerate()
        .map(|(index, d)| (d.column_id, (d, index)))
        .collect()
}

impl<Desc: Deref<Target = ColumnDescMapping>> CellBasedRowDeserializer<Desc> {
    pub fn new(column_mapping: Desc, pk_decoder: OrderedRowDeserializer, pk_to_row_mapping: Vec<Option<usize>>) -> Self {
        let num_cells = column_mapping.len();
        Self {
            columns: column_mapping,
            data: vec![None; num_cells],
            pk_bytes: None,
            pk_decoder,
            pk_to_row_mapping,
        }
    }

    fn raw_key_to_dedup_pk_row(&self, pk: &[u8]) -> Result<Row> {
        let ordered_row = self
            .pk_decoder
            .deserialize(pk)
            .map_err(StorageError::CellBasedTable)?;
        println!("deserialized row from pk: {:#?}", ordered_row);
        Ok(ordered_row.into_row())
    }

    /// Use order key to replace deduped pk datums
    /// `dedup_pk_row` is row with `None` values
    /// in place of pk datums.
    fn dedup_pk_row_to_row(&self, pk: &Row, dedup_pk_row: Row) -> Row {
        // let mut inner = vec![None; self.column_descs.len()];
        let mut inner = dedup_pk_row.0;

        // fill pk
        for (pk_idx, datum) in pk.0.iter().enumerate() {
            if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                inner[row_idx] = datum.clone();
            }
        }
        // fill dedup_pk_row
        // for (dedup_pk_idx, datum) in dedup_pk_row.0.into_iter().enumerate() {
        //     let row_idx = self.dedup_pk_row_to_row_mapping[dedup_pk_idx];
        //     inner[row_idx] = datum.clone();
        // }
        Row(inner)
    }

    pub fn deserialize(
        &mut self,
        pk_with_cell_id: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<u8>, Row)>> {
        let res = self.deserialize_inner(pk_with_cell_id, cell)?;
        if let Some((pk, row)) = res {
            let pk_row = self.raw_key_to_dedup_pk_row(&pk)?;
            Ok(Some((pk, self.dedup_pk_row_to_row(&pk_row, row))))
        } else {
            Ok(res)
        }

    }

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    fn deserialize_inner(
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

    pub fn take(
        &mut self,
    ) -> Option<(Vec<u8>, Row)> {
        let res = self.take_inner();
        if let Some((pk, row)) = res {
            let pk_row = self.raw_key_to_dedup_pk_row(&pk).unwrap(); // FIXME
            Some((pk, self.dedup_pk_row_to_row(&pk_row, row)))
        } else {
            res
        }
    }

    /// Take the remaining data out of the deserializer.
    pub fn take_inner(&mut self) -> Option<(Vec<u8>, Row)> {
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
