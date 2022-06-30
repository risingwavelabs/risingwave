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


use std::ops::Deref;



use risingwave_common::array::Row;
use risingwave_common::catalog::{OrderedColumnDesc};
use risingwave_common::error::{Result};
use risingwave_common::types::{Datum, VirtualNode};
use risingwave_common::util::ordered::{OrderedRowDeserializer};



use crate::cell_based_row_deserializer::{CellBasedRowDeserializer, ColumnDescMapping};


#[derive(Clone)]
pub struct DedupPkCellBasedRowDeserializer<Desc: Deref<Target = ColumnDescMapping>> {
    pk_deserializer: OrderedRowDeserializer,
    inner: CellBasedRowDeserializer<Desc>,

    // Maps pk fields with:
    // 1. same value and memcomparable encoding,
    // 2. corresponding row positions. e.g. _row_id is unlikely to be part of selected row.
    pk_to_row_mapping: Vec<Option<usize>>,
}

impl<Desc: Deref<Target = ColumnDescMapping>> DedupPkCellBasedRowDeserializer<Desc> {
    pub fn new(column_mapping: Desc, pk_descs: &[OrderedColumnDesc]) -> Self {
        let (pk_data_types, pk_order_types) = pk_descs
            .iter()
            .map(|ordered_desc| {
                (
                    ordered_desc.column_desc.data_type.clone(),
                    ordered_desc.order,
                )
            })
            .unzip();
        let pk_deserializer = OrderedRowDeserializer::new(pk_data_types, pk_order_types);

        let pk_to_row_mapping = pk_descs
            .iter()
            .map(|d| {
                let column_desc = &d.column_desc;
                if column_desc.data_type.mem_cmp_eq_value_enc() {
                    column_mapping
                        .get(column_desc.column_id)
                        .map(|(_, index)| index)
                } else {
                    None
                }
            })
            .collect();

        let inner = CellBasedRowDeserializer::new(column_mapping);
        Self { pk_deserializer, inner, pk_to_row_mapping }
    }

    fn replace_dedupped_datums_into_row(&self, pk_datums: Vec<Datum>, row: Row) -> Row {
        let Row(mut row_inner) = row;
        for (pk_idx, datum) in pk_datums.into_iter().enumerate() {
            if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                row_inner[row_idx] = datum;
            }
        }
        Row(row_inner)
    }

    fn replace_dedupped_datums(
        &self,
        raw_result: Option<(VirtualNode, Vec<u8>, Row)>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        if let Some((_vnode, pk, row)) = raw_result {
            let pk_datums = self.pk_deserializer.deserialize(&pk)?;
            Ok(Some((
                _vnode,
                pk,
                self.replace_dedupped_datums_into_row(pk_datums.into_vec(), row),
            )))
        } else {
            Ok(None)
        }
    }

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    pub fn deserialize(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        let raw_result = self.inner.deserialize(raw_key, cell)?;
        self.replace_dedupped_datums(raw_result)
    }

    // TODO: remove this once we refactored lookup in delta join with cell-based table
    pub fn deserialize_without_vnode(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        self.inner.deserialize_without_vnode(raw_key, cell)
    }

    /// Take the remaining data out of the deserializer.
    pub fn take(&mut self) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        Ok(self.inner.take())
    }

    /// Since [`CellBasedRowDeserializer`] can be repetitively used with different inputs,
    /// it needs to be reset so that pk and data are both cleared for the next use.
    pub fn reset(&mut self) {
        self.inner.reset()
    }
}
