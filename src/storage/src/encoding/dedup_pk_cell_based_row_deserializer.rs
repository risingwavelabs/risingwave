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
use risingwave_common::catalog::OrderedColumnDesc;
use risingwave_common::error::Result;
use risingwave_common::types::{Datum, VirtualNode};
use risingwave_common::util::ordered::OrderedRowDeserializer;

use super::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::encoding::{ColumnDescMapping, Decoding};

/// Similar to [`CellBasedRowDeserializer`], but for dedup pk cell encoding.
#[derive(Clone)]
pub struct DedupPkCellBasedRowDeserializer<'a, Desc: Deref<Target = ColumnDescMapping>> {
    pk_deserializer: &'a OrderedRowDeserializer,
    inner: CellBasedRowDeserializer<Desc>,

    // Maps pk fields with:
    // 1. same value and memcomparable encoding,
    // 2. corresponding row positions.
    pk_to_row_mapping: Vec<Option<usize>>,
}

pub fn create_pk_deserializer_from_pk_descs(pk_descs: &[OrderedColumnDesc]) -> OrderedRowDeserializer {
    let (pk_data_types, pk_order_types) = pk_descs
        .iter()
        .map(|ordered_desc| {
            (
                ordered_desc.column_desc.data_type.clone(),
                ordered_desc.order,
            )
        })
        .unzip();
    OrderedRowDeserializer::new(pk_data_types, pk_order_types)
}

pub fn create_pk_to_row_mapping(pk_descs: &[OrderedColumnDesc], column_mapping: impl Deref<Target = ColumnDescMapping>) -> Vec<Option<usize>> {
    pk_descs
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
        .collect()
}

impl<'a, Desc: Deref<Target = ColumnDescMapping>> DedupPkCellBasedRowDeserializer<'a, Desc> {
    /// Create a [`DedupPkCellBasedRowDeserializer`].
    pub fn new(
        pk_deserializer: &'a OrderedRowDeserializer,
        column_mapping: Desc,
        pk_to_row_mapping: Vec<Option<usize>>
    ) -> Self {
        let inner = CellBasedRowDeserializer::new(column_mapping);
        Self {
            pk_deserializer,
            inner,
            pk_to_row_mapping,
        }
    }

    /// Places dedupped pk `Datum`s back into `Row`.
    fn replace_dedupped_datums_into_row(&self, pk_datums: Vec<Datum>, row: Row) -> Row {
        let Row(mut row_inner) = row;
        for (pk_idx, datum) in pk_datums.into_iter().enumerate() {
            if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                row_inner[row_idx] = datum;
            }
        }
        Row(row_inner)
    }

    /// Replaces missing `Datum`s in raw deserialized results.
    fn replace_dedupped_datums(
        &self,
        raw_result: Option<(VirtualNode, Vec<u8>, Row)>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        if let Some((vnode, pk, row)) = raw_result {
            let pk_datums = self.pk_deserializer.deserialize(&pk)?;
            Ok(Some((
                vnode,
                pk,
                self.replace_dedupped_datums_into_row(pk_datums.into_vec(), row),
            )))
        } else {
            Ok(None)
        }
    }

    /// Functionally the same as [`CellBasedRowDeserializer::deserialize`],
    /// but with dedup pk encoding.
    pub fn deserialize(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        let raw_result = self.inner.deserialize(raw_key, cell)?;
        self.replace_dedupped_datums(raw_result)
    }

    /// Functionally the same as [`CellBasedRowDeserializer::deserialize_without_vnode`],
    /// but with dedup pk encoding.
    // TODO: remove this once we refactored lookup in delta join with cell-based table
    pub fn deserialize_without_vnode(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        let raw_result = self.inner.deserialize_without_vnode(raw_key, cell)?;
        self.replace_dedupped_datums(raw_result)
    }

    /// Functionally the same as [`CellBasedRowDeserializer::take`],
    /// but with dedup pk encoding.
    pub fn take(&mut self) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        let raw_result = self.inner.take();
        self.replace_dedupped_datums(raw_result)
    }

    /// Functionally the same as [`CellBasedRowDeserializer::reset`],
    /// but with dedup pk encoding.
    pub fn reset(&mut self) {
        self.inner.reset()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc};
    use risingwave_common::types::{DataType, ScalarImpl, VIRTUAL_NODE_SIZE};
    use risingwave_common::util::ordered::OrderedRowSerializer;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::encoding::cell_based_encoding_util::serialize_pk_and_row;
    use crate::encoding::ColumnDescMapping;

    #[test]
    fn test_cell_based_deserializer() {
        // ----------- specify table schema
        // let pk_indices = [0]; // For reference to know which columns are dedupped.
        let column_ids = vec![
            ColumnId::from(5),
            ColumnId::from(3),
            ColumnId::from(7),
            ColumnId::from(1),
        ];
        let dedup_column_ids = vec![ColumnId::from(3), ColumnId::from(7), ColumnId::from(1)];
        let table_column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
            ColumnDesc::unnamed(column_ids[2], DataType::Int64),
            ColumnDesc::unnamed(column_ids[3], DataType::Float64),
        ];
        let order_types = vec![
            OrderType::Ascending,
            OrderType::Ascending,
            OrderType::Ascending,
            OrderType::Ascending,
        ];

        let pk_serializer = OrderedRowSerializer::new(vec![OrderType::Ascending]);

        // ----------- create data: pk + row
        let row1 = Row(vec![
            Some(ScalarImpl::Int32(2018)), // pk cannot be null
            Some(ScalarImpl::Utf8("abc".to_string())),
            Some(ScalarImpl::Int64(1500)),
            Some(ScalarImpl::Float64(233.3f64.into())),
        ]);
        let dedup_row1 = Row(vec![
            Some(ScalarImpl::Utf8("abc".to_string())),
            Some(ScalarImpl::Int64(1500)),
            Some(ScalarImpl::Float64(233.3f64.into())),
        ]);
        let mut pk1 = vec![0; VIRTUAL_NODE_SIZE];
        pk_serializer.serialize(&Row(vec![Some(ScalarImpl::Int32(2018))]), &mut pk1);

        let row2 = Row(vec![Some(ScalarImpl::Int32(2019)), None, None, None]);
        let dedup_row2 = Row(vec![None, None, None]);
        let mut pk2 = vec![1; VIRTUAL_NODE_SIZE];
        pk_serializer.serialize(&Row(vec![Some(ScalarImpl::Int32(2019))]), &mut pk2);

        let row3 = Row(vec![
            Some(ScalarImpl::Int32(2020)),
            None,
            Some(ScalarImpl::Int64(2021)),
            Some(ScalarImpl::Float64(666.6f64.into())),
        ]);
        let dedup_row3 = Row(vec![
            None,
            Some(ScalarImpl::Int64(2021)),
            Some(ScalarImpl::Float64(666.6f64.into())),
        ]);
        let mut pk3 = vec![2; VIRTUAL_NODE_SIZE];
        pk_serializer.serialize(&Row(vec![Some(ScalarImpl::Int32(2020))]), &mut pk3);

        // ----------- serialize pk and row
        let bytes1 = serialize_pk_and_row(&pk1, &dedup_row1, &dedup_column_ids).unwrap();
        let bytes2 = serialize_pk_and_row(&pk2, &dedup_row2, &dedup_column_ids).unwrap();
        let bytes3 = serialize_pk_and_row(&pk3, &dedup_row3, &dedup_column_ids).unwrap();
        let bytes = [bytes1, bytes2, bytes3]
            .concat()
            .into_iter()
            .flatten()
            .collect_vec();

        // ----------- init dedup pk cell based row deserializer
        let column_mapping = ColumnDescMapping::new(table_column_descs.clone());

        let pk_descs = vec![OrderedColumnDesc {
            column_desc: table_column_descs[0].clone(),
            order: order_types[0],
        }];

        let pk_deserializer = create_pk_deserializer_from_pk_descs(&pk_descs);

        let pk_to_row_mapping = create_pk_to_row_mapping(&pk_descs, column_mapping.clone());

        let mut deserializer = DedupPkCellBasedRowDeserializer::new(&pk_deserializer, column_mapping, pk_to_row_mapping);

        // ----------- deserialize pk and row
        let mut actual = vec![];
        for (key_bytes, value_bytes) in bytes {
            let pk_and_row = deserializer
                .deserialize(&Bytes::from(key_bytes), &Bytes::from(value_bytes))
                .unwrap();
            if let Some((_vnode, _pk, row)) = pk_and_row {
                actual.push(row);
            }
        }
        let (_vnode, _pk, row) = deserializer.take().unwrap().unwrap();
        actual.push(row);

        // ----------- verify results
        let expected = vec![row1, row2, row3];
        assert_eq!(expected, actual);
    }
}
