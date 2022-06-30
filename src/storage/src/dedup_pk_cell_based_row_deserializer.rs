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
        Self {
            pk_deserializer,
            inner,
            pk_to_row_mapping,
        }
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
        let raw_result = self.inner.deserialize_without_vnode(raw_key, cell)?;
        self.replace_dedupped_datums(raw_result)
    }

    /// Take the remaining data out of the deserializer.
    pub fn take(&mut self) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        let raw_result = self.inner.take();
        self.replace_dedupped_datums(raw_result)
    }

    /// Since [`DedupPkCellBasedRowDeserializer`] can be repetitively used with different inputs,
    /// it needs to be reset so that pk and data are both cleared for the next use.
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
    use risingwave_common::util::ordered::{serialize_pk_and_row, OrderedRowSerializer};
    use risingwave_common::util::sort_util::OrderType;

    use super::DedupPkCellBasedRowDeserializer;
    use crate::cell_based_row_deserializer::ColumnDescMapping;

    #[test]
    fn test_cell_based_deserializer() {
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
        // let table_ordered_column_descs =
        //     table_column_descs.iter().zip_eq(order_types.iter()).map(
        //         |(column_desc, order_type)| {
        //             OrderedColumnDesc {
        //                 column_desc: column_desc.clone(),
        //                 order: order_type.clone(),
        //             }
        //         }
        //     );
        //                                                             vec![
        //     OrderedColumnDesc { column_desc: table_column_descs[0].clone(), order:
        // order_types[0]},     OrderedColumnDesc { column_desc:
        // table_column_descs[1].clone(), order: order_types[1]},     OrderedColumnDesc {
        // column_desc: table_column_descs[2].clone(), order: order_types[2]},
        //     OrderedColumnDesc { column_desc: table_column_descs[3].clone(), order:
        // order_types[3]}, ];

        let pk_serializer = OrderedRowSerializer::new(vec![OrderType::Ascending]);

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

        let bytes1 = serialize_pk_and_row(&pk1, &dedup_row1, &dedup_column_ids).unwrap();
        let bytes2 = serialize_pk_and_row(&pk2, &dedup_row2, &dedup_column_ids).unwrap();
        let bytes3 = serialize_pk_and_row(&pk3, &dedup_row3, &dedup_column_ids).unwrap();
        let bytes = [bytes1, bytes2, bytes3]
            .concat()
            .into_iter()
            .flatten()
            .collect_vec();

        // ------- Init dedup pk cell based row deserializer
        let column_mapping = ColumnDescMapping::new(table_column_descs.clone());

        let pk_descs = vec![OrderedColumnDesc {
            column_desc: table_column_descs[0].clone(),
            order: order_types[0],
        }];

        let mut deserializer = DedupPkCellBasedRowDeserializer::new(column_mapping, &pk_descs);

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

        let expected = vec![row1, row2, row3];
        assert_eq!(expected, actual);
    }
}
