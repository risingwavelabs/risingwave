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

use std::sync::Arc;

use bytes::{Buf, Bytes};
use risingwave_common::array::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::value_encoding::deserialize_datum;

use super::row_serde_util::parse_raw_key_to_vnode_and_key;
use super::{ColumnDescMapping, RowDeserialize};

#[derive(Clone)]
pub struct RowBasedDeserializer {
    column_mapping: Arc<ColumnDescMapping>,
}

impl RowDeserialize for RowBasedDeserializer {
    fn create_row_deserializer(column_mapping: Arc<ColumnDescMapping>) -> Self {
        Self { column_mapping }
    }

    fn deserialize(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(risingwave_common::types::VirtualNode, Vec<u8>, Row)> {
        // todo: raw_key will be used in row-based pk dudup later.
        self.deserialize_inner(raw_key, value)
    }
}

impl RowBasedDeserializer {
    fn deserialize_inner(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(VirtualNode, Vec<u8>, Row)> {
        let raw_key = raw_key.as_ref();
        if raw_key.len() < VIRTUAL_NODE_SIZE {
            // vnode + cell_id
            return Err(ErrorCode::InternalError(format!(
                "corrupted key: {:?}",
                Bytes::copy_from_slice(raw_key)
            ))
            .into());
        }

        let (vnode, key_bytes) = parse_raw_key_to_vnode_and_key(raw_key);
        let mut origin_row =
            row_based_deserialize_inner(&self.column_mapping.all_data_types, value.as_ref())?;

        let mut output_row = Vec::with_capacity(self.column_mapping.output_index.len());
        for col_idx in &self.column_mapping.output_index {
            output_row.push(origin_row.0[*col_idx].take());
        }
        Ok((vnode, key_bytes.to_vec(), Row(output_row)))
    }
}

fn row_based_deserialize_inner(data_types: &[DataType], mut row: impl Buf) -> Result<Row> {
    // value encoding
    let mut values = Vec::with_capacity(data_types.len());
    for ty in data_types {
        values.push(deserialize_datum(&mut row, ty)?);
    }
    Ok(Row(values))
}
#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};

    use crate::row_serde::row_based_deserializer::RowBasedDeserializer;
    use crate::row_serde::row_based_serializer::RowBasedSerializer;
    use crate::row_serde::{ColumnDescMapping, RowDeserialize, RowSerialize};
    use crate::table::DEFAULT_VNODE;

    #[test]
    fn test_row_based_serialize_and_deserialize_full_row() {
        let row = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);

        let data_types = vec![
            DataType::Varchar,
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
            DataType::Interval,
        ];
        let column_ids = (0..=row.size() - 1)
            .map(|i| ColumnId::new(i as _))
            .collect_vec();

        let column_descs = data_types
            .iter()
            .zip_eq(column_ids.iter())
            .map(|(d, i)| ColumnDesc::unnamed(*i, d.clone()))
            .collect_vec();

        let mut se = RowBasedSerializer::create_row_serializer(&[], &column_descs, &column_ids);
        let (pk, value) = se.serialize(DEFAULT_VNODE, &[], row.clone()).unwrap();
        // each cell will add a is_none flag (u8)

        let mut de =
            RowBasedDeserializer::create_row_deserializer(ColumnDescMapping::new(column_descs));

        assert_eq!(value.len(), 11 + 2 + 3 + 5 + 9 + 5 + 9 + 17 + 17);
        let row1 = de.deserialize(pk, value).unwrap();
        assert_eq!(DEFAULT_VNODE, row1.0);
        assert_eq!(row, row1.2);
    }

    #[test]
    fn test_row_based_serialize_and_deserialize_partial_row() {
        let row = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);

        let data_types = vec![
            DataType::Varchar,
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
            DataType::Interval,
        ];
        let column_ids = (0..=row.size() - 1)
            .map(|i| ColumnId::new(i as _))
            .collect_vec();

        // remove the first two columns
        let output_column_ids = (2..=row.size() - 1)
            .map(|i| ColumnId::new(i as _))
            .collect_vec();
        let column_descs = data_types
            .iter()
            .zip_eq(column_ids.iter())
            .map(|(d, i)| ColumnDesc::unnamed(*i, d.clone()))
            .collect_vec();

        let mut se = RowBasedSerializer::create_row_serializer(&[], &column_descs, &column_ids);
        let (pk, value) = se.serialize(DEFAULT_VNODE, &[], row).unwrap();
        // each cell will add a is_none flag (u8)

        let mut de = RowBasedDeserializer::create_row_deserializer(ColumnDescMapping::new_partial(
            &column_descs,
            &output_column_ids,
        ));

        let partial_row = Row(vec![
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);

        assert_eq!(value.len(), 11 + 2 + 3 + 5 + 9 + 5 + 9 + 17 + 17);
        let deser_row = de.deserialize(pk, value).unwrap();
        assert_eq!(DEFAULT_VNODE, deser_row.0);
        assert_eq!(partial_row, deser_row.2);
    }
}
