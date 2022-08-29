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

use bytes::Buf;
use memcomparable::from_slice;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::value_encoding::{deserialize_datum, serialize_datum};

use super::ColumnDescMapping;

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Vec<u8> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    result
}

pub fn serialize_column_id(column_id: &ColumnId) -> [u8; 4] {
    let id = column_id.get_id();
    (id as u32 ^ (1 << 31)).to_be_bytes()
}

pub fn deserialize_column_id(bytes: &[u8]) -> Result<ColumnId> {
    let column_id = from_slice::<u32>(bytes)? ^ (1 << 31);
    Ok((column_id as i32).into())
}

pub fn serialize_pk_and_column_id(pk_buf: &[u8], col_id: &ColumnId) -> Result<Vec<u8>> {
    Ok([pk_buf, serialize_column_id(col_id).as_slice()].concat())
}

pub fn parse_raw_key_to_vnode_and_key(raw_key: &[u8]) -> (VirtualNode, &[u8]) {
    let (vnode_bytes, key_bytes) = raw_key.split_at(VIRTUAL_NODE_SIZE);
    let vnode = VirtualNode::from_be_bytes(vnode_bytes.try_into().unwrap());
    (vnode, key_bytes)
}

/// used for streaming table serialize
/// todo(wcy-fdu): remove `RowSerde` trait after all executors using streaming table.
pub fn serialize(row: Row) -> Result<Vec<u8>> {
    let mut value_bytes = vec![];
    for cell in &row.0 {
        value_bytes.extend(serialize_datum(cell)?);
    }
    let res = value_bytes;
    Ok(res)
}

pub fn batch_deserialize(
    column_mapping: Arc<ColumnDescMapping>,
    value: impl AsRef<[u8]>,
) -> Result<Row> {
    let mut origin_row = streaming_deserialize(&column_mapping.all_data_types, value.as_ref())?;

    let mut output_row = Vec::with_capacity(column_mapping.output_index.len());
    for col_idx in &column_mapping.output_index {
        output_row.push(origin_row.0[*col_idx].take());
    }
    Ok(Row(output_row))
}

/// used for streaming table deserialize
pub fn streaming_deserialize(data_types: &[DataType], mut row: impl Buf) -> Result<Row> {
    // value encoding
    let mut values = Vec::with_capacity(data_types.len());
    for ty in data_types {
        values.push(deserialize_datum(&mut row, ty)?);
    }

    Ok(Row(values))
}
