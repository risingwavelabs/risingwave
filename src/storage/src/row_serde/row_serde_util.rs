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
use itertools::Itertools;
use memcomparable::from_slice;
use risingwave_common::array::{ArrayImpl, Row, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::value_encoding::{
    deserialize_datum, serialize_datum_ref, vec_serialize_datum_ref,
};

use super::ColumnDescMapping;

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Vec<u8> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    result
}

pub fn serialize_pk_with_vnode(
    pk: &Row,
    serializer: &OrderedRowSerializer,
    vnode: VirtualNode,
) -> Vec<u8> {
    let pk_bytes = serialize_pk(pk, serializer);
    [&vnode.to_be_bytes(), pk_bytes.as_slice()].concat()
}

pub fn vec_serialize_value(columns: &[&ArrayImpl], vis: Vis) -> Vec<Vec<u8>> {
    match vis {
        Vis::Bitmap(vis) => {
            let rows_num = vis.len();
            let mut buffers = vec![vec![]; rows_num];
            for c in columns {
                for (i, buffer) in buffers.iter_mut().enumerate() {
                    assert_eq!(c.len(), rows_num);
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        if vis.is_set_unchecked(i) {
                            serialize_datum_ref(&c.value_at_unchecked(i), buffer);
                        }
                    }
                }
            }
            buffers
        }
        Vis::Compact(rows_num) => {
            let mut buffers = vec![vec![]; rows_num];
            for c in columns {
                assert_eq!(c.len(), rows_num);
                for (i, buffer) in buffers.iter_mut().enumerate() {
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        serialize_datum_ref(&c.value_at_unchecked(i), buffer);
                    }
                }
            }
            buffers
        }
    }
}

pub fn deserialize_column_id(bytes: &[u8]) -> Result<ColumnId> {
    let column_id = from_slice::<u32>(bytes)? ^ (1 << 31);
    Ok((column_id as i32).into())
}

pub fn parse_raw_key_to_vnode_and_key(raw_key: &[u8]) -> (VirtualNode, &[u8]) {
    let (vnode_bytes, key_bytes) = raw_key.split_at(VIRTUAL_NODE_SIZE);
    let vnode = VirtualNode::from_be_bytes(vnode_bytes.try_into().unwrap());
    (vnode, key_bytes)
}

/// used for batch table deserialize
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
