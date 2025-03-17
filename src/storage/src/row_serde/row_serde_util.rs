// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_hummock_sdk::key::TableKey;

pub fn serialize_pk(pk: impl Row, serializer: &OrderedRowSerde) -> Bytes {
    let mut buf = BytesMut::with_capacity(pk.len());
    pk.memcmp_serialize_into(serializer, &mut buf);
    buf.freeze()
}

pub fn serialize_pk_with_vnode(
    pk: impl Row,
    serializer: &OrderedRowSerde,
    vnode: VirtualNode,
) -> TableKey<Bytes> {
    let mut buffer = BytesMut::new();
    buffer.put_slice(&vnode.to_be_bytes()[..]);
    pk.memcmp_serialize_into(serializer, &mut buffer);
    TableKey(buffer.freeze())
}

pub fn deserialize_pk_with_vnode(
    key: &[u8],
    deserializer: &OrderedRowSerde,
) -> memcomparable::Result<(VirtualNode, OwnedRow)> {
    let vnode = VirtualNode::from_be_bytes(key[0..VirtualNode::SIZE].try_into().unwrap());
    let pk = deserializer.deserialize(&key[VirtualNode::SIZE..])?;
    Ok((vnode, pk))
}
