// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use parse_display::Display;

use crate::array::{Array, ArrayImpl, DataChunk};
use crate::hash::Crc32HashCode;
use crate::row::{Row, RowExt};
use crate::types::{DataType, ScalarRefImpl};
use crate::util::hash_util::Crc32FastBuilder;
use crate::util::row_id::extract_vnode_id_from_row_id;

/// Parallel unit is the minimal scheduling unit.
// TODO: make it a newtype
pub type ParallelUnitId = u32;

/// `VirtualNode` (a.k.a. Vnode) is a minimal partition that a set of keys belong to. It is used for
/// consistent hashing.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
#[display("{0}")]
pub struct VirtualNode(VirtualNodeInner);

/// The internal representation of a virtual node id.
type VirtualNodeInner = u16;
static_assertions::const_assert!(VirtualNodeInner::BITS >= VirtualNode::BITS as u32);

impl From<Crc32HashCode> for VirtualNode {
    fn from(hash_code: Crc32HashCode) -> Self {
        // Take the least significant bits of the hash code.
        // TODO: should we use the most significant bits?
        let inner = (hash_code.value() % Self::COUNT as u64) as VirtualNodeInner;
        VirtualNode(inner)
    }
}

impl VirtualNode {
    /// The number of bits used to represent a virtual node.
    ///
    /// Note: Not all bits of the inner representation are used. One should rely on this constant
    /// to determine the count of virtual nodes.
    pub const BITS: usize = 8;
    /// The total count of virtual nodes.
    pub const COUNT: usize = 1 << Self::BITS;
    /// The size of a virtual node in bytes, in memory or serialized representation.
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// An iterator over all virtual nodes.
pub type AllVirtualNodeIter = std::iter::Map<std::ops::Range<usize>, fn(usize) -> VirtualNode>;

impl VirtualNode {
    /// We may use VirtualNode as a datum in a stream, or store it as a column.
    /// Hence this reifies it as a RW datatype.
    pub const RW_TYPE: DataType = DataType::Int16;
    /// The maximum value of the virtual node.
    pub const MAX: VirtualNode = VirtualNode::from_index(Self::COUNT - 1);
    /// The minimum (zero) value of the virtual node.
    pub const ZERO: VirtualNode = VirtualNode::from_index(0);

    /// Creates a virtual node from the `usize` index.
    pub const fn from_index(index: usize) -> Self {
        debug_assert!(index < Self::COUNT);
        Self(index as _)
    }

    /// Returns the `usize` the virtual node used for indexing.
    pub const fn to_index(self) -> usize {
        self.0 as _
    }

    /// Creates a virtual node from the given scalar representation. Used by `VNODE` expression.
    pub const fn from_scalar(scalar: i16) -> Self {
        debug_assert!((scalar as usize) < Self::COUNT);
        Self(scalar as _)
    }

    /// Returns the scalar representation of the virtual node. Used by `VNODE` expression.
    pub const fn to_scalar(self) -> i16 {
        self.0 as _
    }

    /// Creates a virtual node from the given big-endian bytes representation.
    pub const fn from_be_bytes(bytes: [u8; Self::SIZE]) -> Self {
        let inner = VirtualNodeInner::from_be_bytes(bytes);
        debug_assert!((inner as usize) < Self::COUNT);
        Self(inner)
    }

    /// Returns the big-endian bytes representation of the virtual node.
    pub const fn to_be_bytes(self) -> [u8; Self::SIZE] {
        self.0.to_be_bytes()
    }

    /// Iterates over all virtual nodes.
    pub fn all() -> AllVirtualNodeIter {
        (0..Self::COUNT).map(Self::from_index)
    }
}

impl VirtualNode {
    // `compute_chunk` is used to calculate the `VirtualNode` for the columns in the
    // chunk. When only one column is provided and its type is `Serial`, we consider the column to
    // be the one that contains RowId, and use a special method to skip the calculation of Hash
    // and directly extract the `VirtualNode` from `RowId`.
    pub fn compute_chunk(data_chunk: &DataChunk, keys: &[usize]) -> Vec<VirtualNode> {
        if let Ok(idx) = keys.iter().exactly_one()
            && let ArrayImpl::Serial(serial_array) = &**data_chunk.column_at(*idx)
        {
            return serial_array
                .iter()
                .map(|serial| extract_vnode_id_from_row_id(serial.unwrap().as_row_id()))
                .collect();
        }

        data_chunk
            .get_hash_values(keys, Crc32FastBuilder)
            .into_iter()
            .map(|hash| hash.into())
            .collect()
    }

    // `compute_row` is used to calculate the `VirtualNode` for the corresponding column in a `Row`.
    // Similar to `compute_chunk`, it also contains special handling for serial columns.
    pub fn compute_row(row: impl Row, indices: &[usize]) -> VirtualNode {
        let project = row.project(indices);
        if let Ok(Some(ScalarRefImpl::Serial(s))) = project.iter().exactly_one().as_ref() {
            return extract_vnode_id_from_row_id(s.as_row_id());
        }

        project.hash(Crc32FastBuilder).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::DataChunkTestExt;
    use crate::row::OwnedRow;
    use crate::types::ScalarImpl;
    use crate::util::row_id::RowIdGenerator;

    #[test]
    fn test_serial_key_chunk() {
        let mut gen = RowIdGenerator::new([VirtualNode::from_index(100)]);
        let chunk = format!(
            "SRL I
             {} 1
             {} 2",
            gen.next(),
            gen.next(),
        );

        let chunk = DataChunk::from_pretty(chunk.as_str());
        let vnodes = VirtualNode::compute_chunk(&chunk, &[0]);

        assert_eq!(
            vnodes.as_slice(),
            &[VirtualNode::from_index(100), VirtualNode::from_index(100)]
        );
    }

    #[test]
    fn test_serial_key_row() {
        let mut gen = RowIdGenerator::new([VirtualNode::from_index(100)]);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Serial(gen.next().into())),
            Some(ScalarImpl::Int64(12345)),
        ]);

        let vnode = VirtualNode::compute_row(&row, &[0]);

        assert_eq!(vnode, VirtualNode::from_index(100));
    }

    #[test]
    fn test_serial_key_chunk_multiple_vnodes() {
        let mut gen = RowIdGenerator::new([100, 200].map(VirtualNode::from_index));
        let chunk = format!(
            "SRL I
             {} 1
             {} 2
             {} 3
             {} 4",
            gen.next(),
            gen.next(),
            gen.next(),
            gen.next(),
        );

        let chunk = DataChunk::from_pretty(chunk.as_str());
        let vnodes = VirtualNode::compute_chunk(&chunk, &[0]);

        assert_eq!(
            vnodes.as_slice(),
            &[
                VirtualNode::from_index(100),
                VirtualNode::from_index(200),
                VirtualNode::from_index(100),
                VirtualNode::from_index(200),
            ]
        );
    }
}
