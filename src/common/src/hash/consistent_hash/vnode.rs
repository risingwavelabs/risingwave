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

use std::hash::{BuildHasher, Hasher};

use itertools::Itertools;
use parse_display::Display;

use crate::array::{Array, ArrayImpl, DataChunk};
use crate::hash::HashCode;
use crate::row::{Row, RowExt};
use crate::types::ScalarRefImpl;
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

impl From<HashCode> for VirtualNode {
    fn from(hash_code: HashCode) -> Self {
        // Take the least significant bits of the hash code.
        // TODO: should we use the most significant bits?
        let inner = (hash_code.0 % Self::COUNT as u64) as VirtualNodeInner;
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

impl VirtualNode {
    pub fn compute_chunk<H: BuildHasher>(
        data_chunk: &DataChunk,
        keys: &[usize],
        hash_builder: H,
    ) -> Vec<VirtualNode> {
        if let Ok(idx) = keys.iter().exactly_one() && let ArrayImpl::Serial(serial_array) = data_chunk.column_at(*idx).array_ref() {
            serial_array.iter().map(|serial| HashCode::from(serial.map(|s| extract_vnode_id_from_row_id(s.as_row_id())).unwrap() as u64).to_vnode()).collect()
        } else {
            data_chunk
                .get_hash_values(keys, hash_builder)
                .into_iter()
                .map(|hash| hash.to_vnode())
                .collect_vec()
        }
    }

    pub fn compute_row(row: &impl Row) -> VirtualNode {
        // row should be projected
        let vnode = if let Ok(datum) = row.iter().exactly_one() && let Some(ScalarRefImpl::Serial(s)) = datum.as_ref() {
            HashCode::from(extract_vnode_id_from_row_id(s.as_row_id()) as u64).to_vnode()
        } else {
            row.hash(Crc32FastBuilder).to_vnode()
        };
        vnode
    }
}

/// An iterator over all virtual nodes.
pub type AllVirtualNodeIter = std::iter::Map<std::ops::Range<usize>, fn(usize) -> VirtualNode>;

impl VirtualNode {
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
