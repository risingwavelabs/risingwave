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

use parse_display::Display;

use crate::hash::HashCode;

/// Parallel unit is the minimal scheduling unit.
pub type ParallelUnitId = u32;
pub type VnodeMapping = Vec<ParallelUnitId>;

/// `VirtualNode` (a.k.a. VNode) is a minimal partition that a set of keys belong to. It is used for
/// consistent hashing.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
pub struct VirtualNode(VirtualNodeInner);

type VirtualNodeInner = u8;
static_assertions::const_assert!(VirtualNodeInner::BITS >= VirtualNode::BITS as u32);

impl From<HashCode> for VirtualNode {
    fn from(hash_code: HashCode) -> Self {
        // Take the most significant bits of the hash code.
        let inner = (hash_code.0 >> (u64::BITS - VirtualNode::BITS as u32)) as VirtualNodeInner;
        VirtualNode(inner)
    }
}

impl VirtualNode {
    /// The number of bits used to represent a virtual node.
    pub const BITS: usize = 8;
    /// The total count of virtual nodes.
    pub const COUNT: usize = 1 << Self::BITS;
    /// The size of a virtual node in bytes, in memory or serialized representation.
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

impl VirtualNode {
    /// The maximum value of the virtual node.
    pub const MAX: VirtualNode = VirtualNode::from_index(Self::COUNT - 1);
    /// The minimum (zero) value of the virtual node.
    pub const ZERO: VirtualNode = VirtualNode::from_index(0);

    /// Creates a virtual node from the `usize` index.
    pub const fn from_index(index: usize) -> Self {
        VirtualNode(index as _)
    }

    /// Returns the `usize` the virtual node used for indexing.
    pub const fn to_index(self) -> usize {
        self.0 as _
    }

    /// Creates a virtual node from the given scalar representation. Used by `VNODE` expression.
    pub const fn from_scalar(scalar: i16) -> Self {
        VirtualNode(scalar as _)
    }

    /// Returns the scalar representation of the virtual node. Used by `VNODE` expression.
    pub const fn to_scalar(self) -> i16 {
        self.0 as _
    }

    /// Creates a virtual node from the given big-endian bytes representation.
    pub const fn from_be_bytes(bytes: [u8; Self::SIZE]) -> Self {
        Self(VirtualNodeInner::from_be_bytes(bytes))
    }

    /// Returns the big-endian bytes representation of the virtual node.
    pub const fn to_be_bytes(self) -> [u8; Self::SIZE] {
        self.0.to_be_bytes()
    }

    /// Returns the next virtual node.
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Iterates over all virtual nodes.
    pub fn all() -> impl Iterator<Item = Self> {
        (0..Self::COUNT).map(Self::from_index)
    }
}
