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

use fixedbitset::FixedBitSet;

/// A set of deduplicated column indices.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexSet {
    bitset: FixedBitSet,
}

impl IndexSet {
    /// Create an empty set.
    pub fn empty() -> Self {
        Self {
            bitset: FixedBitSet::new(),
        }
    }

    /// Get the number of indices.
    pub fn len(&self) -> usize {
        self.bitset.count_ones(..)
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.bitset.is_clear()
    }

    /// Check if the given index is contained.
    pub fn contains(&self, index: usize) -> bool {
        self.bitset.contains(index)
    }

    /// Iterator over indices.
    pub fn indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.bitset.ones()
    }

    /// Convert to `Vec<usize>`.
    pub fn to_vec(&self) -> Vec<usize> {
        self.indices().collect()
    }

    /// Convert to `FixedBitSet`.
    pub fn to_bitset(&self) -> FixedBitSet {
        self.bitset.clone()
    }

    /// Iterator over indices as `u32`s. Please only use this when converting to protobuf.
    pub fn indices_as_u32(&self) -> impl Iterator<Item = u32> + '_ {
        self.bitset.ones().map(|i| i as u32)
    }

    /// Convert to `Vec<u32>`. Please only use this when converting to protobuf.
    pub fn to_vec_as_u32(&self) -> Vec<u32> {
        self.indices_as_u32().collect()
    }

    /// Insert an index.
    pub fn insert(&mut self, index: usize) {
        self.bitset.extend_one(index)
    }
}

impl Extend<usize> for IndexSet {
    fn extend<T: IntoIterator<Item = usize>>(&mut self, iter: T) {
        self.bitset.extend(iter)
    }
}

impl From<Vec<usize>> for IndexSet {
    fn from(vec: Vec<usize>) -> Self {
        vec.into_iter().collect()
    }
}

impl FromIterator<usize> for IndexSet {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Self {
        IndexSet {
            bitset: iter.into_iter().collect(),
        }
    }
}
