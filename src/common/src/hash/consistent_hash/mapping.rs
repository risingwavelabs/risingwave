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

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Index;

use educe::Educe;
use itertools::Itertools;
use risingwave_pb::common::{ParallelUnit, ParallelUnitMapping as ParallelUnitMappingProto};
use risingwave_pb::stream_plan::ActorMapping as ActorMappingProto;

use super::bitmap::VnodeBitmapExt;
use super::vnode::{ParallelUnitId, VirtualNode};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::util::compress::compress_data;
use crate::util::iter_util::ZipEqDebug;

// TODO: find a better place for this.
pub type ActorId = u32;

/// Trait for items that can be used as keys in [`VnodeMapping`].
pub trait VnodeMappingItem {
    /// The type of the item.
    ///
    /// Currently, there are two types of items: [`ParallelUnitId`] and [`ActorId`]. We don't use
    /// them directly as the generic parameter because they're the same type aliases.
    type Item: Copy + Ord + Hash + Debug;
}

/// Exapnded mapping from virtual nodes to items, essentially a vector of items and can be indexed
/// by virtual nodes.
pub type ExpandedMapping<T> = Vec<<T as VnodeMappingItem>::Item>;

/// Generic mapping from virtual nodes to items.
///
/// The representation is compressed as described in [`compress_data`], which is optimized for the
/// mapping with a small number of items and good locality.
#[derive(Educe)]
#[educe(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VnodeMapping<T: VnodeMappingItem> {
    original_indices: Vec<u32>,
    data: Vec<T::Item>,
}

#[expect(
    clippy::len_without_is_empty,
    reason = "empty vnode mapping makes no sense"
)]
impl<T: VnodeMappingItem> VnodeMapping<T> {
    /// Create a uniform vnode mapping with a **set** of items.
    ///
    /// For example, if `items` is `[0, 1, 2]`, and the total vnode count is 10, we'll generate
    /// mapping like `[0, 0, 0, 0, 1, 1, 1, 2, 2, 2]`.
    pub fn new_uniform(items: impl ExactSizeIterator<Item = T::Item>) -> Self {
        // If the number of items is greater than the total vnode count, no vnode will be mapped to
        // some items and the mapping will be invalid.
        assert!(items.len() <= VirtualNode::COUNT);

        let mut original_indices = Vec::with_capacity(items.len());
        let mut data = Vec::with_capacity(items.len());

        let hash_shard_size = VirtualNode::COUNT / items.len();
        let mut one_more_count = VirtualNode::COUNT % items.len();
        let mut init_bound = 0;

        for item in items {
            let vnode_count = if one_more_count > 0 {
                one_more_count -= 1;
                hash_shard_size + 1
            } else {
                hash_shard_size
            };
            init_bound += vnode_count;

            original_indices.push(init_bound as u32 - 1);
            data.push(item);
        }

        // Assert that there's no duplicated items.
        debug_assert_eq!(data.iter().duplicates().count(), 0);

        Self {
            original_indices,
            data,
        }
    }

    /// Create a vnode mapping where all vnodes are mapped to the same single item.
    pub fn new_single(item: T::Item) -> Self {
        Self::new_uniform(std::iter::once(item))
    }

    /// The length of the vnode in this mapping, typically [`VirtualNode::COUNT`].
    pub fn len(&self) -> usize {
        self.original_indices
            .last()
            .map(|&i| i as usize + 1)
            .unwrap_or(0)
    }

    /// Get the item mapped to the given `vnode` by binary search.
    ///
    /// Note: to achieve better mapping performance, one should convert the mapping to the
    /// [`ExpandedMapping`] first and directly access the item by index.
    pub fn get(&self, vnode: VirtualNode) -> T::Item {
        self[vnode]
    }

    /// Get the item matched by the virtual nodes indicated by high bits in the given `bitmap`.
    /// Returns `None` if the no virtual node is set in the bitmap.
    pub fn get_matched(&self, bitmap: &Bitmap) -> Option<T::Item> {
        bitmap
            .iter_vnodes()
            .next() // only need to check the first one
            .map(|v| self.get(v))
    }

    /// Iterate over all items in this mapping, in the order of vnodes.
    pub fn iter(&self) -> impl Iterator<Item = T::Item> + '_ {
        self.data
            .iter()
            .copied()
            .zip_eq_debug(
                std::iter::once(0)
                    .chain(self.original_indices.iter().copied().map(|i| i + 1))
                    .tuple_windows()
                    .map(|(a, b)| (b - a) as usize),
            )
            .flat_map(|(item, c)| std::iter::repeat(item).take(c))
    }

    /// Iterate over all vnode-item pairs in this mapping.
    pub fn iter_with_vnode(&self) -> impl Iterator<Item = (VirtualNode, T::Item)> + '_ {
        self.iter()
            .enumerate()
            .map(|(v, item)| (VirtualNode::from_index(v), item))
    }

    /// Iterate over all unique items in this mapping. The order is deterministic.
    pub fn iter_unique(&self) -> impl Iterator<Item = T::Item> + '_ {
        // Note: we can't ensure there's no duplicated items in the `data` after some scaling.
        self.data.iter().copied().sorted().dedup()
    }

    /// Returns the item if it's the only item in this mapping, otherwise returns `None`.
    pub fn to_single(&self) -> Option<T::Item> {
        self.data.iter().copied().dedup().exactly_one().ok()
    }

    /// Convert this vnode mapping to a mapping from items to bitmaps, where each bitmap represents
    /// the vnodes mapped to the item.
    pub fn to_bitmaps(&self) -> HashMap<T::Item, Bitmap> {
        let mut vnode_bitmaps = HashMap::new();

        for (vnode, item) in self.iter_with_vnode() {
            vnode_bitmaps
                .entry(item)
                .or_insert_with(|| BitmapBuilder::zeroed(VirtualNode::COUNT))
                .set(vnode.to_index(), true);
        }

        vnode_bitmaps
            .into_iter()
            .map(|(item, b)| (item, b.finish()))
            .collect()
    }

    /// Create a vnode mapping from the given mapping from items to bitmaps, where each bitmap
    /// represents the vnodes mapped to the item.
    pub fn from_bitmaps(bitmaps: &HashMap<T::Item, Bitmap>) -> Self {
        let mut items = vec![None; VirtualNode::COUNT];

        for (&item, bitmap) in bitmaps {
            assert_eq!(bitmap.len(), VirtualNode::COUNT);
            for idx in bitmap.iter_ones() {
                if let Some(prev) = items[idx].replace(item) {
                    panic!("mapping at index `{idx}` is set to both `{prev:?}` and `{item:?}`");
                }
            }
        }

        let items = items
            .into_iter()
            .enumerate()
            .map(|(i, o)| o.unwrap_or_else(|| panic!("mapping at index `{i}` is not set")))
            .collect_vec();
        Self::from_expanded(&items)
    }

    /// Create a vnode mapping from the expanded slice of items with length [`VirtualNode::COUNT`].
    pub fn from_expanded(items: &[T::Item]) -> Self {
        assert_eq!(items.len(), VirtualNode::COUNT);
        let (original_indices, data) = compress_data(items);
        Self {
            original_indices,
            data,
        }
    }

    /// Convert this vnode mapping to a expanded vector of items with length [`VirtualNode::COUNT`].
    pub fn to_expanded(&self) -> ExpandedMapping<T> {
        self.iter().collect()
    }

    /// Transform this vnode mapping to another type of vnode mapping, with the given mapping from
    /// items of this mapping to items of the other mapping.
    pub fn transform<T2, M>(&self, to_map: &M) -> VnodeMapping<T2>
    where
        T2: VnodeMappingItem,
        M: for<'a> Index<&'a T::Item, Output = T2::Item>,
    {
        VnodeMapping {
            original_indices: self.original_indices.clone(),
            data: self.data.iter().map(|item| to_map[item]).collect(),
        }
    }
}

impl<T: VnodeMappingItem> Index<VirtualNode> for VnodeMapping<T> {
    type Output = T::Item;

    fn index(&self, vnode: VirtualNode) -> &Self::Output {
        let index = self
            .original_indices
            .partition_point(|&i| i < vnode.to_index() as u32);
        &self.data[index]
    }
}

pub mod marker {
    use super::*;

    /// A marker type for items of [`ActorId`].
    pub struct Actor;
    impl VnodeMappingItem for Actor {
        type Item = ActorId;
    }

    /// A marker type for items of [`ParallelUnitId`].
    pub struct ParallelUnit;
    impl VnodeMappingItem for ParallelUnit {
        type Item = ParallelUnitId;
    }
}

/// A mapping from [`VirtualNode`] to [`ActorId`].
pub type ActorMapping = VnodeMapping<marker::Actor>;
/// An expanded mapping from [`VirtualNode`] to [`ActorId`].
pub type ExpandedActorMapping = ExpandedMapping<marker::Actor>;

/// A mapping from [`VirtualNode`] to [`ParallelUnitId`].
pub type ParallelUnitMapping = VnodeMapping<marker::ParallelUnit>;
/// An expanded mapping from [`VirtualNode`] to [`ParallelUnitId`].
pub type ExpandedParallelUnitMapping = ExpandedMapping<marker::ParallelUnit>;

impl ActorMapping {
    /// Transform this actor mapping to a parallel unit mapping, essentially `transform`.
    pub fn to_parallel_unit<M>(&self, to_map: &M) -> ParallelUnitMapping
    where
        M: for<'a> Index<&'a ActorId, Output = ParallelUnitId>,
    {
        self.transform(to_map)
    }

    /// Create an actor mapping from the protobuf representation.
    pub fn from_protobuf(proto: &ActorMappingProto) -> Self {
        assert_eq!(proto.original_indices.len(), proto.data.len());
        Self {
            original_indices: proto.original_indices.clone(),
            data: proto.data.clone(),
        }
    }

    /// Convert this actor mapping to the protobuf representation.
    pub fn to_protobuf(&self) -> ActorMappingProto {
        ActorMappingProto {
            original_indices: self.original_indices.clone(),
            data: self.data.clone(),
        }
    }
}

impl ParallelUnitMapping {
    /// Create a uniform parallel unit mapping from the given parallel units, essentially
    /// `new_uniform`.
    pub fn build(parallel_units: &[ParallelUnit]) -> Self {
        Self::new_uniform(parallel_units.iter().map(|pu| pu.id))
    }

    /// Transform this parallel unit mapping to an actor mapping, essentially `transform`.
    pub fn to_actor(&self, to_map: &HashMap<ParallelUnitId, ActorId>) -> ActorMapping {
        self.transform(to_map)
    }

    /// Create a parallel unit mapping from the protobuf representation.
    pub fn from_protobuf(proto: &ParallelUnitMappingProto) -> Self {
        assert_eq!(proto.original_indices.len(), proto.data.len());
        Self {
            original_indices: proto.original_indices.clone(),
            data: proto.data.clone(),
        }
    }

    /// Convert this parallel unit mapping to the protobuf representation.
    pub fn to_protobuf(&self) -> ParallelUnitMappingProto {
        ParallelUnitMappingProto {
            original_indices: self.original_indices.clone(),
            data: self.data.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use rand::Rng;

    use super::*;
    use crate::util::iter_util::ZipEqDebug;

    struct Test;
    impl VnodeMappingItem for Test {
        type Item = u32;
    }

    struct Test2;
    impl VnodeMappingItem for Test2 {
        type Item = u32;
    }

    type TestMapping = VnodeMapping<Test>;
    type Test2Mapping = VnodeMapping<Test2>;

    const COUNTS: &[usize] = &[1, 3, 12, 42, VirtualNode::COUNT];

    fn uniforms() -> impl Iterator<Item = TestMapping> {
        COUNTS
            .iter()
            .map(|&count| TestMapping::new_uniform(0..count as u32))
    }

    fn randoms() -> impl Iterator<Item = TestMapping> {
        COUNTS.iter().map(|&count| {
            let raw = repeat_with(|| rand::thread_rng().gen_range(0..count as u32))
                .take(VirtualNode::COUNT)
                .collect_vec();
            TestMapping::from_expanded(&raw)
        })
    }

    fn mappings() -> impl Iterator<Item = TestMapping> {
        uniforms().chain(randoms())
    }

    #[test]
    fn test_uniform() {
        for vnode_mapping in uniforms() {
            assert_eq!(vnode_mapping.len(), VirtualNode::COUNT);
            let item_count = vnode_mapping.iter_unique().count();

            let mut check: HashMap<u32, Vec<_>> = HashMap::new();
            for (vnode, item) in vnode_mapping.iter_with_vnode() {
                check.entry(item).or_default().push(vnode);
            }

            assert_eq!(check.len(), item_count);

            let (min, max) = check
                .values()
                .map(|indexes| indexes.len())
                .minmax()
                .into_option()
                .unwrap();

            assert!(max - min <= 1);
        }
    }

    #[test]
    fn test_iter_with_get() {
        for vnode_mapping in mappings() {
            for (vnode, item) in vnode_mapping.iter_with_vnode() {
                assert_eq!(vnode_mapping.get(vnode), item);
            }
        }
    }

    #[test]
    fn test_from_to_bitmaps() {
        for vnode_mapping in mappings() {
            let bitmaps = vnode_mapping.to_bitmaps();
            let new_vnode_mapping = TestMapping::from_bitmaps(&bitmaps);

            assert_eq!(vnode_mapping, new_vnode_mapping);
        }
    }

    #[test]
    fn test_transform() {
        for vnode_mapping in mappings() {
            let transform_map: HashMap<_, _> = vnode_mapping
                .iter_unique()
                .map(|item| (item, item + 1))
                .collect();
            let vnode_mapping_2: Test2Mapping = vnode_mapping.transform(&transform_map);

            for (item, item_2) in vnode_mapping.iter().zip_eq_debug(vnode_mapping_2.iter()) {
                assert_eq!(item + 1, item_2);
            }

            let transform_back_map: HashMap<_, _> =
                transform_map.into_iter().map(|(k, v)| (v, k)).collect();
            let new_vnode_mapping: TestMapping = vnode_mapping_2.transform(&transform_back_map);

            assert_eq!(vnode_mapping, new_vnode_mapping);
        }
    }
}
