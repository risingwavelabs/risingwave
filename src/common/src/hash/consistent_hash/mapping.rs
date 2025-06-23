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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::ops::Index;

use educe::Educe;
use itertools::Itertools;
use risingwave_pb::common::PbWorkerSlotMapping;
use risingwave_pb::stream_plan::ActorMapping as ActorMappingProto;

use super::bitmap::VnodeBitmapExt;
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::hash::VirtualNode;
use crate::util::compress::compress_data;
use crate::util::iter_util::ZipEqDebug;

// TODO: find a better place for this.
pub type ActorId = u32;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ActorAlignmentId(u64);

impl ActorAlignmentId {
    pub fn worker_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn actor_idx(&self) -> u32 {
        self.0 as u32
    }

    pub fn new(worker_id: u32, actor_idx: usize) -> Self {
        Self((worker_id as u64) << 32 | actor_idx as u64)
    }

    pub fn new_single(worker_id: u32) -> Self {
        Self::new(worker_id, 0)
    }
}

impl From<ActorAlignmentId> for u64 {
    fn from(id: ActorAlignmentId) -> Self {
        id.0
    }
}

impl From<u64> for ActorAlignmentId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Display for ActorAlignmentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}/{}]", self.worker_id(), self.actor_idx()))
    }
}

impl Debug for ActorAlignmentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}/{}]", self.worker_id(), self.actor_idx()))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct WorkerSlotId(u64);

impl WorkerSlotId {
    pub fn worker_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn slot_idx(&self) -> u32 {
        self.0 as u32
    }

    pub fn new(worker_id: u32, slot_idx: usize) -> Self {
        Self((worker_id as u64) << 32 | slot_idx as u64)
    }
}

impl From<WorkerSlotId> for u64 {
    fn from(id: WorkerSlotId) -> Self {
        id.0
    }
}

impl From<u64> for WorkerSlotId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Display for WorkerSlotId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}:{}]", self.worker_id(), self.slot_idx()))
    }
}

impl Debug for WorkerSlotId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}:{}]", self.worker_id(), self.slot_idx()))
    }
}

/// Trait for items that can be used as keys in [`VnodeMapping`].
pub trait VnodeMappingItem {
    /// The type of the item.
    ///
    /// Currently, there are two types of items: [`WorkerSlotId`] and [`ActorId`]. We don't use
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
    pub fn new_uniform(items: impl ExactSizeIterator<Item = T::Item>, vnode_count: usize) -> Self {
        // If the number of items is greater than the total vnode count, no vnode will be mapped to
        // some items and the mapping will be invalid.
        assert!(items.len() <= vnode_count);

        let mut original_indices = Vec::with_capacity(items.len());
        let mut data = Vec::with_capacity(items.len());

        let hash_shard_size = vnode_count / items.len();
        let mut one_more_count = vnode_count % items.len();
        let mut init_bound = 0;

        for item in items {
            let count = if one_more_count > 0 {
                one_more_count -= 1;
                hash_shard_size + 1
            } else {
                hash_shard_size
            };
            init_bound += count;

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

    /// Create a vnode mapping with the single item and length of 1.
    ///
    /// Should only be used for singletons. If you want a different vnode count, call
    /// [`VnodeMapping::new_uniform`] with `std::iter::once(item)` and desired length.
    pub fn new_single(item: T::Item) -> Self {
        Self::new_uniform(std::iter::once(item), 1)
    }

    /// The length (or count) of the vnode in this mapping.
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
            .flat_map(|(item, c)| std::iter::repeat_n(item, c))
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
        let vnode_count = self.len();
        let mut vnode_bitmaps = HashMap::new();

        for (vnode, item) in self.iter_with_vnode() {
            vnode_bitmaps
                .entry(item)
                .or_insert_with(|| BitmapBuilder::zeroed(vnode_count))
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
        let vnode_count = bitmaps.values().next().expect("empty bitmaps").len();
        let mut items = vec![None; vnode_count];

        for (&item, bitmap) in bitmaps {
            assert_eq!(bitmap.len(), vnode_count);
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

    /// Create a vnode mapping from the expanded slice of items.
    pub fn from_expanded(items: &[T::Item]) -> Self {
        let (original_indices, data) = compress_data(items);
        Self {
            original_indices,
            data,
        }
    }

    /// Convert this vnode mapping to a expanded vector of items.
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

    /// A marker type for items of [`WorkerSlotId`].
    pub struct WorkerSlot;
    impl VnodeMappingItem for WorkerSlot {
        type Item = WorkerSlotId;
    }

    /// A marker type for items of [`ActorAlignmentId`].
    pub struct ActorAlignment;
    impl VnodeMappingItem for ActorAlignment {
        type Item = ActorAlignmentId;
    }
}

/// A mapping from [`VirtualNode`] to [`ActorId`].
pub type ActorMapping = VnodeMapping<marker::Actor>;
/// An expanded mapping from [`VirtualNode`] to [`ActorId`].
pub type ExpandedActorMapping = ExpandedMapping<marker::Actor>;

/// A mapping from [`VirtualNode`] to [`WorkerSlotId`].
pub type WorkerSlotMapping = VnodeMapping<marker::WorkerSlot>;
/// An expanded mapping from [`VirtualNode`] to [`WorkerSlotId`].
pub type ExpandedWorkerSlotMapping = ExpandedMapping<marker::WorkerSlot>;

/// A mapping from [`VirtualNode`] to [`ActorAlignmentId`].
pub type ActorAlignmentMapping = VnodeMapping<marker::ActorAlignment>;
/// An expanded mapping from [`VirtualNode`] to [`ActorAlignmentId`].
pub type ExpandedActorAlignment = ExpandedMapping<marker::ActorAlignment>;

impl ActorMapping {
    /// Transform the actor mapping to the worker slot mapping. Note that the parameter is a mapping from actor to worker.
    pub fn to_worker_slot(&self, actor_to_worker: &HashMap<ActorId, u32>) -> WorkerSlotMapping {
        let mut worker_actors = HashMap::new();
        for actor_id in self.iter_unique() {
            let worker_id = actor_to_worker
                .get(&actor_id)
                .cloned()
                .unwrap_or_else(|| panic!("location for actor {} not found", actor_id));

            worker_actors
                .entry(worker_id)
                .or_insert(BTreeSet::new())
                .insert(actor_id);
        }

        let mut actor_location = HashMap::new();
        for (worker, actors) in worker_actors {
            for (idx, &actor) in actors.iter().enumerate() {
                actor_location.insert(actor, WorkerSlotId::new(worker, idx));
            }
        }

        self.transform(&actor_location)
    }

    /// Transform the actor mapping to the actor alignment mapping. Note that the parameter is a mapping from actor to worker.
    pub fn to_actor_alignment(
        &self,
        actor_to_worker: &HashMap<ActorId, u32>,
    ) -> ActorAlignmentMapping {
        let mut worker_actors = HashMap::new();

        let challenge = self.iter_unique().collect_vec();

        assert!(challenge.is_sorted());

        for (idx, actor_id) in self.iter_unique().enumerate() {
            let worker_id = actor_to_worker
                .get(&actor_id)
                .cloned()
                .unwrap_or_else(|| panic!("location for actor {} not found", actor_id));

            worker_actors
                .entry(worker_id)
                .or_insert(BTreeSet::new())
                .insert((actor_id, idx));
        }

        let mut actor_location = HashMap::new();
        for (worker, idxes) in worker_actors {
            for (actor, idx) in idxes {
                actor_location.insert(actor, ActorAlignmentId::new(worker, idx));
            }
        }

        self.transform(&actor_location)
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

impl WorkerSlotMapping {
    /// Create a uniform worker mapping from the given worker ids
    pub fn build_from_ids(worker_slot_ids: &[WorkerSlotId], vnode_count: usize) -> Self {
        Self::new_uniform(worker_slot_ids.iter().cloned(), vnode_count)
    }

    /// Create a worker mapping from the protobuf representation.
    pub fn from_protobuf(proto: &PbWorkerSlotMapping) -> Self {
        assert_eq!(proto.original_indices.len(), proto.data.len());
        Self {
            original_indices: proto.original_indices.clone(),
            data: proto.data.iter().map(|&id| WorkerSlotId(id)).collect(),
        }
    }

    /// Convert this worker mapping to the protobuf representation.
    pub fn to_protobuf(&self) -> PbWorkerSlotMapping {
        PbWorkerSlotMapping {
            original_indices: self.original_indices.clone(),
            data: self.data.iter().map(|id| id.0).collect(),
        }
    }
}

impl WorkerSlotMapping {
    /// Transform this worker slot mapping to an actor mapping, essentially `transform`.
    pub fn to_actor(&self, to_map: &HashMap<WorkerSlotId, ActorId>) -> ActorMapping {
        self.transform(to_map)
    }
}

impl ActorAlignmentMapping {
    pub fn from_assignment(
        assignment: BTreeMap<u32, BTreeMap<usize, Vec<usize>>>,
        vnode_size: usize,
    ) -> Self {
        let mut result = HashMap::new();

        for (worker_id, actors) in &assignment {
            for (actor_idx, vnodes) in actors {
                let bitmap = Bitmap::from_vec_with_len(vnodes.clone(), vnode_size);

                result.insert(ActorAlignmentId::new(*worker_id, *actor_idx), bitmap);
            }
        }

        Self::from_bitmaps(&result)
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use rand::Rng;

    use super::*;

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

    const COUNTS: &[usize] = &[1, 3, 12, 42, VirtualNode::COUNT_FOR_TEST];

    fn uniforms() -> impl Iterator<Item = TestMapping> {
        COUNTS
            .iter()
            .map(|&count| TestMapping::new_uniform(0..count as u32, VirtualNode::COUNT_FOR_TEST))
    }

    fn randoms() -> impl Iterator<Item = TestMapping> {
        COUNTS.iter().map(|&count| {
            let raw = repeat_with(|| rand::rng().random_range(0..count as u32))
                .take(VirtualNode::COUNT_FOR_TEST)
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
            assert_eq!(vnode_mapping.len(), VirtualNode::COUNT_FOR_TEST);
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
