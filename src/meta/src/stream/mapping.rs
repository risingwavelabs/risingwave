// Copyright 2023 Singularity Data
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

use derivative::Derivative;
use itertools::Itertools;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{ParallelUnitId, VirtualNode};
use risingwave_common::util::compress::compress_data;
use risingwave_pb::common::{ParallelUnit, ParallelUnitMapping as ParallelUnitMappingProto};
use risingwave_pb::stream_plan::ActorMapping as ActorMappingProto;

use crate::model::{ActorId, FragmentId};

/// Trait for items that can be used as keys in [`VnodeMapping`].
pub trait VnodeMappingItem {
    /// The type of the item.
    ///
    /// Currently, there are two types of items: [`ParallelUnitId`] and [`ActorId`]. We don't use
    /// them directly as the generic parameter because they're the same type aliases.
    type Item: Copy + Eq + Hash + Debug;
}

/// Generic mapping from virtual nodes to items.
///
/// The representation is compressed as described in [`compress_data`], which is optimized for the
/// mapping with a small number of items and good locality.
#[derive(Derivative)]
#[derivative(Debug, Clone, PartialEq, Eq)]
pub struct VnodeMapping<T: VnodeMappingItem> {
    original_indices: Vec<u32>,
    data: Vec<T::Item>,
}

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

    /// Iterate over all items in this mapping, in the order of vnodes.
    pub fn iter(&self) -> impl Iterator<Item = T::Item> + '_ {
        self.data
            .iter()
            .copied()
            .zip_eq(
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
        let mut raw = vec![None; VirtualNode::COUNT];

        for (&item, bitmap) in bitmaps {
            assert_eq!(bitmap.len(), VirtualNode::COUNT);
            for idx in bitmap.iter_ones() {
                if let Some(prev) = raw[idx].replace(item) {
                    panic!("mapping at index `{idx}` is set to both `{prev:?}` and `{item:?}`");
                }
            }
        }

        let items = raw
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
    pub fn to_expanded(&self) -> Vec<T::Item> {
        self.iter().collect()
    }

    /// Transform this vnode mapping to another type of vnode mapping, with the given mapping from
    /// items of this mapping to items of the other mapping.
    pub fn transform<T2: VnodeMappingItem>(
        &self,
        to_map: &HashMap<T::Item, T2::Item>,
    ) -> VnodeMapping<T2> {
        VnodeMapping {
            original_indices: self.original_indices.clone(),
            data: self.data.iter().map(|item| to_map[item]).collect(),
        }
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

/// A mapping from [`VirtualNode`] to [`ParallelUnitId`].
pub type ParallelUnitMapping = VnodeMapping<marker::ParallelUnit>;

impl ActorMapping {
    /// Transform this actor mapping to a parallel unit mapping, essentially `transform`.
    pub fn to_parallel_unit(
        &self,
        to_map: &HashMap<ActorId, ParallelUnitId>,
    ) -> ParallelUnitMapping {
        self.transform(to_map)
    }

    /// Create an actor mapping from the protobuf representation.
    pub fn from_protobuf(proto: &ActorMappingProto) -> Self {
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
        Self {
            original_indices: proto.original_indices.clone(),
            data: proto.data.clone(),
        }
    }

    /// Convert this parallel unit mapping to the protobuf representation.
    // TODO: remove the `fragment_id`
    pub fn to_protobuf(&self, fragment_id: FragmentId) -> ParallelUnitMappingProto {
        ParallelUnitMappingProto {
            fragment_id,
            original_indices: self.original_indices.clone(),
            data: self.data.clone(),
        }
    }
}
