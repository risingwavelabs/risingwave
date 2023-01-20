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

pub trait VnodeMappingItem {
    type Item: Copy + Eq + Hash + Debug;
}

#[derive(Derivative)]
#[derivative(Debug, Clone, PartialEq, Eq)]
pub struct VnodeMapping<T: VnodeMappingItem> {
    original_indices: Vec<u32>,

    data: Vec<T::Item>,
}

impl<T: VnodeMappingItem> VnodeMapping<T> {
    pub fn new_uniform(items: impl ExactSizeIterator<Item = T::Item>) -> Self {
        assert!(items.len() < VirtualNode::COUNT);

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

        Self {
            original_indices,
            data,
        }
    }

    pub fn new_single(item: T::Item) -> Self {
        Self::new_uniform(std::iter::once(item))
    }

    pub fn len(&self) -> usize {
        self.original_indices
            .last()
            .map(|&i| i as usize + 1)
            .unwrap_or(0)
    }

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

    pub fn iter_with_vnode(&self) -> impl Iterator<Item = (VirtualNode, T::Item)> + '_ {
        self.iter()
            .enumerate()
            .map(|(v, item)| (VirtualNode::from_index(v), item))
    }

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

    pub fn from_bitmaps(bitmaps: &HashMap<T::Item, Bitmap>) -> Self {
        let mut raw = vec![None; VirtualNode::COUNT];

        for (&item, bitmap) in bitmaps {
            for idx in bitmap.iter_ones() {
                if let Some(prev) = raw[idx].replace(item) {
                    panic!("mapping at index `{idx}` is set to both `{prev:?}` and `{item:?}`");
                }
            }
        }

        let raw = raw
            .into_iter()
            .enumerate()
            .map(|(i, o)| o.unwrap_or_else(|| panic!("mapping at index `{i}` is not set")))
            .collect_vec();
        Self::from_slice(&raw)
    }

    pub fn from_slice(raw: &[T::Item]) -> Self {
        assert_eq!(raw.len(), VirtualNode::COUNT);
        let (original_indices, data) = compress_data(&raw);
        Self {
            original_indices,
            data,
        }
    }

    pub fn to_vec(&self) -> Vec<T::Item> {
        self.iter().collect()
    }

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

    pub struct Actor;
    impl VnodeMappingItem for Actor {
        type Item = ActorId;
    }

    pub struct ParallelUnit;
    impl VnodeMappingItem for ParallelUnit {
        type Item = ParallelUnitId;
    }
}

pub type ActorMapping = VnodeMapping<marker::Actor>;
pub type ParallelUnitMapping = VnodeMapping<marker::ParallelUnit>;

impl ActorMapping {
    pub fn to_parallel_unit(
        &self,
        to_map: &HashMap<ActorId, ParallelUnitId>,
    ) -> ParallelUnitMapping {
        self.transform(to_map)
    }

    pub fn from_protobuf(proto: &ActorMappingProto) -> Self {
        Self {
            original_indices: proto.original_indices.clone(),
            data: proto.data.clone(),
        }
    }

    pub fn to_protobuf(&self) -> ActorMappingProto {
        ActorMappingProto {
            original_indices: self.original_indices.clone(),
            data: self.data.clone(),
        }
    }
}

impl ParallelUnitMapping {
    /// Build a vnode mapping according to parallel units where the fragment is scheduled.
    /// For example, if `parallel_units` is `[0, 1, 2]`, and the total vnode count is 10, we'll
    /// generate mapping like `[0, 0, 0, 0, 1, 1, 1, 2, 2, 2]`.
    pub fn build(parallel_units: &[ParallelUnit]) -> Self {
        Self::new_uniform(parallel_units.iter().map(|pu| pu.id))
    }

    pub fn to_actor(&self, to_map: &HashMap<ParallelUnitId, ActorId>) -> ActorMapping {
        self.transform(to_map)
    }

    pub fn from_protobuf(proto: &ParallelUnitMappingProto) -> Self {
        Self {
            original_indices: proto.original_indices.clone(),
            data: proto.data.clone(),
        }
    }

    // TODO: remove the `fragment_id`
    pub fn to_protobuf(&self, fragment_id: FragmentId) -> ParallelUnitMappingProto {
        ParallelUnitMappingProto {
            fragment_id,
            original_indices: self.original_indices.clone(),
            data: self.data.clone(),
        }
    }
}
