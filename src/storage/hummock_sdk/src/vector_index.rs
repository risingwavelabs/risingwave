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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::vector_index::PbVariant;
use risingwave_pb::hummock::vector_index_delta::vector_index_init::Config;
use risingwave_pb::hummock::vector_index_delta::{
    PbVectorIndexAdd, PbVectorIndexInit, vector_index_add,
};
use risingwave_pb::hummock::{
    PbDistanceType, PbFlatIndex, PbFlatIndexAdd, PbFlatIndexConfig, PbVectorFileInfo,
    PbVectorIndex, PbVectorIndexDelta, vector_index_delta,
};

use crate::{HummockObjectId, HummockVectorFileId};

#[derive(Clone, Debug, PartialEq)]
pub struct VectorFileInfo {
    pub object_id: HummockVectorFileId,
    pub file_size: u64,
    pub start_vector_id: usize,
    pub vector_count: usize,
    pub meta_offset: usize,
}

impl From<PbVectorFileInfo> for VectorFileInfo {
    fn from(pb: PbVectorFileInfo) -> Self {
        Self {
            object_id: pb.object_id.into(),
            file_size: pb.file_size,
            start_vector_id: pb.start_vector_id.try_into().unwrap(),
            vector_count: pb.vector_count.try_into().unwrap(),
            meta_offset: pb.meta_offset.try_into().unwrap(),
        }
    }
}

impl From<VectorFileInfo> for PbVectorFileInfo {
    fn from(info: VectorFileInfo) -> Self {
        Self {
            object_id: info.object_id.inner(),
            file_size: info.file_size,
            start_vector_id: info.start_vector_id.try_into().unwrap(),
            vector_count: info.vector_count.try_into().unwrap(),
            meta_offset: info.meta_offset.try_into().unwrap(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorStoreInfo {
    pub next_vector_id: usize,
    pub vector_files: Vec<VectorFileInfo>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorStoreInfoDelta {
    pub next_vector_id: usize,
    pub added_vector_files: Vec<VectorFileInfo>,
}

impl VectorStoreInfo {
    fn empty() -> Self {
        Self {
            next_vector_id: 0,
            vector_files: vec![],
        }
    }

    fn apply_vector_store_delta(&mut self, delta: &VectorStoreInfoDelta) {
        for new_vector_file in &delta.added_vector_files {
            if let Some(latest_vector_file) = self.vector_files.last() {
                assert!(
                    new_vector_file.start_vector_id
                        >= latest_vector_file.start_vector_id + latest_vector_file.vector_count,
                    "new vector file's start vector id {} should be greater than the last vector file's start vector id {} + vector count {}",
                    new_vector_file.start_vector_id,
                    latest_vector_file.start_vector_id,
                    latest_vector_file.vector_count
                );
            }
            self.vector_files.push(new_vector_file.clone());
        }
        self.next_vector_id = delta.next_vector_id;
        if let Some(latest_vector_file) = self.vector_files.last() {
            assert!(
                latest_vector_file.start_vector_id + latest_vector_file.vector_count
                    <= self.next_vector_id,
                "next_vector_id {} should be greater than the last vector file's start vector id {} + vector count {}",
                self.next_vector_id,
                latest_vector_file.start_vector_id,
                latest_vector_file.vector_count
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlatIndex {
    pub config: PbFlatIndexConfig,
    pub vector_store_info: VectorStoreInfo,
}

impl FlatIndex {
    fn new(config: &PbFlatIndexConfig) -> FlatIndex {
        FlatIndex {
            config: *config,
            vector_store_info: VectorStoreInfo::empty(),
        }
    }

    fn apply_flat_index_add(&mut self, add: &FlatIndexAdd) {
        self.vector_store_info
            .apply_vector_store_delta(&add.vector_store_info_delta);
    }
}

impl From<PbFlatIndex> for FlatIndex {
    fn from(pb: PbFlatIndex) -> Self {
        Self {
            config: pb.config.unwrap(),
            vector_store_info: VectorStoreInfo {
                next_vector_id: pb.next_vector_id.try_into().unwrap(),
                vector_files: pb
                    .vector_files
                    .into_iter()
                    .map(VectorFileInfo::from)
                    .collect(),
            },
        }
    }
}
impl From<FlatIndex> for PbFlatIndex {
    fn from(index: FlatIndex) -> Self {
        Self {
            config: Some(index.config),
            next_vector_id: index.vector_store_info.next_vector_id.try_into().unwrap(),
            vector_files: index
                .vector_store_info
                .vector_files
                .into_iter()
                .map(PbVectorFileInfo::from)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorIndexImpl {
    Flat(FlatIndex),
}

impl From<PbVariant> for VectorIndexImpl {
    fn from(variant: PbVariant) -> Self {
        match variant {
            PbVariant::Flat(flat_index) => Self::Flat(flat_index.into()),
        }
    }
}

impl From<VectorIndexImpl> for PbVariant {
    fn from(index: VectorIndexImpl) -> Self {
        match index {
            VectorIndexImpl::Flat(flat_index) => PbVariant::Flat(flat_index.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndex {
    pub dimension: usize,
    pub distance_type: PbDistanceType,
    pub inner: VectorIndexImpl,
}

impl VectorIndex {
    pub fn get_objects(&self) -> impl Iterator<Item = (HummockObjectId, u64)> + '_ {
        // DO NOT REMOVE THIS LINE
        // This is to ensure that when adding new variant to `HummockObjectId`,
        // the compiler will warn us if we forget to handle it here.
        match HummockObjectId::Sstable(0.into()) {
            HummockObjectId::Sstable(_) => {}
            HummockObjectId::VectorFile(_) => {}
        };
        match &self.inner {
            VectorIndexImpl::Flat(flat) => flat
                .vector_store_info
                .vector_files
                .iter()
                .map(|file| (HummockObjectId::VectorFile(file.object_id), file.file_size)),
        }
    }
}

impl From<PbVectorIndex> for VectorIndex {
    fn from(pb: PbVectorIndex) -> Self {
        Self {
            dimension: pb.dimension as _,
            distance_type: pb.distance_type.try_into().unwrap(),
            inner: pb.variant.unwrap().into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FlatIndexAdd {
    pub vector_store_info_delta: VectorStoreInfoDelta,
}

impl From<PbFlatIndexAdd> for FlatIndexAdd {
    fn from(add: PbFlatIndexAdd) -> Self {
        Self {
            vector_store_info_delta: VectorStoreInfoDelta {
                next_vector_id: add.next_vector_id.try_into().unwrap(),
                added_vector_files: add
                    .added_vector_files
                    .into_iter()
                    .map(VectorFileInfo::from)
                    .collect(),
            },
        }
    }
}

impl From<FlatIndexAdd> for PbFlatIndexAdd {
    fn from(add: FlatIndexAdd) -> Self {
        Self {
            next_vector_id: add
                .vector_store_info_delta
                .next_vector_id
                .try_into()
                .unwrap(),
            added_vector_files: add
                .vector_store_info_delta
                .added_vector_files
                .into_iter()
                .map(PbVectorFileInfo::from)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VectorIndexAdd {
    Flat(FlatIndexAdd),
}

impl From<PbVectorIndexAdd> for VectorIndexAdd {
    fn from(add: PbVectorIndexAdd) -> Self {
        match add.add.unwrap() {
            vector_index_add::Add::Flat(flat_add) => Self::Flat(flat_add.into()),
        }
    }
}

impl From<VectorIndexAdd> for PbVectorIndexAdd {
    fn from(add: VectorIndexAdd) -> Self {
        match add {
            VectorIndexAdd::Flat(flat_add) => Self {
                add: Some(vector_index_add::Add::Flat(flat_add.into())),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VectorIndexDelta {
    Init(PbVectorIndexInit),
    Adds(Vec<VectorIndexAdd>),
}

impl From<PbVectorIndexDelta> for VectorIndexDelta {
    fn from(delta: PbVectorIndexDelta) -> Self {
        match delta.delta.unwrap() {
            vector_index_delta::Delta::Init(init) => Self::Init(init),
            vector_index_delta::Delta::Adds(adds) => {
                Self::Adds(adds.adds.into_iter().map(Into::into).collect())
            }
        }
    }
}

impl VectorIndexDelta {
    pub fn newly_added_objects(&self) -> impl Iterator<Item = (HummockObjectId, u64)> + '_ {
        match self {
            VectorIndexDelta::Init(_) => None,
            VectorIndexDelta::Adds(adds) => Some(adds.iter().flat_map(|add| {
                match add {
                    VectorIndexAdd::Flat(add) => add
                        .vector_store_info_delta
                        .added_vector_files
                        .iter()
                        .map(|file| (HummockObjectId::VectorFile(file.object_id), file.file_size)),
                }
            })),
        }
        .into_iter()
        .flatten()
    }
}

impl From<VectorIndexDelta> for PbVectorIndexDelta {
    fn from(delta: VectorIndexDelta) -> Self {
        match delta {
            VectorIndexDelta::Init(init) => Self {
                delta: Some(vector_index_delta::Delta::Init(init)),
            },
            VectorIndexDelta::Adds(adds) => Self {
                delta: Some(vector_index_delta::Delta::Adds(
                    vector_index_delta::VectorIndexAdds {
                        adds: adds.into_iter().map(Into::into).collect(),
                    },
                )),
            },
        }
    }
}

impl From<VectorIndex> for PbVectorIndex {
    fn from(index: VectorIndex) -> Self {
        Self {
            dimension: index.dimension as _,
            distance_type: index.distance_type as _,
            variant: Some(index.inner.into()),
        }
    }
}

fn init_vector_index(init: &PbVectorIndexInit) -> VectorIndex {
    let inner = match init.config.as_ref().unwrap() {
        Config::Flat(config) => VectorIndexImpl::Flat(FlatIndex::new(config)),
    };
    VectorIndex {
        dimension: init.dimension as _,
        distance_type: init.distance_type.try_into().unwrap(),
        inner,
    }
}

fn apply_vector_index_add(inner: &mut VectorIndexImpl, add: &VectorIndexAdd) {
    match inner {
        VectorIndexImpl::Flat(flat_index) => {
            #[expect(irrefutable_let_patterns)]
            let VectorIndexAdd::Flat(add) = add else {
                panic!("expect FlatIndexAdd but got {:?}", flat_index);
            };
            flat_index.apply_flat_index_add(add);
        }
    }
}

pub fn apply_vector_index_delta(
    vector_index: &mut HashMap<TableId, VectorIndex>,
    vector_index_delta: &HashMap<TableId, VectorIndexDelta>,
    removed_table_ids: &HashSet<TableId>,
) {
    for (table_id, vector_index_delta) in vector_index_delta {
        match vector_index_delta {
            VectorIndexDelta::Init(init) => {
                vector_index
                    .try_insert(*table_id, init_vector_index(init))
                    .unwrap();
            }
            VectorIndexDelta::Adds(adds) => {
                let inner = &mut vector_index.get_mut(table_id).unwrap().inner;
                for add in adds {
                    apply_vector_index_add(inner, add);
                }
            }
        }
    }

    // Remove the vector index for the tables that are removed
    vector_index.retain(|table_id, _| !removed_table_ids.contains(table_id));
}
