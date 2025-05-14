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

use crate::HummockVectorFileId;

#[derive(Clone, Debug, PartialEq)]
pub struct VectorFileInfo {
    pub object_id: HummockVectorFileId,
}

impl From<PbVectorFileInfo> for VectorFileInfo {
    fn from(pb: PbVectorFileInfo) -> Self {
        Self {
            object_id: pb.object_id.into(),
        }
    }
}

impl From<VectorFileInfo> for PbVectorFileInfo {
    fn from(info: VectorFileInfo) -> Self {
        Self {
            object_id: info.object_id.inner(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlatIndex {
    pub config: PbFlatIndexConfig,
    pub vector_files: Vec<VectorFileInfo>,
}

impl From<PbFlatIndex> for FlatIndex {
    fn from(pb: PbFlatIndex) -> Self {
        Self {
            config: pb.config.unwrap(),
            vector_files: pb
                .vector_files
                .into_iter()
                .map(VectorFileInfo::from)
                .collect(),
        }
    }
}
impl From<FlatIndex> for PbFlatIndex {
    fn from(index: FlatIndex) -> Self {
        Self {
            config: Some(index.config),
            vector_files: index
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
    pub added_vector_files: Vec<VectorFileInfo>,
}

impl From<PbFlatIndexAdd> for FlatIndexAdd {
    fn from(add: PbFlatIndexAdd) -> Self {
        Self {
            added_vector_files: add
                .added_vector_files
                .into_iter()
                .map(VectorFileInfo::from)
                .collect(),
        }
    }
}

impl From<FlatIndexAdd> for PbFlatIndexAdd {
    fn from(add: FlatIndexAdd) -> Self {
        Self {
            added_vector_files: add
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

fn init_flat_index(config: &PbFlatIndexConfig) -> FlatIndex {
    FlatIndex {
        config: *config,
        vector_files: vec![],
    }
}

fn apply_flat_index_add(flat_index: &mut FlatIndex, add: &FlatIndexAdd) {
    flat_index
        .vector_files
        .extend(add.added_vector_files.iter().cloned());
}

fn init_vector_index(init: &PbVectorIndexInit) -> VectorIndex {
    let inner = match init.config.as_ref().unwrap() {
        Config::Flat(config) => VectorIndexImpl::Flat(init_flat_index(config)),
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
            apply_flat_index_add(flat_index, add);
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
