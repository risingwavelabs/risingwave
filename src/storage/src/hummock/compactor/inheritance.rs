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

use std::collections::BTreeSet;

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::hummock::{
    BlockInheritance as PbBlockInheritance, Parent as PbParent,
    SstableInheritance as PbSstableInheritance,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Parent {
    pub sst_obj_id: HummockSstableObjectId,
    pub sst_blk_idx: usize,
}

impl Parent {
    pub fn from_proto(proto: &PbParent) -> Self {
        Self {
            sst_obj_id: proto.sst_obj_id,
            sst_blk_idx: proto.sst_blk_idx as usize,
        }
    }

    pub fn to_proto(&self) -> PbParent {
        PbParent {
            sst_obj_id: self.sst_obj_id,
            sst_blk_idx: self.sst_blk_idx as u64,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockInheritance {
    pub parents: BTreeSet<Parent>,
}

impl BlockInheritance {
    pub fn from_proto(proto: &PbBlockInheritance) -> Self {
        Self {
            parents: proto.parents.iter().map(Parent::from_proto).collect(),
        }
    }

    pub fn to_proto(&self) -> PbBlockInheritance {
        PbBlockInheritance {
            parents: self.parents.iter().map(Parent::to_proto).collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SstableInheritance {
    pub blocks: Vec<BlockInheritance>,
}

impl SstableInheritance {
    pub fn from_proto(proto: &PbSstableInheritance) -> Self {
        Self {
            blocks: proto
                .blocks
                .iter()
                .map(BlockInheritance::from_proto)
                .collect(),
        }
    }

    pub fn to_proto(&self) -> PbSstableInheritance {
        PbSstableInheritance {
            blocks: self.blocks.iter().map(BlockInheritance::to_proto).collect(),
        }
    }
}
