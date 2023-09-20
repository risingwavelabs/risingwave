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

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::hummock::{
    BlockInheritance as PbBlockInheritance, ParentInfo as PbParentInfo,
    SstableInheritance as PbSstableInheritance,
};

const PARENT_INFO_MASK: u64 = 1 << 63;

/// FYI: See format doc in `hummock.proto`.
pub struct ParentInfo;

impl ParentInfo {
    pub fn from_proto(proto: &PbParentInfo) -> BTreeSet<usize> {
        let mut res = BTreeSet::new();
        let mut iter = proto.info.iter();

        while let Some(u) = iter.next() {
            // single
            if u & PARENT_INFO_MASK == 0 {
                res.insert(*u as usize);
                continue;
            }
            // range
            let v = iter.next().unwrap();
            for i in (*u ^ PARENT_INFO_MASK)..(*v ^ PARENT_INFO_MASK) {
                res.insert(i as usize);
            }
        }

        res
    }

    pub fn to_proto(info: &BTreeSet<usize>) -> PbParentInfo {
        let mut proto = PbParentInfo::default();

        let handle = |info: &mut Vec<u64>, range: &[usize]| match range.len() {
            0 => {}
            1 => info.push(range[0] as u64),
            _ => {
                info.push(*range.first().unwrap() as u64 | PARENT_INFO_MASK);
                info.push((*range.last().unwrap() + 1) as u64 | PARENT_INFO_MASK);
            }
        };

        let info = info.iter().copied().collect_vec();

        let mut range = vec![];
        for i in 0..info.len() {
            if i > 0 && info[i - 1] + 1 != info[i] {
                handle(&mut proto.info, &range);
                range.clear();
            }
            range.push(info[i]);
        }
        handle(&mut proto.info, &range);

        proto
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockInheritance {
    pub parents: BTreeMap<HummockSstableObjectId, BTreeSet<usize>>,
}

impl BlockInheritance {
    pub fn from_proto(proto: &PbBlockInheritance) -> Self {
        Self {
            parents: proto
                .parents
                .iter()
                .map(|(sst_obj_id, proto)| (*sst_obj_id, ParentInfo::from_proto(proto)))
                .collect(),
        }
    }

    pub fn to_proto(&self) -> PbBlockInheritance {
        PbBlockInheritance {
            parents: self
                .parents
                .iter()
                .map(|(sst_obj_id, info)| (*sst_obj_id, ParentInfo::to_proto(info)))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enc_dec() {
        let mut si = SstableInheritance::default();
        si.blocks.push(BlockInheritance {
            parents: {
                let mut map = BTreeMap::default();
                map.insert(1, [1, 2, 3, 4, 6, 7, 9, 100, 1000].into_iter().collect());
                map.insert(2, [1, 2, 4, 5, 6, 10, 15, 20, 21, 22].into_iter().collect());
                map
            },
        });
        si.blocks.push(BlockInheritance {
            parents: {
                let mut map = BTreeMap::default();
                map.insert(3, [1, 2, 4, 6, 70].into_iter().collect());
                map.insert(4, [1, 2, 6, 10, 15, 20, 21, 22].into_iter().collect());
                map
            },
        });

        let proto = si.to_proto();
        let siv = SstableInheritance::from_proto(&proto);

        assert_eq!(si, siv);
    }
}
