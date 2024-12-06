// Copyright 2024 RisingWave Labs
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

use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use risingwave_pb::hummock::{PbBloomFilterType, PbKeyRange, PbSstableInfo};

use crate::key_range::KeyRange;
use crate::version::{ObjectIdReader, SstableIdReader};
use crate::{HummockSstableId, HummockSstableObjectId};

#[derive(Debug, PartialEq, Clone, Default)]
pub struct SstableInfoInner {
    pub object_id: u64,
    pub sst_id: u64,
    pub key_range: KeyRange,
    pub file_size: u64,
    pub table_ids: Vec<u32>,
    pub meta_offset: u64,
    pub stale_key_count: u64,
    pub total_key_count: u64,
    pub min_epoch: u64,
    pub max_epoch: u64,
    pub uncompressed_file_size: u64,
    pub range_tombstone_count: u64,
    pub bloom_filter_kind: PbBloomFilterType,
    pub sst_size: u64,
}

impl SstableInfoInner {
    pub fn estimated_encode_len(&self) -> usize {
        let mut basic = size_of::<u64>() // object_id
            + size_of::<u64>() // sstable_id
            + size_of::<u64>() // file_size
            + self.table_ids.len() * size_of::<u32>() // table_ids
            + size_of::<u64>() // meta_offset
            + size_of::<u64>() // stale_key_count
            + size_of::<u64>() // total_key_count
            + size_of::<u64>() // min_epoch
            + size_of::<u64>() // max_epoch
            + size_of::<u64>() // uncompressed_file_size
            + size_of::<u64>() // range_tombstone_count
            + size_of::<u32>() // bloom_filter_kind
            + size_of::<u64>(); // sst_size
        basic += self.key_range.left.len() + self.key_range.right.len() + size_of::<bool>();

        basic
    }

    pub fn to_protobuf(&self) -> PbSstableInfo {
        self.into()
    }
}

impl From<PbSstableInfo> for SstableInfoInner {
    fn from(pb_sstable_info: PbSstableInfo) -> Self {
        assert!(pb_sstable_info.table_ids.is_sorted());
        Self {
            object_id: pb_sstable_info.object_id,
            sst_id: pb_sstable_info.sst_id,
            key_range: {
                // Due to the stripped key range, the key range may be `None`
                if let Some(pb_keyrange) = pb_sstable_info.key_range {
                    KeyRange {
                        left: pb_keyrange.left.into(),
                        right: pb_keyrange.right.into(),
                        right_exclusive: pb_keyrange.right_exclusive,
                    }
                } else {
                    KeyRange::inf()
                }
            },
            file_size: pb_sstable_info.file_size,
            table_ids: pb_sstable_info.table_ids.clone(),
            meta_offset: pb_sstable_info.meta_offset,
            stale_key_count: pb_sstable_info.stale_key_count,
            total_key_count: pb_sstable_info.total_key_count,
            min_epoch: pb_sstable_info.min_epoch,
            max_epoch: pb_sstable_info.max_epoch,
            uncompressed_file_size: pb_sstable_info.uncompressed_file_size,
            range_tombstone_count: pb_sstable_info.range_tombstone_count,
            bloom_filter_kind: PbBloomFilterType::try_from(pb_sstable_info.bloom_filter_kind)
                .unwrap(),
            sst_size: if pb_sstable_info.sst_size == 0 {
                pb_sstable_info.file_size
            } else {
                pb_sstable_info.sst_size
            },
        }
    }
}

impl From<&PbSstableInfo> for SstableInfoInner {
    fn from(pb_sstable_info: &PbSstableInfo) -> Self {
        assert!(pb_sstable_info.table_ids.is_sorted());
        Self {
            object_id: pb_sstable_info.object_id,
            sst_id: pb_sstable_info.sst_id,
            key_range: {
                if let Some(pb_keyrange) = &pb_sstable_info.key_range {
                    KeyRange {
                        left: pb_keyrange.left.clone().into(),
                        right: pb_keyrange.right.clone().into(),
                        right_exclusive: pb_keyrange.right_exclusive,
                    }
                } else {
                    KeyRange::inf()
                }
            },
            file_size: pb_sstable_info.file_size,
            table_ids: pb_sstable_info.table_ids.clone(),
            meta_offset: pb_sstable_info.meta_offset,
            stale_key_count: pb_sstable_info.stale_key_count,
            total_key_count: pb_sstable_info.total_key_count,
            min_epoch: pb_sstable_info.min_epoch,
            max_epoch: pb_sstable_info.max_epoch,
            uncompressed_file_size: pb_sstable_info.uncompressed_file_size,
            range_tombstone_count: pb_sstable_info.range_tombstone_count,
            bloom_filter_kind: PbBloomFilterType::try_from(pb_sstable_info.bloom_filter_kind)
                .unwrap(),
            sst_size: if pb_sstable_info.sst_size == 0 {
                pb_sstable_info.file_size
            } else {
                pb_sstable_info.sst_size
            },
        }
    }
}

impl From<SstableInfoInner> for PbSstableInfo {
    fn from(sstable_info: SstableInfoInner) -> Self {
        assert!(sstable_info.sst_size > 0);
        assert!(sstable_info.table_ids.is_sorted());
        PbSstableInfo {
            object_id: sstable_info.object_id,
            sst_id: sstable_info.sst_id,
            key_range: {
                let keyrange = sstable_info.key_range;
                if keyrange.inf_key_range() {
                    // For empty key range, we don't need to encode it
                    // Timetravel will use the default key range to stripped the PbSstableInfo
                    // Note: If new fields are added, using Default to implement stripped may not work, resulting in an increase in encode size.
                    None
                } else {
                    let pb_key_range = PbKeyRange {
                        left: keyrange.left.into(),
                        right: keyrange.right.into(),
                        right_exclusive: keyrange.right_exclusive,
                    };
                    Some(pb_key_range)
                }
            },

            file_size: sstable_info.file_size,
            table_ids: sstable_info.table_ids.clone(),
            meta_offset: sstable_info.meta_offset,
            stale_key_count: sstable_info.stale_key_count,
            total_key_count: sstable_info.total_key_count,
            min_epoch: sstable_info.min_epoch,
            max_epoch: sstable_info.max_epoch,
            uncompressed_file_size: sstable_info.uncompressed_file_size,
            range_tombstone_count: sstable_info.range_tombstone_count,
            bloom_filter_kind: sstable_info.bloom_filter_kind.into(),
            sst_size: sstable_info.sst_size,
        }
    }
}

impl From<&SstableInfoInner> for PbSstableInfo {
    fn from(sstable_info: &SstableInfoInner) -> Self {
        assert!(sstable_info.sst_size > 0);
        assert!(sstable_info.table_ids.is_sorted());
        PbSstableInfo {
            object_id: sstable_info.object_id,
            sst_id: sstable_info.sst_id,
            key_range: {
                let keyrange = &sstable_info.key_range;
                if keyrange.inf_key_range() {
                    None
                } else {
                    let pb_key_range = PbKeyRange {
                        left: keyrange.left.to_vec(),
                        right: keyrange.right.to_vec(),
                        right_exclusive: keyrange.right_exclusive,
                    };
                    Some(pb_key_range)
                }
            },

            file_size: sstable_info.file_size,
            table_ids: sstable_info.table_ids.clone(),
            meta_offset: sstable_info.meta_offset,
            stale_key_count: sstable_info.stale_key_count,
            total_key_count: sstable_info.total_key_count,
            min_epoch: sstable_info.min_epoch,
            max_epoch: sstable_info.max_epoch,
            uncompressed_file_size: sstable_info.uncompressed_file_size,
            range_tombstone_count: sstable_info.range_tombstone_count,
            bloom_filter_kind: sstable_info.bloom_filter_kind.into(),
            sst_size: sstable_info.sst_size,
        }
    }
}

impl SstableInfo {
    pub fn remove_key_range(&mut self) {
        let mut sst = self.get_inner();
        sst.key_range = KeyRange::default();
        *self = sst.into()
    }
}

impl SstableIdReader for SstableInfoInner {
    fn sst_id(&self) -> HummockSstableId {
        self.sst_id
    }
}

impl ObjectIdReader for SstableInfoInner {
    fn object_id(&self) -> HummockSstableObjectId {
        self.object_id
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct SstableInfo(Arc<SstableInfoInner>);

impl From<&PbSstableInfo> for SstableInfo {
    fn from(s: &PbSstableInfo) -> Self {
        SstableInfo(SstableInfoInner::from(s).into())
    }
}

impl From<PbSstableInfo> for SstableInfo {
    fn from(s: PbSstableInfo) -> Self {
        SstableInfo(SstableInfoInner::from(s).into())
    }
}

impl From<SstableInfo> for PbSstableInfo {
    fn from(s: SstableInfo) -> Self {
        (&s).into()
    }
}

impl From<SstableInfoInner> for SstableInfo {
    fn from(s: SstableInfoInner) -> Self {
        Self(s.into())
    }
}

impl From<&SstableInfo> for PbSstableInfo {
    fn from(s: &SstableInfo) -> Self {
        s.0.as_ref().into()
    }
}

impl Deref for SstableInfo {
    type Target = SstableInfoInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SstableInfo {
    pub fn get_inner(&self) -> SstableInfoInner {
        (*self.0).clone()
    }
}

impl SstableIdReader for SstableInfo {
    fn sst_id(&self) -> HummockSstableId {
        self.sst_id
    }
}

impl ObjectIdReader for SstableInfo {
    fn object_id(&self) -> HummockSstableObjectId {
        self.object_id
    }
}
