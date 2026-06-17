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

use std::collections::BTreeMap;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_pb::hummock::{
    PbBloomFilterType, PbKeyRange, PbSstableFilterLayout, PbSstableFilterType, PbSstableInfo,
    PbVnodeStatistics, PbVnodeUserKeyRange,
};

use crate::key::UserKey;
use crate::key_range::KeyRange;
use crate::version::{ObjectIdReader, SstableIdReader};
use crate::{HummockSstableId, HummockSstableObjectId};

pub type VnodeUserKeyRange = (UserKey<Bytes>, UserKey<Bytes>);

#[derive(Debug, PartialEq, Clone, Default)]
pub struct VnodeStatistics {
    /// Per-vnode user key ranges as closed intervals: [`min_user_key`, `max_user_key`].
    vnode_user_key_ranges: BTreeMap<VirtualNode, VnodeUserKeyRange>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SstableInfoInner {
    pub object_id: HummockSstableObjectId,
    pub sst_id: HummockSstableId,
    pub key_range: KeyRange,
    pub file_size: u64,
    pub table_ids: Vec<TableId>,
    pub meta_offset: u64,
    pub stale_key_count: u64,
    pub total_key_count: u64,
    pub min_epoch: u64,
    pub max_epoch: u64,
    pub uncompressed_file_size: u64,
    pub range_tombstone_count: u64,
    pub filter_type: PbSstableFilterType,
    pub filter_layout: PbSstableFilterLayout,
    pub sst_size: u64,
    pub vnode_statistics: Option<VnodeStatistics>,
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
            + size_of::<u64>(); // sst_size
        basic += size_of::<u32>(); // filter_type
        if self.filter_layout != PbSstableFilterLayout::Unspecified {
            basic += size_of::<u32>(); // filter_layout
        }
        basic += self.key_range.left.len() + self.key_range.right.len() + size_of::<bool>();
        if let Some(vnode_statistics) = &self.vnode_statistics {
            for (min_key, max_key) in vnode_statistics.vnode_user_key_ranges.values() {
                basic += size_of::<u32>() + min_key.encoded_len() + max_key.encoded_len();
            }
        }

        basic
    }

    pub fn to_protobuf(&self) -> PbSstableInfo {
        self.into()
    }
}

#[cfg(any(test, feature = "test"))]
impl Default for SstableInfoInner {
    fn default() -> Self {
        PbSstableInfo::default().into()
    }
}

fn filter_metadata_from_pb(
    filter_type: Option<i32>,
    filter_layout: Option<i32>,
    bloom_filter_kind: i32,
) -> (PbSstableFilterType, PbSstableFilterLayout) {
    let bloom_filter_kind = PbBloomFilterType::try_from(bloom_filter_kind)
        .expect("invalid legacy bloom_filter_kind in SST info");

    // Exactly one metadata format is active for each SST: legacy SSTs use
    // `bloom_filter_kind`, while new SSTs use `filter_type` plus optional `filter_layout`.
    if let Some(filter_type) = filter_type {
        assert_eq!(
            bloom_filter_kind,
            PbBloomFilterType::BloomFilterUnspecified,
            "new SST filter metadata must not set legacy bloom_filter_kind"
        );
        let filter_type =
            PbSstableFilterType::try_from(filter_type).expect("invalid filter_type in SST info");
        assert_ne!(
            filter_type,
            PbSstableFilterType::SstableFilterUnspecified,
            "new SST filter metadata must use a resolved filter_type"
        );

        let filter_layout = filter_layout
            .map(|filter_layout| {
                PbSstableFilterLayout::try_from(filter_layout)
                    .expect("invalid filter_layout in SST info")
            })
            .unwrap_or(PbSstableFilterLayout::Unspecified);
        if filter_type == PbSstableFilterType::SstableFilterNone {
            assert_eq!(
                filter_layout,
                PbSstableFilterLayout::Unspecified,
                "SST filter metadata with no filter must not set filter_layout"
            );
        }

        return (filter_type, filter_layout);
    }

    assert!(
        filter_layout.is_none(),
        "legacy SST filter metadata must not set filter_layout"
    );
    match bloom_filter_kind {
        PbBloomFilterType::BloomFilterUnspecified => (
            PbSstableFilterType::SstableFilterNone,
            PbSstableFilterLayout::Unspecified,
        ),
        PbBloomFilterType::Sstable => (
            PbSstableFilterType::SstableFilterXor16,
            PbSstableFilterLayout::Plain,
        ),
        PbBloomFilterType::Blocked => (
            PbSstableFilterType::SstableFilterXor16,
            PbSstableFilterLayout::Blocked,
        ),
    }
}

fn assert_resolved_filter_metadata(
    filter_type: PbSstableFilterType,
    filter_layout: PbSstableFilterLayout,
) {
    assert_ne!(
        filter_type,
        PbSstableFilterType::SstableFilterUnspecified,
        "SST filter metadata must use a resolved filter_type"
    );
    if filter_type == PbSstableFilterType::SstableFilterNone {
        assert_eq!(
            filter_layout,
            PbSstableFilterLayout::Unspecified,
            "SST filter metadata with no filter must not set filter_layout"
        );
    }
}

impl From<&PbVnodeStatistics> for VnodeStatistics {
    fn from(info: &PbVnodeStatistics) -> Self {
        Self {
            vnode_user_key_ranges: info
                .vnode_user_key_ranges
                .iter()
                .map(|(&vnode, range)| {
                    let min_key = UserKey::decode(&range.min_key).copy_into();
                    let max_key = UserKey::decode(&range.max_key).copy_into();

                    // assert shared same vnode and table-id
                    assert_eq!(min_key.table_id, max_key.table_id);
                    assert_eq!(min_key.get_vnode_id(), max_key.get_vnode_id());

                    (VirtualNode::from_index(vnode as usize), (min_key, max_key))
                })
                .collect(),
        }
    }
}

impl From<PbVnodeStatistics> for VnodeStatistics {
    fn from(info: PbVnodeStatistics) -> Self {
        (&info).into()
    }
}

impl From<&VnodeStatistics> for PbVnodeStatistics {
    fn from(info: &VnodeStatistics) -> Self {
        Self {
            vnode_user_key_ranges: info
                .vnode_user_key_ranges
                .iter()
                .map(|(vnode, (min_key, max_key))| {
                    (
                        vnode.to_index() as u32,
                        PbVnodeUserKeyRange {
                            min_key: min_key.encode(),
                            max_key: max_key.encode(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<VnodeStatistics> for PbVnodeStatistics {
    fn from(info: VnodeStatistics) -> Self {
        (&info).into()
    }
}

impl From<PbSstableInfo> for SstableInfoInner {
    fn from(pb_sstable_info: PbSstableInfo) -> Self {
        assert!(pb_sstable_info.table_ids.is_sorted());
        let (filter_type, filter_layout) = filter_metadata_from_pb(
            pb_sstable_info.filter_type,
            pb_sstable_info.filter_layout,
            pb_sstable_info.bloom_filter_kind,
        );
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
            table_ids: pb_sstable_info.table_ids,
            meta_offset: pb_sstable_info.meta_offset,
            stale_key_count: pb_sstable_info.stale_key_count,
            total_key_count: pb_sstable_info.total_key_count,
            min_epoch: pb_sstable_info.min_epoch,
            max_epoch: pb_sstable_info.max_epoch,
            uncompressed_file_size: pb_sstable_info.uncompressed_file_size,
            range_tombstone_count: pb_sstable_info.range_tombstone_count,
            filter_type,
            filter_layout,
            sst_size: if pb_sstable_info.sst_size == 0 {
                pb_sstable_info.file_size
            } else {
                pb_sstable_info.sst_size
            },
            vnode_statistics: pb_sstable_info
                .vnode_statistics
                .as_ref()
                .map(VnodeStatistics::from),
        }
    }
}

impl From<&PbSstableInfo> for SstableInfoInner {
    fn from(pb_sstable_info: &PbSstableInfo) -> Self {
        assert!(pb_sstable_info.table_ids.is_sorted());
        let (filter_type, filter_layout) = filter_metadata_from_pb(
            pb_sstable_info.filter_type,
            pb_sstable_info.filter_layout,
            pb_sstable_info.bloom_filter_kind,
        );
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
            filter_type,
            filter_layout,
            sst_size: if pb_sstable_info.sst_size == 0 {
                pb_sstable_info.file_size
            } else {
                pb_sstable_info.sst_size
            },
            vnode_statistics: pb_sstable_info
                .vnode_statistics
                .as_ref()
                .map(VnodeStatistics::from),
        }
    }
}

impl From<SstableInfoInner> for PbSstableInfo {
    fn from(sstable_info: SstableInfoInner) -> Self {
        assert!(sstable_info.table_ids.is_sorted());
        assert_resolved_filter_metadata(sstable_info.filter_type, sstable_info.filter_layout);
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
            table_ids: sstable_info.table_ids,
            meta_offset: sstable_info.meta_offset,
            stale_key_count: sstable_info.stale_key_count,
            total_key_count: sstable_info.total_key_count,
            min_epoch: sstable_info.min_epoch,
            max_epoch: sstable_info.max_epoch,
            uncompressed_file_size: sstable_info.uncompressed_file_size,
            range_tombstone_count: sstable_info.range_tombstone_count,
            bloom_filter_kind: PbBloomFilterType::BloomFilterUnspecified as i32,
            filter_type: Some(sstable_info.filter_type.into()),
            filter_layout: (sstable_info.filter_layout != PbSstableFilterLayout::Unspecified)
                .then_some(sstable_info.filter_layout.into()),
            sst_size: sstable_info.sst_size,
            vnode_statistics: sstable_info
                .vnode_statistics
                .as_ref()
                .map(PbVnodeStatistics::from),
        }
    }
}

impl From<&SstableInfoInner> for PbSstableInfo {
    fn from(sstable_info: &SstableInfoInner) -> Self {
        assert!(sstable_info.table_ids.is_sorted());
        assert_resolved_filter_metadata(sstable_info.filter_type, sstable_info.filter_layout);
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
            bloom_filter_kind: PbBloomFilterType::BloomFilterUnspecified as i32,
            filter_type: Some(sstable_info.filter_type.into()),
            filter_layout: (sstable_info.filter_layout != PbSstableFilterLayout::Unspecified)
                .then_some(sstable_info.filter_layout.into()),
            sst_size: sstable_info.sst_size,
            vnode_statistics: sstable_info
                .vnode_statistics
                .as_ref()
                .map(PbVnodeStatistics::from),
        }
    }
}

impl VnodeStatistics {
    pub fn from_map(vnode_user_key_ranges: BTreeMap<VirtualNode, VnodeUserKeyRange>) -> Self {
        Self {
            vnode_user_key_ranges,
        }
    }

    /// Returns vnode user key range (`min_user_key`, `max_user_key`) if available.
    pub fn get_vnode_user_key_range(&self, vnode: VirtualNode) -> Option<&VnodeUserKeyRange> {
        self.vnode_user_key_ranges.get(&vnode)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn vnode_user_key_ranges(&self) -> &BTreeMap<VirtualNode, VnodeUserKeyRange> {
        &self.vnode_user_key_ranges
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

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
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

    pub fn set_inner(&mut self, inner: SstableInfoInner) {
        self.0 = Arc::new(inner);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_metadata_from_pb() {
        let cases = [
            (
                "new filter metadata",
                None,
                Some(PbSstableFilterType::SstableFilterXor8),
                Some(PbSstableFilterLayout::Plain),
                PbSstableFilterType::SstableFilterXor8,
                PbSstableFilterLayout::Plain,
            ),
            (
                "legacy blocked bloom kind",
                Some(PbBloomFilterType::Blocked),
                None,
                None,
                PbSstableFilterType::SstableFilterXor16,
                PbSstableFilterLayout::Blocked,
            ),
            (
                "legacy plain bloom kind",
                Some(PbBloomFilterType::Sstable),
                None,
                None,
                PbSstableFilterType::SstableFilterXor16,
                PbSstableFilterLayout::Plain,
            ),
            (
                "explicit no filter",
                None,
                Some(PbSstableFilterType::SstableFilterNone),
                None,
                PbSstableFilterType::SstableFilterNone,
                PbSstableFilterLayout::Unspecified,
            ),
        ];

        for (
            case_name,
            bloom_filter_kind,
            filter_type,
            filter_layout,
            expected_filter_type,
            expected_filter_layout,
        ) in cases
        {
            let sst_info = SstableInfoInner::from(PbSstableInfo {
                bloom_filter_kind: bloom_filter_kind
                    .unwrap_or(PbBloomFilterType::BloomFilterUnspecified)
                    .into(),
                filter_type: filter_type.map(Into::into),
                filter_layout: filter_layout.map(Into::into),
                ..Default::default()
            });

            assert_eq!(sst_info.filter_type, expected_filter_type, "{case_name}");
            assert_eq!(
                sst_info.filter_layout, expected_filter_layout,
                "{case_name}"
            );
        }

        let sst_info = SstableInfoInner {
            filter_type: PbSstableFilterType::SstableFilterNone,
            filter_layout: PbSstableFilterLayout::Unspecified,
            ..Default::default()
        };
        let pb_sst_info = PbSstableInfo::from(sst_info);
        assert_eq!(
            pb_sst_info.bloom_filter_kind,
            PbBloomFilterType::BloomFilterUnspecified as i32
        );
        assert_eq!(
            pb_sst_info.filter_type,
            Some(PbSstableFilterType::SstableFilterNone as i32)
        );
        assert_eq!(pb_sst_info.filter_layout, None);
    }
}
