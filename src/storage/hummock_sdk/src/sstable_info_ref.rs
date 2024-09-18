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

use std::sync::Arc;

use risingwave_pb::hummock::{PbBloomFilterType, PbHummockVersion, PbSstableInfo};

use crate::change_log::EpochNewChangeLogCommon;
use crate::key_range::KeyRange;
use crate::sstable_info::SstableInfo;
use crate::version::{HummockVersion, HummockVersionCommon, HummockVersionDeltaCommon};

pub type SstableInfoType = SstableInfoRef;
pub type EpochNewChangeLogType = EpochNewChangeLogCommon<SstableInfoType>;
pub type HummockVersionType = HummockVersionCommon<SstableInfoType>;
pub type HummockVersionDeltaType = HummockVersionDeltaCommon<SstableInfoType>;

pub trait SstableInfoReader {
    fn object_id(&self) -> u64;
    fn sst_id(&self) -> u64;
    fn key_range(&self) -> &KeyRange;
    fn file_size(&self) -> u64;
    fn table_ids(&self) -> &[u32];
    fn meta_offset(&self) -> u64;
    fn stale_key_count(&self) -> u64;
    fn total_key_count(&self) -> u64;
    fn min_epoch(&self) -> u64;
    fn max_epoch(&self) -> u64;
    fn uncompressed_file_size(&self) -> u64;
    fn range_tombstone_count(&self) -> u64;
    fn bloom_filter_kind(&self) -> PbBloomFilterType;
    fn sst_size(&self) -> u64;
}

pub trait SstableInfoWriter {
    fn set_sst_id(&mut self, v: u64);
    fn set_sst_size(&mut self, v: u64);
    fn set_table_ids(&mut self, v: Vec<u32>);
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct SstableInfoRef {
    sstable_info: Arc<SstableInfo>,
}

impl From<&PbSstableInfo> for SstableInfoRef {
    fn from(pb: &PbSstableInfo) -> Self {
        // TODO: get arc from global manager
        Self {
            sstable_info: Arc::new(SstableInfo::from(pb)),
        }
    }
}

impl From<PbSstableInfo> for SstableInfoRef {
    fn from(pb: PbSstableInfo) -> Self {
        (&pb).into()
    }
}

impl From<&SstableInfoRef> for PbSstableInfo {
    fn from(v: &SstableInfoRef) -> Self {
        (*v.sstable_info).clone().into()
    }
}

impl From<SstableInfoRef> for PbSstableInfo {
    fn from(v: SstableInfoRef) -> Self {
        (&v).into()
    }
}

/// Note that this conversion is inefficient. It should be used only in tests.
impl From<HummockVersion> for HummockVersionCommon<SstableInfoRef> {
    fn from(value: HummockVersion) -> Self {
        let pb = PbHummockVersion::from(value);
        HummockVersionCommon::from(&pb)
    }
}

impl SstableInfoReader for SstableInfoRef {
    fn object_id(&self) -> u64 {
        self.sstable_info.object_id()
    }

    fn sst_id(&self) -> u64 {
        self.sstable_info.sst_id()
    }

    fn key_range(&self) -> &KeyRange {
        self.sstable_info.key_range()
    }

    fn file_size(&self) -> u64 {
        self.sstable_info.file_size()
    }

    fn table_ids(&self) -> &[u32] {
        self.sstable_info.table_ids()
    }

    fn meta_offset(&self) -> u64 {
        self.sstable_info.meta_offset()
    }

    fn stale_key_count(&self) -> u64 {
        self.sstable_info.stale_key_count()
    }

    fn total_key_count(&self) -> u64 {
        self.sstable_info.total_key_count()
    }

    fn min_epoch(&self) -> u64 {
        self.sstable_info.min_epoch()
    }

    fn max_epoch(&self) -> u64 {
        self.sstable_info.max_epoch()
    }

    fn uncompressed_file_size(&self) -> u64 {
        self.sstable_info.uncompressed_file_size()
    }

    fn range_tombstone_count(&self) -> u64 {
        self.sstable_info.range_tombstone_count()
    }

    fn bloom_filter_kind(&self) -> PbBloomFilterType {
        self.sstable_info.bloom_filter_kind()
    }

    fn sst_size(&self) -> u64 {
        self.sstable_info.sst_size()
    }
}

/// Note that these setters are inefficient. Use a builder instead if necessary.
impl SstableInfoWriter for SstableInfoRef {
    fn set_sst_id(&mut self, v: u64) {
        let mut origin = (*self.sstable_info).clone();
        origin.set_sst_id(v);
        self.sstable_info = Arc::new(origin);
    }

    fn set_sst_size(&mut self, v: u64) {
        let mut origin = (*self.sstable_info).clone();
        origin.set_sst_size(v);
        self.sstable_info = Arc::new(origin);
    }

    fn set_table_ids(&mut self, v: Vec<u32>) {
        let mut origin = (*self.sstable_info).clone();
        origin.set_table_ids(v);
        self.sstable_info = Arc::new(origin);
    }
}

impl From<SstableInfo> for SstableInfoRef {
    fn from(sst: SstableInfo) -> Self {
        Self {
            sstable_info: Arc::new(sst),
        }
    }
}
