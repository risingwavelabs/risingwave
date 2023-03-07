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

#![feature(async_closure)]
#![feature(drain_filter)]
#![feature(hash_drain_filter)]
#![feature(lint_reasons)]
#![feature(map_many_mut)]
#![feature(bound_map)]

mod key_cmp;

#[macro_use]
extern crate num_derive;

use std::cmp::Ordering;

pub use key_cmp::*;
use risingwave_pb::common::{batch_query_epoch, BatchQueryEpoch};
use risingwave_pb::hummock::SstableInfo;

use crate::compaction_group::StaticCompactionGroupId;
use crate::key_range::KeyRangeCommon;
use crate::table_stats::{to_prost_table_stats_map, ProstTableStatsMap, TableStatsMap};

pub mod compact;
pub mod compaction_group;
pub mod filter_key_extractor;
pub mod key;
pub mod key_range;
pub mod prost_key_range;
pub mod table_stats;

pub type HummockSstableId = u64;
pub type HummockRefCount = u64;
pub type HummockVersionId = u64;
pub type HummockContextId = u32;
pub type HummockEpoch = u64;
pub type HummockCompactionTaskId = u64;
pub type CompactionGroupId = u64;
pub const INVALID_VERSION_ID: HummockVersionId = 0;
pub const FIRST_VERSION_ID: HummockVersionId = 1;
pub const SPLIT_TABLE_COMPACTION_GROUP_ID_HEAD: u64 = 1u64 << 56;
pub const SINGLE_TABLE_COMPACTION_GROUP_ID_HEAD: u64 = 2u64 << 56;

#[macro_export]
/// This is wrapper for `info` log.
///
/// In our CI tests, we frequently create and drop tables, and checkpoint in all barriers, which may
/// cause many events. However, these events are not expected to be frequent in production usage, so
/// we print an info log for every these events. But these events are frequent in CI, and produce
/// many logs in CI, and we may want to downgrade the log level of these event log to debug.
/// Therefore, we provide this macro to wrap the `info` log, which will produce `info` log when
/// `debug_assertions` is not enabled, and `debug` log when `debug_assertions` is enabled.
macro_rules! info_in_release {
    ($($arg:tt)*) => {
        {
            #[cfg(debug_assertions)]
            {
                use tracing::debug;
                debug!($($arg)*);
            }
            #[cfg(not(debug_assertions))]
            {
                use tracing::info;
                info!($($arg)*);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalSstableInfo {
    pub compaction_group_id: CompactionGroupId,
    pub sst_info: SstableInfo,
    pub table_stats: TableStatsMap,
}

impl LocalSstableInfo {
    pub fn new(
        compaction_group_id: CompactionGroupId,
        sst_info: SstableInfo,
        table_stats: TableStatsMap,
    ) -> Self {
        Self {
            compaction_group_id,
            sst_info,
            table_stats,
        }
    }

    pub fn with_compaction_group(
        compaction_group_id: CompactionGroupId,
        sst_info: SstableInfo,
    ) -> Self {
        Self::new(compaction_group_id, sst_info, TableStatsMap::default())
    }

    pub fn with_stats(sst_info: SstableInfo, table_stats: TableStatsMap) -> Self {
        Self::new(
            StaticCompactionGroupId::StateDefault as CompactionGroupId,
            sst_info,
            table_stats,
        )
    }

    pub fn for_test(sst_info: SstableInfo) -> Self {
        Self {
            compaction_group_id: StaticCompactionGroupId::StateDefault as CompactionGroupId,
            sst_info,
            table_stats: Default::default(),
        }
    }

    pub fn file_size(&self) -> u64 {
        self.sst_info.file_size
    }
}

#[derive(Debug, Clone)]
pub struct ExtendedSstableInfo {
    pub compaction_group_id: CompactionGroupId,
    pub sst_info: SstableInfo,
    pub table_stats: ProstTableStatsMap,
}

impl ExtendedSstableInfo {
    pub fn new(
        compaction_group_id: CompactionGroupId,
        sst_info: SstableInfo,
        table_stats: ProstTableStatsMap,
    ) -> Self {
        Self {
            compaction_group_id,
            sst_info,
            table_stats,
        }
    }

    pub fn with_compaction_group(
        compaction_group_id: CompactionGroupId,
        sst_info: SstableInfo,
    ) -> Self {
        Self::new(compaction_group_id, sst_info, ProstTableStatsMap::default())
    }
}

impl From<LocalSstableInfo> for ExtendedSstableInfo {
    fn from(value: LocalSstableInfo) -> Self {
        Self {
            compaction_group_id: value.compaction_group_id,
            sst_info: value.sst_info,
            table_stats: to_prost_table_stats_map(value.table_stats),
        }
    }
}

impl PartialEq for LocalSstableInfo {
    fn eq(&self, other: &Self) -> bool {
        self.compaction_group_id == other.compaction_group_id && self.sst_info == other.sst_info
    }
}

/// Package read epoch of hummock, it be used for `wait_epoch`
#[derive(Debug, Clone)]
pub enum HummockReadEpoch {
    /// We need to wait the `max_committed_epoch`
    Committed(HummockEpoch),
    /// We need to wait the `max_current_epoch`
    Current(HummockEpoch),
    /// We don't need to wait epoch, we usually do stream reading with it.
    NoWait(HummockEpoch),
    /// We don't need to wait epoch.
    Backup(HummockEpoch),
}

impl From<BatchQueryEpoch> for HummockReadEpoch {
    fn from(e: BatchQueryEpoch) -> Self {
        match e.epoch.unwrap() {
            batch_query_epoch::Epoch::Committed(epoch) => HummockReadEpoch::Committed(epoch),
            batch_query_epoch::Epoch::Current(epoch) => HummockReadEpoch::Current(epoch),
            batch_query_epoch::Epoch::Backup(epoch) => HummockReadEpoch::Backup(epoch),
        }
    }
}

pub fn to_committed_batch_query_epoch(epoch: u64) -> BatchQueryEpoch {
    BatchQueryEpoch {
        epoch: Some(batch_query_epoch::Epoch::Committed(epoch)),
    }
}

impl HummockReadEpoch {
    pub fn get_epoch(&self) -> HummockEpoch {
        *match self {
            HummockReadEpoch::Committed(epoch) => epoch,
            HummockReadEpoch::Current(epoch) => epoch,
            HummockReadEpoch::NoWait(epoch) => epoch,
            HummockReadEpoch::Backup(epoch) => epoch,
        }
    }
}
pub struct SstIdRange {
    // inclusive
    pub start_id: HummockSstableId,
    // exclusive
    pub end_id: HummockSstableId,
}

impl SstIdRange {
    pub fn new(start_id: HummockSstableId, end_id: HummockSstableId) -> Self {
        Self { start_id, end_id }
    }

    pub fn peek_next_sst_id(&self) -> Option<HummockSstableId> {
        if self.start_id < self.end_id {
            return Some(self.start_id);
        }
        None
    }

    /// Pops and returns next SST id.
    pub fn get_next_sst_id(&mut self) -> Option<HummockSstableId> {
        let next_id = self.peek_next_sst_id();
        self.start_id += 1;
        next_id
    }
}

pub fn can_concat(ssts: &[SstableInfo]) -> bool {
    let len = ssts.len();
    for i in 0..len - 1 {
        if ssts[i]
            .key_range
            .as_ref()
            .unwrap()
            .compare_right_with(&ssts[i + 1].key_range.as_ref().unwrap().left)
            != Ordering::Less
        {
            return false;
        }
    }
    true
}
