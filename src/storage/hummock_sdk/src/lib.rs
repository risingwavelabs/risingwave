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

#![feature(async_closure)]
#![feature(extract_if)]
#![feature(hash_extract_if)]
#![feature(map_many_mut)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#![feature(let_chains)]
#![feature(btree_cursors)]
#![feature(strict_overflow_ops)]

mod key_cmp;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};

pub use key_cmp::*;
use risingwave_common::util::epoch::EPOCH_SPILL_TIME_MASK;
use risingwave_pb::common::{batch_query_epoch, BatchQueryEpoch};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sstable_info::SstableInfo;
use tracing::warn;

use crate::key_range::KeyRangeCommon;
use crate::table_stats::TableStatsMap;

pub mod change_log;
pub mod compact;
pub mod compact_task;
pub mod compaction_group;
pub mod key;
pub mod key_range;
pub mod level;
pub mod prost_key_range;
pub mod sstable_info;
pub mod state_table_info;
pub mod table_stats;
pub mod table_watermark;
pub mod time_travel;
pub mod version;
pub use frontend_version::{FrontendHummockVersion, FrontendHummockVersionDelta};
mod frontend_version;

pub use compact::*;
use risingwave_common::catalog::TableId;

use crate::table_watermark::TableWatermarks;

pub type HummockSstableObjectId = u64;
pub type HummockSstableId = u64;
pub type HummockRefCount = u64;
pub type HummockContextId = u32;
pub type HummockEpoch = u64;
pub type HummockCompactionTaskId = u64;
pub type CompactionGroupId = u64;

#[derive(Debug, Clone, PartialEq, Copy, Ord, PartialOrd, Eq, Hash)]
pub struct HummockVersionId(u64);

impl Display for HummockVersionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for HummockVersionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for HummockVersionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(<u64 as Deserialize>::deserialize(deserializer)?))
    }
}

impl HummockVersionId {
    pub const MAX: Self = Self(i64::MAX as _);

    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }
}

impl Add<u64> for HummockVersionId {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub for HummockVersionId {
    type Output = u64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

pub const INVALID_VERSION_ID: HummockVersionId = HummockVersionId(0);
pub const FIRST_VERSION_ID: HummockVersionId = HummockVersionId(1);
pub const SPLIT_TABLE_COMPACTION_GROUP_ID_HEAD: u64 = 1u64 << 56;
pub const SINGLE_TABLE_COMPACTION_GROUP_ID_HEAD: u64 = 2u64 << 56;
pub const OBJECT_SUFFIX: &str = "data";
pub const HUMMOCK_SSTABLE_OBJECT_ID_MAX_DECIMAL_LENGTH: usize = 20;

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

#[derive(Default, Debug)]
pub struct SyncResult {
    /// The size of all synced shared buffers.
    pub sync_size: usize,
    /// The `sst_info` of sync.
    pub uncommitted_ssts: Vec<LocalSstableInfo>,
    /// The collected table watermarks written by state tables.
    pub table_watermarks: HashMap<TableId, TableWatermarks>,
    /// Sstable that holds the uncommitted old value
    pub old_value_ssts: Vec<LocalSstableInfo>,
}

#[derive(Debug, Clone)]
pub struct LocalSstableInfo {
    pub sst_info: SstableInfo,
    pub table_stats: TableStatsMap,
    pub created_at: u64,
}

impl LocalSstableInfo {
    pub fn new(sst_info: SstableInfo, table_stats: TableStatsMap, created_at: u64) -> Self {
        Self {
            sst_info,
            table_stats,
            created_at,
        }
    }

    pub fn for_test(sst_info: SstableInfo) -> Self {
        Self {
            sst_info,
            table_stats: Default::default(),
            created_at: u64::MAX,
        }
    }

    pub fn file_size(&self) -> u64 {
        assert_eq!(self.sst_info.file_size, self.sst_info.sst_size);
        self.sst_info.file_size
    }
}

impl PartialEq for LocalSstableInfo {
    fn eq(&self, other: &Self) -> bool {
        self.sst_info == other.sst_info
    }
}

/// Package read epoch of hummock, it be used for `wait_epoch`
#[derive(Debug, Clone, Copy)]
pub enum HummockReadEpoch {
    /// We need to wait the `committed_epoch` of the read table
    Committed(HummockEpoch),
    /// We need to wait the `committed_epoch` of the read table and also the hummock version to the version id
    BatchQueryCommitted(HummockEpoch, HummockVersionId),
    /// We don't need to wait epoch, we usually do stream reading with it.
    NoWait(HummockEpoch),
    /// We don't need to wait epoch.
    Backup(HummockEpoch),
    TimeTravel(HummockEpoch),
}

impl From<BatchQueryEpoch> for HummockReadEpoch {
    fn from(e: BatchQueryEpoch) -> Self {
        match e.epoch.unwrap() {
            batch_query_epoch::Epoch::Committed(epoch) => HummockReadEpoch::BatchQueryCommitted(
                epoch.epoch,
                HummockVersionId::new(epoch.hummock_version_id),
            ),
            batch_query_epoch::Epoch::Current(epoch) => {
                if epoch != HummockEpoch::MAX {
                    warn!(
                        epoch,
                        "ignore specified current epoch and set it to u64::MAX"
                    );
                }
                HummockReadEpoch::NoWait(HummockEpoch::MAX)
            }
            batch_query_epoch::Epoch::Backup(epoch) => HummockReadEpoch::Backup(epoch),
            batch_query_epoch::Epoch::TimeTravel(epoch) => HummockReadEpoch::TimeTravel(epoch),
        }
    }
}

pub fn test_batch_query_epoch() -> BatchQueryEpoch {
    BatchQueryEpoch {
        epoch: Some(batch_query_epoch::Epoch::Current(u64::MAX)),
    }
}

impl HummockReadEpoch {
    pub fn get_epoch(&self) -> HummockEpoch {
        *match self {
            HummockReadEpoch::Committed(epoch)
            | HummockReadEpoch::BatchQueryCommitted(epoch, _)
            | HummockReadEpoch::NoWait(epoch)
            | HummockReadEpoch::Backup(epoch)
            | HummockReadEpoch::TimeTravel(epoch) => epoch,
        }
    }

    pub fn is_read_committed(&self) -> bool {
        match self {
            HummockReadEpoch::Committed(_)
            | HummockReadEpoch::TimeTravel(_)
            | HummockReadEpoch::BatchQueryCommitted(_, _) => true,
            HummockReadEpoch::NoWait(_) | HummockReadEpoch::Backup(_) => false,
        }
    }
}
pub struct SstObjectIdRange {
    // inclusive
    pub start_id: HummockSstableObjectId,
    // exclusive
    pub end_id: HummockSstableObjectId,
}

impl SstObjectIdRange {
    pub fn new(start_id: HummockSstableObjectId, end_id: HummockSstableObjectId) -> Self {
        Self { start_id, end_id }
    }

    pub fn peek_next_sst_object_id(&self) -> Option<HummockSstableObjectId> {
        if self.start_id < self.end_id {
            return Some(self.start_id);
        }
        None
    }

    /// Pops and returns next SST id.
    pub fn get_next_sst_object_id(&mut self) -> Option<HummockSstableObjectId> {
        let next_id = self.peek_next_sst_object_id();
        self.start_id += 1;
        next_id
    }
}

pub fn can_concat(ssts: &[SstableInfo]) -> bool {
    let len = ssts.len();
    for i in 1..len {
        if ssts[i - 1]
            .key_range
            .compare_right_with(&ssts[i].key_range.left)
            != Ordering::Less
        {
            return false;
        }
    }
    true
}

pub fn full_key_can_concat(ssts: &[SstableInfo]) -> bool {
    let len = ssts.len();
    for i in 1..len {
        let sst_1 = &ssts[i - 1];
        let sst_2 = &ssts[i];

        if sst_1.key_range.right_exclusive {
            if KeyComparator::compare_encoded_full_key(
                &sst_1.key_range.right,
                &sst_2.key_range.left,
            )
            .is_gt()
            {
                return false;
            }
        } else if KeyComparator::compare_encoded_full_key(
            &sst_1.key_range.right,
            &sst_2.key_range.left,
        )
        .is_ge()
        {
            return false;
        }
    }
    true
}

const CHECKPOINT_DIR: &str = "checkpoint";
const CHECKPOINT_NAME: &str = "0";
const ARCHIVE_DIR: &str = "archive";

pub fn version_checkpoint_path(root_dir: &str) -> String {
    format!("{}/{}/{}", root_dir, CHECKPOINT_DIR, CHECKPOINT_NAME)
}

pub fn version_archive_dir(root_dir: &str) -> String {
    format!("{}/{}", root_dir, ARCHIVE_DIR)
}

pub fn version_checkpoint_dir(checkpoint_path: &str) -> String {
    checkpoint_path.trim_end_matches(|c| c != '/').to_owned()
}

/// Represents an epoch with a gap.
///
/// When a spill of the mem table occurs between two epochs, `EpochWithGap` generates an offset.
/// This offset is encoded when performing full key encoding. When returning to the upper-level
/// interface, a pure epoch with the lower 16 bits set to 0 should be returned.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug, PartialOrd, Ord)]
pub struct EpochWithGap(u64);

impl EpochWithGap {
    #[allow(unused_variables)]
    pub fn new(epoch: u64, spill_offset: u16) -> Self {
        // We only use 48 high bit to store epoch and use 16 low bit to store spill offset. But for MAX epoch,
        // we still keep `u64::MAX` because we have use it in delete range and persist this value to sstable files.
        //  So for compatibility, we must skip checking it for u64::MAX. See bug description in https://github.com/risingwavelabs/risingwave/issues/13717
        if risingwave_common::util::epoch::is_max_epoch(epoch) {
            EpochWithGap::new_max_epoch()
        } else {
            debug_assert!((epoch & EPOCH_SPILL_TIME_MASK) == 0);
            EpochWithGap(epoch + spill_offset as u64)
        }
    }

    pub fn new_from_epoch(epoch: u64) -> Self {
        EpochWithGap::new(epoch, 0)
    }

    pub fn new_min_epoch() -> Self {
        EpochWithGap(0)
    }

    pub fn new_max_epoch() -> Self {
        EpochWithGap(HummockEpoch::MAX)
    }

    // return the epoch_with_gap(epoch + spill_offset)
    pub(crate) fn as_u64(&self) -> HummockEpoch {
        self.0
    }

    // return the epoch_with_gap(epoch + spill_offset)
    pub fn from_u64(epoch_with_gap: u64) -> Self {
        EpochWithGap(epoch_with_gap)
    }

    // return the pure epoch without spill offset
    pub fn pure_epoch(&self) -> HummockEpoch {
        self.0 & !EPOCH_SPILL_TIME_MASK
    }

    pub fn offset(&self) -> u64 {
        self.0 & EPOCH_SPILL_TIME_MASK
    }
}

pub fn get_sst_data_path(
    obj_prefix: &str,
    path_prefix: &str,
    object_id: HummockSstableObjectId,
) -> String {
    let mut path = String::with_capacity(
        path_prefix.len()
            + "/".len()
            + obj_prefix.len()
            + HUMMOCK_SSTABLE_OBJECT_ID_MAX_DECIMAL_LENGTH
            + ".".len()
            + OBJECT_SUFFIX.len(),
    );
    path.push_str(path_prefix);
    path.push('/');
    path.push_str(obj_prefix);
    path.push_str(&object_id.to_string());
    path.push('.');
    path.push_str(OBJECT_SUFFIX);
    path
}

pub fn get_object_id_from_path(path: &str) -> HummockSstableObjectId {
    use itertools::Itertools;
    let split = path.split(&['/', '.']).collect_vec();
    assert!(split.len() > 2);
    assert_eq!(split[split.len() - 1], OBJECT_SUFFIX);
    split[split.len() - 2]
        .parse::<HummockSstableObjectId>()
        .expect("valid sst id")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use sstable_info::SstableInfoInner;

    use super::*;

    #[test]
    fn test_object_id_decimal_max_length() {
        let len = HummockSstableObjectId::MAX.to_string().len();
        assert_eq!(len, HUMMOCK_SSTABLE_OBJECT_ID_MAX_DECIMAL_LENGTH)
    }

    #[test]
    fn test_full_key_concat() {
        let key1 = b"\0\0\0\x08\0\0\0\x0112-3\0\0\0\0\x04\0\x1c\x16l'\xe2\0\0";
        let key2 = b"\0\0\0\x08\0\0\0\x0112-3\0\0\0\0\x04\0\x1c\x16l \x12\0\0";

        let sst_1 = SstableInfoInner {
            key_range: key_range::KeyRange {
                left: Bytes::from(key1.to_vec()),
                right: Bytes::from(key1.to_vec()),
                right_exclusive: false,
            },
            ..Default::default()
        };

        let sst_2 = SstableInfoInner {
            key_range: key_range::KeyRange {
                left: Bytes::from(key2.to_vec()),
                right: Bytes::from(key2.to_vec()),
                right_exclusive: false,
            },
            ..Default::default()
        };

        let sst_3 = SstableInfoInner {
            key_range: key_range::KeyRange {
                left: Bytes::from(key1.to_vec()),
                right: Bytes::from(key2.to_vec()),
                right_exclusive: false,
            },
            ..Default::default()
        };

        assert!(full_key_can_concat(&[sst_1.clone().into(), sst_2.into()]));

        assert!(!full_key_can_concat(&[sst_1.into(), sst_3.into()]));
    }
}
