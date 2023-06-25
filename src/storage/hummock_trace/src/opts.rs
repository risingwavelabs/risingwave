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

use bincode::{Decode, Encode};
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_hummock_sdk::HummockReadEpoch;

use crate::TracedBytes;

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedPrefetchOptions {
    pub exhaust_iter: bool,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TracedCachePolicy {
    Disable,
    Fill(TracedCachePriority),
    NotFill,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TracedCachePriority {
    High,
    Low,
}

impl From<CachePriority> for TracedCachePriority {
    fn from(value: CachePriority) -> Self {
        match value {
            CachePriority::High => Self::High,
            CachePriority::Low => Self::Low,
        }
    }
}

impl From<TracedCachePriority> for CachePriority {
    fn from(value: TracedCachePriority) -> Self {
        match value {
            TracedCachePriority::High => Self::High,
            TracedCachePriority::Low => Self::Low,
        }
    }
}

#[derive(Copy, Encode, Decode, PartialEq, Eq, Debug, Clone, Hash)]
pub struct TracedTableId {
    pub table_id: u32,
}

impl From<TableId> for TracedTableId {
    fn from(value: TableId) -> Self {
        Self {
            table_id: value.table_id,
        }
    }
}

impl From<TracedTableId> for TableId {
    fn from(value: TracedTableId) -> Self {
        Self {
            table_id: value.table_id,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedReadOptions {
    pub prefix_hint: Option<TracedBytes>,
    pub ignore_range_tombstone: bool,
    pub prefetch_options: TracedPrefetchOptions,
    pub cache_policy: TracedCachePolicy,

    pub retention_seconds: Option<u32>,
    pub table_id: TracedTableId,
    pub read_version_from_backup: bool,
}

impl TracedReadOptions {
    pub fn for_test(table_id: u32) -> Self {
        Self {
            prefix_hint: Some(TracedBytes::from(vec![0])),
            ignore_range_tombstone: true,
            prefetch_options: TracedPrefetchOptions { exhaust_iter: true },
            cache_policy: TracedCachePolicy::Disable,
            retention_seconds: None,
            table_id: TracedTableId { table_id },
            read_version_from_backup: true,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedWriteOptions {
    pub epoch: u64,
    pub table_id: TracedTableId,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct TracedTableOption {
    pub retention_seconds: Option<u32>,
}

impl From<TableOption> for TracedTableOption {
    fn from(value: TableOption) -> Self {
        Self {
            retention_seconds: value.retention_seconds,
        }
    }
}

impl From<TracedTableOption> for TableOption {
    fn from(value: TracedTableOption) -> Self {
        Self {
            retention_seconds: value.retention_seconds,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct TracedNewLocalOptions {
    pub table_id: TracedTableId,
    pub is_consistent_op: bool,
    pub table_option: TracedTableOption,
    pub is_replicated: bool,
}

#[cfg(test)]
impl TracedNewLocalOptions {
    pub(crate) fn for_test(table_id: u32) -> Self {
        Self {
            table_id: TracedTableId { table_id },
            is_consistent_op: true,
            table_option: TracedTableOption {
                retention_seconds: None,
            },
            is_replicated: false,
        }
    }
}

pub type TracedHummockEpoch = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Decode, Encode)]
pub enum TracedHummockReadEpoch {
    Committed(TracedHummockEpoch),
    Current(TracedHummockEpoch),
    NoWait(TracedHummockEpoch),
    Backup(TracedHummockEpoch),
}

impl From<HummockReadEpoch> for TracedHummockReadEpoch {
    fn from(value: HummockReadEpoch) -> Self {
        match value {
            HummockReadEpoch::Committed(epoch) => Self::Committed(epoch),
            HummockReadEpoch::Current(epoch) => Self::Current(epoch),
            HummockReadEpoch::NoWait(epoch) => Self::NoWait(epoch),
            HummockReadEpoch::Backup(epoch) => Self::Backup(epoch),
        }
    }
}

impl From<TracedHummockReadEpoch> for HummockReadEpoch {
    fn from(value: TracedHummockReadEpoch) -> Self {
        match value {
            TracedHummockReadEpoch::Committed(epoch) => Self::Committed(epoch),
            TracedHummockReadEpoch::Current(epoch) => Self::Current(epoch),
            TracedHummockReadEpoch::NoWait(epoch) => Self::NoWait(epoch),
            TracedHummockReadEpoch::Backup(epoch) => Self::Backup(epoch),
        }
    }
}
