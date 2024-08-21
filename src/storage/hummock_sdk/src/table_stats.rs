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

use std::borrow::Borrow;
use std::collections::HashMap;

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::PbTableStats;

use crate::version::HummockVersion;

pub type TableStatsMap = HashMap<u32, TableStats>;

pub type PbTableStatsMap = HashMap<u32, PbTableStats>;

#[derive(Default, Debug, Clone)]
pub struct TableStats {
    pub total_key_size: i64,
    pub total_value_size: i64,
    pub total_key_count: i64,

    // `compressed_size_in_sstable`` represents the size that the table takes up in the output sst, and is only meaningful when `TableStats` is the sstable builder output.
    pub compressed_size_in_sstable: u64,
}

impl From<&TableStats> for PbTableStats {
    fn from(value: &TableStats) -> Self {
        Self {
            total_key_size: value.total_key_size,
            total_value_size: value.total_value_size,
            total_key_count: value.total_key_count,
            compressed_size_in_sstable: value.compressed_size_in_sstable,
        }
    }
}

impl From<TableStats> for PbTableStats {
    fn from(value: TableStats) -> Self {
        (&value).into()
    }
}

impl From<&PbTableStats> for TableStats {
    fn from(value: &PbTableStats) -> Self {
        Self {
            total_key_size: value.total_key_size,
            total_value_size: value.total_value_size,
            total_key_count: value.total_key_count,
            compressed_size_in_sstable: value.compressed_size_in_sstable,
        }
    }
}

impl TableStats {
    pub fn add(&mut self, other: &TableStats) {
        self.total_key_size += other.total_key_size;
        self.total_value_size += other.total_value_size;
        self.total_key_count += other.total_key_count;
        self.compressed_size_in_sstable += other.compressed_size_in_sstable;
    }
}

pub fn add_prost_table_stats(this: &mut PbTableStats, other: &PbTableStats) {
    this.total_key_size += other.total_key_size;
    this.total_value_size += other.total_value_size;
    this.total_key_count += other.total_key_count;
    this.compressed_size_in_sstable += other.compressed_size_in_sstable;
}

pub fn add_prost_table_stats_map(this: &mut PbTableStatsMap, other: &PbTableStatsMap) {
    for (table_id, stats) in other {
        add_prost_table_stats(this.entry(*table_id).or_default(), stats);
    }
}

pub fn add_table_stats_map(this: &mut TableStatsMap, other: &TableStatsMap) {
    for (table_id, stats) in other {
        this.entry(*table_id).or_default().add(stats);
    }
}

pub fn to_prost_table_stats_map(
    table_stats: impl Borrow<TableStatsMap>,
) -> HashMap<u32, PbTableStats> {
    table_stats
        .borrow()
        .iter()
        .map(|(t, s)| (*t, s.into()))
        .collect()
}

pub fn from_prost_table_stats_map(
    table_stats: impl Borrow<HashMap<u32, PbTableStats>>,
) -> HashMap<u32, TableStats> {
    table_stats
        .borrow()
        .iter()
        .map(|(t, s)| (*t, s.into()))
        .collect()
}

pub fn purge_prost_table_stats(
    table_stats: &mut PbTableStatsMap,
    hummock_version: &HummockVersion,
) -> bool {
    let prev_count = table_stats.len();
    table_stats.retain(|table_id, _| {
        hummock_version
            .state_table_info
            .info()
            .contains_key(&TableId::new(*table_id))
    });
    prev_count != table_stats.len()
}
