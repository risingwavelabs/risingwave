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

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};

use risingwave_pb::hummock::{HummockVersion, PbTableStats};

use crate::compaction_group::hummock_version_ext::HummockVersionExt;

pub type TableStatsMap = HashMap<u32, TableStats>;

pub type PbTableStatsMap = HashMap<u32, PbTableStats>;

#[derive(Default, Debug, Clone)]
pub struct TableStats {
    pub total_key_size: i64,
    pub total_value_size: i64,
    pub total_key_count: i64,
}

impl From<&TableStats> for PbTableStats {
    fn from(value: &TableStats) -> Self {
        Self {
            total_key_size: value.total_key_size,
            total_value_size: value.total_value_size,
            total_key_count: value.total_key_count,
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
        }
    }
}

impl TableStats {
    pub fn add(&mut self, other: &TableStats) {
        self.total_key_size += other.total_key_size;
        self.total_value_size += other.total_value_size;
        self.total_key_count += other.total_key_count;
    }
}

pub fn add_prost_table_stats(this: &mut PbTableStats, other: &PbTableStats) {
    this.total_key_size += other.total_key_size;
    this.total_value_size += other.total_value_size;
    this.total_key_count += other.total_key_count;
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
) {
    let mut all_tables_in_version: HashSet<u32> = HashSet::default();
    for group in hummock_version.levels.keys() {
        hummock_version.level_iter(*group, |level| {
            all_tables_in_version
                .extend(level.table_infos.iter().flat_map(|s| s.table_ids.clone()));
            true
        })
    }
    table_stats.retain(|k, _| all_tables_in_version.contains(k));
}
