// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod compaction_picker;
mod level_selector;
mod overlap_strategy;
mod tier_compaction_picker;

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::sync::Arc;

use itertools::Itertools;
use prost::Message;
use risingwave_common::error::Result;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{
    CompactMetrics, CompactTask, HummockVersion, Level, TableSetStatistics,
};

use crate::hummock::compaction::level_selector::{DynamicLevelSelector, LevelSelector};
use crate::hummock::compaction::overlap_strategy::{
    HashStrategy, OverlapStrategy, RangeOverlapStrategy,
};
use crate::hummock::compaction::CompactionMode::{ConsistentHashMode, RangeMode};
use crate::hummock::level_handler::LevelHandler;
use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::model::Transactional;
use crate::storage;
use crate::storage::{MetaStore, Transaction};

/// Hummock `compact_status` key
/// `cf(hummock_default)`: `hummock_compact_status_key` -> `CompactStatus`
pub(crate) const HUMMOCK_COMPACT_STATUS_KEY: &str = "compact_status";
const DEFAULT_MAX_COMPACTION_BYTES: u64 = 4 * 1024 * 1024 * 1024; // 4GB
const DEFAULT_MIN_COMPACTION_BYTES: u64 = 128 * 1024 * 1024; // 128MB
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 1024 * 1024 * 1024; // 1GB

// decrease this configure when the generation of checkpoint barrier is not frequent.
const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: usize = 16;

const MAX_LEVEL: usize = 6;

pub struct CompactStatus {
    pub(crate) level_handlers: Vec<LevelHandler>,
    pub(crate) next_compact_task_id: u64,
    compaction_selector: Arc<dyn LevelSelector>,
}

impl Debug for CompactStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactStatus")
            .field("level_handlers", &self.level_handlers)
            .field("next_compact_task_id", &self.next_compact_task_id)
            .field("compaction_selector", &self.compaction_selector.name())
            .finish()
    }
}

impl PartialEq for CompactStatus {
    fn eq(&self, other: &Self) -> bool {
        self.level_handlers.eq(&other.level_handlers)
            && self.next_compact_task_id == other.next_compact_task_id
            && self.compaction_selector.name() == other.compaction_selector.name()
    }
}

impl Clone for CompactStatus {
    fn clone(&self) -> Self {
        Self {
            level_handlers: self.level_handlers.clone(),
            compaction_selector: self.compaction_selector.clone(),
            next_compact_task_id: self.next_compact_task_id,
        }
    }
}

pub struct SearchResult {
    select_level: Level,
    target_level: Level,
    split_ranges: Vec<KeyRange>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CompactionMode {
    RangeMode,
    ConsistentHashMode,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompactionConfig {
    pub max_bytes_for_level_base: u64,
    pub max_level: usize,
    pub max_bytes_for_level_multiplier: u64,
    pub max_compaction_bytes: u64,
    pub min_compaction_bytes: u64,
    pub level0_tigger_file_numer: usize,
    pub level0_tier_compact_file_number: usize,
    pub compaction_mode: CompactionMode,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_bytes_for_level_base: DEFAULT_MAX_BYTES_FOR_LEVEL_BASE, // 1GB
            max_bytes_for_level_multiplier: 10,
            max_level: MAX_LEVEL,
            max_compaction_bytes: DEFAULT_MAX_COMPACTION_BYTES,
            min_compaction_bytes: DEFAULT_MIN_COMPACTION_BYTES,
            level0_tigger_file_numer: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER * 2,
            level0_tier_compact_file_number: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER,
            compaction_mode: ConsistentHashMode,
        }
    }
}

impl CompactStatus {
    pub fn new(config: Arc<CompactionConfig>) -> CompactStatus {
        let mut level_handlers = vec![];
        for level in 0..=config.max_level {
            level_handlers.push(LevelHandler::new(level as u32));
        }
        let overlap_strategy = match config.compaction_mode {
            RangeMode => Arc::new(RangeOverlapStrategy::default()) as Arc<dyn OverlapStrategy>,
            ConsistentHashMode => Arc::new(HashStrategy::default()),
        };
        CompactStatus {
            level_handlers,
            next_compact_task_id: 1,
            // TODO: create selector and overlap strategy by configure.
            compaction_selector: Arc::new(DynamicLevelSelector::new(config, overlap_strategy)),
        }
    }

    fn cf_name() -> &'static str {
        HUMMOCK_DEFAULT_CF_NAME
    }

    fn key() -> &'static str {
        HUMMOCK_COMPACT_STATUS_KEY
    }

    pub async fn get<S: MetaStore>(
        meta_store: &S,
        config: Arc<CompactionConfig>,
    ) -> Result<Option<CompactStatus>> {
        match meta_store
            .get_cf(CompactStatus::cf_name(), CompactStatus::key().as_bytes())
            .await
            .map(|v| risingwave_pb::hummock::CompactStatus::decode(&mut Cursor::new(v)).unwrap())
        {
            Ok(compact_status) => {
                let mut status = CompactStatus::new(config);
                status.level_handlers = compact_status.level_handlers.iter().map_into().collect();
                status.next_compact_task_id = compact_status.next_compact_task_id;
                Ok(Some(status))
            }
            Err(err) => {
                if !matches!(err, storage::Error::ItemNotFound(_)) {
                    return Err(err.into());
                }
                Ok(None)
            }
        }
    }

    pub fn get_compact_task(&mut self, levels: &[Level]) -> Option<CompactTask> {
        // When we compact the files, we must make the result of compaction meet the following
        // conditions, for any user key, the epoch of it in the file existing in the lower
        // layer must be larger.

        let ret = match self.pick_compaction(levels) {
            Some(ret) => ret,
            None => return None,
        };

        let select_level_id = ret.select_level.level_idx;
        let target_level_id = ret.target_level.level_idx;

        let compact_task = CompactTask {
            input_ssts: vec![ret.select_level, ret.target_level],
            splits: ret
                .split_ranges
                .iter()
                .map(|v| v.clone().into())
                .collect_vec(),
            watermark: HummockEpoch::MAX,
            sorted_output_ssts: vec![],
            task_id: self.next_compact_task_id,
            target_level: target_level_id,
            is_target_ultimate_and_leveling: target_level_id as usize
                == self.level_handlers.len() - 1
                && select_level_id > 0,
            metrics: Some(CompactMetrics {
                read_level_n: Some(TableSetStatistics {
                    level_idx: select_level_id,
                    size_kb: 0,
                    cnt: 0,
                }),
                read_level_nplus1: Some(TableSetStatistics {
                    level_idx: target_level_id,
                    size_kb: 0,
                    cnt: 0,
                }),
                write: Some(TableSetStatistics {
                    level_idx: target_level_id,
                    size_kb: 0,
                    cnt: 0,
                }),
            }),
            task_status: false,
            // TODO: fill with compaction group info
            prefix_pairs: vec![],
            vnode_mappings: vec![],
        };
        self.next_compact_task_id += 1;
        Some(compact_task)
    }

    fn pick_compaction(&mut self, levels: &[Level]) -> Option<SearchResult> {
        self.compaction_selector.pick_compaction(
            self.next_compact_task_id,
            levels,
            &mut self.level_handlers,
        )
    }

    /// Declares a task is either finished or canceled.
    pub fn report_compact_task(&mut self, compact_task: &CompactTask) {
        for level in &compact_task.input_ssts {
            self.level_handlers[level.level_idx as usize].remove_task(compact_task.task_id);
        }
    }

    pub fn cancel_compaction_tasks_if<F: Fn(u64) -> bool>(&mut self, should_cancel: F) -> u32 {
        let mut count: u32 = 0;
        for level in &mut self.level_handlers {
            for pending_task_id in level.pending_tasks_ids() {
                if should_cancel(pending_task_id) {
                    level.remove_task(pending_task_id);
                    count += 1;
                }
            }
        }
        count
    }

    /// Applies the compact task result and get a new hummock version.
    pub fn apply_compact_result(
        compact_task: &CompactTask,
        based_hummock_version: HummockVersion,
    ) -> HummockVersion {
        let mut new_version = based_hummock_version;
        new_version.safe_epoch = std::cmp::max(new_version.safe_epoch, compact_task.watermark);
        let mut removed_table: HashSet<u64> = HashSet::default();
        for input_level in &compact_task.input_ssts {
            for table in &input_level.table_infos {
                removed_table.insert(table.id);
            }
        }
        if compact_task.target_level == 0 {
            assert_eq!(compact_task.input_ssts[0].level_idx, 0);
            let mut new_table_infos = vec![];
            let mut find_remove_position = false;
            for (idx, table) in new_version.levels[0].table_infos.iter().enumerate() {
                if !removed_table.contains(&table.id) {
                    new_table_infos.push(new_version.levels[0].table_infos[idx].clone());
                } else if !find_remove_position {
                    new_table_infos.extend(compact_task.sorted_output_ssts.clone());
                    find_remove_position = true;
                }
            }
            new_version.levels[compact_task.target_level as usize].table_infos = new_table_infos;
        } else {
            for input_level in &compact_task.input_ssts {
                new_version.levels[input_level.level_idx as usize]
                    .table_infos
                    .retain(|sst| !removed_table.contains(&sst.id));
            }
            new_version.levels[compact_task.target_level as usize]
                .table_infos
                .extend(compact_task.sorted_output_ssts.clone());
            new_version.levels[compact_task.target_level as usize]
                .table_infos
                .sort_by(|sst1, sst2| {
                    let a = KeyRange::from(sst1.key_range.as_ref().unwrap());
                    let b = KeyRange::from(sst2.key_range.as_ref().unwrap());
                    a.cmp(&b)
                });
        }
        new_version
    }
}

impl Transactional for CompactStatus {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> Result<()> {
        trx.put(
            CompactStatus::cf_name().to_string(),
            CompactStatus::key().as_bytes().to_vec(),
            risingwave_pb::hummock::CompactStatus::from(self).encode_to_vec(),
        );
        Ok(())
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> Result<()> {
        trx.delete(
            CompactStatus::cf_name().to_string(),
            CompactStatus::key().as_bytes().to_vec(),
        );
        Ok(())
    }
}

impl Default for CompactStatus {
    fn default() -> Self {
        Self::new(Arc::new(CompactionConfig::default()))
    }
}

impl From<&CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: &CompactStatus) -> Self {
        risingwave_pb::hummock::CompactStatus {
            level_handlers: status.level_handlers.iter().map_into().collect(),
            next_compact_task_id: status.next_compact_task_id,
        }
    }
}

impl From<&risingwave_pb::hummock::CompactStatus> for CompactStatus {
    fn from(status: &risingwave_pb::hummock::CompactStatus) -> Self {
        CompactStatus {
            level_handlers: status.level_handlers.iter().map_into().collect(),
            next_compact_task_id: status.next_compact_task_id,
            compaction_selector: Arc::new(DynamicLevelSelector::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_serde() -> Result<()> {
        let origin = CompactStatus {
            next_compact_task_id: 4,
            ..Default::default()
        };
        let ser = risingwave_pb::hummock::CompactStatus::from(&origin).encode_to_vec();
        let de = risingwave_pb::hummock::CompactStatus::decode(&mut Cursor::new(ser));
        let de = (&de.unwrap()).into();
        assert_eq!(origin, de);

        Ok(())
    }
}
