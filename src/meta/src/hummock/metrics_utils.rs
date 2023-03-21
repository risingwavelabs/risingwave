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

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::{enumerate, Itertools};
use prost::Message;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId, HummockEpoch, HummockVersionId};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactionConfig, HummockPinnedSnapshot, HummockPinnedVersion, HummockVersion,
    HummockVersionStats,
};

use super::compaction::{get_compression_algorithm, DynamicLevelSelectorCore};
use crate::hummock::compaction::CompactStatus;
use crate::rpc::metrics::MetaMetrics;

pub fn trigger_version_stat(
    metrics: &MetaMetrics,
    current_version: &HummockVersion,
    version_stats: &HummockVersionStats,
) {
    metrics
        .max_committed_epoch
        .set(current_version.max_committed_epoch as i64);
    metrics
        .version_size
        .set(current_version.encoded_len() as i64);
    metrics.safe_epoch.set(current_version.safe_epoch as i64);
    metrics.current_version_id.set(current_version.id as i64);
    metrics.version_stats.reset();
    for (table_id, stats) in &version_stats.table_stats {
        let table_id = format!("{}", table_id);
        metrics
            .version_stats
            .with_label_values(&[&table_id, "total_key_count"])
            .set(stats.total_key_count);
        metrics
            .version_stats
            .with_label_values(&[&table_id, "total_key_size"])
            .set(stats.total_key_size);
        metrics
            .version_stats
            .with_label_values(&[&table_id, "total_value_size"])
            .set(stats.total_value_size);
    }
}

pub fn trigger_sst_stat(
    metrics: &MetaMetrics,
    compact_status: Option<&CompactStatus>,
    current_version: &HummockVersion,
    compaction_group_id: CompactionGroupId,
) {
    let level_sst_cnt = |level_idx: usize| {
        let mut sst_num = 0;
        current_version.level_iter(compaction_group_id, |level| {
            if level.level_idx == level_idx as u32 {
                sst_num += level.table_infos.len();
            }
            true
        });
        sst_num
    };
    let level_sst_size = |level_idx: usize| {
        let mut level_sst_size = 0;
        current_version.level_iter(compaction_group_id, |level| {
            if level.level_idx == level_idx as u32 {
                level_sst_size += level.total_file_size;
            }
            true
        });
        level_sst_size / 1024
    };

    let mut compacting_task_stat: BTreeMap<(usize, usize), usize> = BTreeMap::default();
    for idx in 0..current_version.num_levels(compaction_group_id) {
        let sst_num = level_sst_cnt(idx);
        let level_label = format!("cg{}_L{}", compaction_group_id, idx);
        metrics
            .level_sst_num
            .with_label_values(&[&level_label])
            .set(sst_num as i64);
        metrics
            .level_file_size
            .with_label_values(&[&level_label])
            .set(level_sst_size(idx) as i64);
        if let Some(compact_status) = compact_status {
            let compact_cnt = compact_status.level_handlers[idx].get_pending_file_count();
            metrics
                .level_compact_cnt
                .with_label_values(&[&level_label])
                .set(compact_cnt as i64);

            let compacting_task = compact_status.level_handlers[idx].get_pending_tasks();
            let mut pending_task_ids: HashSet<u64> = HashSet::default();
            for task in compacting_task {
                if pending_task_ids.contains(&task.task_id) {
                    continue;
                }

                let key = (idx, task.target_level as usize);
                let count = compacting_task_stat.entry(key).or_insert(0);
                *count += 1;

                pending_task_ids.insert(task.task_id);
            }
        }
    }

    tracing::info!("LSM Compacting STAT {:?}", compacting_task_stat);

    for ((select, target), compacting_task_count) in compacting_task_stat {
        let label_str = format!("cg{} L{} -> L{}", compaction_group_id, select, target);
        metrics
            .level_compact_task_cnt
            .with_label_values(&[&label_str])
            .set(compacting_task_count as _);
    }

    let level_label = format!("cg{}_l0_sub", compaction_group_id);
    let sst_num = current_version
        .levels
        .get(&compaction_group_id)
        .and_then(|level| level.l0.as_ref().map(|l0| l0.sub_levels.len()))
        .unwrap_or(0);
    metrics
        .level_sst_num
        .with_label_values(&[&level_label])
        .set(sst_num as i64);

    let previous_time = metrics.time_after_last_observation.load(Ordering::Relaxed);
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if current_time - previous_time > 600
        && metrics
            .time_after_last_observation
            .compare_exchange(
                previous_time,
                current_time,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    {
        if let Some(compact_status) = compact_status {
            for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
                let sst_num = level_sst_cnt(idx);
                let sst_size = level_sst_size(idx);
                let compact_cnt = level_handler.get_pending_file_count();
                tracing::info!(
                    "Level {} has {} SSTs, the total size of which is {}KB, while {} of those are being compacted to bottom levels",
                    idx,
                    sst_num,
                    sst_size,
                    compact_cnt,
                );
            }
        }
    }
}

pub fn remove_compaction_group_in_sst_stat(
    metrics: &MetaMetrics,
    compaction_group_id: CompactionGroupId,
) {
    let mut idx = 0;
    loop {
        let level_label = format!("{}_{}", idx, compaction_group_id);
        let should_continue = metrics
            .level_sst_num
            .remove_label_values(&[&level_label])
            .is_ok();
        metrics
            .level_file_size
            .remove_label_values(&[&level_label])
            .ok();
        metrics
            .level_compact_cnt
            .remove_label_values(&[&level_label])
            .ok();
        if !should_continue {
            break;
        }
        idx += 1;
    }

    let level_label = format!("cg{}_l0_sub", compaction_group_id);
    metrics
        .level_sst_num
        .remove_label_values(&[&level_label])
        .ok();
}

pub fn trigger_pin_unpin_version_state(
    metrics: &MetaMetrics,
    pinned_versions: &BTreeMap<HummockContextId, HummockPinnedVersion>,
) {
    if let Some(m) = pinned_versions.values().map(|v| v.min_pinned_id).min() {
        metrics.min_pinned_version_id.set(m as i64);
    } else {
        metrics
            .min_pinned_version_id
            .set(HummockVersionId::MAX as _);
    }
}

pub fn trigger_pin_unpin_snapshot_state(
    metrics: &MetaMetrics,
    pinned_snapshots: &BTreeMap<HummockContextId, HummockPinnedSnapshot>,
) {
    if let Some(m) = pinned_snapshots
        .values()
        .map(|v| v.minimal_pinned_snapshot)
        .min()
    {
        metrics.min_pinned_epoch.set(m as i64);
    } else {
        metrics.min_pinned_epoch.set(HummockEpoch::MAX as _);
    }
}

pub fn trigger_safepoint_stat(metrics: &MetaMetrics, safepoints: &[HummockVersionId]) {
    if let Some(sp) = safepoints.iter().min() {
        metrics.min_safepoint_version_id.set(*sp as _);
    } else {
        metrics
            .min_safepoint_version_id
            .set(HummockVersionId::MAX as _);
    }
}

pub fn trigger_stale_ssts_stat(metrics: &MetaMetrics, total_number: usize) {
    metrics.stale_ssts_count.set(total_number as _);
}

// Triggers a report on compact_pending_bytes_needed
pub fn trigger_lsm_stat(
    metrics: &MetaMetrics,
    compaction_config: Arc<CompactionConfig>,
    levels: &Levels,
    compaction_group_id: CompactionGroupId,
) {
    let group_label = compaction_group_id.to_string();
    // compact_pending_bytes
    let dynamic_level_core = DynamicLevelSelectorCore::new(compaction_config.clone());
    let ctx = dynamic_level_core.calculate_level_base_size(levels);
    {
        let compact_pending_bytes_needed =
            dynamic_level_core.compact_pending_bytes_needed_with_ctx(levels, &ctx);

        metrics
            .compact_pending_bytes
            .with_label_values(&[&group_label])
            .set(compact_pending_bytes_needed as _);
    }

    {
        // compact_level_compression_ratio
        let level_compression_ratio = levels
            .get_levels()
            .iter()
            .map(|level| {
                let ratio = if level.get_uncompressed_file_size() == 0 {
                    0.0
                } else {
                    level.get_total_file_size() as f64 / level.get_uncompressed_file_size() as f64
                };

                (level.get_level_idx(), ratio)
            })
            .collect_vec();

        for (level_index, compression_ratio) in level_compression_ratio {
            let compression_algorithm_label = get_compression_algorithm(
                compaction_config.as_ref(),
                ctx.base_level,
                level_index as usize,
            );

            metrics
                .compact_level_compression_ratio
                .with_label_values(&[
                    &group_label,
                    &level_index.to_string(),
                    &compression_algorithm_label,
                ])
                .set(compression_ratio);
        }
    }
}
