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

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::enumerate;
use prometheus::Histogram;
use prost::Message;
use risingwave_pb::hummock::{CompactMetrics, HummockVersion, TableSetStatistics};

use crate::hummock::compaction::CompactStatus;
use crate::rpc::metrics::MetaMetrics;

pub fn trigger_commit_stat(metrics: &MetaMetrics, current_version: &HummockVersion) {
    metrics
        .max_committed_epoch
        .set(current_version.max_committed_epoch as i64);
    let uncommitted_sst_num = current_version
        .uncommitted_epochs
        .iter()
        .fold(0, |accum, elem| accum + elem.tables.len());
    metrics.uncommitted_sst_num.set(uncommitted_sst_num as i64);
    metrics
        .version_size
        .set(current_version.encoded_len() as i64);
}

pub fn trigger_sst_stat(
    metrics: &MetaMetrics,
    compact_status: &CompactStatus,
    current_version: &HummockVersion,
) {
    let level_sst_cnt = |level_idx: usize| current_version.levels[level_idx].table_infos.len();
    for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
        let sst_num = level_sst_cnt(idx);
        let compact_cnt = level_handler.get_pending_file_count();
        let level_label = String::from("L") + &idx.to_string();
        metrics
            .level_sst_num
            .get_metric_with_label_values(&[&level_label])
            .unwrap()
            .set(sst_num as i64);
        metrics
            .level_compact_cnt
            .get_metric_with_label_values(&[&level_label])
            .unwrap()
            .set(compact_cnt as i64);
    }

    use std::sync::atomic::AtomicU64;

    static TIME_AFTER_LAST_OBSERVATION: AtomicU64 = AtomicU64::new(0);
    let previous_time = TIME_AFTER_LAST_OBSERVATION.load(Ordering::Relaxed);
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if current_time - previous_time > 600
        && TIME_AFTER_LAST_OBSERVATION
            .compare_exchange(
                previous_time,
                current_time,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    {
        for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
            let sst_num = level_sst_cnt(idx);
            let compact_cnt = level_handler.get_pending_file_count();
            tracing::info!(
                "Level {} has {} SSTs, {} of those are being compacted to bottom levels",
                idx,
                sst_num,
                compact_cnt,
            );
        }
    }
}

fn single_level_stat_bytes<T: FnMut(String) -> Histogram>(
    mut metric_vec: T,
    level_stat: &TableSetStatistics,
) {
    if level_stat.size_kb > 0 {
        let level_label = String::from("L") + &level_stat.level_idx.to_string();
        metric_vec(level_label).observe(level_stat.size_kb as f64);
    }
}

fn single_level_stat_sstn<T: FnMut(String) -> Histogram>(
    mut metric_vec: T,
    level_stat: &TableSetStatistics,
) {
    if level_stat.cnt > 0 {
        let level_label = String::from("L") + &level_stat.level_idx.to_string();
        metric_vec(level_label).observe(level_stat.cnt as f64);
    }
}

pub fn trigger_rw_stat(metrics: &MetaMetrics, compact_metrics: &CompactMetrics) {
    metrics
        .level_compact_frequency
        .get_metric_with_label_values(&[&(String::from("L")
            + &compact_metrics
                .read_level_n
                .as_ref()
                .unwrap()
                .level_idx
                .to_string())])
        .unwrap()
        .inc();

    single_level_stat_bytes(
        |label| metrics.level_compact_read_curr.with_label_values(&[&label]),
        compact_metrics.read_level_n.as_ref().unwrap(),
    );
    single_level_stat_bytes(
        |label| metrics.level_compact_read_next.with_label_values(&[&label]),
        compact_metrics.read_level_nplus1.as_ref().unwrap(),
    );
    single_level_stat_bytes(
        |label| metrics.level_compact_write.with_label_values(&[&label]),
        compact_metrics.write.as_ref().unwrap(),
    );

    single_level_stat_sstn(
        |label| {
            metrics
                .level_compact_read_sstn_curr
                .with_label_values(&[&label])
        },
        compact_metrics.read_level_n.as_ref().unwrap(),
    );
    single_level_stat_sstn(
        |label| {
            metrics
                .level_compact_read_sstn_next
                .with_label_values(&[&label])
        },
        compact_metrics.read_level_nplus1.as_ref().unwrap(),
    );
    single_level_stat_sstn(
        |label| {
            metrics
                .level_compact_write_sstn
                .with_label_values(&[&label])
        },
        compact_metrics.write.as_ref().unwrap(),
    );
}
