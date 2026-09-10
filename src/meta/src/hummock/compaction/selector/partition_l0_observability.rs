// Copyright 2026 RisingWave Labs
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

use risingwave_hummock_sdk::CompactionGroupId;

use crate::hummock::compaction::picker::{PartitionL0GrowthOutcome, PartitionL0GrowthStatistic};
use crate::rpc::metrics::MetaMetrics;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PartitionL0CandidateInfo {
    pub(super) partition_index: usize,
    pub(super) partition_depth: u64,
    pub(super) max_overlap_depth: u64,
    pub(super) total_sst_ref_count: u64,
    pub(super) total_object_count: u64,
    pub(super) runnable_sst_ref_count: u64,
    pub(super) runnable_object_count: u64,
    pub(super) total_file_size: u64,
    pub(super) runnable_file_size: u64,
    pub(super) total_referenced_object_size: u64,
    pub(super) runnable_referenced_object_size: u64,
    pub(super) raw_global_l0_score: u64,
    pub(super) adjusted_global_l0_score: u64,
    pub(super) partition_score: u64,
    pub(super) base_current_size: u64,
    pub(super) base_incoming_size: u64,
    pub(super) base_outgoing_size: u64,
    pub(super) base_effective_size: u64,
    pub(super) base_target_size: u64,
}

#[derive(Debug)]
pub(super) struct PartitionL0CompactionObservation {
    pub(super) candidate: PartitionL0CandidateInfo,
    pub(super) picker: &'static str,
    pub(super) outcome: &'static str,
    pub(super) min_seed_depth: u64,
    pub(super) selected_l0_level_count: u64,
    pub(super) selected_l0_sst_ref_count: u64,
    pub(super) selected_l0_size: u64,
    pub(super) selected_l0_referenced_object_size: u64,
    pub(super) target_sst_ref_count: u64,
    pub(super) target_size: u64,
    pub(super) target_referenced_object_size: u64,
    pub(super) growth: PartitionL0GrowthStatistic,
}

pub(super) fn report_partition_l0_observations(
    group_id: CompactionGroupId,
    metrics: &MetaMetrics,
    observations: &[PartitionL0CompactionObservation],
) {
    let group_label = group_id.to_string();
    for observation in observations {
        metrics
            .partition_l0_compaction_total
            .with_label_values(&[&group_label, observation.picker, observation.outcome])
            .inc();

        if observation.outcome == "candidate" {
            observe_candidate(metrics, &group_label, observation);
            continue;
        }

        // Attempted inputs are useful in debug logs, but only scheduled tasks belong in task-shape
        // histograms. Otherwise a range-conflict retry would be counted as executed compaction.
        if observation.outcome == "selected" {
            observe_selected_task(metrics, &group_label, observation);
            log_selected_task(group_id, observation);
        } else {
            tracing::debug!(
                target: "risingwave_meta::hummock::partition_l0",
                compaction_group_id = %group_id,
                ?observation,
                "partition-aware L0 compaction attempt did not produce a task"
            );
        }
    }
}

fn observe_candidate(
    metrics: &MetaMetrics,
    group_label: &str,
    observation: &PartitionL0CompactionObservation,
) {
    let mut byte_observations = vec![
        ("partition-total", observation.candidate.total_file_size),
        (
            "partition-runnable",
            observation.candidate.runnable_file_size,
        ),
        (
            "partition-referenced-object-total",
            observation.candidate.total_referenced_object_size,
        ),
        (
            "partition-referenced-object-runnable",
            observation.candidate.runnable_referenced_object_size,
        ),
        ("base-current", observation.candidate.base_current_size),
        ("base-incoming", observation.candidate.base_incoming_size),
        ("base-outgoing", observation.candidate.base_outgoing_size),
        ("base-effective", observation.candidate.base_effective_size),
    ];
    // `u64::MAX` means that the dynamic base level has no finite target yet. It is not a byte
    // observation and must not poison the histogram sum.
    if observation.candidate.base_target_size != u64::MAX {
        byte_observations.push(("base-target", observation.candidate.base_target_size));
    }
    for (kind, value) in byte_observations {
        metrics
            .partition_l0_compaction_bytes
            .with_label_values(&[group_label, kind])
            .observe(value as f64);
    }
    for (kind, value) in [
        ("partition-depth", observation.candidate.partition_depth),
        ("max-overlap-depth", observation.candidate.max_overlap_depth),
        (
            "partition-sst-refs",
            observation.candidate.total_sst_ref_count,
        ),
        (
            "partition-objects",
            observation.candidate.total_object_count,
        ),
        (
            "runnable-sst-refs",
            observation.candidate.runnable_sst_ref_count,
        ),
        (
            "runnable-objects",
            observation.candidate.runnable_object_count,
        ),
    ] {
        metrics
            .partition_l0_compaction_count
            .with_label_values(&[group_label, kind])
            .observe(value as f64);
    }
    for (kind, value) in [
        ("raw-global", observation.candidate.raw_global_l0_score),
        (
            "adjusted-global",
            observation.candidate.adjusted_global_l0_score,
        ),
        ("partition", observation.candidate.partition_score),
    ] {
        metrics
            .partition_l0_compaction_score
            .with_label_values(&[group_label, kind])
            .observe(value as f64);
    }
}

fn observe_selected_task(
    metrics: &MetaMetrics,
    group_label: &str,
    observation: &PartitionL0CompactionObservation,
) {
    // Only ordinary ToBase has an initial seed and optional donors. The outcome sum is the
    // number of donor attempts, including duplicate/subset plans that add no new SST.
    if observation.picker == "to-base" {
        for (kind, value) in PartitionL0GrowthOutcome::LABELS
            .iter()
            .zip(observation.growth.outcomes)
        {
            metrics
                .partition_l0_compaction_count
                .with_label_values(&[group_label, kind])
                .observe(value as f64);
        }
        for (kind, value) in [
            ("initial-l0", observation.growth.initial_l0_size),
            (
                "growth-added",
                observation
                    .selected_l0_size
                    .saturating_sub(observation.growth.initial_l0_size),
            ),
        ] {
            metrics
                .partition_l0_compaction_bytes
                .with_label_values(&[group_label, kind])
                .observe(value as f64);
        }
    }
    for (kind, value) in [
        ("selected-l0", observation.selected_l0_size),
        (
            "selected-l0-referenced-object",
            observation.selected_l0_referenced_object_size,
        ),
        ("target", observation.target_size),
        (
            "target-referenced-object",
            observation.target_referenced_object_size,
        ),
    ] {
        metrics
            .partition_l0_compaction_bytes
            .with_label_values(&[group_label, kind])
            .observe(value as f64);
    }
    for (kind, value) in [
        ("min-seed-depth", observation.min_seed_depth),
        ("selected-l0-levels", observation.selected_l0_level_count),
        (
            "selected-l0-sst-refs",
            observation.selected_l0_sst_ref_count,
        ),
        ("target-sst-refs", observation.target_sst_ref_count),
        (
            "selected-partition-depth",
            observation.candidate.partition_depth,
        ),
        (
            "selected-max-overlap-depth",
            observation.candidate.max_overlap_depth,
        ),
    ] {
        metrics
            .partition_l0_compaction_count
            .with_label_values(&[group_label, kind])
            .observe(value as f64);
    }
}

fn log_selected_task(group_id: CompactionGroupId, observation: &PartitionL0CompactionObservation) {
    tracing::info!(
        target: "risingwave_meta::hummock::partition_l0",
        compaction_group_id = %group_id,
        picker = observation.picker,
        partition_index = observation.candidate.partition_index,
        partition_depth = observation.candidate.partition_depth,
        max_overlap_depth = observation.candidate.max_overlap_depth,
        min_seed_depth = observation.min_seed_depth,
        raw_global_l0_score = observation.candidate.raw_global_l0_score,
        adjusted_global_l0_score = observation.candidate.adjusted_global_l0_score,
        partition_score = observation.candidate.partition_score,
        partition_total_bytes = observation.candidate.total_file_size,
        partition_runnable_bytes = observation.candidate.runnable_file_size,
        partition_sst_refs = observation.candidate.total_sst_ref_count,
        partition_objects = observation.candidate.total_object_count,
        selected_l0_levels = observation.selected_l0_level_count,
        selected_l0_sst_refs = observation.selected_l0_sst_ref_count,
        selected_l0_bytes = observation.selected_l0_size,
        selected_l0_referenced_object_bytes = observation.selected_l0_referenced_object_size,
        target_sst_refs = observation.target_sst_ref_count,
        target_bytes = observation.target_size,
        target_referenced_object_bytes = observation.target_referenced_object_size,
        growth_initial_l0_bytes = observation.growth.initial_l0_size,
        growth_outcomes = ?PartitionL0GrowthOutcome::LABELS.iter()
            .zip(observation.growth.outcomes).collect::<Vec<_>>(),
        "partition-aware L0 compaction selected"
    );
}
