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

//! Normalize overlapping table ranges with the existing plan, recheck, and apply loop.

use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_hummock_sdk::compact_task::{ReportTask, is_compaction_task_expired};
use risingwave_hummock_sdk::compaction_group::{StateTableId, group_split};
use risingwave_hummock_sdk::version::{GroupDelta, GroupDeltas, HummockVersion};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::{CompatibilityVersion, PbGroupConstruct, PbStateTableInfoDelta};

use super::super::compaction_group_manager::CompactionGroupManager;
use super::CompactionGroupStatistic;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::transaction::HummockVersionTransaction;
use crate::hummock::manager::{HummockManager, commit_multi_var};
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_id};

#[derive(Debug, PartialEq, Eq)]
struct NormalizePlan {
    parent_group_id: CompactionGroupId,
    parent_table_ids: Vec<StateTableId>,
    boundary_table_id: StateTableId,
}

impl NormalizePlan {
    fn split_key(&self) -> Bytes {
        group_split::build_split_full_key(self.boundary_table_id, VirtualNode::ZERO)
            .encode()
            .into()
    }

    fn split_table_ids(&self) -> (Vec<StateTableId>, Vec<StateTableId>) {
        let split_full_key =
            group_split::build_split_full_key(self.boundary_table_id, VirtualNode::ZERO);
        let (table_ids_left, table_ids_right) =
            group_split::split_table_ids_with_table_id_and_vnode(
                &self.parent_table_ids,
                split_full_key.user_key.table_id,
                split_full_key.user_key.get_vnode_id(),
            );
        assert!(!table_ids_left.is_empty() && !table_ids_right.is_empty());
        (table_ids_left, table_ids_right)
    }
}

fn gen_normalize_plan(
    left: &CompactionGroupStatistic,
    right: &CompactionGroupStatistic,
) -> Option<NormalizePlan> {
    let left_table_ids = left.table_statistic.keys().copied().collect_vec();

    if left_table_ids.len() <= 1 {
        return None;
    }

    let left_max = *left_table_ids.last().unwrap();
    let right_min = *right.table_statistic.keys().next().unwrap();
    if left_max < right_min {
        return None;
    }

    let boundary_index = left_table_ids.partition_point(|&table_id| table_id < right_min);
    if boundary_index == 0 || boundary_index >= left_table_ids.len() {
        return None;
    }
    let boundary_table_id = left_table_ids[boundary_index];

    Some(NormalizePlan {
        parent_group_id: left.group_id,
        parent_table_ids: left_table_ids,
        boundary_table_id,
    })
}

fn build_normalize_plan_from_group_statistics(
    groups: &[CompactionGroupStatistic],
) -> Option<NormalizePlan> {
    // `calculate_compaction_group_statistic()` iterates all version levels, so newly created or
    // transiently empty groups can appear here without any member tables.
    let mut groups = groups
        .iter()
        .filter(|group| !group.table_statistic.is_empty())
        .collect_vec();
    groups.sort_by_key(|group| *group.table_statistic.keys().next().unwrap());

    groups
        .split(|group| {
            group
                .compaction_group_config
                .compaction_config
                .disable_auto_group_scheduling
                .unwrap_or(false)
        })
        .find_map(|segment| {
            segment
                .windows(2)
                .find_map(|pair| gen_normalize_plan(pair[0], pair[1]))
        })
}

fn collect_normalize_group_statistics(
    version: &HummockVersion,
    compaction_group_manager: &CompactionGroupManager,
) -> Result<Vec<CompactionGroupStatistic>> {
    let mut groups = vec![];
    for group_id in version.levels.keys() {
        let table_ids = version
            .state_table_info
            .compaction_group_member_table_ids(*group_id)
            .iter()
            .copied()
            .collect_vec();
        if table_ids.is_empty() {
            continue;
        }

        let group_config = compaction_group_manager
            .try_get_compaction_group_config(*group_id)
            .ok_or_else(|| {
                Error::CompactionGroup(format!(
                    "group {} config not found during normalize",
                    group_id
                ))
            })?;
        groups.push(CompactionGroupStatistic {
            group_id: *group_id,
            group_size: 0,
            table_statistic: table_ids
                .into_iter()
                .map(|table_id| (table_id, 0))
                .collect(),
            compaction_group_config: group_config,
        });
    }
    Ok(groups)
}

impl HummockManager {
    async fn build_normalize_plan(&self) -> Option<NormalizePlan> {
        let groups = self.calculate_compaction_group_statistic().await;
        build_normalize_plan_from_group_statistics(&groups)
    }

    async fn apply_normalize_plan(&self, plan: &NormalizePlan) -> Result<bool> {
        let (table_ids_right, boundary_table_id, new_compaction_group_id) = {
            let mut versioning_guard = self
                .versioning
                .write_with_process_name("apply_normalize_plan")
                .await;
            let versioning = versioning_guard.deref_mut();
            let mut compaction_group_manager = self
                .compaction_group_manager
                .write_with_process_name("apply_normalize_plan")
                .await;

            let groups = collect_normalize_group_statistics(
                &versioning.current_version,
                &compaction_group_manager,
            )?;
            let Some(current_plan) = build_normalize_plan_from_group_statistics(&groups) else {
                return Ok(false);
            };

            if &current_plan != plan {
                return Ok(false);
            }

            let (_table_ids_left, table_ids_right) = plan.split_table_ids();

            let config = compaction_group_manager
                .try_get_compaction_group_config(plan.parent_group_id)
                .ok_or_else(|| {
                    Error::CompactionGroup(format!(
                        "parent group {} config not found",
                        plan.parent_group_id
                    ))
                })?
                .compaction_config()
                .as_ref()
                .clone();

            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            let mut version = HummockVersionTransaction::new(
                &mut versioning.current_version,
                &mut versioning.hummock_version_deltas,
                &mut versioning.table_change_log,
                self.env.notification_manager(),
                None,
                &self.metrics,
                &self.env.opts,
                &self.version_stat_tx,
            );
            let mut new_version_delta = version.new_delta();
            let split_key = plan.split_key();
            let split_sst_count = new_version_delta
                .latest_version()
                .count_new_ssts_in_group_split(plan.parent_group_id, split_key.clone());
            let new_sst_start_id = next_sstable_id(&self.env, split_sst_count).await?;
            let new_compaction_group_id = next_compaction_group_id(&self.env).await?;

            #[expect(deprecated)]
            new_version_delta.group_deltas.insert(
                new_compaction_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupConstruct(Box::new(PbGroupConstruct {
                        group_config: Some(config.clone()),
                        group_id: new_compaction_group_id,
                        parent_group_id: plan.parent_group_id,
                        new_sst_start_id,
                        table_ids: vec![],
                        version: CompatibilityVersion::LATEST as _,
                        split_key: Some(split_key.into()),
                    }))],
                },
            );

            new_version_delta.with_latest_version(|version, new_version_delta| {
                for &table_id in &table_ids_right {
                    let info = version
                        .state_table_info
                        .info()
                        .get(&table_id)
                        .expect("table should exist before normalize split");
                    assert!(
                        new_version_delta
                            .state_table_info_delta
                            .insert(
                                table_id,
                                PbStateTableInfoDelta {
                                    committed_epoch: info.committed_epoch,
                                    compaction_group_id: new_compaction_group_id,
                                }
                            )
                            .is_none()
                    );
                }
            });
            new_version_delta.pre_apply();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
            versioning.mark_next_time_travel_version_snapshot();
            if !self.env.opts.compaction_deterministic_test {
                for group_id in [plan.parent_group_id, new_compaction_group_id] {
                    self.try_send_compaction_request(group_id, TaskType::Dynamic);
                }
            }

            (
                table_ids_right,
                plan.boundary_table_id,
                new_compaction_group_id,
            )
        };

        self.cancel_expired_normalize_split_tasks(plan.parent_group_id)
            .await?;
        self.try_update_write_limits(&[plan.parent_group_id, new_compaction_group_id])
            .await;
        self.metrics
            .split_compaction_group_count
            .with_label_values(&[&plan.parent_group_id.to_string()])
            .inc();
        tracing::info!(target: super::TRACE_TARGET,
            "normalize split success: parent_group={} boundary_table_id={} moved_tables={:?} new_group_id={}",
            plan.parent_group_id,
            boundary_table_id,
            table_ids_right,
            new_compaction_group_id
        );

        Ok(true)
    }

    async fn cancel_expired_normalize_split_tasks(
        &self,
        parent_group_id: CompactionGroupId,
    ) -> Result<()> {
        let mut canceled_tasks = vec![];
        let compaction_guard = self
            .compaction
            .write_with_process_name("cancel_expired_normalize_split_tasks")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("cancel_expired_normalize_split_tasks")
            .await;
        let versioning = versioning_guard.deref_mut();
        let compact_task_assignments =
            compaction_guard.get_compact_task_assignments_by_group_id(parent_group_id);
        let Some(levels) = versioning.current_version.levels.get(&parent_group_id) else {
            return Ok(());
        };
        compact_task_assignments
            .into_iter()
            .for_each(|task_assignment| {
                let task = &task_assignment.compact_task;
                if is_compaction_task_expired(
                    task.compaction_group_version_id,
                    levels.compaction_group_version_id,
                ) {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                        object_timestamps: HashMap::default(),
                    });
                }
            });
        canceled_tasks.sort_by_key(|task| task.task_id);
        canceled_tasks.dedup_by_key(|task| task.task_id);

        if !canceled_tasks.is_empty() {
            self.report_compact_tasks_impl(canceled_tasks, compaction_guard, versioning_guard)
                .await?;
        }

        Ok(())
    }

    /// Normalize overlapping adjacent compaction groups by split only.
    ///
    /// The algorithm repeatedly scans adjacent groups by `min(table_id)` and if
    /// `max(left) >= min(right)`, it splits `left` at the first table id `>= min(right)`.
    /// Each step is planned from a read snapshot, then revalidated and applied with a short write
    /// transaction.
    pub async fn normalize_overlapping_compaction_groups(&self) -> Result<usize> {
        self.normalize_overlapping_compaction_groups_with_limit(usize::MAX)
            .await
    }

    pub async fn normalize_overlapping_compaction_groups_with_limit(
        &self,
        max_splits: usize,
    ) -> Result<usize> {
        let mut split_count = 0usize;
        while split_count < max_splits {
            let Some(plan) = self.build_normalize_plan().await else {
                break;
            };

            if !self.apply_normalize_plan(&plan).await? {
                tracing::debug!(target: super::TRACE_TARGET,
                    parent_group_id = %plan.parent_group_id,
                    boundary_table_id = %plan.boundary_table_id,
                    "normalize plan became stale before apply"
                );
                break;
            }
            split_count += 1;
        }

        Ok(split_count)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::group;
    use super::{NormalizePlan, build_normalize_plan_from_group_statistics, gen_normalize_plan};

    #[test]
    fn test_gen_normalize_plan_returns_none_for_single_table_group() {
        let left = group(1.into(), &[10], false);
        let right = group(2.into(), &[5, 20], false);

        assert_eq!(None, gen_normalize_plan(&left, &right));
    }

    #[test]
    fn test_gen_normalize_plan_returns_none_for_non_overlapping_groups() {
        let left = group(1.into(), &[1, 2, 3], false);
        let right = group(2.into(), &[4, 5, 6], false);

        assert_eq!(None, gen_normalize_plan(&left, &right));
    }

    #[test]
    fn test_gen_normalize_plan_returns_none_when_boundary_cannot_split_parent() {
        let left = group(1.into(), &[5, 6, 7], false);
        let right = group(2.into(), &[4, 8], false);

        assert_eq!(None, gen_normalize_plan(&left, &right));
    }

    #[test]
    fn test_gen_normalize_plan_generates_expected_boundary() {
        let left = group(1.into(), &[1, 4, 7], false);
        let right = group(2.into(), &[2, 5, 8], false);

        assert_eq!(
            Some(NormalizePlan {
                parent_group_id: 1.into(),
                parent_table_ids: vec![1.into(), 4.into(), 7.into()],
                boundary_table_id: 4.into(),
            }),
            gen_normalize_plan(&left, &right)
        );
    }

    #[test]
    fn test_build_normalize_plan_skips_disabled_boundary_and_continues_later_segment() {
        let groups = vec![
            group(1.into(), &[1, 4, 7], false),
            group(2.into(), &[2, 5, 8], true),
            group(3.into(), &[10, 13, 16], false),
            group(4.into(), &[11, 14, 17], false),
        ];

        assert_eq!(
            Some(NormalizePlan {
                parent_group_id: 3.into(),
                parent_table_ids: vec![10.into(), 13.into(), 16.into()],
                boundary_table_id: 13.into(),
            }),
            build_normalize_plan_from_group_statistics(&groups)
        );
    }
}
