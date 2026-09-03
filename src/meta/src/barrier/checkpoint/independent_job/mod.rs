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

use std::collections::{HashMap, HashSet};
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::id::{FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::PbSubscriptionUpstreamInfo;
use risingwave_pb::stream_plan::barrier::PbBarrierKind;

pub(crate) mod batch_refresh_job;
pub(crate) mod creating_job;

pub(crate) use batch_refresh_job::{
    BatchRefreshJobCheckpointControl, BatchRefreshJobTriggerContext, BatchRefreshLogicalFragments,
    BatchRefreshRenderResult,
};
pub(crate) use creating_job::CreatingStreamingJobControl;

use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::{CollectionNotifier, NotifierStarter};
use crate::barrier::partial_graph::{CollectedBarrier, PartialGraphManager};
use crate::barrier::{BackfillProgress, BarrierKind, FragmentBackfillProgress, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;

/// Build a fake `BarrierInfo` for independent partial-graph barriers.
///
/// Shared by both `CreatingStreamingJobControl` and `BatchRefreshJobCheckpointControl`.
fn new_fake_barrier(
    prev_epoch_fake_physical_time: &mut u64,
    pending_non_checkpoint_barriers: &mut Vec<u64>,
    kind: PbBarrierKind,
) -> BarrierInfo {
    let prev_epoch = TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
    *prev_epoch_fake_physical_time += 1;
    let curr_epoch = TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
    let kind = match kind {
        PbBarrierKind::Unspecified => unreachable!(),
        PbBarrierKind::Initial => {
            assert!(pending_non_checkpoint_barriers.is_empty());
            BarrierKind::Initial
        }
        PbBarrierKind::Barrier => {
            pending_non_checkpoint_barriers.push(prev_epoch.value().0);
            BarrierKind::Barrier
        }
        PbBarrierKind::Checkpoint => {
            pending_non_checkpoint_barriers.push(prev_epoch.value().0);
            BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
        }
    };
    BarrierInfo {
        prev_epoch,
        curr_epoch,
        kind,
    }
}

// ── Enum unifying independent checkpoint job types ──────────────────────────

/// The type-specific running state of a streaming job that checkpoints independently from the
/// database's main graph.
pub(crate) enum IndependentCheckpointJob {
    CreatingStreamingJob(CreatingStreamingJobControl),
    BatchRefresh(BatchRefreshJobCheckpointControl),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IndependentCheckpointJobStatus {
    /// The initial barrier cannot be completed until the database has committed the snapshot
    /// epoch.
    Initial { snapshot_epoch: u64 },
    /// The upstream database has committed the snapshot epoch, so barriers may complete.
    Ready,
}

/// The lifecycle shared by all independent checkpoint jobs.
pub(crate) enum IndependentCheckpointJobControl {
    Running {
        status: IndependentCheckpointJobStatus,
        job_id: JobId,
        partial_graph_id: PartialGraphId,
        job: IndependentCheckpointJob,
    },
    Resetting {
        /// Keep changelog pins until compute acknowledges the partial-graph reset. Before that
        /// acknowledgment, snapshot-backfill executors may still be stopping and reading logs.
        pinned_upstream_tables: HashSet<TableId>,
        subscriptions_to_drop: Vec<PbSubscriptionUpstreamInfo>,
        notifiers: Vec<CollectionNotifier>,
    },
}

impl IndependentCheckpointJob {
    fn can_drop_independently(&self) -> bool {
        match self {
            Self::CreatingStreamingJob(j) => j.can_drop_independently(),
            Self::BatchRefresh(_) => true,
        }
    }

    fn pinned_upstream_tables(&self) -> &HashSet<TableId> {
        match self {
            Self::CreatingStreamingJob(j) => j.pinned_upstream_tables(),
            Self::BatchRefresh(j) => j.pinned_upstream_tables(),
        }
    }
}

impl IndependentCheckpointJobStatus {
    fn on_upstream_database_ack_completed(&mut self, committed_epoch: u64) {
        if let Self::Initial { snapshot_epoch } = self
            && committed_epoch >= *snapshot_epoch
        {
            *self = Self::Ready;
        }
    }
}

impl IndependentCheckpointJobControl {
    pub(crate) fn creating_streaming_job(
        job_id: JobId,
        partial_graph_id: PartialGraphId,
        status: IndependentCheckpointJobStatus,
        job: CreatingStreamingJobControl,
    ) -> Self {
        Self::Running {
            status,
            job_id,
            partial_graph_id,
            job: IndependentCheckpointJob::CreatingStreamingJob(job),
        }
    }

    pub(crate) fn batch_refresh(
        job_id: JobId,
        partial_graph_id: PartialGraphId,
        status: IndependentCheckpointJobStatus,
        job: BatchRefreshJobCheckpointControl,
    ) -> Self {
        Self::Running {
            status,
            job_id,
            partial_graph_id,
            job: IndependentCheckpointJob::BatchRefresh(job),
        }
    }

    pub(crate) fn running(&self) -> Option<&IndependentCheckpointJob> {
        match self {
            Self::Running { job, .. } => Some(job),
            Self::Resetting { .. } => None,
        }
    }

    pub(crate) fn running_mut(&mut self) -> Option<&mut IndependentCheckpointJob> {
        match self {
            Self::Running { job, .. } => Some(job),
            Self::Resetting { .. } => None,
        }
    }

    pub(crate) fn ready_mut(&mut self) -> Option<&mut IndependentCheckpointJob> {
        match self {
            Self::Running {
                status: IndependentCheckpointJobStatus::Ready,
                job,
                ..
            } => Some(job),
            Self::Running {
                status: IndependentCheckpointJobStatus::Initial { .. },
                ..
            }
            | Self::Resetting { .. } => None,
        }
    }

    pub(crate) fn on_upstream_database_ack_completed(&mut self, committed_epoch: u64) {
        if let Self::Running { status, .. } = self {
            status.on_upstream_database_ack_completed(committed_epoch);
        }
    }

    pub(crate) fn gen_backfill_progress(&self) -> Option<BackfillProgress> {
        match self.running()? {
            IndependentCheckpointJob::CreatingStreamingJob(j) => Some(j.gen_backfill_progress()),
            IndependentCheckpointJob::BatchRefresh(j) => j.gen_backfill_progress(),
        }
    }

    /// Collect a barrier and return whether a checkpoint should be forced in the next barrier.
    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        let job = self
            .running_mut()
            .expect("barriers should only be collected from a running partial graph");
        match job {
            IndependentCheckpointJob::CreatingStreamingJob(j) => j.collect(collected_barrier),
            IndependentCheckpointJob::BatchRefresh(j) => j.collect(collected_barrier),
        }
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match self.running() {
            Some(IndependentCheckpointJob::CreatingStreamingJob(j)) => {
                j.gen_fragment_backfill_progress()
            }
            Some(IndependentCheckpointJob::BatchRefresh(j)) => j.gen_fragment_backfill_progress(),
            None => vec![],
        }
    }

    pub(crate) fn pinned_upstream_tables(&self) -> &HashSet<TableId> {
        match self {
            Self::Running { job, .. } => job.pinned_upstream_tables(),
            Self::Resetting {
                pinned_upstream_tables,
                ..
            } => pinned_upstream_tables,
        }
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match self.running()? {
            IndependentCheckpointJob::CreatingStreamingJob(j) => j.fragment_infos(),
            IndependentCheckpointJob::BatchRefresh(j) => j.fragment_infos(),
        }
    }

    pub(crate) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        epoch: u64,
    ) {
        match self {
            Self::Running {
                status: IndependentCheckpointJobStatus::Ready,
                job: IndependentCheckpointJob::CreatingStreamingJob(j),
                ..
            } => j.ack_completed(partial_graph_manager, epoch),
            Self::Running {
                status: IndependentCheckpointJobStatus::Ready,
                job: IndependentCheckpointJob::BatchRefresh(j),
                ..
            } => j.ack_completed(partial_graph_manager, epoch),
            Self::Running {
                status: IndependentCheckpointJobStatus::Initial { .. },
                ..
            } => {
                panic!("an initial job should transition to ready before completing a barrier")
            }
            Self::Resetting { .. } => {
                // The job was dropped while the completing task was running in the background.
                // The partial graph has already been reset, so skip the ack.
            }
        }
    }

    pub(crate) fn on_partial_graph_reset(self) -> Vec<PbSubscriptionUpstreamInfo> {
        match self {
            Self::Resetting {
                subscriptions_to_drop,
                notifiers,
                ..
            } => {
                for notifier in notifiers {
                    notifier.notify_collected();
                }
                subscriptions_to_drop
            }
            Self::Running { .. } => {
                panic!("should be resetting when receiving reset partial graph resp")
            }
        }
    }

    pub(crate) fn drop(
        &mut self,
        notifier: Option<&mut NotifierStarter>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        match self {
            Self::Resetting { notifiers, .. } => {
                notifiers.extend(notifier.map(NotifierStarter::add_notify));
                true
            }
            Self::Running { job, .. } if !job.can_drop_independently() => false,
            Self::Running {
                job_id,
                partial_graph_id,
                job,
                ..
            } => {
                let subscriptions_to_drop = job
                    .pinned_upstream_tables()
                    .iter()
                    .map(|upstream_mv_table_id| PbSubscriptionUpstreamInfo {
                        subscriber_id: job_id.as_subscriber_id(),
                        upstream_mv_table_id: *upstream_mv_table_id,
                    })
                    .collect();
                // Resetting must keep owning the pins after the concrete job is replaced.
                let pinned_upstream_tables = job.pinned_upstream_tables().clone();
                partial_graph_manager.reset_partial_graphs([*partial_graph_id]);
                *self = Self::Resetting {
                    pinned_upstream_tables,
                    subscriptions_to_drop,
                    notifiers: notifier
                        .map(NotifierStarter::add_notify)
                        .into_iter()
                        .collect(),
                };
                true
            }
        }
    }

    /// Reset during database recovery.
    ///
    /// Returns `true` if the partial graph was already resetting (from a prior drop),
    /// meaning caller should not issue a new reset request.
    pub(crate) fn reset(self) -> bool {
        match self {
            // Running jobs have no reset state to drain. Recovery will issue the partial-graph
            // reset after rebuilding its reset plan.
            Self::Running { .. } => false,
            Self::Resetting { notifiers, .. } => {
                for notifier in notifiers {
                    notifier.notify_collected();
                }
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resetting_has_shared_inactive_behavior_and_keeps_pin() {
        let pinned_upstream_tables = HashSet::from([TableId::new(7)]);
        let job = IndependentCheckpointJobControl::Resetting {
            pinned_upstream_tables: pinned_upstream_tables.clone(),
            subscriptions_to_drop: vec![],
            notifiers: vec![],
        };

        assert!(job.running().is_none());
        assert!(job.gen_backfill_progress().is_none());
        assert!(job.gen_fragment_backfill_progress().is_empty());
        assert_eq!(job.pinned_upstream_tables(), &pinned_upstream_tables);
        assert!(job.fragment_infos().is_none());
        assert!(job.reset());
    }

    #[test]
    fn test_reset_completion_returns_subscriptions_to_drop() {
        let subscriptions_to_drop = vec![PbSubscriptionUpstreamInfo {
            subscriber_id: JobId::new(8).as_subscriber_id(),
            upstream_mv_table_id: TableId::new(7),
        }];
        let job = IndependentCheckpointJobControl::Resetting {
            pinned_upstream_tables: HashSet::from([TableId::new(7)]),
            subscriptions_to_drop: subscriptions_to_drop.clone(),
            notifiers: vec![],
        };

        assert_eq!(job.on_partial_graph_reset(), subscriptions_to_drop);
    }

    #[test]
    fn test_initial_status_transitions_on_upstream_database_ack() {
        let mut status = IndependentCheckpointJobStatus::Initial { snapshot_epoch: 10 };

        status.on_upstream_database_ack_completed(9);
        assert_eq!(
            status,
            IndependentCheckpointJobStatus::Initial { snapshot_epoch: 10 }
        );
        status.on_upstream_database_ack_completed(10);
        assert_eq!(status, IndependentCheckpointJobStatus::Ready);
        status.on_upstream_database_ack_completed(11);
        assert_eq!(status, IndependentCheckpointJobStatus::Ready);
    }
}
