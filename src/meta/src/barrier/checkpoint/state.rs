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

use std::collections::HashSet;
use std::mem::take;

use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::util::epoch::Epoch;

use crate::barrier::info::{
    BarrierInfo, InflightDatabaseInfo, InflightStreamingJobInfo, InflightSubscriptionInfo,
    SharedActorInfos,
};
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};

/// The latest state of `GlobalBarrierWorker` after injecting the latest barrier.
pub(crate) struct BarrierWorkerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,

    /// The `prev_epoch` of pending non checkpoint barriers
    pending_non_checkpoint_barriers: Vec<u64>,

    /// Inflight running actors info.
    pub(super) inflight_graph_info: InflightDatabaseInfo,

    pub(super) inflight_subscription_info: InflightSubscriptionInfo,

    /// Whether the cluster is paused.
    is_paused: bool,
}

impl BarrierWorkerState {
    pub(super) fn new(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        Self {
            in_flight_prev_epoch: TracedEpoch::new(Epoch::now()),
            pending_non_checkpoint_barriers: vec![],
            inflight_graph_info: InflightDatabaseInfo::empty(database_id, shared_actor_infos),
            inflight_subscription_info: InflightSubscriptionInfo::default(),
            is_paused: false,
        }
    }

    pub fn recovery(
        database_id: DatabaseId,
        shared_actor_infos: SharedActorInfos,
        in_flight_prev_epoch: TracedEpoch,
        jobs: impl Iterator<Item = InflightStreamingJobInfo>,
        inflight_subscription_info: InflightSubscriptionInfo,
        is_paused: bool,
    ) -> Self {
        Self {
            in_flight_prev_epoch,
            pending_non_checkpoint_barriers: vec![],
            inflight_graph_info: InflightDatabaseInfo::recover(
                database_id,
                jobs,
                shared_actor_infos,
            ),
            inflight_subscription_info,
            is_paused,
        }
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    fn set_is_paused(&mut self, is_paused: bool) {
        if self.is_paused != is_paused {
            tracing::info!(
                currently_paused = self.is_paused,
                newly_paused = is_paused,
                "update paused state"
            );
            self.is_paused = is_paused;
        }
    }

    pub fn in_flight_prev_epoch(&self) -> &TracedEpoch {
        &self.in_flight_prev_epoch
    }

    /// Returns the `BarrierInfo` for the next barrier, and updates the state.
    pub fn next_barrier_info(
        &mut self,
        command: Option<&Command>,
        is_checkpoint: bool,
        curr_epoch: TracedEpoch,
    ) -> Option<BarrierInfo> {
        if self.inflight_graph_info.is_empty()
            && !matches!(&command, Some(Command::CreateStreamingJob { .. }))
        {
            return None;
        };
        assert!(
            self.in_flight_prev_epoch.value() < curr_epoch.value(),
            "curr epoch regress. {} > {}",
            self.in_flight_prev_epoch.value(),
            curr_epoch.value()
        );
        let prev_epoch = self.in_flight_prev_epoch.clone();
        self.in_flight_prev_epoch = curr_epoch.clone();
        self.pending_non_checkpoint_barriers
            .push(prev_epoch.value().0);
        let kind = if is_checkpoint {
            let epochs = take(&mut self.pending_non_checkpoint_barriers);
            BarrierKind::Checkpoint(epochs)
        } else {
            BarrierKind::Barrier
        };
        Some(BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        })
    }

    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    ///
    /// Return (`graph_info`, `subscription_info`, `table_ids_to_commit`, `jobs_to_wait`, `prev_is_paused`)
    pub fn apply_command(
        &mut self,
        command: Option<&Command>,
    ) -> (
        InflightDatabaseInfo,
        InflightSubscriptionInfo,
        HashSet<TableId>,
        HashSet<TableId>,
        bool,
    ) {
        // update the fragment_infos outside pre_apply
        let fragment_changes = if let Some(Command::CreateStreamingJob {
            job_type: CreateStreamingJobType::SnapshotBackfill(_),
            ..
        }) = command
        {
            None
        } else if let Some(fragment_changes) = command.and_then(Command::fragment_changes) {
            self.inflight_graph_info.pre_apply(&fragment_changes);
            Some(fragment_changes)
        } else {
            None
        };
        if let Some(command) = &command {
            self.inflight_subscription_info.pre_apply(command);
        }

        let info = self.inflight_graph_info.clone();
        let subscription_info = self.inflight_subscription_info.clone();

        if let Some(fragment_changes) = fragment_changes {
            self.inflight_graph_info.post_apply(&fragment_changes);
        }

        let mut table_ids_to_commit: HashSet<_> = info.existing_table_ids().collect();
        let mut jobs_to_wait = HashSet::new();
        if let Some(Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge)) = command {
            for (table_id, (_, graph_info)) in jobs_to_merge {
                jobs_to_wait.insert(*table_id);
                table_ids_to_commit.extend(graph_info.existing_table_ids());
                self.inflight_graph_info.add_existing(graph_info.clone());
            }
        }

        if let Some(command) = command {
            self.inflight_subscription_info.post_apply(command);
        }

        let prev_is_paused = self.is_paused();
        let curr_is_paused = match command {
            Some(Command::Pause) => true,
            Some(Command::Resume) => false,
            _ => prev_is_paused,
        };
        self.set_is_paused(curr_is_paused);

        (
            info,
            subscription_info,
            table_ids_to_commit,
            jobs_to_wait,
            prev_is_paused,
        )
    }
}
