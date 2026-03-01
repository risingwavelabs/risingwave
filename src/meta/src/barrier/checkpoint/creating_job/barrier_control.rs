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

use std::collections::VecDeque;
use std::mem::take;
use std::ops::Bound::Unbounded;
use std::ops::{Bound, RangeBounds};
use std::time::Instant;

use prometheus::HistogramTimer;
use risingwave_common::id::JobId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::debug;

use crate::barrier::BarrierKind;
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::CollectedBarrier;
use crate::rpc::metrics::GLOBAL_META_METRICS;

#[derive(Debug)]
struct CreatingStreamingJobEpochState {
    epoch: u64,
    resps: Vec<BarrierCompleteResponse>,
    notifiers: Vec<Notifier>,
    kind: BarrierKind,
    first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
    enqueue_time: Instant,
}

#[derive(Debug)]
pub(super) struct CreatingStreamingJobBarrierControl {
    job_id: JobId,
    // newer epoch at the front. `push_front` and `pop_back`
    inflight_barrier_queue: VecDeque<CreatingStreamingJobEpochState>,
    snapshot_epoch: u64,
    max_collected_epoch: Option<u64>,
    max_committed_epoch: Option<u64>,
    // newer epoch at the front. `push_front` and `pop_back`
    pending_barriers_to_complete: VecDeque<CreatingStreamingJobEpochState>,
    completing_barrier: Option<(CreatingStreamingJobEpochState, HistogramTimer)>,

    // metrics
    consuming_snapshot_barrier_latency: LabelGuardedHistogram,
    consuming_log_store_barrier_latency: LabelGuardedHistogram,

    wait_commit_latency: LabelGuardedHistogram,
    inflight_barrier_num: LabelGuardedIntGauge,
}

impl CreatingStreamingJobBarrierControl {
    pub(super) fn new(job_id: JobId, snapshot_epoch: u64, committed_epoch: Option<u64>) -> Self {
        let table_id_str = format!("{}", job_id);
        Self {
            job_id,
            inflight_barrier_queue: Default::default(),
            snapshot_epoch,
            max_collected_epoch: committed_epoch,
            max_committed_epoch: committed_epoch,
            pending_barriers_to_complete: Default::default(),
            completing_barrier: None,

            consuming_snapshot_barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[table_id_str.as_str(), "consuming_snapshot"]),
            consuming_log_store_barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[table_id_str.as_str(), "consuming_log_store"]),
            wait_commit_latency: GLOBAL_META_METRICS
                .snapshot_backfill_wait_commit_latency
                .with_guarded_label_values(&[&table_id_str]),
            inflight_barrier_num: GLOBAL_META_METRICS
                .snapshot_backfill_inflight_barrier_num
                .with_guarded_label_values(&[&table_id_str]),
        }
    }

    pub(super) fn inflight_barrier_count(&self) -> usize {
        self.inflight_barrier_queue.len()
    }

    fn latest_epoch(&self) -> Option<u64> {
        self.inflight_barrier_queue
            .front()
            .map(|epoch| epoch.epoch)
            .or(self.max_collected_epoch)
    }

    pub(super) fn max_committed_epoch(&self) -> Option<u64> {
        self.max_committed_epoch
    }

    pub(super) fn is_empty(&self) -> bool {
        self.inflight_barrier_queue.is_empty()
            && self.pending_barriers_to_complete.is_empty()
            && self.completing_barrier.is_none()
    }

    pub(super) fn enqueue_epoch(
        &mut self,
        epoch: u64,
        kind: BarrierKind,
        notifiers: Vec<Notifier>,
        first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
    ) {
        debug!(
            epoch,
            job_id = %self.job_id,
            "creating job enqueue epoch"
        );
        if first_create_info.is_some() {
            assert!(
                kind.is_checkpoint(),
                "first barrier must be checkpoint barrier"
            );
        }
        match &kind {
            BarrierKind::Initial => {
                unreachable!("should not inject initial barrier here");
            }
            BarrierKind::Barrier | BarrierKind::Checkpoint(_) => {
                if let Some(latest_epoch) = self.latest_epoch() {
                    assert!(epoch > latest_epoch, "{} {}", epoch, latest_epoch);
                }
            }
        }

        let epoch_state = CreatingStreamingJobEpochState {
            epoch,
            resps: vec![],
            notifiers,
            kind,
            first_create_info,
            enqueue_time: Instant::now(),
        };
        self.inflight_barrier_queue.push_front(epoch_state);
        self.inflight_barrier_num
            .set(self.inflight_barrier_queue.len() as _);
    }

    pub(super) fn collect(&mut self, collected_barrier: CollectedBarrier) {
        let mut state = self
            .inflight_barrier_queue
            .pop_back()
            .expect("non-empty when collected");
        assert_eq!(state.epoch, collected_barrier.epoch.prev);
        assert!(state.resps.is_empty());
        state.resps.extend(collected_barrier.resps.into_values());
        self.add_collected(state);

        self.inflight_barrier_num
            .set(self.inflight_barrier_queue.len() as _);
    }

    /// Return Some((epoch, resps, `is_first_commit`))
    ///
    /// Only epoch within the `epoch_end_bound` can be started.
    /// Usually `epoch_end_bound` is the upstream committed epoch. This is to ensure that
    /// the creating job won't have higher committed epoch than the upstream.
    pub(super) fn start_completing(
        &mut self,
        epoch_end_bound: Bound<u64>,
    ) -> Option<(
        u64,
        Vec<BarrierCompleteResponse>,
        Option<CreateSnapshotBackfillJobCommandInfo>,
    )> {
        assert!(self.completing_barrier.is_none());
        let epoch_range: (Bound<u64>, Bound<u64>) = (Unbounded, epoch_end_bound);
        while let Some(epoch_state) = self.pending_barriers_to_complete.back()
            && epoch_range.contains(&epoch_state.epoch)
        {
            let mut epoch_state = self
                .pending_barriers_to_complete
                .pop_back()
                .expect("non-empty");
            let epoch = epoch_state.epoch;
            let first_create_info = epoch_state.first_create_info.take();
            if first_create_info.is_some() {
                assert!(epoch_state.kind.is_checkpoint());
            } else if !epoch_state.kind.is_checkpoint() {
                continue;
            }

            let resps = take(&mut epoch_state.resps);
            self.completing_barrier = Some((epoch_state, self.wait_commit_latency.start_timer()));
            return Some((epoch, resps, first_create_info));
        }
        None
    }

    /// Ack on completing a checkpoint barrier.
    ///
    /// Return the upstream epoch to be notified when there is any.
    pub(super) fn ack_completed(&mut self, completed_epoch: u64) {
        let (epoch_state, wait_commit_timer) =
            self.completing_barrier.take().expect("should exist");
        wait_commit_timer.observe_duration();
        assert_eq!(epoch_state.epoch, completed_epoch);
        for notifier in epoch_state.notifiers {
            notifier.notify_collected();
        }
        if let Some(prev_max_committed_epoch) = self.max_committed_epoch.replace(completed_epoch) {
            assert!(completed_epoch > prev_max_committed_epoch);
        }
    }

    fn add_collected(&mut self, epoch_state: CreatingStreamingJobEpochState) {
        if let Some(prev_epoch_state) = self.pending_barriers_to_complete.front() {
            assert!(prev_epoch_state.epoch < epoch_state.epoch);
        }
        if let Some(max_collected_epoch) = self.max_collected_epoch {
            match &epoch_state.kind {
                BarrierKind::Initial => {
                    unreachable!("should not collect initial barrier here")
                }
                BarrierKind::Barrier | BarrierKind::Checkpoint(_) => {
                    assert!(epoch_state.epoch > max_collected_epoch);
                }
            }
        }
        self.max_collected_epoch = Some(epoch_state.epoch);
        let barrier_latency = epoch_state.enqueue_time.elapsed().as_secs_f64();
        let barrier_latency_metrics = if epoch_state.epoch < self.snapshot_epoch {
            &self.consuming_snapshot_barrier_latency
        } else {
            &self.consuming_log_store_barrier_latency
        };
        barrier_latency_metrics.observe(barrier_latency);
        self.pending_barriers_to_complete.push_front(epoch_state);
    }
}
