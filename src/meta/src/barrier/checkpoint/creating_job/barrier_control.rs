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

use std::collections::{BTreeMap, VecDeque};
use std::mem::take;
use std::ops::Bound::Unbounded;
use std::ops::{Bound, RangeBounds};
use std::time::Instant;

use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_meta_model::WorkerId;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::debug;

use crate::barrier::utils::{NodeToCollect, is_valid_after_worker_err};
use crate::rpc::metrics::GLOBAL_META_METRICS;

#[derive(Debug)]
struct CreatingStreamingJobEpochState {
    epoch: u64,
    node_to_collect: NodeToCollect,
    resps: Vec<BarrierCompleteResponse>,
    is_checkpoint: bool,
    enqueue_time: Instant,
}

#[derive(Debug)]
pub(super) struct CreatingStreamingJobBarrierControl {
    table_id: TableId,
    // key is prev_epoch of barrier
    inflight_barrier_queue: BTreeMap<u64, CreatingStreamingJobEpochState>,
    backfill_epoch: u64,
    initial_epoch: Option<u64>,
    max_collected_epoch: Option<u64>,
    // newer epoch at the front.
    pending_barriers_to_complete: VecDeque<CreatingStreamingJobEpochState>,
    completing_barrier: Option<(CreatingStreamingJobEpochState, HistogramTimer)>,

    // metrics
    consuming_snapshot_barrier_latency: LabelGuardedHistogram<2>,
    consuming_log_store_barrier_latency: LabelGuardedHistogram<2>,

    wait_commit_latency: LabelGuardedHistogram<1>,
    inflight_barrier_num: LabelGuardedIntGauge<1>,
}

impl CreatingStreamingJobBarrierControl {
    pub(super) fn new(table_id: TableId, backfill_epoch: u64) -> Self {
        let table_id_str = format!("{}", table_id.table_id);
        Self {
            table_id,
            inflight_barrier_queue: Default::default(),
            backfill_epoch,
            initial_epoch: None,
            max_collected_epoch: None,
            pending_barriers_to_complete: Default::default(),
            completing_barrier: None,

            consuming_snapshot_barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[&table_id_str, "consuming_snapshot"]),
            consuming_log_store_barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[&table_id_str, "consuming_log_store"]),
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

    pub(super) fn is_valid_after_worker_err(&mut self, worker_id: WorkerId) -> bool {
        self.inflight_barrier_queue
            .values_mut()
            .all(|state| is_valid_after_worker_err(&mut state.node_to_collect, worker_id))
    }

    fn latest_epoch(&self) -> Option<u64> {
        self.inflight_barrier_queue
            .last_key_value()
            .map(|(epoch, _)| *epoch)
            .or(self.max_collected_epoch)
    }

    pub(super) fn max_collected_epoch(&self) -> Option<u64> {
        self.max_collected_epoch
    }

    pub(super) fn is_empty(&self) -> bool {
        self.inflight_barrier_queue.is_empty()
            && self.pending_barriers_to_complete.is_empty()
            && self.completing_barrier.is_none()
    }

    pub(super) fn enqueue_epoch(
        &mut self,
        epoch: u64,
        node_to_collect: NodeToCollect,
        is_checkpoint: bool,
    ) {
        debug!(
            epoch,
            ?node_to_collect,
            table_id = self.table_id.table_id,
            "creating job enqueue epoch"
        );
        if self.initial_epoch.is_none() {
            self.initial_epoch = Some(epoch);
            assert!(is_checkpoint, "first barrier must be checkpoint barrier");
        }
        if let Some(latest_epoch) = self.latest_epoch() {
            assert!(epoch > latest_epoch, "{} {}", epoch, latest_epoch);
        }
        let epoch_state = CreatingStreamingJobEpochState {
            epoch,
            node_to_collect,
            resps: vec![],
            is_checkpoint,
            enqueue_time: Instant::now(),
        };
        if epoch_state.node_to_collect.is_empty() && self.inflight_barrier_queue.is_empty() {
            self.add_collected(epoch_state);
        } else {
            self.inflight_barrier_queue.insert(epoch, epoch_state);
        }
        self.inflight_barrier_num
            .set(self.inflight_barrier_queue.len() as _);
    }

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) {
        debug!(
            epoch,
            worker_id,
            table_id = self.table_id.table_id,
            "collect barrier from worker"
        );

        let state = self
            .inflight_barrier_queue
            .get_mut(&epoch)
            .expect("should exist");
        assert!(state.node_to_collect.remove(&worker_id).is_some());
        state.resps.push(resp);
        while let Some((_, state)) = self.inflight_barrier_queue.first_key_value()
            && state.node_to_collect.is_empty()
        {
            let (_, state) = self.inflight_barrier_queue.pop_first().expect("non-empty");
            self.add_collected(state);
        }

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
    ) -> Option<(u64, Vec<BarrierCompleteResponse>, bool)> {
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
            let is_first = self.initial_epoch.expect("should have set") == epoch;
            if is_first {
                assert!(epoch_state.is_checkpoint);
            } else if !epoch_state.is_checkpoint {
                continue;
            }

            let resps = take(&mut epoch_state.resps);
            self.completing_barrier = Some((epoch_state, self.wait_commit_latency.start_timer()));
            return Some((epoch, resps, is_first));
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
    }

    fn add_collected(&mut self, epoch_state: CreatingStreamingJobEpochState) {
        assert!(epoch_state.node_to_collect.is_empty());
        if let Some(prev_epoch_state) = self.pending_barriers_to_complete.front() {
            assert!(prev_epoch_state.epoch < epoch_state.epoch);
        }
        if let Some(max_collected_epoch) = self.max_collected_epoch {
            assert!(epoch_state.epoch > max_collected_epoch);
        }
        self.max_collected_epoch = Some(epoch_state.epoch);
        let barrier_latency = epoch_state.enqueue_time.elapsed().as_secs_f64();
        let barrier_latency_metrics = if epoch_state.epoch < self.backfill_epoch {
            &self.consuming_snapshot_barrier_latency
        } else {
            &self.consuming_log_store_barrier_latency
        };
        barrier_latency_metrics.observe(barrier_latency);
        self.pending_barriers_to_complete.push_front(epoch_state);
    }
}
