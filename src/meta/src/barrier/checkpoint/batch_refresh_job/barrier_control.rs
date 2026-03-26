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

//! Barrier stats for batch refresh jobs.
//!
//! Implements `PartialGraphStat` so that `PartialGraphManager` can track
//! barrier latency and inflight counts on behalf of batch refresh jobs.
//! Structurally identical to `CreatingStreamingJobBarrierStats`.

use risingwave_common::id::JobId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_common::util::epoch::EpochPair;

use crate::barrier::partial_graph::PartialGraphStat;
use crate::rpc::metrics::GLOBAL_META_METRICS;

pub(super) struct BatchRefreshBarrierStats {
    barrier_latency: LabelGuardedHistogram,
    inflight_barrier_num: LabelGuardedIntGauge,
}

impl BatchRefreshBarrierStats {
    pub(super) fn new(job_id: JobId, _snapshot_epoch: u64) -> Self {
        let table_id_str = format!("{}", job_id);
        Self {
            barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[table_id_str.as_str(), "batch_refresh_snapshot"]),
            inflight_barrier_num: GLOBAL_META_METRICS
                .snapshot_backfill_inflight_barrier_num
                .with_guarded_label_values(&[&table_id_str]),
        }
    }
}

impl PartialGraphStat for BatchRefreshBarrierStats {
    fn observe_barrier_latency(&self, _epoch: EpochPair, barrier_latency_secs: f64) {
        self.barrier_latency.observe(barrier_latency_secs);
    }

    fn observe_barrier_num(&self, inflight_barrier_num: usize, _collected_barrier_num: usize) {
        self.inflight_barrier_num.set(inflight_barrier_num as _);
    }
}
