// Copyright 2022 RisingWave Labs
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

use std::cell::Cell;
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use risingwave_common::catalog::TableId;

use crate::monitor::HummockStateStoreMetrics;

// Example: RW_HUMMOCK_ITER_PROFILE=38:128,42:1.
static SAMPLING: LazyLock<Vec<(u32, NonZeroU32)>> = LazyLock::new(|| {
    let Ok(value) = std::env::var("RW_HUMMOCK_ITER_PROFILE") else {
        return Vec::new();
    };
    value
        .split(',')
        .filter_map(|entry| {
            let parsed = entry
                .split_once(':')
                .and_then(|(table, rate)| Some((table.parse().ok()?, rate.parse().ok()?)));
            if parsed.is_none() {
                tracing::warn!(entry, "Invalid RW_HUMMOCK_ITER_PROFILE entry");
            }
            parsed
        })
        .collect()
});

#[derive(Default)]
pub(super) struct IterInitProfile {
    pub snapshot: Duration,
    pub build: Duration,
    pub sst_meta: Duration,
    pub bloom: Duration,
    pub seek: Duration,
    pub seek_awaits: [Duration; 3],
}

impl IterInitProfile {
    pub fn sample(table_id: TableId) -> Option<Self> {
        let (_, every) = SAMPLING
            .iter()
            .find(|(id, _)| *id == table_id.as_raw_id())?;
        (every.get() == 1 || rand::random_range(0..every.get()) == 0).then(Self::default)
    }

    pub fn report(
        &self,
        metrics: &HummockStateStoreMetrics,
        table_id: TableId,
        wall: Duration,
        poll: Duration,
    ) {
        let table = table_id.to_string();
        let latency = if wall >= Duration::from_millis(10) {
            "ge_10ms"
        } else {
            "lt_10ms"
        };
        for (phase, duration) in [
            ("wall", wall),
            ("poll", poll),
            ("between_polls", wall.saturating_sub(poll)),
            ("snapshot", self.snapshot),
            ("build", self.build),
            ("sst_meta", self.sst_meta),
            ("bloom", self.bloom),
            ("initial_seek", self.seek),
            (
                "seek_cooperative_budget",
                self.seek_awaits[SeekAwait::CooperativeBudget as usize],
            ),
            (
                "seek_prefetch",
                self.seek_awaits[SeekAwait::Prefetch as usize],
            ),
            (
                "seek_demand_block",
                self.seek_awaits[SeekAwait::DemandBlock as usize],
            ),
        ] {
            metrics
                .iter_init_profile_duration
                .with_label_values(&[&table, latency, phase])
                .observe(duration.as_secs_f64());
        }
    }
}

/// Time spent polling includes OS preemption. Time between polls includes both waiting for
/// dependencies and waiting to run after a wakeup.
pub(super) async fn measure_polls<F: Future>(future: F) -> (F::Output, Duration) {
    let mut future = Box::pin(future);
    let mut duration = Duration::ZERO;
    let output = poll_fn(|cx| {
        let start = Instant::now();
        let polled = future.as_mut().poll(cx);
        duration += start.elapsed();
        polled
    })
    .await;
    (output, duration)
}

tokio::task_local! {
    static SEEK_AWAITS: Cell<[Duration; 3]>;
}

pub(in crate::hummock) enum SeekAwait {
    CooperativeBudget,
    Prefetch,
    DemandBlock,
}

impl SeekAwait {
    pub fn active() -> bool {
        !SAMPLING.is_empty() && SEEK_AWAITS.try_with(|_| ()).is_ok()
    }

    pub fn record(self, start: Option<Instant>) {
        if let Some(start) = start {
            SEEK_AWAITS.with(|times| {
                let mut durations = times.get();
                durations[self as usize] += start.elapsed();
                times.set(durations);
            });
        }
    }
}

pub(super) async fn measure_seek<F: Future>(future: F) -> (F::Output, [Duration; 3]) {
    Box::pin(SEEK_AWAITS.scope(Cell::new([Duration::ZERO; 3]), async {
        let result = future.await;
        (result, SEEK_AWAITS.with(Cell::get))
    }))
    .await
}
