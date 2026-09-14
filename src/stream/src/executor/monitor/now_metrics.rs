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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::Registry;
use risingwave_common::metrics::{LabelGuardedIntGauge, LabelGuardedIntGaugeVec};
use risingwave_common::register_guarded_int_gauge_vec_with_registry;

use crate::task::FragmentId;

/// Aggregates the latest samples of live NOW executors on this compute node.
#[derive(Clone)]
pub struct NowMetrics {
    streaming_clock_ms: LabelGuardedIntGaugeVec,
    wall_clock_drift_ms: LabelGuardedIntGaugeVec,
    state: Arc<Mutex<NowMetricsState>>,
}

#[derive(Default)]
struct NowMetricsState {
    next_executor_id: u64,
    fragments: HashMap<FragmentId, FragmentMetrics>,
}

struct FragmentMetrics {
    // Use a separate identity for each executor lifetime, including during recovery.
    samples: HashMap<u64, (i64, i64)>,
    streaming_clock_ms: LabelGuardedIntGauge,
    wall_clock_drift_ms: LabelGuardedIntGauge,
}

impl FragmentMetrics {
    fn refresh(&self) {
        let streaming_clock_ms = self.samples.values().map(|&(clock, _)| clock).min();
        let barrier_ms = self.samples.values().map(|&(_, barrier)| barrier).max();
        if let (Some(clock), Some(barrier)) = (streaming_clock_ms, barrier_ms) {
            self.streaming_clock_ms.set(clock);
            self.wall_clock_drift_ms.set(barrier.saturating_sub(clock));
        }
    }
}

impl NowMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            streaming_clock_ms: register_guarded_int_gauge_vec_with_registry!(
                "stream_now_streaming_clock_ms",
                "Minimum latest streaming NOW() timestamp (milliseconds since Unix epoch) \
                 among live executors with an emitted watermark in this fragment on this compute node.",
                &["fragment_id"],
                registry,
            )
            .unwrap(),
            wall_clock_drift_ms: register_guarded_int_gauge_vec_with_registry!(
                "stream_now_wall_clock_drift_ms",
                "Latest processed barrier epoch minus the minimum latest streaming NOW() timestamp \
                 among live executors with an emitted watermark in this fragment on this compute node, \
                 in milliseconds. This is relative to processed barriers, not scrape-time wall clock.",
                &["fragment_id"],
                registry,
            )
            .unwrap(),
            state: Default::default(),
        }
    }

    pub fn for_executor(&self, fragment_id: FragmentId) -> NowExecutorMetrics {
        let mut state = self.state.lock();
        let executor_id = state.next_executor_id;
        state.next_executor_id += 1;
        NowExecutorMetrics {
            metrics: self.clone(),
            fragment_id,
            executor_id,
        }
    }
}

pub struct NowExecutorMetrics {
    metrics: NowMetrics,
    fragment_id: FragmentId,
    executor_id: u64,
}

impl NowExecutorMetrics {
    pub fn update(&self, streaming_clock_ms: i64, barrier_ms: i64) {
        let mut state = self.metrics.state.lock();
        let fragment = state.fragments.entry(self.fragment_id).or_insert_with(|| {
            let label = self.fragment_id.to_string();
            FragmentMetrics {
                samples: HashMap::new(),
                streaming_clock_ms: self
                    .metrics
                    .streaming_clock_ms
                    .with_guarded_label_values(&[&label]),
                wall_clock_drift_ms: self
                    .metrics
                    .wall_clock_drift_ms
                    .with_guarded_label_values(&[&label]),
            }
        });
        fragment
            .samples
            .insert(self.executor_id, (streaming_clock_ms, barrier_ms));
        fragment.refresh();
    }
}

impl Drop for NowExecutorMetrics {
    fn drop(&mut self) {
        let mut state = self.metrics.state.lock();
        if let Some(fragment) = state.fragments.get_mut(&self.fragment_id) {
            fragment.samples.remove(&self.executor_id);
            if fragment.samples.is_empty() {
                state.fragments.remove(&self.fragment_id);
            } else {
                fragment.refresh();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(registry: &Registry, name: &str) -> HashMap<String, i64> {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .map(|family| {
                family
                    .get_metric()
                    .iter()
                    .map(|metric| {
                        let labels = metric.get_label();
                        assert_eq!(labels.len(), 1);
                        assert_eq!(labels[0].name(), "fragment_id");
                        (
                            labels[0].value().to_owned(),
                            metric.get_gauge().as_ref().unwrap().value() as i64,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn test_fragment_minimum_advances_and_fragments_are_independent() {
        let registry = Registry::new();
        let metrics = NowMetrics::new(&registry);
        let slow = metrics.for_executor(1.into());
        let fast = metrics.clone().for_executor(1.into());
        let other = metrics.for_executor(2.into());
        assert!(registry.gather().is_empty());

        slow.update(100, 150);
        fast.update(200, 250);
        other.update(400, 450);
        assert_eq!(
            values(&registry, "stream_now_streaming_clock_ms"),
            HashMap::from([("1".into(), 100), ("2".into(), 400)])
        );
        assert_eq!(
            values(&registry, "stream_now_wall_clock_drift_ms"),
            HashMap::from([("1".into(), 150), ("2".into(), 50)])
        );

        // The minimum is over current samples, not a historical low-water mark.
        slow.update(300, 350);
        assert_eq!(values(&registry, "stream_now_streaming_clock_ms")["1"], 200);
        assert_eq!(
            values(&registry, "stream_now_wall_clock_drift_ms")["1"],
            150
        );
        fast.update(350, 350);
        slow.update(350, 350);
        assert_eq!(values(&registry, "stream_now_streaming_clock_ms")["1"], 350);
        assert_eq!(values(&registry, "stream_now_wall_clock_drift_ms")["1"], 0);
    }

    #[test]
    fn test_executor_drop_and_fragment_recreation() {
        let registry = Registry::new();
        let metrics = NowMetrics::new(&registry);
        let slow = metrics.for_executor(1.into());
        let fast = metrics.for_executor(1.into());
        slow.update(100, 200);
        fast.update(250, 300);
        drop(slow);
        assert_eq!(values(&registry, "stream_now_streaming_clock_ms")["1"], 250);
        assert_eq!(values(&registry, "stream_now_wall_clock_drift_ms")["1"], 50);

        drop(fast);
        // Guarded metrics expose the final sample for one scrape before removing it.
        registry.gather();
        assert!(registry.gather().is_empty());
        assert!(metrics.state.lock().fragments.is_empty());

        let recovered = metrics.for_executor(1.into());
        recovered.update(400, 500);
        // Dropping an executor that has never emitted must not remove another's sample.
        drop(metrics.for_executor(1.into()));
        assert_eq!(values(&registry, "stream_now_streaming_clock_ms")["1"], 400);
        assert_eq!(
            values(&registry, "stream_now_wall_clock_drift_ms")["1"],
            100
        );
    }
}
