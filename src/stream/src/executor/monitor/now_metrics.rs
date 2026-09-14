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

use prometheus::Registry;
use risingwave_common::metrics::{LabelGuardedIntGauge, LabelGuardedIntGaugeVec};
use risingwave_common::register_guarded_int_gauge_vec_with_registry;

use crate::task::FragmentId;

/// Metrics for singleton NOW executors, labeled by fragment.
#[derive(Clone)]
pub struct NowMetrics {
    streaming_clock_ms: LabelGuardedIntGaugeVec,
    wall_clock_drift_ms: LabelGuardedIntGaugeVec,
}

impl NowMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            streaming_clock_ms: register_guarded_int_gauge_vec_with_registry!(
                "stream_now_streaming_clock_ms",
                "Latest streaming NOW() timestamp (milliseconds since Unix epoch) \
                 emitted as a watermark by this fragment on this compute node.",
                &["fragment_id"],
                registry,
            )
            .unwrap(),
            wall_clock_drift_ms: register_guarded_int_gauge_vec_with_registry!(
                "stream_now_wall_clock_drift_ms",
                "Latest processed barrier epoch minus the latest streaming NOW() timestamp \
                 emitted as a watermark by this fragment on this compute node, \
                 in milliseconds. This is relative to processed barriers, not scrape-time wall clock.",
                &["fragment_id"],
                registry,
            )
            .unwrap(),
        }
    }

    pub fn for_executor(&self, fragment_id: FragmentId) -> NowExecutorMetrics {
        let label = fragment_id.to_string();
        NowExecutorMetrics {
            streaming_clock_ms: self.streaming_clock_ms.with_guarded_label_values(&[&label]),
            wall_clock_drift_ms: self
                .wall_clock_drift_ms
                .with_guarded_label_values(&[&label]),
        }
    }
}

pub struct NowExecutorMetrics {
    streaming_clock_ms: LabelGuardedIntGauge,
    wall_clock_drift_ms: LabelGuardedIntGauge,
}

impl NowExecutorMetrics {
    pub fn update(&self, streaming_clock_ms: i64, barrier_ms: i64) {
        self.streaming_clock_ms.set(streaming_clock_ms);
        self.wall_clock_drift_ms
            .set(barrier_ms.saturating_sub(streaming_clock_ms));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
    fn test_latest_sample_and_independent_fragments() {
        let registry = Registry::new();
        let metrics = NowMetrics::new(&registry);
        assert!(registry.gather().is_empty());

        let executor = metrics.for_executor(1.into());
        let other = metrics.clone().for_executor(2.into());
        executor.update(100, 150);
        other.update(400, 450);
        assert_eq!(
            values(&registry, "stream_now_streaming_clock_ms"),
            HashMap::from([("1".into(), 100), ("2".into(), 400)])
        );
        assert_eq!(
            values(&registry, "stream_now_wall_clock_drift_ms"),
            HashMap::from([("1".into(), 50), ("2".into(), 50)])
        );

        executor.update(300, 400);
        assert_eq!(
            values(&registry, "stream_now_streaming_clock_ms"),
            HashMap::from([("1".into(), 300), ("2".into(), 400)])
        );
        assert_eq!(
            values(&registry, "stream_now_wall_clock_drift_ms"),
            HashMap::from([("1".into(), 100), ("2".into(), 50)])
        );

        executor.update(400, 400);
        assert_eq!(values(&registry, "stream_now_wall_clock_drift_ms")["1"], 0);
    }

    #[test]
    fn test_executor_drop_and_fragment_recreation() {
        let registry = Registry::new();
        let metrics = NowMetrics::new(&registry);
        let executor = metrics.for_executor(1.into());
        executor.update(100, 200);
        drop(executor);
        // Guarded metrics expose the final sample for one scrape before removing it.
        registry.gather();
        assert!(registry.gather().is_empty());

        let recovered = metrics.for_executor(1.into());
        recovered.update(400, 500);
        assert_eq!(values(&registry, "stream_now_streaming_clock_ms")["1"], 400);
        assert_eq!(
            values(&registry, "stream_now_wall_clock_drift_ms")["1"],
            100
        );

        // Recovery can recreate the label before the next scrape cleans it up.
        drop(recovered);
        let recovered = metrics.for_executor(1.into());
        recovered.update(600, 650);
        assert_eq!(values(&registry, "stream_now_streaming_clock_ms")["1"], 600);
        assert_eq!(values(&registry, "stream_now_wall_clock_drift_ms")["1"], 50);
        drop(recovered);
        registry.gather();
        assert!(registry.gather().is_empty());
    }
}
