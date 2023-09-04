// Copyright 2023 RisingWave Labs
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

use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{Histogram, HistogramVec};
use risingwave_common::config::StorageMetricLevel;

/// For all `Relabeled*Vec` below,
/// - when `metric_level` <= `relabel_threshold`, they behaves exactly the same as their inner
///   metric.
/// - when `metric_level` > `relabel_threshold`, all their input label values are rewrite to "" when
/// calling `with_label_values`. That's means the metric vec is aggregated into a single metric.

/// These wrapper classes add a `metric_level` field to corresponding metric.
/// We could have use one single struct to represent all `MetricVec<T: MetricVecBuilder>`, rather
/// than specializing them one by one. However, that's undoable because prometheus crate doesn't
/// export `MetricVecBuilder` implementation like `HistogramVecBuilder`.

#[derive(Clone, Debug)]
pub struct RelabeledHistogramVec {
    relabel_threshold: StorageMetricLevel,
    metric_level: StorageMetricLevel,
    metric: HistogramVec,
}

impl RelabeledHistogramVec {
    pub fn with_metric_level(
        metric_level: StorageMetricLevel,
        metric: HistogramVec,
        relabel_threshold: StorageMetricLevel,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
        }
    }

    pub fn with_label_values(&self, vals: &[&str]) -> Histogram {
        if self.metric_level > self.relabel_threshold {
            return self.metric.with_label_values(&vec![""; vals.len()]);
        }
        self.metric.with_label_values(vals)
    }
}

#[derive(Clone, Debug)]
pub struct RelabeledCounterVec {
    relabel_threshold: StorageMetricLevel,
    metric_level: StorageMetricLevel,
    metric: GenericCounterVec<AtomicU64>,
}

impl RelabeledCounterVec {
    pub fn with_metric_level(
        metric_level: StorageMetricLevel,
        metric: GenericCounterVec<AtomicU64>,
        relabel_threshold: StorageMetricLevel,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
        }
    }

    pub fn with_label_values(&self, vals: &[&str]) -> GenericCounter<AtomicU64> {
        if self.metric_level > self.relabel_threshold {
            return self.metric.with_label_values(&vec![""; vals.len()]);
        }
        self.metric.with_label_values(vals)
    }
}
