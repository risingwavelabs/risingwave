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

use crate::config::MetricLevel;

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
    relabel_threshold: MetricLevel,
    metric_level: MetricLevel,
    metric: HistogramVec,

    /// The first `relabel_num` labels will be relabeled to empty string
    ///
    /// For example, if `relabel_num` is 1, and the input labels are `["actor_id",
    /// "fragment_id", "table_id"]`, when threshold is reached, the label values will be
    /// `["", "<original_fragment_id>", "<original_table_id>"]`.
    relabel_num: usize,
}

impl RelabeledHistogramVec {
    pub fn with_metric_level(
        metric_level: MetricLevel,
        metric: HistogramVec,
        relabel_threshold: MetricLevel,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
            relabel_num: usize::MAX,
        }
    }

    pub fn with_metric_level_relabel_n(
        metric_level: MetricLevel,
        metric: HistogramVec,
        relabel_threshold: MetricLevel,
        relabel_num: usize,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
            relabel_num,
        }
    }

    pub fn with_label_values(&self, vals: &[&str]) -> Histogram {
        if self.metric_level > self.relabel_threshold {
            // relabel first n labels to empty string
            let mut relabeled_vals = vals.to_vec();
            for label in relabeled_vals.iter_mut().take(self.relabel_num) {
                *label = "";
            }
            return self.metric.with_label_values(&relabeled_vals);
        }
        self.metric.with_label_values(vals)
    }
}

#[derive(Clone, Debug)]
pub struct RelabeledCounterVec {
    relabel_threshold: MetricLevel,
    metric_level: MetricLevel,
    metric: GenericCounterVec<AtomicU64>,
    relabel_num: usize,
}

impl RelabeledCounterVec {
    pub fn with_metric_level(
        metric_level: MetricLevel,
        metric: GenericCounterVec<AtomicU64>,
        relabel_threshold: MetricLevel,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
            relabel_num: usize::MAX,
        }
    }

    pub fn with_metric_level_relabel_n(
        metric_level: MetricLevel,
        metric: GenericCounterVec<AtomicU64>,
        relabel_threshold: MetricLevel,
        relabel_num: usize,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
            relabel_num,
        }
    }

    pub fn with_label_values(&self, vals: &[&str]) -> GenericCounter<AtomicU64> {
        if self.metric_level > self.relabel_threshold {
            // relabel first n labels to empty string
            let mut relabeled_vals = vals.to_vec();
            for label in relabeled_vals.iter_mut().take(self.relabel_num) {
                *label = "";
            }
            return self.metric.with_label_values(&relabeled_vals);
        }
        self.metric.with_label_values(vals)
    }
}
