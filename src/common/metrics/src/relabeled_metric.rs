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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use prometheus::core::{Collector, MetricVec, MetricVecBuilder};
use prometheus::{HistogramVec, IntCounterVec};

use crate::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
    LabelGuardedMetric, LabelGuardedMetricVec, LazyLabelGuardedMetrics, MetricLevel,
};

/// For all `Relabeled*Vec` below,
/// - when `metric_level` <= `relabel_threshold`, they behave exactly the same as their inner
///   metric.
/// - when `metric_level` > `relabel_threshold`, the first `relabel_num` labels are rewrite to "" when
///   calling `with_label_values`. That's means the metric vec is aggregated into a single metric.
///
/// These wrapper classes add a `metric_level` field to corresponding metric.
/// We could have use one single struct to represent all `MetricVec<T: MetricVecBuilder>`, rather
/// than specializing them one by one. However, that's undoable because prometheus crate doesn't
/// export `MetricVecBuilder` implementation like `HistogramVecBuilder`.
///
/// ## Note
///
/// CAUTION! Relabelling might cause expected result!
///
/// For counters (including histogram because it uses counters internally), it's usually natural
/// to sum up the count from multiple labels.
///
/// For the rest (such as Gauge), the semantics becomes "any/last of the recorded value". Please be
/// cautious.
#[derive(Clone, Debug)]
pub struct RelabeledMetricVec<M> {
    relabel_threshold: MetricLevel,
    metric_level: MetricLevel,
    metric: M,

    /// The first `relabel_num` labels will be relabeled to empty string
    ///
    /// For example, if `relabel_num` is 1, and the input labels are `["actor_id",
    /// "fragment_id", "table_id"]`, when threshold is reached, the label values will be
    /// `["", "<original_fragment_id>", "<original_table_id>"]`.
    relabel_num: usize,
}

impl<M> RelabeledMetricVec<M> {
    pub fn with_metric_level(
        metric_level: MetricLevel,
        metric: M,
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
        metric: M,
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

    fn relabel_impl<V: AsRef<str> + std::fmt::Debug>(&self, vals: &[V]) -> Option<Vec<String>> {
        if self.metric_level > self.relabel_threshold {
            // relabel first n labels to empty string
            let mut relabeled_vals = vals
                .iter()
                .map(|v| v.as_ref().to_owned())
                .collect::<Vec<_>>();
            for label in relabeled_vals.iter_mut().take(self.relabel_num) {
                *label = String::new();
            }
            Some(relabeled_vals)
        } else {
            None
        }
    }
}

#[easy_ext::ext(MetricVecRelabelExt)]
impl<M> M
where
    M: Sized,
{
    /// Equivalent to [`RelabeledMetricVec::with_metric_level`].
    pub fn relabel(
        self,
        metric_level: MetricLevel,
        relabel_threshold: MetricLevel,
    ) -> RelabeledMetricVec<M> {
        RelabeledMetricVec::with_metric_level(metric_level, self, relabel_threshold)
    }

    /// Equivalent to [`RelabeledMetricVec::with_metric_level_relabel_n`].
    pub fn relabel_n(
        self,
        metric_level: MetricLevel,
        relabel_threshold: MetricLevel,
        relabel_num: usize,
    ) -> RelabeledMetricVec<M> {
        RelabeledMetricVec::with_metric_level_relabel_n(
            metric_level,
            self,
            relabel_threshold,
            relabel_num,
        )
    }

    /// Equivalent to [`RelabeledMetricVec::with_metric_level_relabel_n`] with `metric_level` set to
    /// `MetricLevel::Debug` and `relabel_num` set to 1.
    pub fn relabel_debug_1(self, relabel_threshold: MetricLevel) -> RelabeledMetricVec<M> {
        RelabeledMetricVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            self,
            relabel_threshold,
            1,
        )
    }
}

impl<T: MetricVecBuilder> RelabeledMetricVec<MetricVec<T>> {
    pub fn with_label_values<V: AsRef<str> + std::fmt::Debug>(&self, vals: &[V]) -> T::M {
        if let Some(relabeled_vals) = self.relabel_impl(vals) {
            return self.metric.with_label_values(&relabeled_vals);
        }
        self.metric.with_label_values(vals)
    }

    pub fn reset(&self) {
        self.metric.reset();
    }
}

impl<T: MetricVecBuilder> RelabeledMetricVec<LabelGuardedMetricVec<T>> {
    pub fn with_guarded_label_values<V: AsRef<str> + std::fmt::Debug>(
        &self,
        vals: &[V],
    ) -> LabelGuardedMetric<T::M> {
        if let Some(relabeled_vals) = self.relabel_impl(vals) {
            return self.metric.with_guarded_label_values(&relabeled_vals);
        }
        self.metric.with_guarded_label_values(vals)
    }

    pub fn lazy_guarded_metrics(&self, labels: Vec<String>) -> LazyLabelGuardedMetrics<T> {
        if let Some(relabeled_vals) = self.relabel_impl(labels.as_slice()) {
            return self.metric.clone().lazy_guarded_metrics(relabeled_vals);
        }
        self.metric.clone().lazy_guarded_metrics(labels)
    }
}

impl<T: Collector> Collector for RelabeledMetricVec<T> {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.metric.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.metric.collect()
    }
}

pub type RelabeledCounterVec = RelabeledMetricVec<IntCounterVec>;
pub type RelabeledHistogramVec = RelabeledMetricVec<HistogramVec>;

pub type RelabeledGuardedHistogramVec = RelabeledMetricVec<LabelGuardedHistogramVec>;
pub type RelabeledGuardedIntCounterVec = RelabeledMetricVec<LabelGuardedIntCounterVec>;

/// CAUTION! Relabelling a Gauge might cause expected result!
///
/// See [`RelabeledMetricVec`] for details.
pub type RelabeledGuardedIntGaugeVec = RelabeledMetricVec<LabelGuardedIntGaugeVec>;

/// Defines how live gauge values are combined after their labels are relabeled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GaugeAggregation {
    Sum,
    Min,
}

/// Extension methods for relabeling guarded integer gauges with explicit aggregation semantics.
pub trait IntGaugeVecRelabelExt {
    /// Relabels the first label according to the configured metric level and aggregates live
    /// contributions whenever that effective label value is empty.
    fn relabel_debug_1_with_aggregation(
        self,
        metric_level: MetricLevel,
        aggregation: GaugeAggregation,
    ) -> RelabeledAggregatedIntGaugeVec;
}

impl IntGaugeVecRelabelExt for LabelGuardedIntGaugeVec {
    fn relabel_debug_1_with_aggregation(
        self,
        metric_level: MetricLevel,
        aggregation: GaugeAggregation,
    ) -> RelabeledAggregatedIntGaugeVec {
        RelabeledAggregatedIntGaugeVec {
            metric: RelabeledMetricVec::with_metric_level_relabel_n(
                MetricLevel::Debug,
                self,
                metric_level,
                1,
            ),
            aggregation,
            groups: Arc::new(AggregatedGaugeGroups::default()),
        }
    }
}

/// A guarded integer gauge vec that preserves gauge semantics after actor labels are relabeled.
#[derive(Clone, Debug)]
pub struct RelabeledAggregatedIntGaugeVec {
    metric: RelabeledGuardedIntGaugeVec,
    aggregation: GaugeAggregation,
    groups: Arc<AggregatedGaugeGroups>,
}

impl RelabeledAggregatedIntGaugeVec {
    pub fn with_guarded_label_values<V: AsRef<str> + std::fmt::Debug>(
        &self,
        vals: &[V],
    ) -> RelabeledAggregatedIntGauge {
        let relabeled_vals = self.metric.relabel_impl(vals);
        let first_label_is_empty = match &relabeled_vals {
            Some(labels) => labels.first().is_some_and(String::is_empty),
            None => vals.first().is_some_and(|label| label.as_ref().is_empty()),
        };

        if !first_label_is_empty {
            let gauge = match relabeled_vals {
                Some(labels) => self.metric.metric.with_guarded_label_values(&labels),
                None => self.metric.metric.with_guarded_label_values(vals),
            };
            return RelabeledAggregatedIntGauge {
                inner: RelabeledAggregatedIntGaugeInner::Passthrough(gauge),
            };
        }

        let labels = relabeled_vals
            .unwrap_or_else(|| vals.iter().map(|label| label.as_ref().to_owned()).collect())
            .into_boxed_slice();
        let group = self
            .groups
            .get_or_create(labels, self.aggregation, |labels| {
                self.metric.metric.with_guarded_label_values(labels)
            });
        let id = self.groups.next_id.fetch_add(1, Ordering::Relaxed);
        RelabeledAggregatedIntGauge {
            inner: RelabeledAggregatedIntGaugeInner::Aggregated(Arc::new(
                AggregatedGaugeContribution { id, group },
            )),
        }
    }
}

impl Collector for RelabeledAggregatedIntGaugeVec {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.metric.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.metric.collect()
    }
}

#[derive(Debug, Default)]
struct AggregatedGaugeGroups {
    inner: Mutex<HashMap<Box<[String]>, Weak<AggregatedGaugeGroup>>>,
    next_id: AtomicU64,
}

impl AggregatedGaugeGroups {
    fn get_or_create(
        self: &Arc<Self>,
        labels: Box<[String]>,
        aggregation: GaugeAggregation,
        create_gauge: impl FnOnce(&[String]) -> LabelGuardedMetric<prometheus::IntGauge>,
    ) -> Arc<AggregatedGaugeGroup> {
        let mut groups = self.inner.lock();
        if let Some(group) = groups.get(&labels).and_then(Weak::upgrade) {
            return group;
        }

        let group = Arc::new(AggregatedGaugeGroup {
            gauge: create_gauge(&labels),
            aggregation,
            state: Mutex::new(AggregatedGaugeState::default()),
            labels: labels.clone(),
            groups: Arc::downgrade(self),
        });
        groups.insert(labels, Arc::downgrade(&group));
        group
    }
}

#[derive(Debug, Default)]
struct AggregatedGaugeState {
    values: HashMap<u64, i64>,
    aggregate: Option<i64>,
}

#[derive(Debug)]
struct AggregatedGaugeGroup {
    gauge: LabelGuardedMetric<prometheus::IntGauge>,
    aggregation: GaugeAggregation,
    state: Mutex<AggregatedGaugeState>,
    labels: Box<[String]>,
    groups: Weak<AggregatedGaugeGroups>,
}

impl AggregatedGaugeGroup {
    fn set(&self, id: u64, value: i64) {
        let mut state = self.state.lock();
        self.set_locked(&mut state, id, value);
    }

    fn set_locked(&self, state: &mut AggregatedGaugeState, id: u64, value: i64) {
        let previous = state.values.insert(id, value);
        let aggregate = match self.aggregation {
            GaugeAggregation::Sum => state
                .aggregate
                .unwrap_or(0)
                .wrapping_sub(previous.unwrap_or(0))
                .wrapping_add(value),
            GaugeAggregation::Min => {
                if previous == state.aggregate && previous.is_some_and(|old| value > old) {
                    *state
                        .values
                        .values()
                        .min()
                        .expect("the updated value exists")
                } else {
                    state.aggregate.map_or(value, |current| current.min(value))
                }
            }
        };
        state.aggregate = Some(aggregate);
        self.gauge.set(aggregate);
    }

    fn add(&self, id: u64, delta: i64) {
        let mut state = self.state.lock();
        let current = state.values.get(&id).copied().unwrap_or(0);
        self.set_locked(&mut state, id, current.wrapping_add(delta));
    }

    fn get(&self) -> i64 {
        self.state.lock().aggregate.unwrap_or(0)
    }

    fn remove(&self, id: u64) {
        let mut state = self.state.lock();
        let Some(previous) = state.values.remove(&id) else {
            return;
        };
        let aggregate = match self.aggregation {
            GaugeAggregation::Sum => state.aggregate.unwrap_or(0).wrapping_sub(previous),
            GaugeAggregation::Min => state.values.values().copied().min().unwrap_or(0),
        };
        state.aggregate = (!state.values.is_empty()).then_some(aggregate);
        self.gauge.set(aggregate);
    }
}

impl Drop for AggregatedGaugeGroup {
    fn drop(&mut self) {
        let Some(groups) = self.groups.upgrade() else {
            return;
        };
        let mut entries = groups.inner.lock();
        if entries
            .get(&self.labels)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
        {
            entries.remove(&self.labels);
        }
    }
}

#[derive(Clone, Debug)]
pub struct RelabeledAggregatedIntGauge {
    inner: RelabeledAggregatedIntGaugeInner,
}

#[derive(Clone, Debug)]
enum RelabeledAggregatedIntGaugeInner {
    Passthrough(LabelGuardedMetric<prometheus::IntGauge>),
    Aggregated(Arc<AggregatedGaugeContribution>),
}

#[derive(Debug)]
struct AggregatedGaugeContribution {
    id: u64,
    group: Arc<AggregatedGaugeGroup>,
}

impl Drop for AggregatedGaugeContribution {
    fn drop(&mut self) {
        self.group.remove(self.id);
    }
}

impl RelabeledAggregatedIntGauge {
    /// Creates a single-contribution gauge for tests and placeholder metrics.
    pub fn test_int_gauge<const N: usize>(_aggregation: GaugeAggregation) -> Self {
        Self {
            inner: RelabeledAggregatedIntGaugeInner::Passthrough(
                crate::LabelGuardedIntGauge::test_int_gauge::<N>(),
            ),
        }
    }

    pub fn set(&self, value: i64) {
        match &self.inner {
            RelabeledAggregatedIntGaugeInner::Passthrough(gauge) => gauge.set(value),
            RelabeledAggregatedIntGaugeInner::Aggregated(contribution) => {
                contribution.group.set(contribution.id, value);
            }
        }
    }

    /// Updates this contribution when a value exists, or excludes it from aggregation otherwise.
    /// A passthrough gauge keeps the conventional zero value when no value exists.
    pub fn set_optional(&self, value: Option<i64>) {
        match &self.inner {
            RelabeledAggregatedIntGaugeInner::Passthrough(gauge) => {
                gauge.set(value.unwrap_or_default());
            }
            RelabeledAggregatedIntGaugeInner::Aggregated(contribution) => match value {
                Some(value) => contribution.group.set(contribution.id, value),
                None => contribution.group.remove(contribution.id),
            },
        }
    }

    pub fn add(&self, value: i64) {
        match &self.inner {
            RelabeledAggregatedIntGaugeInner::Passthrough(gauge) => gauge.add(value),
            RelabeledAggregatedIntGaugeInner::Aggregated(contribution) => {
                contribution.group.add(contribution.id, value);
            }
        }
    }

    pub fn sub(&self, value: i64) {
        self.add(value.wrapping_neg());
    }

    pub fn inc(&self) {
        self.add(1);
    }

    pub fn dec(&self) {
        self.sub(1);
    }

    pub fn get(&self) -> i64 {
        match &self.inner {
            RelabeledAggregatedIntGaugeInner::Passthrough(gauge) => gauge.get(),
            RelabeledAggregatedIntGaugeInner::Aggregated(contribution) => contribution.group.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    fn test_vec(
        registry: &Registry,
        level: MetricLevel,
        aggregation: GaugeAggregation,
    ) -> RelabeledAggregatedIntGaugeVec {
        crate::register_guarded_int_gauge_vec_with_registry!(
            "test_relabel_aggregated_gauge",
            "test",
            &["actor_id", "key"],
            registry,
        )
        .unwrap()
        .relabel_debug_1_with_aggregation(level, aggregation)
    }

    fn values(registry: &Registry) -> Vec<i64> {
        let Some(family) = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "test_relabel_aggregated_gauge")
        else {
            return vec![];
        };
        family
            .get_metric()
            .iter()
            .map(|metric| metric.get_gauge().value() as i64)
            .collect()
    }

    #[test]
    fn test_sum_aggregation_update_clone_and_drop() {
        for level in [
            MetricLevel::Disabled,
            MetricLevel::Critical,
            MetricLevel::Info,
        ] {
            let registry = Registry::new();
            let vec = test_vec(&registry, level, GaugeAggregation::Sum);
            let actor_1 = vec.with_guarded_label_values(&["1", "shared"]);
            let actor_2 = vec.with_guarded_label_values(&["2", "shared"]);

            actor_1.set(5);
            actor_2.set(7);
            assert_eq!(values(&registry), [12]);

            actor_1.set(9);
            assert_eq!(values(&registry), [16]);

            let actor_1_clone = actor_1.clone();
            drop(actor_1);
            assert_eq!(values(&registry), [16]);
            drop(actor_1_clone);
            assert_eq!(values(&registry), [7]);

            drop(actor_2);
            assert_eq!(values(&registry), [0]);
            assert!(values(&registry).is_empty());
        }
    }

    #[test]
    fn test_min_aggregation_ignores_uninitialized_handles() {
        for level in [
            MetricLevel::Disabled,
            MetricLevel::Critical,
            MetricLevel::Info,
        ] {
            let registry = Registry::new();
            let vec = test_vec(&registry, level, GaugeAggregation::Min);
            let actor_1 = vec.with_guarded_label_values(&["1", "shared"]);
            let actor_2 = vec.with_guarded_label_values(&["2", "shared"]);

            actor_1.set(100);
            assert_eq!(values(&registry), [100]);
            actor_2.set(200);
            assert_eq!(values(&registry), [100]);
            actor_1.set(300);
            assert_eq!(values(&registry), [200]);
            drop(actor_2);
            assert_eq!(values(&registry), [300]);
        }
    }

    #[test]
    fn test_min_aggregation_ignores_absent_contributions() {
        for level in [MetricLevel::Critical, MetricLevel::Info] {
            let registry = Registry::new();
            let vec = test_vec(&registry, level, GaugeAggregation::Min);
            let empty_actor = vec.with_guarded_label_values(&["1", "shared"]);
            let pending_actor = vec.with_guarded_label_values(&["2", "shared"]);

            empty_actor.set_optional(None);
            pending_actor.set_optional(Some(100));
            assert_eq!(values(&registry), [100]);

            empty_actor.set_optional(Some(200));
            assert_eq!(values(&registry), [100]);

            pending_actor.set_optional(None);
            assert_eq!(values(&registry), [200]);

            empty_actor.set_optional(None);
            assert_eq!(values(&registry), [0]);
        }
    }

    #[test]
    fn test_debug_passthrough_and_distinct_groups() {
        let registry = Registry::new();
        let vec = test_vec(&registry, MetricLevel::Debug, GaugeAggregation::Sum);
        let actor_1 = vec.with_guarded_label_values(&["1", "shared"]);
        let actor_2 = vec.with_guarded_label_values(&["2", "shared"]);
        actor_1.set(5);
        actor_2.set(7);
        let mut collected = values(&registry);
        collected.sort_unstable();
        assert_eq!(collected, [5, 7]);
        let mut actor_ids = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "test_relabel_aggregated_gauge")
            .unwrap()
            .get_metric()
            .iter()
            .map(|metric| crate::get_label::<String>(metric, "actor_id").unwrap())
            .collect::<Vec<_>>();
        actor_ids.sort_unstable();
        assert_eq!(actor_ids, ["1", "2"]);

        let registry = Registry::new();
        let vec = test_vec(&registry, MetricLevel::Info, GaugeAggregation::Sum);
        let first = vec.with_guarded_label_values(&["1", "first"]);
        let second = vec.with_guarded_label_values(&["2", "second"]);
        first.set(5);
        second.set(7);
        let mut collected = values(&registry);
        collected.sort_unstable();
        assert_eq!(collected, [5, 7]);
    }

    #[test]
    fn test_aggregation_follows_the_effective_first_label() {
        let registry = Registry::new();
        let vec = test_vec(&registry, MetricLevel::Debug, GaugeAggregation::Sum);
        let first = vec.with_guarded_label_values(&["", "shared"]);
        let second = vec.with_guarded_label_values(&["", "shared"]);

        first.set(5);
        second.set(7);

        assert_eq!(values(&registry), [12]);
    }

    #[test]
    fn test_concurrent_adds_and_sets_keep_exported_value_consistent() {
        let registry = Registry::new();
        let vec = test_vec(&registry, MetricLevel::Info, GaugeAggregation::Sum);
        let gauge = vec.with_guarded_label_values(&["1", "shared"]);
        let threads = (0..4)
            .map(|thread_index| {
                let gauge = gauge.clone();
                std::thread::spawn(move || {
                    for value in 0..10_000 {
                        if thread_index % 2 == 0 {
                            gauge.add(1);
                        } else {
                            gauge.set(value);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(values(&registry), [gauge.get()]);
    }
}
