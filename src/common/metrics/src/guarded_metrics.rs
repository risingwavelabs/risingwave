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

use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::{
    Atomic, AtomicF64, AtomicI64, AtomicU64, Collector, Desc, GenericCounter, GenericLocalCounter,
    MetricVec, MetricVecBuilder,
};
use prometheus::local::{LocalHistogram, LocalIntCounter};
use prometheus::proto::MetricFamily;
use prometheus::Histogram;
use thiserror_ext::AsReport;
use tracing::warn;

#[macro_export]
macro_rules! register_guarded_histogram_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        $crate::register_guarded_histogram_vec_with_registry! {
            {prometheus::histogram_opts!($NAME, $HELP)},
            $LABELS_NAMES,
            $REGISTRY
        }
    }};
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $BUCKETS:expr, $REGISTRY:expr $(,)?) => {{
        $crate::register_guarded_histogram_vec_with_registry! {
            {prometheus::histogram_opts!($NAME, $HELP, $BUCKETS)},
            $LABELS_NAMES,
            $REGISTRY
        }
    }};
    ($HOPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::HistogramVec::new($HOPTS, $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_histogram_builder(inner);
            let label_guarded = $crate::LabelGuardedHistogramVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_gauge_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::GaugeVec::new(prometheus::opts!($NAME, $HELP), $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_gauge_builder(inner);
            let label_guarded = $crate::LabelGuardedGaugeVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_int_gauge_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::IntGaugeVec::new(prometheus::opts!($NAME, $HELP), $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_gauge_builder(inner);
            let label_guarded = $crate::LabelGuardedIntGaugeVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_int_counter_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::IntCounterVec::new(prometheus::opts!($NAME, $HELP), $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_counter_builder(inner);
            let label_guarded = $crate::LabelGuardedIntCounterVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

// put TAITs in a separate module to avoid "non-defining opaque type use in defining scope"
mod tait {
    use prometheus::core::{
        Atomic, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec, MetricVec,
        MetricVecBuilder,
    };
    use prometheus::{Histogram, HistogramVec};

    pub type VecBuilderOfCounter<P: Atomic> = impl MetricVecBuilder<M = GenericCounter<P>>;
    pub type VecBuilderOfGauge<P: Atomic> = impl MetricVecBuilder<M = GenericGauge<P>>;
    pub type VecBuilderOfHistogram = impl MetricVecBuilder<M = Histogram>;

    pub fn __extract_counter_builder<P: Atomic>(
        vec: GenericCounterVec<P>,
    ) -> MetricVec<VecBuilderOfCounter<P>> {
        vec
    }

    pub fn __extract_gauge_builder<P: Atomic>(
        vec: GenericGaugeVec<P>,
    ) -> MetricVec<VecBuilderOfGauge<P>> {
        vec
    }

    pub fn __extract_histogram_builder(vec: HistogramVec) -> MetricVec<VecBuilderOfHistogram> {
        vec
    }
}
pub use tait::*;

use crate::guarded_metrics::lazy_guarded_metric::{lazy_guarded_metrics, LazyGuardedMetrics};

pub type LabelGuardedHistogramVec<const N: usize> = LabelGuardedMetricVec<VecBuilderOfHistogram, N>;
pub type LabelGuardedIntCounterVec<const N: usize> =
    LabelGuardedMetricVec<VecBuilderOfCounter<AtomicU64>, N>;
pub type LabelGuardedIntGaugeVec<const N: usize> =
    LabelGuardedMetricVec<VecBuilderOfGauge<AtomicI64>, N>;
pub type LabelGuardedGaugeVec<const N: usize> =
    LabelGuardedMetricVec<VecBuilderOfGauge<AtomicF64>, N>;

pub type LabelGuardedHistogram<const N: usize> =
    LabelGuardedMetric<LazyGuardedMetrics<VecBuilderOfHistogram, N>, N>;
pub type LabelGuardedIntCounter<const N: usize> =
    LabelGuardedMetric<LazyGuardedMetrics<VecBuilderOfCounter<AtomicU64>, N>, N>;
pub type LabelGuardedIntGauge<const N: usize> =
    LabelGuardedMetric<LazyGuardedMetrics<VecBuilderOfGauge<AtomicI64>, N>, N>;
pub type LabelGuardedGauge<const N: usize> =
    LabelGuardedMetric<LazyGuardedMetrics<VecBuilderOfGauge<AtomicF64>, N>, N>;

pub type LabelGuardedLocalHistogram<const N: usize> = LabelGuardedMetric<LocalHistogram, N>;
pub type LabelGuardedLocalIntCounter<const N: usize> = LabelGuardedMetric<LocalIntCounter, N>;

fn gen_test_label<const N: usize>() -> [&'static str; N] {
    const TEST_LABELS: [&str; 5] = ["test1", "test2", "test3", "test4", "test5"];
    (0..N)
        .map(|i| TEST_LABELS[i])
        .collect_vec()
        .try_into()
        .unwrap()
}

#[derive(Default)]
struct LabelGuardedMetricsInfo<const N: usize> {
    labeled_metrics_count: HashMap<[String; N], usize>,
    uncollected_removed_labels: HashSet<[String; N]>,
}

impl<const N: usize> LabelGuardedMetricsInfo<N> {
    fn register_new_label(mutex: &Arc<Mutex<Self>>, labels: &[&str; N]) -> LabelGuard<N> {
        let mut guard = mutex.lock();
        let label_string = labels.map(|str| str.to_string());
        guard.uncollected_removed_labels.remove(&label_string);
        *guard
            .labeled_metrics_count
            .entry(label_string.clone())
            .or_insert(0) += 1;
        LabelGuard {
            labels: label_string,
            info: mutex.clone(),
        }
    }
}

/// `LabelGuardedMetricVec` enhances the [`MetricVec`] to ensure the set of labels to be
/// correctly removed from the Prometheus client once being dropped. This is useful for metrics
/// that are associated with an object that can be dropped, such as streaming jobs, fragments,
/// actors, batch tasks, etc.
///
/// When a set labels is dropped, it will record it in the `uncollected_removed_labels` set.
/// Once the metrics has been collected, it will finally remove the metrics of the labels.
///
/// See also [`LabelGuardedMetricsInfo`] and [`LabelGuard::drop`].
///
/// # Arguments
///
/// * `T` - The type of the raw metrics vec.
/// * `N` - The number of labels.
#[derive(Clone)]
pub struct LabelGuardedMetricVec<T: MetricVecBuilder, const N: usize> {
    inner: MetricVec<T>,
    info: Arc<Mutex<LabelGuardedMetricsInfo<N>>>,
    labels: [&'static str; N],
}

impl<T: MetricVecBuilder, const N: usize> Debug for LabelGuardedMetricVec<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("LabelGuardedMetricVec<{}, {}>", type_name::<T>(), N).as_str())
            .field("label", &self.labels)
            .finish()
    }
}

impl<T: MetricVecBuilder, const N: usize> Collector for LabelGuardedMetricVec<T, N> {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut guard = self.info.lock();
        let ret = self.inner.collect();
        for labels in guard.uncollected_removed_labels.drain() {
            if let Err(e) = self
                .inner
                .remove_label_values(&labels.each_ref().map(|s| s.as_str()))
            {
                warn!(
                    error = %e.as_report(),
                    "err when delete metrics of {:?} of labels {:?}",
                    self.inner.desc().first().expect("should have desc").fq_name,
                    self.labels,
                );
            }
        }
        ret
    }
}

pub(crate) mod lazy_guarded_metric {
    use std::marker::PhantomData;
    use std::ops::Deref;
    use std::sync::{Arc, LazyLock};

    use prometheus::core::{MetricVec, MetricVecBuilder};

    use crate::guarded_metrics::LabelGuard;

    type GuardedMetricsLazyLock<T: MetricVecBuilder, const N: usize> =
        Arc<LazyLock<T::M, impl FnOnce() -> T::M>>;

    #[derive(Clone)]
    pub struct LazyGuardedMetrics<T: MetricVecBuilder, const N: usize> {
        inner: GuardedMetricsLazyLock<T, N>,
        _phantom: PhantomData<T>,
    }

    pub(super) fn lazy_guarded_metrics<T: MetricVecBuilder, const N: usize>(
        metric_vec: MetricVec<T>,
        guard: Arc<LabelGuard<N>>,
    ) -> LazyGuardedMetrics<T, N> {
        LazyGuardedMetrics {
            inner: Arc::new(LazyLock::new(move || {
                metric_vec.with_label_values(&guard.labels.each_ref().map(|s| s.as_str()))
            })),
            _phantom: PhantomData,
        }
    }

    impl<T: MetricVecBuilder, const N: usize> Deref for LazyGuardedMetrics<T, N> {
        type Target = T::M;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }
}

impl<T: MetricVecBuilder, const N: usize> LabelGuardedMetricVec<T, N> {
    pub fn new(inner: MetricVec<T>, labels: &[&'static str; N]) -> Self {
        Self {
            inner,
            info: Default::default(),
            labels: *labels,
        }
    }

    /// This is similar to the `with_label_values` of the raw metrics vec.
    /// We need to pay special attention that, unless for some special purpose,
    /// we should not drop the returned `LabelGuardedMetric` immediately after
    /// using it, such as `metrics.with_guarded_label_values(...).inc();`,
    /// because after dropped the label will be regarded as not used any more,
    /// and the internal raw metrics will be removed and reset.
    ///
    /// Instead, we should store the returned `LabelGuardedMetric` in a scope with longer
    /// lifetime so that the labels can be regarded as being used in its whole life scope.
    /// This is also the recommended way to use the raw metrics vec.
    pub fn with_guarded_label_values(
        &self,
        labels: &[&str; N],
    ) -> LabelGuardedMetric<LazyGuardedMetrics<T, N>, N> {
        let guard = Arc::new(LabelGuardedMetricsInfo::register_new_label(
            &self.info, labels,
        ));
        LabelGuardedMetric {
            inner: lazy_guarded_metrics(self.inner.clone(), guard.clone()),
            _guard: guard,
        }
    }

    pub fn with_test_label(&self) -> LabelGuardedMetric<LazyGuardedMetrics<T, N>, N> {
        let labels: [&'static str; N] = gen_test_label::<N>();
        self.with_guarded_label_values(&labels)
    }
}

impl<const N: usize> LabelGuardedIntCounterVec<N> {
    pub fn test_int_counter_vec() -> Self {
        let registry = prometheus::Registry::new();
        register_guarded_int_counter_vec_with_registry!(
            "test",
            "test",
            &gen_test_label::<N>(),
            &registry
        )
        .unwrap()
    }
}

impl<const N: usize> LabelGuardedIntGaugeVec<N> {
    pub fn test_int_gauge_vec() -> Self {
        let registry = prometheus::Registry::new();
        register_guarded_int_gauge_vec_with_registry!(
            "test",
            "test",
            &gen_test_label::<N>(),
            &registry
        )
        .unwrap()
    }
}

impl<const N: usize> LabelGuardedGaugeVec<N> {
    pub fn test_gauge_vec() -> Self {
        let registry = prometheus::Registry::new();
        register_guarded_gauge_vec_with_registry!("test", "test", &gen_test_label::<N>(), &registry)
            .unwrap()
    }
}

impl<const N: usize> LabelGuardedHistogramVec<N> {
    pub fn test_histogram_vec() -> Self {
        let registry = prometheus::Registry::new();
        register_guarded_histogram_vec_with_registry!(
            "test",
            "test",
            &gen_test_label::<N>(),
            &registry
        )
        .unwrap()
    }
}

struct LabelGuard<const N: usize> {
    labels: [String; N],
    info: Arc<Mutex<LabelGuardedMetricsInfo<N>>>,
}

impl<const N: usize> Drop for LabelGuard<N> {
    fn drop(&mut self) {
        let mut guard = self.info.lock();
        let count = guard.labeled_metrics_count.get_mut(&self.labels).expect(
            "should exist because the current existing dropping one means the count is not zero",
        );
        *count -= 1;
        if *count == 0 {
            guard
                .labeled_metrics_count
                .remove(&self.labels)
                .expect("should exist");
            guard.uncollected_removed_labels.insert(self.labels.clone());
        }
    }
}

#[derive(Clone)]
pub struct LabelGuardedMetric<T, const N: usize> {
    inner: T,
    _guard: Arc<LabelGuard<N>>,
}

impl<T, const N: usize> Debug for LabelGuardedMetric<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabelGuardedMetric").finish()
    }
}

impl<T, const N: usize> Deref for LabelGuardedMetric<T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<const N: usize> LabelGuardedHistogram<N> {
    pub fn test_histogram() -> Self {
        LabelGuardedHistogramVec::<N>::test_histogram_vec().with_test_label()
    }
}

impl<const N: usize> LabelGuardedIntCounter<N> {
    pub fn test_int_counter() -> Self {
        LabelGuardedIntCounterVec::<N>::test_int_counter_vec().with_test_label()
    }
}

impl<const N: usize> LabelGuardedIntGauge<N> {
    pub fn test_int_gauge() -> Self {
        LabelGuardedIntGaugeVec::<N>::test_int_gauge_vec().with_test_label()
    }
}

impl<const N: usize> LabelGuardedGauge<N> {
    pub fn test_gauge() -> Self {
        LabelGuardedGaugeVec::<N>::test_gauge_vec().with_test_label()
    }
}

pub trait MetricWithLocal {
    type Local;
    fn local(&self) -> Self::Local;
}

impl MetricWithLocal for Histogram {
    type Local = LocalHistogram;

    fn local(&self) -> Self::Local {
        self.local()
    }
}

impl<P: Atomic> MetricWithLocal for GenericCounter<P> {
    type Local = GenericLocalCounter<P>;

    fn local(&self) -> Self::Local {
        self.local()
    }
}

impl<T, const N: usize> LabelGuardedMetric<LazyGuardedMetrics<T, N>, N>
where
    T: MetricVecBuilder,
    T::M: MetricWithLocal,
{
    pub fn local(&self) -> LabelGuardedMetric<<T::M as MetricWithLocal>::Local, N> {
        LabelGuardedMetric {
            inner: self.inner.local(),
            _guard: self._guard.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;

    use crate::LabelGuardedIntCounterVec;

    #[test]
    fn test_label_guarded_metrics_drop() {
        let vec = LabelGuardedIntCounterVec::<3>::test_int_counter_vec();
        let m1_1 = vec.with_guarded_label_values(&["1", "2", "3"]);
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        let m1_2 = vec.with_guarded_label_values(&["1", "2", "3"]);
        let m1_3 = m1_2.clone();
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        let m2 = vec.with_guarded_label_values(&["2", "2", "3"]);
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        drop(m1_3);
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        drop(m2);
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        drop(m1_1);
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        drop(m1_2);
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(0, vec.collect().pop().unwrap().get_metric().len());
    }
}
