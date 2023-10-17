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

use std::any::type_name;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::{
    Atomic, AtomicI64, AtomicU64, Collector, GenericCounter, GenericCounterVec, GenericGauge,
    GenericGaugeVec, MetricVec, MetricVecBuilder,
};
use prometheus::{Histogram, HistogramVec};
use tracing::warn;

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

#[macro_export]
macro_rules! register_guarded_histogram_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        $crate::register_guarded_histogram_vec_with_registry! {
            {prometheus::histogram_opts!($NAME, $HELP)},
            $LABELS_NAMES,
            $REGISTRY
        }
    }};
    ($HOPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let result =
            prometheus::register_histogram_vec_with_registry!($HOPTS, $LABELS_NAMES, $REGISTRY);
        result.map(|inner| {
            let inner = $crate::metrics::__extract_histogram_builder(inner);
            $crate::metrics::LabelGuardedHistogramVec::new(inner, { $LABELS_NAMES })
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_int_gauge_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let result = prometheus::register_int_gauge_vec_with_registry!(
            prometheus::opts!($NAME, $HELP),
            $LABELS_NAMES,
            $REGISTRY
        );
        result.map(|inner| {
            let inner = $crate::metrics::__extract_gauge_builder(inner);
            $crate::metrics::LabelGuardedIntGaugeVec::new(inner, { $LABELS_NAMES })
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_int_counter_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let result = prometheus::register_int_counter_vec_with_registry!(
            prometheus::opts!($NAME, $HELP),
            $LABELS_NAMES,
            $REGISTRY
        );
        result.map(|inner| {
            let inner = $crate::metrics::__extract_counter_builder(inner);
            $crate::metrics::LabelGuardedIntCounterVec::new(inner, { $LABELS_NAMES })
        })
    }};
}

pub type VecBuilderOfCounter<P: Atomic> = impl MetricVecBuilder<M = GenericCounter<P>>;
pub type VecBuilderOfGauge<P: Atomic> = impl MetricVecBuilder<M = GenericGauge<P>>;
pub type VecBuilderOfHistogram = impl MetricVecBuilder<M = Histogram>;

pub type LabelGuardedHistogramVec<const N: usize> = LabelGuardedMetricVec<VecBuilderOfHistogram, N>;
pub type LabelGuardedIntCounterVec<const N: usize> =
    LabelGuardedMetricVec<VecBuilderOfCounter<AtomicU64>, N>;
pub type LabelGuardedIntGaugeVec<const N: usize> =
    LabelGuardedMetricVec<VecBuilderOfGauge<AtomicI64>, N>;

pub type LabelGuardedHistogram<const N: usize> = LabelGuardedMetric<VecBuilderOfHistogram, N>;
pub type LabelGuardedIntCounter<const N: usize> =
    LabelGuardedMetric<VecBuilderOfCounter<AtomicU64>, N>;
pub type LabelGuardedIntGauge<const N: usize> = LabelGuardedMetric<VecBuilderOfGauge<AtomicI64>, N>;

fn gen_test_label<const N: usize>() -> [&'static str; N] {
    const TEST_LABELS: [&str; 5] = ["test1", "test2", "test3", "test4", "test5"];
    (0..N)
        .map(|i| TEST_LABELS[i])
        .collect_vec()
        .try_into()
        .unwrap()
}

#[derive(Clone)]
pub struct LabelGuardedMetricVec<T: MetricVecBuilder, const N: usize> {
    inner: MetricVec<T>,
    labeled_metrics_count: Arc<Mutex<HashMap<[String; N], usize>>>,
    labels: [&'static str; N],
}

impl<T: MetricVecBuilder, const N: usize> Debug for LabelGuardedMetricVec<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("LabelGuardedMetricVec<{}, {}>", type_name::<T>(), N).as_str())
            .field("label", &self.labels)
            .finish()
    }
}

impl<T: MetricVecBuilder, const N: usize> LabelGuardedMetricVec<T, N> {
    pub fn new(inner: MetricVec<T>, labels: &[&'static str; N]) -> Self {
        Self {
            inner,
            labeled_metrics_count: Default::default(),
            labels: *labels,
        }
    }

    pub fn with_label_values(&self, labels: &[&str; N]) -> LabelGuardedMetric<T, N> {
        let mut count_guard = self.labeled_metrics_count.lock();
        let label_string = labels.map(|str| str.to_string());
        *count_guard.entry(label_string).or_insert(0) += 1;
        let inner = self.inner.with_label_values(labels);
        LabelGuardedMetric {
            inner: Arc::new(LabelGuardedMetricInner {
                inner,
                labels: labels.map(|str| str.to_string()),
                vec: self.inner.clone(),
                labeled_metrics_count: self.labeled_metrics_count.clone(),
            }),
        }
    }

    pub fn with_test_label(&self) -> LabelGuardedMetric<T, N> {
        let labels: [&'static str; N] = gen_test_label::<N>();
        self.with_label_values(&labels)
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

#[derive(Clone)]
struct LabelGuardedMetricInner<T: MetricVecBuilder, const N: usize> {
    inner: T::M,
    labels: [String; N],
    vec: MetricVec<T>,
    labeled_metrics_count: Arc<Mutex<HashMap<[String; N], usize>>>,
}

impl<T: MetricVecBuilder, const N: usize> Drop for LabelGuardedMetricInner<T, N> {
    fn drop(&mut self) {
        let mut count_guard = self.labeled_metrics_count.lock();
        let count = count_guard.get_mut(&self.labels).expect(
            "should exist because the current existing dropping one means the count is not zero",
        );
        *count -= 1;
        if *count == 0 {
            count_guard.remove(&self.labels).expect("should exist");
            if let Err(e) = self
                .vec
                .remove_label_values(&self.labels.each_ref().map(|s| s.as_str()))
            {
                warn!(
                    "err when delete metrics of {:?} of labels {:?}. Err {:?}",
                    self.vec.desc().first().expect("should have desc").fq_name,
                    self.labels,
                    e,
                );
            }
        }
    }
}

#[derive(Clone)]
pub struct LabelGuardedMetric<T: MetricVecBuilder, const N: usize> {
    inner: Arc<LabelGuardedMetricInner<T, N>>,
}

impl<T: MetricVecBuilder, const N: usize> Debug for LabelGuardedMetric<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabelGuardedMetric").finish()
    }
}

impl<T: MetricVecBuilder, const N: usize> Deref for LabelGuardedMetric<T, N> {
    type Target = T::M;

    fn deref(&self) -> &Self::Target {
        &self.inner.inner
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

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;

    use crate::metrics::LabelGuardedIntCounterVec;

    #[test]
    fn test_label_guarded_metrics_drop() {
        let vec = LabelGuardedIntCounterVec::<3>::test_int_counter_vec();
        let m1_1 = vec.with_label_values(&["1", "2", "3"]);
        assert_eq!(1, vec.inner.collect().pop().unwrap().get_metric().len());
        let m1_2 = vec.with_label_values(&["1", "2", "3"]);
        let m1_3 = m1_2.clone();
        assert_eq!(1, vec.inner.collect().pop().unwrap().get_metric().len());
        let m2 = vec.with_label_values(&["2", "2", "3"]);
        assert_eq!(2, vec.inner.collect().pop().unwrap().get_metric().len());
        drop(m1_3);
        assert_eq!(2, vec.inner.collect().pop().unwrap().get_metric().len());
        drop(m2);
        assert_eq!(1, vec.inner.collect().pop().unwrap().get_metric().len());
        drop(m1_1);
        assert_eq!(1, vec.inner.collect().pop().unwrap().get_metric().len());
        drop(m1_2);
        assert_eq!(0, vec.inner.collect().pop().unwrap().get_metric().len());
    }
}
