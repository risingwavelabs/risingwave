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

use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
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
        let result = prometheus::register_histogram_vec_with_registry!(
            prometheus::histogram_opts!($NAME, $HELP),
            $LABELS_NAMES,
            $REGISTRY
        );
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

pub type LabelGuardedHistogram = LabelGuardedMetric<VecBuilderOfHistogram>;
pub type LabelGuardedIntCounter = LabelGuardedMetric<VecBuilderOfCounter<AtomicU64>>;
pub type LabelGuardedIntGauge = LabelGuardedMetric<VecBuilderOfGauge<AtomicI64>>;

fn gen_test_label<const N: usize>() -> [&'static str; N] {
    vec!["test"; N].try_into().unwrap()
}

#[derive(Clone)]
pub struct LabelGuardedMetricVec<T: MetricVecBuilder, const N: usize> {
    inner: MetricVec<T>,
    _labels: [&'static str; N],
}

impl<T: MetricVecBuilder, const N: usize> LabelGuardedMetricVec<T, N> {
    pub fn new(inner: MetricVec<T>, labels: &[&'static str; N]) -> Self {
        Self {
            inner,
            _labels: *labels,
        }
    }

    pub fn with_label_values(&self, labels: &[&str; N]) -> LabelGuardedMetric<T> {
        let inner = self.inner.with_label_values(labels);
        LabelGuardedMetric {
            inner: Arc::new(LabelGuardedMetricInner {
                inner,
                labels: labels.iter().map(|s| s.to_string()).collect(),
                vec: self.inner.clone(),
            }),
        }
    }

    pub fn with_test_label(&self) -> LabelGuardedMetric<T> {
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
struct LabelGuardedMetricInner<T: MetricVecBuilder> {
    inner: T::M,
    labels: Vec<String>,
    vec: MetricVec<T>,
}

impl<T: MetricVecBuilder> Drop for LabelGuardedMetricInner<T> {
    fn drop(&mut self) {
        if let Err(e) = self.vec.remove_label_values(
            self.labels
                .iter()
                .map(|s| s.as_str())
                .collect_vec()
                .as_slice(),
        ) {
            warn!(
                "err when delete metrics of {:?} of labels {:?}. Err {:?}",
                self.vec.desc().first().expect("should have desc").fq_name,
                self.labels,
                e,
            );
        }
    }
}

#[derive(Clone)]
pub struct LabelGuardedMetric<T: MetricVecBuilder> {
    inner: Arc<LabelGuardedMetricInner<T>>,
}

impl<T: MetricVecBuilder> Deref for LabelGuardedMetric<T> {
    type Target = T::M;

    fn deref(&self) -> &Self::Target {
        &self.inner.inner
    }
}

impl LabelGuardedHistogram {
    pub fn test_histogram() -> Self {
        LabelGuardedHistogramVec::<1>::test_histogram_vec().with_test_label()
    }
}

impl LabelGuardedIntCounter {
    pub fn test_int_counter() -> Self {
        LabelGuardedIntCounterVec::<1>::test_int_counter_vec().with_test_label()
    }
}

impl LabelGuardedIntGauge {
    pub fn test_int_gauge() -> Self {
        LabelGuardedIntGaugeVec::<1>::test_int_gauge_vec().with_test_label()
    }
}
