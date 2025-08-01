// Copyright 2025 RisingWave Labs
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

#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
use std::ops::Deref;
use std::sync::LazyLock;

use hytra::TrAdder;
use prometheus::core::{Atomic, AtomicU64, GenericCounter, GenericGauge};
use prometheus::proto::Metric;
use prometheus::register_int_counter_with_registry;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

mod error_metrics;
mod gauge_ext;
mod guarded_metrics;
mod metrics;
pub mod monitor;
mod relabeled_metric;

pub use error_metrics::*;
pub use gauge_ext::*;
pub use guarded_metrics::*;
pub use metrics::*;
pub use relabeled_metric::*;

#[derive(Debug)]
pub struct TrAdderAtomic(TrAdder<i64>);

impl Atomic for TrAdderAtomic {
    type T = i64;

    fn new(val: i64) -> Self {
        let v = TrAdderAtomic(TrAdder::new());
        v.0.inc(val);
        v
    }

    fn set(&self, _val: i64) {
        panic!("TrAdderAtomic doesn't support set operation.")
    }

    fn get(&self) -> i64 {
        self.0.get()
    }

    fn inc_by(&self, delta: i64) {
        self.0.inc(delta)
    }

    fn dec_by(&self, delta: i64) {
        self.0.inc(-delta)
    }
}

pub type TrAdderGauge = GenericGauge<TrAdderAtomic>;

/// [`MetricsLayer`] is a struct used for monitoring the frequency of certain specific logs and
/// counting them using Prometheus metrics. Currently, it is used to monitor the frequency of retry
/// occurrences of aws sdk.
pub struct MetricsLayer {
    pub aws_sdk_retry_counts: GenericCounter<AtomicU64>,
}

impl<S> Layer<S> for MetricsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // Currently one retry will only generate one debug log,
        // so we can monitor the number of retry only through the metadata target.
        // Refer to <https://docs.rs/aws-smithy-client/0.55.3/src/aws_smithy_client/retry.rs.html>
        self.aws_sdk_retry_counts.inc();
    }
}

impl MetricsLayer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        static AWS_SDK_RETRY_COUNTS: LazyLock<GenericCounter<AtomicU64>> = LazyLock::new(|| {
            let registry = crate::monitor::GLOBAL_METRICS_REGISTRY.deref();
            register_int_counter_with_registry!(
                "aws_sdk_retry_counts",
                "Total number of aws sdk retry happens",
                registry
            )
            .unwrap()
        });

        Self {
            aws_sdk_retry_counts: AWS_SDK_RETRY_COUNTS.deref().clone(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum MetricLevel {
    #[default]
    Disabled = 0,
    Critical = 1,
    Info = 2,
    Debug = 3,
}

impl clap::ValueEnum for MetricLevel {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Disabled, Self::Critical, Self::Info, Self::Debug]
    }

    fn to_possible_value<'a>(&self) -> ::std::option::Option<clap::builder::PossibleValue> {
        match self {
            Self::Disabled => Some(clap::builder::PossibleValue::new("disabled").alias("0")),
            Self::Critical => Some(clap::builder::PossibleValue::new("critical")),
            Self::Info => Some(clap::builder::PossibleValue::new("info").alias("1")),
            Self::Debug => Some(clap::builder::PossibleValue::new("debug")),
        }
    }
}

impl PartialEq<Self> for MetricLevel {
    fn eq(&self, other: &Self) -> bool {
        (*self as u8).eq(&(*other as u8))
    }
}

impl PartialOrd for MetricLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (*self as u8).partial_cmp(&(*other as u8))
    }
}

pub fn get_label<T: std::str::FromStr>(metric: &Metric, label: &str) -> Option<T> {
    metric
        .get_label()
        .iter()
        .find(|lp| lp.name() == label)
        .and_then(|lp| lp.value().parse::<T>().ok())
}

// Must ensure the label exists and can be parsed into `T`
pub fn get_label_infallible<T: std::str::FromStr>(metric: &Metric, label: &str) -> T {
    get_label(metric, label).expect("label not found or can't be parsed")
}
