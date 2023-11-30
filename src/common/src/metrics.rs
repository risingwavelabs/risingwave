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
use std::sync::LazyLock;

use hytra::TrAdder;
use prometheus::core::{Atomic, AtomicU64, GenericCounter, GenericGauge};
use prometheus::register_int_counter_with_registry;
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::monitor::GLOBAL_METRICS_REGISTRY;

mod error_metrics;
mod guarded_metrics;
mod relabeled_metric;

pub use error_metrics::*;
pub use guarded_metrics::*;
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
            let registry = GLOBAL_METRICS_REGISTRY.deref();
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
