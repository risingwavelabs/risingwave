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

use hytra::TrAdder;
use prometheus::core::{Atomic, AtomicU64, GenericCounter, GenericGauge};
use prometheus::{register_int_counter_with_registry, Registry};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;
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

pub struct CustomLayer {
    pub aws_sdk_retry_counts: GenericCounter<AtomicU64>,
}

impl<S> Layer<S> for CustomLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() == "aws_smithy_client::retry"
            && event.metadata().level() == &tracing::Level::DEBUG
            && event.metadata().name().contains("retry.rs:363")
        {
            self.aws_sdk_retry_counts.inc();
        }
    }
}

impl CustomLayer {
    pub fn new(registry: Registry) -> Self {
        let aws_sdk_retry_counts = register_int_counter_with_registry!(
            "aws_sdk_retry_counts",
            "Total number of aws sdk retry happens",
            registry
        )
        .unwrap();

        Self {
            aws_sdk_retry_counts,
        }
    }
}
