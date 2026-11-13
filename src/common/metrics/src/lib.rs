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

#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#![feature(register_tool)]
#![feature(trait_alias)]
#![register_tool(rw)]

use hytra::TrAdder;
use prometheus::core::{Atomic, GenericGauge};
use prometheus::proto::Metric;

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
