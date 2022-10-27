// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod my_stats;
pub mod process_linux;
pub mod rwlock;

use futures::Future;
use prometheus::core::{
    AtomicI64, AtomicU64, Collector, GenericCounter, GenericCounterVec, GenericGauge, Metric,
};
use prometheus::{Histogram, HistogramVec};
#[cfg(not(madsim))]
use tokio::task::futures::TaskLocalFuture;
#[cfg(not(madsim))]
use tokio::task_local;

use crate::monitor::my_stats::MyHistogram;

/// Define extension method `print` used in `print_statistics`.
pub trait Print {
    fn print(&self);
}

impl Print for GenericCounter<AtomicU64> {
    fn print(&self) {
        let desc = &self.desc()[0].fq_name;
        let counter = self.metric().get_counter().get_value() as u64;
        println!("{desc} COUNT : {counter}");
    }
}

impl Print for GenericGauge<AtomicI64> {
    fn print(&self) {
        let desc = &self.desc()[0].fq_name;
        let counter = self.get();
        println!("{desc} COUNT : {counter}");
    }
}

impl Print for Histogram {
    fn print(&self) {
        let desc = &self.desc()[0].fq_name;

        let histogram = MyHistogram::from_prom_hist(self.metric().get_histogram());
        let p50 = histogram.get_percentile(50.0);
        let p95 = histogram.get_percentile(95.0);
        let p99 = histogram.get_percentile(99.0);
        let p100 = histogram.get_percentile(100.0);

        let sample_count = self.get_sample_count();
        let sample_sum = self.get_sample_sum();
        println!("{desc} P50 : {p50} P95 : {p95} P99 : {p99} P100 : {p100} COUNT : {sample_count} SUM : {sample_sum}");
    }
}

impl Print for HistogramVec {
    fn print(&self) {
        let desc = &self.desc()[0].fq_name;
        println!("{desc} {:?}", self);
    }
}

impl Print for GenericCounterVec<AtomicU64> {
    fn print(&self) {
        let desc = &self.desc()[0].fq_name;
        println!("{desc} {:?}", self);
    }
}

#[cfg(not(madsim))]
pub type TraceConcurrentId = u64;

#[cfg(not(madsim))]
task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static CONCURRENT_ID: TraceConcurrentId;
}

#[cfg(not(madsim))]
pub fn task_local_scope<F: Future>(
    actor_id: TraceConcurrentId,
    f: F,
) -> TaskLocalFuture<TraceConcurrentId, F> {
    CONCURRENT_ID.scope(actor_id, f)
}

#[cfg(not(madsim))]
pub fn task_local_get() -> TraceConcurrentId {
    CONCURRENT_ID.get()
}
