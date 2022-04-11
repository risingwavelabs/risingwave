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

use prometheus::core::{AtomicU64, Collector, GenericCounter, Metric};
use prometheus::Histogram;
use risingwave_storage::for_all_metrics;
use risingwave_storage::monitor::StateStoreMetrics;

use super::my_stats::MyStateStoreStats;
use crate::utils::my_stats::MyHistogram;

#[derive(Default)]
pub(crate) struct DisplayStats {
    pub(crate) prev_stat: MyStateStoreStats,
    pub(crate) cur_stat: MyStateStoreStats,
}

impl DisplayStats {
    pub(crate) fn update_stat(&mut self) {
        // (Ting Sun) TODO: eliminate this clone
        self.prev_stat = self.cur_stat.clone();
        self.cur_stat = MyStateStoreStats::from_prom_stats(&StateStoreMetrics::new(
            prometheus::Registry::new(),
        ));
    }

    pub(crate) fn display_write_batch(&mut self) {
        let perf = self.display_batch_inner();

        println!(
            "
    writebatch
      {}
      OPS: {}  {} bytes/sec",
            perf.histogram, perf.qps, perf.bytes_pre_sec
        );
    }

    pub(crate) fn display_delete_random(&mut self) {
        let perf = self.display_batch_inner();

        println!(
            "
    deleterandom
      {}
      OPS: {}  {} bytes/sec",
            perf.histogram, perf.qps, perf.bytes_pre_sec
        );
    }

    fn display_batch_inner(&mut self) -> PerfMetrics {
        let prev_latency_hist = &self.prev_stat.write_batch_duration;
        let cur_latency_hist = &self.cur_stat.write_batch_duration;

        let time_consume = cur_latency_hist.total_sum - prev_latency_hist.total_sum;

        let ops = {
            let written_batch_num = cur_latency_hist.total_count - prev_latency_hist.total_count;
            written_batch_num as f64 / time_consume
        };

        let bytes_pre_sec = {
            let prev_histogram = &self.prev_stat.write_batch_size;
            let cur_histogram = &self.cur_stat.write_batch_size;

            let written_bytes = cur_histogram.total_sum - prev_histogram.total_sum;
            written_bytes / time_consume
        };

        PerfMetrics {
            histogram: MyHistogram::from_diff(prev_latency_hist, cur_latency_hist),
            qps: ops,
            bytes_pre_sec,
        }
    }
}

pub(crate) struct PerfMetrics {
    histogram: MyHistogram,
    qps: f64,
    bytes_pre_sec: f64,
}

/// Define extension method `print` used in `print_statistics`.
trait Print {
    fn print(&self);
}

impl Print for GenericCounter<AtomicU64> {
    fn print(&self) {
        let desc = &self.desc()[0].fq_name;
        let counter = self.metric().get_counter().get_value() as u64;
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

macro_rules! print_statistics {
    ($( $name:ident: $type:ty ),* ,) => {
        pub(crate) fn print_statistics(stats: &risingwave_storage::monitor::StateStoreMetrics) {
            println!("STATISTICS:");
            // print for all fields
            $( stats.$name.print(); )*
            println!();
        }
    }
}
for_all_metrics! { print_statistics }
