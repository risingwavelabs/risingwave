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

use risingwave_common::monitor::my_stats::MyHistogram;
use risingwave_storage::monitor::StateStoreMetrics;

use super::my_stats::MyStateStoreStats;

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
