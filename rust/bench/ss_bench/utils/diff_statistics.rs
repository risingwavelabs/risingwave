use prometheus::core::Metric;
use risingwave_storage::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

use super::my_state_store_stats::MyStateStoreStats;
use crate::utils::my_histogram::MyHistogram;

#[derive(Default)]
pub(crate) struct StatDiff {
    pub(crate) prev_stat: MyStateStoreStats,
    pub(crate) cur_stat: MyStateStoreStats,
}

impl StatDiff {
    pub(crate) fn update_stat(&mut self) { 
        // (Ting Sun) TODO: eliminate this clone
        self.prev_stat = self.cur_stat.clone();
        self.cur_stat = MyStateStoreStats::from_prom_stats(&**DEFAULT_STATE_STORE_STATS);
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
        // let metric = self.prev_stat.batch_write_latency.metric();
        // let prev_latency_hist = metric.get_histogram();
        // let metric = self.cur_stat.batch_write_latency.metric();
        // let cur_latency_hist = metric.get_histogram();

        // let metric = self.prev_stat.batch_write_latency.metric();
        let prev_latency_hist = &self.prev_stat.batch_write_latency;
        // let metric = self.cur_stat.batch_write_latency.metric();
        let cur_latency_hist = &self.cur_stat.batch_write_latency;

        let latency = MyHistogram::from_diff(prev_latency_hist, cur_latency_hist);
        let time_consume = cur_latency_hist.total_sum - prev_latency_hist.total_sum;

        let ops = {
            let written_batch_num = cur_latency_hist.total_count - prev_latency_hist.total_count;
            written_batch_num as f64 / time_consume
        };

        let bytes_pre_sec = {
            let prev_histogram = &self.prev_stat.batch_write_size;
            let cur_histogram = &self.cur_stat.batch_write_size;

            let written_bytes = cur_histogram.total_sum - prev_histogram.total_sum;
            written_bytes / time_consume
        };

        PerfMetrics {
            histogram: latency,
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
