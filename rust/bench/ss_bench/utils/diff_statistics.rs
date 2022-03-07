use prometheus::core::Metric;
use risingwave_storage::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

use crate::utils::my_histogram::MyHistogram;

pub(crate) struct StatDiff {
    pub(crate) prev_stat: StateStoreStats,
    pub(crate) cur_stat: StateStoreStats,
}

impl StatDiff {
    pub(crate) fn update_stat(&mut self) {
        let stat = (**DEFAULT_STATE_STORE_STATS).clone();
        // (Ting Sun) TODO: eliminate this clone
        let prev = self.cur_stat.clone();
        self.cur_stat = stat;
        self.prev_stat = prev;
    }

    pub(crate) fn display_write_batch(&mut self) {
        let metric = self.prev_stat.batch_write_latency.metric();
        let prev_latency_hist = metric.get_histogram();
        let metric = self.cur_stat.batch_write_latency.metric();
        let cur_latency_hist = metric.get_histogram();

        let latency = MyHistogram::from_diff(prev_latency_hist, cur_latency_hist);

        let time_consume = cur_latency_hist.get_sample_sum() - prev_latency_hist.get_sample_sum();

        let ops = {
            let written_batch_num =
                cur_latency_hist.get_sample_count() - prev_latency_hist.get_sample_count();
            written_batch_num as f64 / time_consume
        };

        let bytes_pre_sec = {
            let metric = self.prev_stat.batch_write_size.metric();
            let prev_histogram = metric.get_histogram();
            let metric = self.cur_stat.batch_write_size.metric();
            let cur_histogram = metric.get_histogram();

            let written_bytes = cur_histogram.get_sample_sum() - prev_histogram.get_sample_sum();
            written_bytes / time_consume
        };

        println!(
            "
    writebatch
      {}
      OPS: {}  {} bytes/sec",
            latency, ops, bytes_pre_sec
        );
    }
}
