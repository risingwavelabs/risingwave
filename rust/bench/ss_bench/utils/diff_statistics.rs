use itertools::Itertools;
use prometheus::core::Metric;
use prometheus::proto::Histogram;
use risingwave_storage::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

use crate::utils::metric_display::MetricDisplay;

pub(crate) struct DiffStat {
    pub(crate) prev_stat: StateStoreStats,
    pub(crate) cur_stat: StateStoreStats,
}

pub(crate) struct MyHistogram {
    pub(crate) upper_bound_list: Vec<f64>,
    pub(crate) count_list: Vec<u64>,
    pub(crate) total_count: u64,
    pub(crate) total_sum: f64,
}

impl DiffStat {
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

        let latency = {
            let latency_hist = DiffStat::histogram_diff(prev_latency_hist, cur_latency_hist);
            MetricDisplay::new(&latency_hist)
        };

        let time_comsume = cur_latency_hist.get_sample_sum() - prev_latency_hist.get_sample_sum();

        let ops = {
            let written_batch_num =
                cur_latency_hist.get_sample_count() - prev_latency_hist.get_sample_count();
            written_batch_num as f64 / time_comsume
        };

        let bytes_pre_sec = {
            let metric = self.prev_stat.batch_write_size.metric();
            let prev_histogram = metric.get_histogram();
            let metric = self.cur_stat.batch_write_size.metric();
            let cur_histogram = metric.get_histogram();

            let written_bytes = cur_histogram.get_sample_sum() - prev_histogram.get_sample_sum();
            written_bytes / time_comsume
        };

        println!(
            "
        writebatch
          {}
          OPS: {}  {} bytes/sec",
            latency, ops, bytes_pre_sec
        );
    }

    fn histogram_diff(prev: &Histogram, cur: &Histogram) -> MyHistogram {
        let prev_buckets = prev.get_bucket();
        let cur_buckets = cur.get_bucket();
        let bucket_bounds = prev_buckets
            .iter()
            .map(|bucket| bucket.get_upper_bound())
            .collect_vec();

        MyHistogram {
            upper_bound_list: bucket_bounds,
            count_list: prev_buckets
                .iter()
                .zip_eq(cur_buckets.iter())
                .map(|(pb, cb)| cb.get_cumulative_count() - pb.get_cumulative_count())
                .collect_vec(),
            total_sum: cur.get_sample_sum() - prev.get_sample_sum(),
            total_count: cur.get_sample_count() - prev.get_sample_count(),
        }
    }
}
