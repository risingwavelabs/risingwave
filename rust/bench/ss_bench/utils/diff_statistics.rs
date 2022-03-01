
use itertools::Itertools;
use prometheus::core::{Metric};
use prometheus::proto::Histogram;
use risingwave_storage::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

use crate::utils::metric_display::MetricDisplay;

// #[derive(Default)]
pub(crate) struct DiffStat {
    // pub(crate) prev_histogram: BTreeMap<String, Histogram>,
    // pub(crate) prev_counter: BTreeMap<String, GenericCounter<AtomicU64>>,
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
        let stat = DEFAULT_STATE_STORE_STATS.clone();
        let stat = (*stat).clone();
        // (Ting Sun) TODO: eliminate this clone
        let prev = self.cur_stat.clone();
        self.cur_stat = stat;
        self.prev_stat = prev;
    }

    pub(crate) fn display_write_batch(&mut self) {
        // let batched_write_counts =  self.cur_stat.batched_write_counts.get()
        //  - self.prev_stat.batched_write_counts.get();
        let metric = self.prev_stat.batch_write_latency.metric();
        let prev_histogram = metric.get_histogram();
        let metric = self.cur_stat.batch_write_latency.metric();
        let cur_histogram = metric.get_histogram();

        let latency_hist = DiffStat::histogram_diff(prev_histogram, cur_histogram);
        let latency = MetricDisplay::new(&latency_hist);

        println!("hello {}", latency);

        //     println!(
        //         "
        // writebatch
        //   {}
        //   KV ingestion OPS: {}  {} bytes/sec",
        //         latency, perf.qps, perf.bytes_pre_sec
        //     );
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
                .zip(cur_buckets.iter())
                .map(|(pb, cb)| cb.get_cumulative_count() - pb.get_cumulative_count())
                .collect_vec(),
            total_sum: cur.get_sample_sum() - prev.get_sample_sum(),
            total_count: cur.get_sample_count() - prev.get_sample_count(),
        }
    }
}
