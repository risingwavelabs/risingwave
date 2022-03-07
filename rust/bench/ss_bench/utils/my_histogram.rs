use std::fmt::{Display, Formatter};

use itertools::Itertools;
use prometheus::core::{AtomicU64, Collector, GenericCounter, Metric};
use prometheus::proto::Histogram;

pub(crate) struct MyHistogram {
    pub(crate) upper_bound_list: Vec<f64>,
    pub(crate) count_list: Vec<u64>,
    pub(crate) total_count: u64,
    pub(crate) total_sum: f64,
}

impl MyHistogram {
    pub(crate) fn from_prom_hist(histogram: &Histogram) -> MyHistogram {
        let mut upper_bound_list = Vec::new();
        let mut count_list = Vec::new();

        let total_count = histogram.get_sample_count();
        let total_sum = histogram.get_sample_sum();

        let buckets = histogram.get_bucket();
        for bucket in buckets {
            let upper_bound = bucket.get_upper_bound();
            let count = bucket.get_cumulative_count();
            upper_bound_list.push(upper_bound);
            count_list.push(count);
        }

        MyHistogram {
            upper_bound_list,
            count_list,
            total_count,
            total_sum,
        }
    }

    pub(crate) fn from_diff(prev: &Histogram, cur: &Histogram) -> MyHistogram {
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

    pub(crate) fn get_percentile(&self, p: f64) -> f64 {
        let sample_count = self.total_count;

        // empty bucket may appear
        if sample_count == 0 {
            return 0.0;
        }
        let threshold = (sample_count as f64 * (p / 100.0_f64)).ceil() as u64;
        let mut last_upper_bound = 0.0;
        let mut last_count = 0;
        for (&upper_bound, &count) in self.upper_bound_list.iter().zip_eq(self.count_list.iter()) {
            if count >= threshold {
                // assume scale linearly within this bucket,
                // return a value bwtween last_upper_bound and upper_bound
                let right_left_diff = upper_bound - last_upper_bound;
                return last_upper_bound
                    + right_left_diff * (threshold - last_count) as f64
                        / (count - last_count) as f64;
            }
            last_upper_bound = upper_bound;
            last_count = count;
        }
        0.0
    }
}

impl Display for MyHistogram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // calculate latencies statistics
        let mean = self.total_sum / self.total_count as f64;
        let p50 = self.get_percentile(50.0);
        let p90 = self.get_percentile(90.0);
        let p99 = self.get_percentile(99.0);
        let p100 = self.get_percentile(100.0);

        write!(
            f,
            "latency:
        mean: {},
        p50: {},
        p90: {},
        p99: {},
        p100: {};",
            mean, p50, p90, p99, p100
        )
    }
}

#[cfg(test)]
mod tests {
    use prometheus::{histogram_opts, register_histogram_with_registry, Registry};

    use super::*;

    #[test]
    fn test_proc_histogram_basic() {
        fn new_simple_histogram(upper_bound: u64) -> MyHistogram {
            let registry = Registry::new();
            let buckets = (1..=upper_bound).map(|x| x as f64).collect::<Vec<f64>>();
            let opts = histogram_opts!("test_histogram", "test_histogram", buckets);

            let histogram = register_histogram_with_registry!(opts, registry).unwrap();

            for value in 1..=upper_bound {
                histogram.observe(value as f64);
            }

            MyHistogram::from_prom_hist(histogram.metric().get_histogram())
        }

        let histogram = new_simple_histogram(999);
        assert_eq!(histogram.get_percentile(50.0) as u64, 500);
        assert_eq!(histogram.get_percentile(90.0) as u64, 900);
        assert_eq!(histogram.get_percentile(99.0) as u64, 990);
        assert_eq!(histogram.get_percentile(99.9) as u64, 999);
        assert_eq!(histogram.get_percentile(100.0) as u64, 999);

        let histogram = new_simple_histogram(1000);
        assert_eq!(histogram.get_percentile(50.0) as u64, 500);
        assert_eq!(histogram.get_percentile(90.0) as u64, 900);
        assert_eq!(histogram.get_percentile(99.0) as u64, 990);
        assert_eq!(histogram.get_percentile(99.9) as u64, 1000);
        assert_eq!(histogram.get_percentile(100.0) as u64, 1000);

        let histogram = new_simple_histogram(9999);
        assert_eq!(histogram.get_percentile(50.0) as u64, 5000);
        assert_eq!(histogram.get_percentile(90.0) as u64, 9000);
        assert_eq!(histogram.get_percentile(99.0) as u64, 9900);
        assert_eq!(histogram.get_percentile(99.9) as u64, 9990);
        assert_eq!(histogram.get_percentile(100.0) as u64, 9999);
    }

    #[test]
    fn test_proc_histogram_uneven_distributed() {
        let registry = Registry::new();
        let buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let opts = histogram_opts!("test_histogram", "test_histogram", buckets);
        let histogram = register_histogram_with_registry!(opts, registry).unwrap();

        let mut i = 0.005;
        while i < 10.0 {
            histogram.observe(i);
            i += 0.005;
        }

        let histogram = MyHistogram::from_prom_hist(histogram.metric().get_histogram());
        assert_eq!(histogram.get_percentile(50.0), 5.0);
        assert_eq!(histogram.get_percentile(90.0), 9.004004004004004);
        assert_eq!(histogram.get_percentile(99.0), 9.904904904904905);
        assert_eq!(histogram.get_percentile(99.9), 9.994994994994995);
        assert_eq!(histogram.get_percentile(100.0), 10.0);
    }

    #[test]
    fn test_proc_histogram_realistic() {
        let registry = Registry::new();
        let buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let opts = histogram_opts!("test_histogram", "test_histogram", buckets);
        let histogram = register_histogram_with_registry!(opts, registry).unwrap();

        histogram.observe(0.0012);
        histogram.observe(0.0013);
        histogram.observe(0.003);

        histogram.observe(0.0132);
        histogram.observe(0.0143);
        histogram.observe(0.0146);
        histogram.observe(0.0249);

        histogram.observe(0.99);

        histogram.observe(6.11);
        histogram.observe(7.833);

        let histogram = MyHistogram::from_prom_hist(histogram.metric().get_histogram());
        assert_eq!(histogram.get_percentile(50.0), 0.0175);
        assert_eq!(histogram.get_percentile(90.0), 7.5);
        assert_eq!(histogram.get_percentile(99.0), 10.00);
        assert_eq!(histogram.get_percentile(99.9), 10.00);
        assert_eq!(histogram.get_percentile(100.0), 10.00);
    }
}
