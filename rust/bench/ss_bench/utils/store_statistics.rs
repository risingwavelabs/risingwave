use prometheus::core::{AtomicU64, Collector, GenericCounter, Metric};
use prometheus::Histogram;
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;

fn proc_counter(counter: &GenericCounter<AtomicU64>) {
    let desc = &counter.desc()[0].fq_name;
    let counter = counter.metric().get_counter().get_value() as u64;
    println!("{desc} COUNT : {counter}");
}

fn get_percentile(histogram: &Histogram, p: f64) -> f64 {
    let metric = histogram.metric();
    let histogram = metric.get_histogram();
    let sample_count = histogram.get_sample_count();
    let buckets = histogram.get_bucket();

    // empty bucket may appear
    if sample_count == 0 {
        return 0.0;
    }
    let threshold = (sample_count as f64 * (p / 100.0_f64)).ceil() as u64;
    let mut last_upper_bound = 0.0;
    let mut last_count = 0;
    for bucket in buckets {
        let upper_bound = bucket.get_upper_bound();
        let count = bucket.get_cumulative_count();
        if count >= threshold {
            // assume scale linearly within this bucket,
            // return a value bwtween last_upper_bound and upper_bound
            let right_left_diff = upper_bound - last_upper_bound;
            return last_upper_bound
                + right_left_diff * (threshold - last_count) as f64 / (count - last_count) as f64;
        }
        last_upper_bound = upper_bound;
        last_count = count;
    }
    0.0
}

fn proc_histogram(histogram: &Histogram) {
    let desc = &histogram.desc()[0].fq_name;

    let p50 = get_percentile(histogram, 50.0);
    let p95 = get_percentile(histogram, 95.0);
    let p99 = get_percentile(histogram, 99.0);
    let p100 = get_percentile(histogram, 100.0);

    let sample_count = histogram.get_sample_count();
    let sample_sum = histogram.get_sample_sum();

    println!("{desc} P50 : {p50} P95 : {p95} P99 : {p99} P100 : {p100} COUNT : {sample_count} SUM : {sample_sum}");
}

pub(crate) fn print_statistics() {
    println!("STATISTICS:");

    let stat = DEFAULT_STATE_STORE_STATS.clone();

    // ----- TODO(Ting Sun): use macro to simplify the implementation -----
    proc_counter(&stat.get_counts);
    proc_counter(&stat.range_scan_counts);
    proc_counter(&stat.reverse_range_scan_counts);
    proc_counter(&stat.batched_write_counts);
    proc_counter(&stat.batch_write_tuple_counts);
    proc_counter(&stat.iter_counts);
    proc_counter(&stat.iter_next_counts);

    proc_histogram(&stat.get_latency);
    proc_histogram(&stat.get_key_size);
    proc_histogram(&stat.get_value_size);
    proc_histogram(&stat.get_snapshot_latency);
    proc_histogram(&stat.batch_write_latency);
    proc_histogram(&stat.batch_write_size);
    proc_histogram(&stat.batch_write_build_table_latency);
    proc_histogram(&stat.batch_write_add_l0_latency);
    proc_histogram(&stat.iter_seek_latency);
    proc_histogram(&stat.iter_next_latency);

    println!();
}

#[cfg(test)]
mod tests {
    use prometheus::{histogram_opts, register_histogram_with_registry, Registry};

    use super::*;

    #[test]
    fn test_proc_histogram_basic() {
        fn new_simple_histogram(upper_bound: u64) -> Histogram {
            let registry = Registry::new();
            let buckets = (1..=upper_bound).map(|x| x as f64).collect::<Vec<f64>>();
            let opts = histogram_opts!("test_histogram", "test_histogram", buckets);

            let histogram = register_histogram_with_registry!(opts, registry).unwrap();

            for value in 1..=upper_bound {
                histogram.observe(value as f64);
            }

            histogram
        }

        let histogram = new_simple_histogram(999);
        assert_eq!(get_percentile(&histogram, 50.0) as u64, 500);
        assert_eq!(get_percentile(&histogram, 90.0) as u64, 900);
        assert_eq!(get_percentile(&histogram, 99.0) as u64, 990);
        assert_eq!(get_percentile(&histogram, 99.9) as u64, 999);
        assert_eq!(get_percentile(&histogram, 100.0) as u64, 999);

        let histogram = new_simple_histogram(1000);
        assert_eq!(get_percentile(&histogram, 50.0) as u64, 500);
        assert_eq!(get_percentile(&histogram, 90.0) as u64, 900);
        assert_eq!(get_percentile(&histogram, 99.0) as u64, 990);
        assert_eq!(get_percentile(&histogram, 99.9) as u64, 1000);
        assert_eq!(get_percentile(&histogram, 100.0) as u64, 1000);

        let histogram = new_simple_histogram(9999);
        assert_eq!(get_percentile(&histogram, 50.0) as u64, 5000);
        assert_eq!(get_percentile(&histogram, 90.0) as u64, 9000);
        assert_eq!(get_percentile(&histogram, 99.0) as u64, 9900);
        assert_eq!(get_percentile(&histogram, 99.9) as u64, 9990);
        assert_eq!(get_percentile(&histogram, 100.0) as u64, 9999);
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

        assert_eq!(get_percentile(&histogram, 50.0), 5.0);
        assert_eq!(get_percentile(&histogram, 90.0), 9.004004004004004);
        assert_eq!(get_percentile(&histogram, 99.0), 9.904904904904905);
        assert_eq!(get_percentile(&histogram, 99.9), 9.994994994994995);
        assert_eq!(get_percentile(&histogram, 100.0), 10.0);
    }

    #[test]
    fn test_proc_histogram_realistic() {
        let registry = Registry::new();
        let buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let opts = histogram_opts!("test_histogram", "test_histogram", buckets);
        let histogram = register_histogram_with_registry!(opts, registry).unwrap();

        histogram.observe(0.001);
        histogram.observe(0.003);

        histogram.observe(0.0113);
        histogram.observe(0.0143);
        histogram.observe(0.0249);

        histogram.observe(0.041);
        histogram.observe(0.042);

        histogram.observe(0.098);
        histogram.observe(0.99);

        histogram.observe(6.11);

        assert_eq!(get_percentile(&histogram, 50.0), 0.025);
        assert_eq!(get_percentile(&histogram, 90.0), 1.00);
        assert_eq!(get_percentile(&histogram, 99.0), 10.00);
        assert_eq!(get_percentile(&histogram, 99.9), 10.00);
        assert_eq!(get_percentile(&histogram, 100.0), 10.00);
    }
}
