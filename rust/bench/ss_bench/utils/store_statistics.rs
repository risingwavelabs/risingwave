use prometheus::core::{AtomicU64, Collector, GenericCounter, Metric};
use prometheus::Histogram;
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;

fn proc_counter(counter: &GenericCounter<AtomicU64>) {
    let desc = &counter.desc()[0].fq_name;
    let counter = counter.metric().get_counter().get_value() as u64;
    println!("{desc} COUNT : {counter}");
}

fn proc_histogram(histogram: &Histogram) {
    let desc = &histogram.desc()[0].fq_name;

    let metric = histogram.metric();
    let histogram = metric.get_histogram();
    let sample_count = histogram.get_sample_count();
    let sample_sum = histogram.get_sample_sum();
    let buckets = histogram.get_bucket();

    let get_quantile = |p: f64| -> f64 {
        // empty bucket may appear
        if sample_count == 0 {
            return 0.0;
        }
        let threshold = (sample_count as f64 * p / 100.0) as u64;
        let mut last_upper_bound = 0.0;
        let mut last_count = 0;
        for bucket in buckets {
            let upper_bound = bucket.get_upper_bound();
            let count = bucket.get_cumulative_count();
            if count >= threshold {
                // assume scale linearly within this bucket,
                // return the propositional value bwtween last_upper_bound and upper_bound
                let right_left_diff = upper_bound - last_upper_bound;
                return last_upper_bound
                    + right_left_diff * (threshold - last_count) as f64
                        / (count - last_count) as f64;
            }
            last_upper_bound = upper_bound;
            last_count = count;
        }
        0.0
    };

    let p50 = get_quantile(50.0);
    let p95 = get_quantile(95.0);
    let p99 = get_quantile(99.0);
    let p100 = get_quantile(100.0);

    println!("{desc} P50 : {p50} P95 : {p95} P99 : {p99} P100 : {p100} COUNT : {sample_count} SUM : {sample_sum}");
}

pub(crate) fn print_statistics() {
    println!("STATISTICS:");

    let stat = DEFAULT_STATE_STORE_STATS.clone();

    // ----- TODO(Ting Sun): use macro to simplify the implementation -----
    proc_counter(&stat.get_bytes);
    proc_counter(&stat.get_counts);
    proc_counter(&stat.put_bytes);
    proc_counter(&stat.range_scan_counts);
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
