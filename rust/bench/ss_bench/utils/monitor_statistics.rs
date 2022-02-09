use prometheus::core::{AtomicU64, Collector, GenericCounter, Metric};
use prometheus::Histogram;
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;

fn proc_histogram(histogram: Histogram) {
    let metric = histogram.metric();
    let metric_str = format!("{:#?}", metric);
    let str_lines = metric_str.split('\n').collect::<Vec<&str>>();
    let mut sample_count = 0;
    let mut sample_sum = 0.0;
    // We get metrics in buckets by parsing the fmt string.
    // This method is not elegant, but we cannot find a better way since the interface is not
    // provided.
    let mut buckets = Vec::new();
    let mut i = 0;
    while i < str_lines.len() {
        if str_lines[i].contains("bucket {") {
            let cumulative_count = str_lines[i + 1]
                .split(": ")
                .nth(1)
                .unwrap()
                .parse::<u64>()
                .unwrap();
            // empty bucket may appear
            if cumulative_count == 0 {
                i += 4;
                continue;
            }
            let upper_bound = str_lines[i + 2]
                .split(": ")
                .nth(1)
                .unwrap()
                .parse::<f64>()
                .unwrap();
            buckets.push((cumulative_count, upper_bound));
            i += 4;
        } else if let Some(count) = str_lines[i].split("sample_count: ").nth(1) {
            sample_count = count.parse::<u64>().unwrap();
            i += 1;
        } else if let Some(sum) = str_lines[i].split("sample_sum: ").nth(1) {
            sample_sum = sum.parse::<f64>().unwrap();
            i += 1;
        } else {
            i += 1;
        }
    }

    let get_quantile = |percent: f64| -> f64 {
        let thereshold = (sample_count as f64 * percent) as u64;
        for (count, upper_bound) in &buckets {
            if *count >= thereshold {
                return *upper_bound;
            }
        }
        0.0
    };

    let p50 = get_quantile(0.5);
    let p95 = get_quantile(0.95);
    let p99 = get_quantile(0.99);
    let p100 = match buckets.len() {
        0 => 0.0,
        _ => buckets[buckets.len() - 1].1,
    };

    let desc = histogram.desc()[0].fq_name.clone();

    println!("{desc} P50 : {p50} P95 : {p95} P99 : {p99} P100 : {p100} COUNT : {sample_count} SUM : {sample_sum}");
}

fn proc_counter(counter: GenericCounter<AtomicU64>) {
    let desc = counter.desc()[0].fq_name.clone();
    let metric = counter.metric().get_gauge().get_value();
    println!("{desc} COUNT : {metric}");
}

pub(crate) async fn print_statistics() {
    println!("STATISTICS:");

    let stat = DEFAULT_STATE_STORE_STATS.clone();

    // ----- TODO(Ting Sun): use macro to simplify the implementation -----
    proc_counter(stat.get_bytes.clone());
    proc_counter(stat.get_counts.clone());
    proc_counter(stat.put_bytes.clone());
    proc_counter(stat.range_scan_counts.clone());
    proc_counter(stat.batched_write_counts.clone());
    proc_counter(stat.batch_write_tuple_counts.clone());
    proc_counter(stat.iter_counts.clone());
    proc_counter(stat.iter_next_counts.clone());

    proc_histogram(stat.get_latency.clone());
    proc_histogram(stat.get_key_size.clone());
    proc_histogram(stat.get_value_size.clone());
    proc_histogram(stat.get_snapshot_latency.clone());
    proc_histogram(stat.batch_write_latency.clone());
    proc_histogram(stat.batch_write_size.clone());
    proc_histogram(stat.batch_write_build_table_latency.clone());
    proc_histogram(stat.batch_write_add_l0_latency.clone());
    proc_histogram(stat.iter_seek_latency.clone());
    proc_histogram(stat.iter_next_latency.clone());

    println!();
}
