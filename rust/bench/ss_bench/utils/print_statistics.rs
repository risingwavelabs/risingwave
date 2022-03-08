use prometheus::core::{AtomicU64, Collector, GenericCounter, Metric};
use prometheus::Histogram;
use risingwave_storage::for_all_metrics;

use crate::utils::my_metrics::MyHistogram;

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
        pub(crate) fn print_statistics(stats: &risingwave_storage::monitor::StateStoreStats) {
            println!("STATISTICS:");
            // print for all fields
            $( stats.$name.print(); )*
            println!();
        }
    }
}
for_all_metrics! { print_statistics }
