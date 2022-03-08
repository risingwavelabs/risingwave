use prometheus::core::{AtomicU64, GenericCounter, Metric};
use prometheus::{
    histogram_opts, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, Registry,
};
use risingwave_storage::for_all_metrics;
use risingwave_storage::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

use crate::utils::my_histogram::MyHistogram;

// macro_rules! one_field {
//     ($name: ident, Histogram) => {
//         let metric = $name.metric();
//         let histogram = MyHistogram::from_prom_hist(metric.get_histogram());
//         // println!("{}", histogram);
//         histogram
//     };
//     ($name: ident, GenericCounter<AtomicU64>) => {
//         let metric = $name.metric();
//         let counter = metric.get_counter().get_value() as u64;
//         // println!("{}", counter);
//         counter
//     };
// }
macro_rules! one_field {
    (Histogram) => {
        MyHistogram
    };
    (GenericCounter < AtomicU64 >) => {
        MyCounter
    };
}

pub enum MyMetric {
    Histogram,
    GenericCounter,
}

// 写个枚举

// macro_rules! clone_my_state_store_stats {
//     ($( $name:ident: $type:ty ),* ,) => {
//         pub struct MyStateStoreStats {
//             $(
//                 // pub $name: $type,
//                 pub $name: MyMetric,
//             )*
//         }
//     }
// }

// for_all_metrics! { clone_my_state_store_stats }

#[derive(Clone, Default)]
pub(crate) struct MyStateStoreStats {
    pub(crate) batch_write_latency: MyHistogram,
    pub(crate) batch_write_size: MyHistogram,
}

impl MyStateStoreStats {
    pub(crate) fn from_prom_stats(stats: &StateStoreStats) -> Self {
        Self {
            batch_write_latency: MyHistogram::from_prom_hist(
                stats.batch_write_latency.metric().get_histogram(),
            ),
            batch_write_size: MyHistogram::from_prom_hist(
                stats.batch_write_size.metric().get_histogram(),
            ),
        }
    }
}
