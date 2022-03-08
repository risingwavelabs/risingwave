use prometheus::core::Metric;
use risingwave_storage::monitor::StateStoreStats;

use super::my_histogram::MyHistogram;

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
