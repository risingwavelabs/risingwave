// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};

use prometheus::local::{LocalHistogram, LocalIntCounter};
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedLocalIntCounter;
use risingwave_hummock_sdk::table_stats::TableStatsMap;

use super::HummockStateStoreMetrics;
use crate::monitor::CompactorMetrics;

thread_local!(static LOCAL_METRICS: RefCell<HashMap<u32,LocalStoreMetrics>> = RefCell::new(HashMap::default()));

#[derive(Default, Debug)]
pub struct StoreLocalStatistic {
    pub cache_data_block_miss: u64,
    pub cache_data_block_total: u64,
    pub cache_meta_block_miss: u64,
    pub cache_meta_block_total: u64,
    pub cache_data_prefetch_count: u64,
    pub cache_data_prefetch_block_count: u64,

    // include multiple versions of one key.
    pub total_key_count: u64,
    pub skip_multi_version_key_count: u64,
    pub skip_delete_key_count: u64,
    pub processed_key_count: u64,
    pub bloom_filter_true_negative_counts: u64,
    pub remote_io_time: Arc<AtomicU64>,
    pub bloom_filter_check_counts: u64,
    pub get_shared_buffer_hit_counts: u64,
    pub staging_imm_iter_count: u64,
    pub staging_sst_iter_count: u64,
    pub overlapping_iter_count: u64,
    pub non_overlapping_iter_count: u64,
    pub sub_iter_count: u64,
    pub found_key: bool,

    pub staging_imm_get_count: u64,
    pub staging_sst_get_count: u64,
    pub overlapping_get_count: u64,
    pub non_overlapping_get_count: u64,

    #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
    reported: AtomicBool,
    #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
    added: AtomicBool,

    /// The stats of key skipped by watermark for each table.
    /// Used by `SkipWatermarkIterator`.
    /// Generalize it in the future if there're other iterators that'll also drop keys.
    pub skipped_by_watermark_table_stats: TableStatsMap,
}

impl StoreLocalStatistic {
    pub fn add(&mut self, other: &StoreLocalStatistic) {
        self.add_count(other);
        self.add_histogram(other);
        self.bloom_filter_true_negative_counts += other.bloom_filter_true_negative_counts;
        self.remote_io_time.fetch_add(
            other.remote_io_time.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.bloom_filter_check_counts += other.bloom_filter_check_counts;

        #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
        if other.added.fetch_or(true, Ordering::Relaxed) || other.reported.load(Ordering::Relaxed) {
            tracing::error!("double added\n{:#?}", other);
        }
    }

    fn report(&self, metrics: &mut LocalStoreMetrics) {
        metrics.add_count(self);
        metrics.add_histogram(self);
        let t = self.remote_io_time.load(Ordering::Relaxed) as f64;
        if t > 0.0 {
            metrics.remote_io_time.observe(t / 1000.0);
        }

        metrics.collect_count += 1;
        if metrics.collect_count > FLUSH_LOCAL_METRICS_TIMES {
            metrics.flush();
            metrics.collect_count = 0;
        }
        #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
        if self.reported.fetch_or(true, Ordering::Relaxed) || self.added.load(Ordering::Relaxed) {
            tracing::error!("double reported\n{:#?}", self);
        }
    }

    pub fn discard(self) {
        #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
        {
            self.reported.fetch_or(true, Ordering::Relaxed);
        }
    }

    pub fn report_compactor(&self, metrics: &CompactorMetrics) {
        let t = self.remote_io_time.load(Ordering::Relaxed) as f64;
        if t > 0.0 {
            metrics.remote_read_time.observe(t / 1000.0);
        }
        if self.processed_key_count > 0 {
            metrics
                .iter_scan_key_counts
                .with_label_values(&["processed"])
                .inc_by(self.processed_key_count);
        }

        if self.skip_multi_version_key_count > 0 {
            metrics
                .iter_scan_key_counts
                .with_label_values(&["skip_multi_version"])
                .inc_by(self.skip_multi_version_key_count);
        }

        if self.skip_delete_key_count > 0 {
            metrics
                .iter_scan_key_counts
                .with_label_values(&["skip_delete"])
                .inc_by(self.skip_delete_key_count);
        }

        if self.total_key_count > 0 {
            metrics
                .iter_scan_key_counts
                .with_label_values(&["total"])
                .inc_by(self.total_key_count);
        }

        #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
        if self.reported.fetch_or(true, Ordering::Relaxed) || self.added.load(Ordering::Relaxed) {
            tracing::error!("double reported\n{:#?}", self);
        }
    }

    fn report_bloom_filter_metrics(&self, metrics: &BloomFilterLocalMetrics) {
        if self.bloom_filter_check_counts == 0 {
            return;
        }
        // checks SST bloom filters
        metrics
            .bloom_filter_true_negative_counts
            .inc_by(self.bloom_filter_true_negative_counts);
        metrics
            .bloom_filter_check_counts
            .inc_by(self.bloom_filter_check_counts);
        metrics.read_req_check_bloom_filter_counts.inc();

        if self.bloom_filter_check_counts > self.bloom_filter_true_negative_counts {
            if !self.found_key {
                // false positive
                // checks SST bloom filters (at least one bloom filter return true) but returns
                // nothing
                metrics.read_req_positive_but_non_exist_counts.inc();
            }
            // positive
            // checks SST bloom filters and at least one bloom filter returns positive
            metrics.read_req_bloom_filter_positive_counts.inc();
        }
    }

    pub fn flush_all() {
        LOCAL_METRICS.with_borrow_mut(|local_metrics| {
            for metrics in local_metrics.values_mut() {
                if metrics.collect_count > 0 {
                    metrics.flush();
                    metrics.collect_count = 0;
                }
            }
        });
    }

    pub fn ignore(&self) {
        #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
        self.reported.store(true, Ordering::Relaxed);
    }

    #[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
    fn need_report(&self) -> bool {
        self.cache_data_block_miss != 0
            || self.cache_data_block_total != 0
            || self.cache_meta_block_miss != 0
            || self.cache_meta_block_total != 0
            || self.cache_data_prefetch_count != 0
            || self.skip_multi_version_key_count != 0
            || self.skip_delete_key_count != 0
            || self.processed_key_count != 0
            || self.bloom_filter_true_negative_counts != 0
            || self.remote_io_time.load(Ordering::Relaxed) != 0
            || self.bloom_filter_check_counts != 0
    }
}

#[cfg(all(debug_assertions, not(any(madsim, test, feature = "test"))))]
impl Drop for StoreLocalStatistic {
    fn drop(&mut self) {
        if !self.reported.load(Ordering::Relaxed)
            && !self.added.load(Ordering::Relaxed)
            && self.need_report()
        {
            tracing::error!("local stats lost!\n{:#?}", self);
        }
    }
}

struct LocalStoreMetrics {
    cache_data_block_total: LabelGuardedLocalIntCounter,
    cache_data_block_miss: LabelGuardedLocalIntCounter,
    cache_meta_block_total: LabelGuardedLocalIntCounter,
    cache_meta_block_miss: LabelGuardedLocalIntCounter,
    cache_data_prefetch_count: LabelGuardedLocalIntCounter,
    cache_data_prefetch_block_count: LabelGuardedLocalIntCounter,
    remote_io_time: LocalHistogram,
    processed_key_count: LabelGuardedLocalIntCounter,
    skip_multi_version_key_count: LabelGuardedLocalIntCounter,
    skip_delete_key_count: LabelGuardedLocalIntCounter,
    total_key_count: LabelGuardedLocalIntCounter,
    get_shared_buffer_hit_counts: LocalIntCounter,
    staging_imm_iter_count: LocalHistogram,
    staging_sst_iter_count: LocalHistogram,
    overlapping_iter_count: LocalHistogram,
    non_overlapping_iter_count: LocalHistogram,
    sub_iter_count: LocalHistogram,
    iter_filter_metrics: BloomFilterLocalMetrics,
    get_filter_metrics: BloomFilterLocalMetrics,
    collect_count: usize,

    staging_imm_get_count: LocalHistogram,
    staging_sst_get_count: LocalHistogram,
    overlapping_get_count: LocalHistogram,
    non_overlapping_get_count: LocalHistogram,
}

const FLUSH_LOCAL_METRICS_TIMES: usize = 32;

impl LocalStoreMetrics {
    pub fn new(metrics: &HummockStateStoreMetrics, table_id_label: &str) -> Self {
        let cache_data_block_total = metrics
            .sst_store_block_request_counts
            .with_guarded_label_values(&[table_id_label, "data_total"])
            .local();

        let cache_data_block_miss = metrics
            .sst_store_block_request_counts
            .with_guarded_label_values(&[table_id_label, "data_miss"])
            .local();

        let cache_meta_block_total = metrics
            .sst_store_block_request_counts
            .with_guarded_label_values(&[table_id_label, "meta_total"])
            .local();
        let cache_data_prefetch_count = metrics
            .sst_store_block_request_counts
            .with_guarded_label_values(&[table_id_label, "prefetch_count"])
            .local();
        let cache_data_prefetch_block_count = metrics
            .sst_store_block_request_counts
            .with_guarded_label_values(&[table_id_label, "prefetch_data_count"])
            .local();

        let cache_meta_block_miss = metrics
            .sst_store_block_request_counts
            .with_guarded_label_values(&[table_id_label, "meta_miss"])
            .local();

        let remote_io_time = metrics
            .remote_read_time
            .with_label_values(&[table_id_label])
            .local();

        let processed_key_count = metrics
            .iter_scan_key_counts
            .with_guarded_label_values(&[table_id_label, "processed"])
            .local();

        let skip_multi_version_key_count = metrics
            .iter_scan_key_counts
            .with_guarded_label_values(&[table_id_label, "skip_multi_version"])
            .local();

        let skip_delete_key_count = metrics
            .iter_scan_key_counts
            .with_guarded_label_values(&[table_id_label, "skip_delete"])
            .local();

        let total_key_count = metrics
            .iter_scan_key_counts
            .with_guarded_label_values(&[table_id_label, "total"])
            .local();

        let get_shared_buffer_hit_counts = metrics
            .get_shared_buffer_hit_counts
            .with_label_values(&[table_id_label])
            .local();

        let staging_imm_iter_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "staging-imm-iter"])
            .local();
        let staging_sst_iter_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "staging-sst-iter"])
            .local();
        let overlapping_iter_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "committed-overlapping-iter"])
            .local();
        let non_overlapping_iter_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "committed-non-overlapping-iter"])
            .local();
        let sub_iter_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "sub-iter"])
            .local();
        let get_filter_metrics = BloomFilterLocalMetrics::new(metrics, table_id_label, "get");
        let iter_filter_metrics = BloomFilterLocalMetrics::new(metrics, table_id_label, "iter");

        let staging_imm_get_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "staging-imm-get"])
            .local();
        let staging_sst_get_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "staging-sst-get"])
            .local();
        let overlapping_get_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "committed-overlapping-get"])
            .local();
        let non_overlapping_get_count = metrics
            .iter_merge_sstable_counts
            .with_label_values(&[table_id_label, "committed-non-overlapping-get"])
            .local();

        Self {
            cache_data_block_total,
            cache_data_block_miss,
            cache_meta_block_total,
            cache_meta_block_miss,
            cache_data_prefetch_count,
            cache_data_prefetch_block_count,
            remote_io_time,
            processed_key_count,
            skip_multi_version_key_count,
            skip_delete_key_count,
            total_key_count,
            get_shared_buffer_hit_counts,
            staging_imm_iter_count,
            staging_sst_iter_count,
            overlapping_iter_count,
            sub_iter_count,
            non_overlapping_iter_count,
            get_filter_metrics,
            iter_filter_metrics,
            collect_count: 0,
            staging_imm_get_count,
            staging_sst_get_count,
            overlapping_get_count,
            non_overlapping_get_count,
        }
    }

    pub fn flush(&mut self) {
        self.remote_io_time.flush();
        self.iter_filter_metrics.flush();
        self.get_filter_metrics.flush();
        self.flush_histogram();
        self.flush_count();
    }
}

macro_rules! add_local_metrics_histogram {
    ($($x:ident),*) => (
        impl LocalStoreMetrics {
            fn add_histogram(&self, stats: &StoreLocalStatistic) {
                $(
                    self.$x.observe(stats.$x as f64);
                )*
            }

            fn flush_histogram(&mut self) {
                $(
                    self.$x.flush();
                )*
            }
        }

        impl StoreLocalStatistic {
            fn add_histogram(&mut self, other: &StoreLocalStatistic) {
                $(
                    self.$x += other.$x;
                )*
            }
        }
    )
}

add_local_metrics_histogram!(
    staging_imm_iter_count,
    staging_sst_iter_count,
    overlapping_iter_count,
    non_overlapping_iter_count,
    sub_iter_count,
    staging_imm_get_count,
    staging_sst_get_count,
    overlapping_get_count,
    non_overlapping_get_count
);

macro_rules! add_local_metrics_count {
    ($($x:ident),*) => (
        impl LocalStoreMetrics {
            fn add_count(&self, stats: &StoreLocalStatistic) {
                $(
                    self.$x.inc_by(stats.$x);
                )*
            }

            fn flush_count(&mut self) {
                $(
                    self.$x.flush();
                )*
            }
        }

        impl StoreLocalStatistic {
            fn add_count(&mut self, other: &StoreLocalStatistic) {
                $(
                    self.$x += other.$x;
                )*
            }
        }
    )
}

add_local_metrics_count!(
    cache_data_block_total,
    cache_data_block_miss,
    cache_meta_block_total,
    cache_meta_block_miss,
    cache_data_prefetch_count,
    cache_data_prefetch_block_count,
    skip_multi_version_key_count,
    skip_delete_key_count,
    get_shared_buffer_hit_counts,
    total_key_count,
    processed_key_count
);

macro_rules! define_bloom_filter_metrics {
    ($($x:ident),*) => (
        struct BloomFilterLocalMetrics {
            $($x: LabelGuardedLocalIntCounter,)*
        }

        impl BloomFilterLocalMetrics {
            pub fn new(metrics: &HummockStateStoreMetrics, table_id_label: &str, oper_type: &str) -> Self {
                // checks SST bloom filters
                Self {
                    $($x: metrics.$x.with_guarded_label_values(&[table_id_label, oper_type]).local(),)*
                }
            }

            pub fn flush(&mut self) {
                $(
                    self.$x.flush();
                )*
            }
        }
    )
}

define_bloom_filter_metrics!(
    read_req_check_bloom_filter_counts,
    bloom_filter_check_counts,
    bloom_filter_true_negative_counts,
    read_req_positive_but_non_exist_counts,
    read_req_bloom_filter_positive_counts
);

pub struct GetLocalMetricsGuard {
    metrics: Arc<HummockStateStoreMetrics>,
    table_id: TableId,
    pub local_stats: StoreLocalStatistic,
}

impl GetLocalMetricsGuard {
    pub fn new(metrics: Arc<HummockStateStoreMetrics>, table_id: TableId) -> Self {
        Self {
            metrics,
            table_id,
            local_stats: StoreLocalStatistic::default(),
        }
    }
}

impl Drop for GetLocalMetricsGuard {
    fn drop(&mut self) {
        LOCAL_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics
                .entry(self.table_id.table_id)
                .or_insert_with(|| {
                    LocalStoreMetrics::new(
                        self.metrics.as_ref(),
                        self.table_id.to_string().as_str(),
                    )
                });
            self.local_stats.report(table_metrics);
            self.local_stats
                .report_bloom_filter_metrics(&table_metrics.get_filter_metrics);
        });
    }
}

pub struct IterLocalMetricsGuard {
    metrics: Arc<HummockStateStoreMetrics>,
    table_id: TableId,
    pub local_stats: StoreLocalStatistic,
}

impl IterLocalMetricsGuard {
    pub fn new(
        metrics: Arc<HummockStateStoreMetrics>,
        table_id: TableId,
        local_stats: StoreLocalStatistic,
    ) -> Self {
        Self {
            metrics,
            table_id,
            local_stats,
        }
    }
}

impl Drop for IterLocalMetricsGuard {
    fn drop(&mut self) {
        LOCAL_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics
                .entry(self.table_id.table_id)
                .or_insert_with(|| {
                    LocalStoreMetrics::new(
                        self.metrics.as_ref(),
                        self.table_id.to_string().as_str(),
                    )
                });
            self.local_stats.report(table_metrics);
            self.local_stats
                .report_bloom_filter_metrics(&table_metrics.iter_filter_metrics);
        });
    }
}
