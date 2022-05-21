// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::monitor::StateStoreMetrics;

#[derive(Default)]
pub struct StoreLocalStatistic {
    pub cache_data_block_miss: u64,
    pub cache_data_block_total: u64,
    pub cache_meta_block_miss: u64,
    pub cache_meta_block_total: u64,

    // include multiple versions of one key.
    pub scan_key_count: u64,
    pub processed_key_count: u64,
    pub bloom_filter_true_negative_count: u64,
    pub bloom_filter_might_positive_count: u64,
}

impl StoreLocalStatistic {
    pub fn add(&mut self, other: &StoreLocalStatistic) {
        self.cache_meta_block_miss += other.cache_data_block_miss;
        self.cache_meta_block_total += other.cache_meta_block_total;

        self.cache_data_block_miss += other.cache_data_block_miss;
        self.cache_data_block_total += other.cache_data_block_total;

        self.scan_key_count += other.scan_key_count;
        self.processed_key_count += other.processed_key_count;
        self.bloom_filter_true_negative_count += other.bloom_filter_true_negative_count;
        self.bloom_filter_might_positive_count += other.bloom_filter_might_positive_count;
    }

    pub fn report(&self, metrics: &StateStoreMetrics) {
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["data_total"])
            .inc_by(self.cache_data_block_total);
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["data_miss"])
            .inc_by(self.cache_data_block_miss);
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["meta_total"])
            .inc_by(self.cache_meta_block_total);
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["meta_miss"])
            .inc_by(self.cache_meta_block_miss);
        metrics
            .bloom_filter_true_negative_counts
            .inc_by(self.bloom_filter_true_negative_count);
        metrics
            .bloom_filter_might_positive_counts
            .inc_by(self.bloom_filter_might_positive_count);
    }
}
