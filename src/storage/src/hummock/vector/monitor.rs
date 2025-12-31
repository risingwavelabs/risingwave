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

use risingwave_common::catalog::TableId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};

use crate::monitor::HummockStateStoreMetrics;
use crate::vector::hnsw::HnswStats;

macro_rules! for_all_cache_counter {
    ($($name:ident),+) => {
        struct VectorStoreCacheMetrics {
            $(
                $name: LabelGuardedIntCounter,
            )+
        }

        impl VectorStoreCacheMetrics {

            fn new(metrics: &HummockStateStoreMetrics, table_id: TableId, mode: &str) -> Self {
                let table_id_label = format!("{}", table_id);
                $(
                    let $name = metrics
                        .vector_object_request_counts
                        .with_guarded_label_values(&[table_id_label.as_str(), stringify!($name), mode]);
                )+

                Self {
                    $($name,)+
                }
            }

            fn report(&self, stat: VectorStoreCacheStats) {
                $(
                    self.$name.inc_by(stat.$name);
                )+
            }
        }

        #[derive(Default)]
        pub struct VectorStoreCacheStats {
            $(pub $name: u64,)+
        }
    };
    () => {
        for_all_cache_counter! {
            file_block_total, file_block_miss, file_meta_total, file_meta_miss, hnsw_graph_total, hnsw_graph_miss
        }
    }
}

for_all_cache_counter!();

impl VectorStoreCacheStats {
    pub fn report(self, table_id: TableId, mode: &'static str, metrics: &HummockStateStoreMetrics) {
        type StatsType = HashMap<(TableId, &'static str), VectorStoreCacheMetrics>;
        thread_local! {
            static THREAD_STATS: RefCell<StatsType> = RefCell::new(HashMap::new());
        }

        THREAD_STATS.with_borrow_mut(move |global| {
            let metrics = global
                .entry((table_id, mode))
                .or_insert_with(|| VectorStoreCacheMetrics::new(metrics, table_id, mode));
            metrics.report(self)
        });
    }
}

struct HnswMetrics {
    distances_computed: LabelGuardedHistogram,
    n_hops: LabelGuardedHistogram,
}

impl HnswMetrics {
    fn new(
        metrics: &HummockStateStoreMetrics,
        table_id: TableId,
        mode: &'static str,
        top_n: usize,
        ef: usize,
    ) -> Self {
        let table_id_label = format!("{}", table_id);
        let top_n_label = format!("{}", top_n);
        let ef_label = format!("{}", ef);
        let distances_computed = metrics.vector_request_stats.with_guarded_label_values(&[
            table_id_label.as_str(),
            "distances_computed",
            mode,
            top_n_label.as_str(),
            ef_label.as_str(),
        ]);
        let n_hops = metrics.vector_request_stats.with_guarded_label_values(&[
            table_id_label.as_str(),
            "n_hops",
            mode,
            top_n_label.as_str(),
            ef_label.as_str(),
        ]);
        Self {
            distances_computed,
            n_hops,
        }
    }
}

pub fn report_hnsw_stat(
    metrics: &HummockStateStoreMetrics,
    table_id: TableId,
    mode: &'static str,
    top_n: usize,
    ef: usize,
    stats: impl IntoIterator<Item = HnswStats>,
) {
    type StatsType = HashMap<(TableId, &'static str, usize, usize), HnswMetrics>;
    thread_local! {
        static THREAD_STATS: RefCell<StatsType> = RefCell::new(HashMap::new());
    }

    THREAD_STATS.with_borrow_mut(move |global| {
        let metrics = global
            .entry((table_id, mode, top_n, ef))
            .or_insert_with(|| HnswMetrics::new(metrics, table_id, mode, top_n, ef));
        let distance_computed = metrics.distances_computed.local();
        let n_hop = metrics.n_hops.local();
        for stat in stats {
            distance_computed.observe(stat.distances_computed as _);
            n_hop.observe(stat.n_hops as _);
        }
    });
}
