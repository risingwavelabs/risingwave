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

use std::borrow::Borrow;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    IntGauge, Registry, register_int_counter_vec_with_registry, register_int_gauge_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

pub static GLOBAL_RECENT_FILTER_METRICS: LazyLock<RecentFilterMetrics> =
    LazyLock::new(|| RecentFilterMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct RecentFilterMetrics {
    pub recent_filter_items: IntGauge,
    pub recent_filter_ops: GenericCounterVec<AtomicU64>,
    pub recent_filter_inserts: GenericCounter<AtomicU64>,
    pub recent_filter_queries: GenericCounter<AtomicU64>,
}

impl RecentFilterMetrics {
    fn new(registry: &Registry) -> Self {
        let recent_filter_items = register_int_gauge_with_registry!(
            "recent_filter_items",
            "Item numbers of the recent filter.",
            registry
        )
        .unwrap();

        let recent_filter_ops = register_int_counter_vec_with_registry!(
            "recent_filter_ops",
            "Ops of the recent filter.",
            &["op"],
            registry
        )
        .unwrap();

        let recent_filter_inserts = recent_filter_ops.with_label_values(&["insert"]);
        let recent_filter_queries = recent_filter_ops.with_label_values(&["query"]);

        Self {
            recent_filter_items,
            recent_filter_ops,
            recent_filter_inserts,
            recent_filter_queries,
        }
    }
}

#[derive(Debug)]
pub struct RecentFilter<K> {
    refresh_interval: Duration,
    inner: RwLock<RecentFilterInner<K>>,
}

#[derive(Debug)]
struct RecentFilterInner<K> {
    last_refresh: Instant,
    layers: VecDeque<RwLock<HashSet<K>>>,
}

impl<K> RecentFilterInner<K> {
    fn try_rotate(&mut self, refresh_interval: &Duration) {
        if &self.last_refresh.elapsed() > refresh_interval {
            if let Some(removed) = self.layers.pop_front() {
                GLOBAL_RECENT_FILTER_METRICS
                    .recent_filter_items
                    .sub(removed.read().len() as i64);
            }

            if let Some(latest) = self.layers.back() {
                GLOBAL_RECENT_FILTER_METRICS
                    .recent_filter_items
                    .add(latest.read().len() as i64);
            }

            self.layers.push_back(RwLock::new(HashSet::new()));
            self.last_refresh = Instant::now();
        }
    }
}

impl<K> RecentFilter<K> {
    pub fn new(layers: usize, refresh_interval: Duration) -> Self {
        assert!(layers > 0);
        let layers = (0..layers)
            .map(|_| HashSet::new())
            .map(RwLock::new)
            .collect();
        let inner = RecentFilterInner {
            last_refresh: Instant::now(),
            layers,
        };
        let inner = RwLock::new(inner);
        Self {
            refresh_interval,
            inner,
        }
    }

    pub fn insert(&self, key: K)
    where
        K: Hash + Eq,
    {
        if let Some(mut inner) = self.inner.try_write() {
            inner.try_rotate(&self.refresh_interval);
        }

        {
            let inner = self.inner.read();
            inner.layers.back().unwrap().write().insert(key);
        }

        GLOBAL_RECENT_FILTER_METRICS.recent_filter_inserts.inc();
    }

    pub fn extend(&self, keys: impl IntoIterator<Item = K>)
    where
        K: Hash + Eq,
    {
        if let Some(mut inner) = self.inner.try_write() {
            inner.try_rotate(&self.refresh_interval);
        }

        let len = {
            let mut len = 0;
            let inner = self.inner.read();
            let mut guard = inner.layers.back().unwrap().write();
            for key in keys {
                guard.insert(key);
                len += 1;
            }
            len
        };

        GLOBAL_RECENT_FILTER_METRICS
            .recent_filter_inserts
            .inc_by(len);
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq,
    {
        GLOBAL_RECENT_FILTER_METRICS.recent_filter_queries.inc();

        let inner = self.inner.read();
        for layer in inner.layers.iter().rev() {
            if layer.read().contains(key) {
                return true;
            }
        }
        false
    }

    pub fn contains_one<'a, Q>(&self, keys: impl Iterator<Item = &'a Q> + Clone) -> bool
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq + 'a,
    {
        GLOBAL_RECENT_FILTER_METRICS.recent_filter_queries.inc();

        let inner = self.inner.read();
        for layer in inner.layers.iter().rev() {
            for key in keys.clone() {
                if layer.read().contains(key) {
                    return true;
                }
            }
        }
        false
    }
}
