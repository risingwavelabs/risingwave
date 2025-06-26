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
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::hummock::{GLOBAL_RECENT_FILTER_METRICS, RecentFilterTrait};

pub struct SimpleRecentFilter<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    layers: RwLock<Layers<T>>,
    /// refresh interval
    refresh: Duration,
}

struct Layers<T> {
    current: RwLock<HashSet<T>>,
    others: VecDeque<HashSet<T>>,
    /// Last updated time.
    updated: Instant,
}

impl<T> Debug for SimpleRecentFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleRecentFilter").finish()
    }
}

impl<T> Clone for SimpleRecentFilter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Inner<T> {
    fn maybe_rotate(&self) {
        if let Some(mut layers) = self.layers.try_write() {
            if layers.updated.elapsed() >= self.refresh {
                let mut current = HashSet::new();
                std::mem::swap(&mut current, &mut layers.current.write());
                let old = layers.others.pop_back();
                let diff = current.len() - old.map(|s| s.len()).unwrap_or_default();
                layers.others.push_front(current);
                layers.updated = Instant::now();
                GLOBAL_RECENT_FILTER_METRICS
                    .recent_filter_items
                    .add(diff as _);
            }
        }
    }
}

impl<T> SimpleRecentFilter<T> {
    pub fn new(layers: usize, refresh: Duration) -> Self {
        assert!(
            layers > 0,
            "simple recent filter must have at least one layer"
        );
        let layers = Layers {
            current: RwLock::new(HashSet::new()),
            others: std::iter::repeat_with(|| HashSet::new())
                .take(layers - 1)
                .collect(),
            updated: Instant::now(),
        };
        let inner = Inner {
            layers: RwLock::new(layers),
            refresh,
        };
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<T> RecentFilterTrait for SimpleRecentFilter<T>
where
    T: Eq + Hash,
{
    type Item = T;

    fn insert(&self, item: Self::Item)
    where
        Self::Item: Eq + Hash,
    {
        self.inner.maybe_rotate();
        self.inner.layers.read().current.write().insert(item);
        GLOBAL_RECENT_FILTER_METRICS.recent_filter_inserts.inc();
    }

    fn extend(&self, iter: impl IntoIterator<Item = Self::Item>)
    where
        Self::Item: Eq + Hash,
    {
        self.inner.maybe_rotate();
        let mut cnt = 0;
        self.inner
            .layers
            .read()
            .current
            .write()
            .extend(iter.into_iter().inspect(|_| cnt += 1));
        GLOBAL_RECENT_FILTER_METRICS
            .recent_filter_inserts
            .inc_by(cnt);
    }

    fn contains<Q>(&self, item: &Q) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let layers = self.inner.layers.read();
        if layers.current.read().contains(item) {
            GLOBAL_RECENT_FILTER_METRICS.recent_filter_hit.inc();
            return true;
        }
        for layer in &layers.others {
            if layer.contains(item) {
                GLOBAL_RECENT_FILTER_METRICS.recent_filter_hit.inc();
                return true;
            }
        }

        GLOBAL_RECENT_FILTER_METRICS.recent_filter_miss.inc();
        false
    }

    fn contains_any<'a, Q>(&self, iter: impl IntoIterator<Item = &'a Q>) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + 'a,
    {
        let layers = self.inner.layers.read();
        {
            let current = layers.current.read();
            for item in iter {
                if current.contains(item) {
                    GLOBAL_RECENT_FILTER_METRICS.recent_filter_hit.inc();
                    return true;
                }
                for layer in &layers.others {
                    if layer.contains(item) {
                        GLOBAL_RECENT_FILTER_METRICS.recent_filter_hit.inc();
                        return true;
                    }
                }
            }
        }

        GLOBAL_RECENT_FILTER_METRICS.recent_filter_miss.inc();
        false
    }
}
