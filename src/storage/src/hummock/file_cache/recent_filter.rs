// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

pub trait RecentFilterKey = Eq + Ord + Send + Sync + Debug + Clone + 'static;

#[derive(Debug)]
pub struct RecentFilter<K>
where
    K: RecentFilterKey,
{
    refresh_interval: Duration,
    inner: RwLock<CacheRefillFilterInner<K>>,
}

#[derive(Debug)]
struct CacheRefillFilterInner<K>
where
    K: RecentFilterKey,
{
    last_refresh: Instant,
    layers: VecDeque<RwLock<BTreeSet<K>>>,
}

impl<K> RecentFilter<K>
where
    K: RecentFilterKey,
{
    pub fn new(layers: usize, refresh_interval: Duration) -> Self {
        assert!(layers > 0);
        let layers = (0..layers)
            .map(|_| BTreeSet::new())
            .map(RwLock::new)
            .collect();
        let inner = CacheRefillFilterInner {
            last_refresh: Instant::now(),
            layers,
        };
        let inner = RwLock::new(inner);
        Self {
            refresh_interval,
            inner,
        }
    }

    pub fn insert(&self, key: K) {
        if let Some(mut inner) = self.inner.try_write() {
            if inner.last_refresh.elapsed() > self.refresh_interval {
                inner.layers.pop_front();
                inner.layers.push_back(RwLock::new(BTreeSet::new()));
                inner.last_refresh = Instant::now();
            }
        }

        let inner = self.inner.read();
        inner.layers.back().unwrap().write().insert(key);
    }

    pub fn contains(&self, key: &K) -> bool {
        let inner = self.inner.read();
        for layer in inner.layers.iter().rev() {
            if layer.read().contains(key) {
                return true;
            }
        }
        false
    }
}
