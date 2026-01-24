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
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hash};
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;

use crate::hummock::RecentFilterTrait;
use crate::hummock::simple::SimpleRecentFilter;

struct Inner<T> {
    shards: Vec<SimpleRecentFilter<T>>,
    build_hasher: BuildHasherDefault<ahash::AHasher>,
}

pub struct ShardedRecentFilter<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Debug for ShardedRecentFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedRecentFilter").finish()
    }
}

impl<T> Clone for ShardedRecentFilter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> ShardedRecentFilter<T> {
    pub fn new(layers: usize, refresh: Duration, shards: usize) -> Self {
        let shards = std::iter::repeat_with(|| SimpleRecentFilter::new(layers, refresh))
            .take(shards)
            .collect_vec();
        let build_hasher = BuildHasherDefault::<ahash::AHasher>::default();
        let inner = Arc::new(Inner {
            shards,
            build_hasher,
        });
        Self { inner }
    }

    fn shard<Q>(&self, item: &Q) -> usize
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.inner.build_hasher.hash_one(item) as usize % self.inner.shards.len()
    }
}

impl<T> RecentFilterTrait for ShardedRecentFilter<T>
where
    T: Hash + Eq,
{
    type Item = T;

    fn insert(&self, item: Self::Item)
    where
        Self::Item: Eq + Hash,
    {
        self.inner.shards[self.shard(&item)].insert(item);
    }

    fn extend(&self, iter: impl IntoIterator<Item = Self::Item>)
    where
        Self::Item: Eq + Hash,
    {
        iter.into_iter()
            .map(|item| (self.shard(&item), item))
            .into_group_map()
            .into_iter()
            .for_each(|(shard, items)| {
                self.inner.shards[shard].extend(items);
            });
    }

    fn contains<Q>(&self, item: &Q) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.inner.shards[self.shard(item)].contains(item)
    }

    fn contains_any<'a, Q>(&self, iter: impl IntoIterator<Item = &'a Q>) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + 'a,
    {
        iter.into_iter()
            .map(|item| (self.shard(item), item))
            .into_group_map()
            .into_iter()
            .any(|(shard, items)| self.inner.shards[shard].contains_any(items))
    }
}
