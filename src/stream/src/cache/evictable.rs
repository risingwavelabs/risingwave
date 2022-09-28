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

use std::alloc::{Allocator, Global};
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};

use lru::{DefaultHasher, LruCache};

/// A wrapper for [`LruCache`] which provides manual eviction.
pub struct EvictableHashMap<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    pub(super) inner: LruCache<K, V, S, A>,

    /// Target capacity to keep when calling `evict_to_target_cap`.
    target_cap: usize,
}

impl<K: Hash + Eq, V, A: Clone + Allocator> EvictableHashMap<K, V, DefaultHasher, A> {
    /// Create a [`EvictableHashMap`] with the given target capacity and allocator.
    pub fn new_in(target_cap: usize, alloc: A) -> Self {
        Self::with_hasher_in(target_cap, DefaultHasher::new(), alloc)
    }
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> EvictableHashMap<K, V, S, A> {
    /// Create a [`EvictableHashMap`] with the given target capacity, hasher and allocator.
    pub fn with_hasher_in(target_cap: usize, hasher: S, alloc: A) -> Self {
        Self {
            inner: LruCache::unbounded_with_hasher_in(hasher, alloc),
            target_cap,
        }
    }
}

impl<K: Hash + Eq, V> EvictableHashMap<K, V> {
    /// Create a [`EvictableHashMap`] with the given target capacity.
    pub fn new(target_cap: usize) -> EvictableHashMap<K, V> {
        EvictableHashMap::with_hasher(target_cap, DefaultHasher::new())
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> EvictableHashMap<K, V, S> {
    /// Create a [`EvictableHashMap`] with the given target capacity and haser.
    pub fn with_hasher(target_cap: usize, hasher: S) -> Self {
        Self {
            inner: LruCache::unbounded_with_hasher(hasher),
            target_cap,
        }
    }
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> EvictableHashMap<K, V, S, A> {
    pub fn target_cap(&self) -> usize {
        self.target_cap
    }

    /// Evict items in the map and only keep up-to `target_cap` items.
    pub fn evict_to_target_cap(&mut self) {
        self.inner.resize(self.target_cap);
        self.inner.resize(usize::MAX);
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_k, v)| v)
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.iter_mut().map(|(_k, v)| v)
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for EvictableHashMap<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for EvictableHashMap<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
