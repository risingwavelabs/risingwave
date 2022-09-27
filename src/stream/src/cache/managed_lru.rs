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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::{DefaultHasher, LruCache};

pub struct ManagedLruCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    pub(super) inner: LruCache<K, V, S, A>,
    /// The entry with epoch less than water should be evicted.
    pub(super) watermark_epoch: Arc<AtomicU64>,
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> ManagedLruCache<K, V, S, A> {
    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        let epoch = self.watermark_epoch.load(Ordering::Relaxed);
        self.inner.evict_by_epoch(epoch);
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

impl<K, V, S, A: Clone + Allocator> Deref for ManagedLruCache<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for ManagedLruCache<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
