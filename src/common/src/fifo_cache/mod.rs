// Copyright 2024 RisingWave Labs
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

use std::hash::{DefaultHasher, Hash, Hasher};

mod cache;
mod ghost;
mod most;
mod small;

pub use cache::{FIFOCache, LookupResponse};

pub trait CacheKey: Eq + Send + Hash + Clone {}
impl<T: Eq + Send + Hash + Clone> CacheKey for T {}
pub trait CacheValue: Send + Clone {}
impl<T: Send + Clone> CacheValue for T {}

const KIND_MAIN: usize = 8;

const KIND_SMALL: usize = 4;

pub struct CacheItem<K: CacheKey, V: CacheValue> {
    pub key: K,
    pub value: V,
    pub flag: usize,
}

impl<K: CacheKey, V: CacheValue> CacheItem<K, V> {
    pub fn new(key: K, value: V, cost: usize) -> Self {
        Self {
            key,
            value,
            flag: cost << 4,
        }
    }

    #[inline(always)]
    pub fn get_freq(&self) -> usize {
        self.flag & 3
    }

    pub fn inc_freq(&mut self) {
        let freq = self.get_freq();
        if freq < 3 {
            self.flag += 1;
        }
    }

    pub fn reset_freq(&mut self) {
        let freq = self.get_freq();
        self.flag -= freq;
    }

    pub fn dec_freq(&mut self) -> bool {
        let freq = self.get_freq();
        if freq > 0 {
            self.flag -= 1;
            return true;
        }
        false
    }

    pub fn unmark(&mut self) {
        self.flag = set_kind(self.flag, 0);
    }

    pub fn mark_main(&mut self) {
        self.flag = set_kind(self.flag, KIND_MAIN);
    }

    pub fn mark_small(&mut self) {
        self.flag = set_kind(self.flag, KIND_SMALL);
    }

    pub fn cost(&self) -> usize {
        self.flag >> 4
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::default();
        self.key.hash(&mut hasher);
        hasher.finish()
    }
}

const KIND_MASK: usize = usize::MAX - 12;
fn set_kind(flag: usize, val: usize) -> usize {
    (flag & KIND_MASK) | val
}
