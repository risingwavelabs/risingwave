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

use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use super::{EstimateSize, KvSize};

mod heap;
pub mod lru;
pub use heap::*;

pub struct MutGuard<'a, V: EstimateSize> {
    inner: &'a mut V,
    // The size of the original value
    original_val_size: usize,
    // The total size of a collection
    total_size: &'a mut KvSize,
}

impl<'a, V: EstimateSize> MutGuard<'a, V> {
    pub fn new(inner: &'a mut V, total_size: &'a mut KvSize) -> Self {
        let original_val_size = inner.estimated_size();
        Self {
            inner,
            original_val_size,
            total_size,
        }
    }
}

impl<'a, V: EstimateSize> Drop for MutGuard<'a, V> {
    fn drop(&mut self) {
        self.total_size.add_size(self.inner.estimated_size());
        self.total_size.sub_size(self.original_val_size);
    }
}

impl<'a, V: EstimateSize> Deref for MutGuard<'a, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, V: EstimateSize> DerefMut for MutGuard<'a, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

pub struct UnsafeMutGuard<V: EstimateSize> {
    inner: NonNull<V>,
    // The size of the original value
    original_val_size: usize,
    // The total size of a collection
    total_size: NonNull<KvSize>,
}

impl<V: EstimateSize> UnsafeMutGuard<V> {
    pub fn new(inner: &mut V, total_size: &mut KvSize) -> Self {
        let original_val_size = inner.estimated_size();
        Self {
            inner: inner.into(),
            original_val_size,
            total_size: total_size.into(),
        }
    }

    /// # Safety
    ///
    /// 1. Only 1 `MutGuard` should be held for each value.
    /// 2. The returned `MutGuard` should not be moved to other threads.
    pub unsafe fn as_mut_guard<'a>(&mut self) -> MutGuard<'a, V> {
        MutGuard {
            inner: self.inner.as_mut(),
            original_val_size: self.original_val_size,
            total_size: self.total_size.as_mut(),
        }
    }
}
