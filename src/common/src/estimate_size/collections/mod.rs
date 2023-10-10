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

use super::{EstimateSize, KvSize};

mod heap;
pub mod lru;
pub use heap::*;
pub mod vecdeque;
pub use vecdeque::EstimatedVecDeque as VecDeque;
pub mod hashmap;
pub use hashmap::EstimatedHashMap as HashMap;

mod private {
    use super::*;

    /// A trait that dispatches the size update method regarding the mutability of the reference
    /// to the [`KvSize`].
    pub trait GenericKvSize {
        fn update_size(&mut self, from: usize, to: usize);
    }

    /// For mutable references, the size can be directly updated.
    impl GenericKvSize for &'_ mut KvSize {
        fn update_size(&mut self, from: usize, to: usize) {
            self.add_size(to);
            self.sub_size(from);
        }
    }

    /// For immutable references, the size is updated atomically.
    impl GenericKvSize for &'_ KvSize {
        fn update_size(&mut self, from: usize, to: usize) {
            self.update_size_atomic(from, to)
        }
    }
}

/// A guard holding a mutable reference to a value in a collection. When dropped, the size of the
/// collection will be updated.
pub struct MutGuard<'a, V, S = &'a mut KvSize>
where
    V: EstimateSize,
    S: private::GenericKvSize,
{
    inner: &'a mut V,
    // The size of the original value
    original_val_size: usize,
    // The total size of a collection
    total_size: S,
}

/// Similar to [`MutGuard`], but the size is updated atomically. Useful for creating shared
/// references to the entries in a collection.
pub type AtomicMutGuard<'a, V> = MutGuard<'a, V, &'a KvSize>;

impl<'a, V, S> MutGuard<'a, V, S>
where
    V: EstimateSize,
    S: private::GenericKvSize,
{
    pub fn new(inner: &'a mut V, total_size: S) -> Self {
        let original_val_size = inner.estimated_size();
        Self {
            inner,
            original_val_size,
            total_size,
        }
    }
}

impl<'a, V, S> Drop for MutGuard<'a, V, S>
where
    V: EstimateSize,
    S: private::GenericKvSize,
{
    fn drop(&mut self) {
        self.total_size
            .update_size(self.original_val_size, self.inner.estimated_size());
    }
}

impl<'a, V, S> Deref for MutGuard<'a, V, S>
where
    V: EstimateSize,
    S: private::GenericKvSize,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, V, S> DerefMut for MutGuard<'a, V, S>
where
    V: EstimateSize,
    S: private::GenericKvSize,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}
