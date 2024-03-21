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

use crate::EstimateSize;

#[derive(Clone)]
pub struct VecWithKvSize<T: EstimateSize> {
    inner: Vec<T>,
    kv_heap_size: usize,
}

impl<T: EstimateSize> Default for VecWithKvSize<T> {
    fn default() -> Self {
        Self {
            inner: vec![],
            kv_heap_size: 0,
        }
    }
}

impl<T: EstimateSize> VecWithKvSize<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_kv_size(&self) -> usize {
        self.kv_heap_size
    }

    pub fn push(&mut self, value: T) {
        self.kv_heap_size = self
            .kv_heap_size
            .saturating_add(value.estimated_heap_size());
        self.inner.push(value);
    }

    pub fn into_inner(self) -> Vec<T> {
        self.inner
    }

    pub fn inner(&self) -> &Vec<T> {
        &self.inner
    }
}

impl<T: EstimateSize> IntoIterator for VecWithKvSize<T> {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}
