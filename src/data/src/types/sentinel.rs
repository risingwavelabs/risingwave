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

use enum_as_inner::EnumAsInner;
use risingwave_common_estimate_size::EstimateSize;

/// [`Sentinelled<T>`] wraps type `T` to provide smallest (smaller than any normal `T` value) and largest
/// (larger than ant normal `T` value) sentinel value for `T`.
///
/// Sentinel is a very common technique used to simplify tree/list/array algorithms. The main idea is to
/// insert sentinel node to the beginning or/and the end, so that algorithms don't need to handle complex
/// edge cases.
#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum Sentinelled<T> {
    Smallest,
    Normal(T),
    Largest,
}

impl<T> Sentinelled<T> {
    pub fn as_normal_expect(&self) -> &T {
        self.as_normal().expect("expect normal key")
    }

    pub fn is_sentinel(&self) -> bool {
        matches!(self, Self::Smallest | Self::Largest)
    }

    pub fn cmp_by(
        &self,
        other: &Self,
        cmp_fn: impl FnOnce(&T, &T) -> std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        use Sentinelled::*;
        match (self, other) {
            (Smallest, Smallest) => std::cmp::Ordering::Equal,
            (Smallest, _) => std::cmp::Ordering::Less,
            (_, Smallest) => std::cmp::Ordering::Greater,
            (Largest, Largest) => std::cmp::Ordering::Equal,
            (Largest, _) => std::cmp::Ordering::Greater,
            (_, Largest) => std::cmp::Ordering::Less,
            (Normal(a), Normal(b)) => cmp_fn(a, b),
        }
    }

    pub fn map<U>(self, map_fn: impl FnOnce(T) -> U) -> Sentinelled<U> {
        use Sentinelled::*;
        match self {
            Smallest => Smallest,
            Normal(inner) => Normal(map_fn(inner)),
            Largest => Largest,
        }
    }
}

impl<T> From<T> for Sentinelled<T> {
    fn from(inner: T) -> Self {
        Self::Normal(inner)
    }
}

impl<T> PartialOrd for Sentinelled<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Sentinelled<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp_by(other, T::cmp)
    }
}

impl<T: EstimateSize> EstimateSize for Sentinelled<T> {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Self::Smallest => 0,
            Self::Normal(inner) => inner.estimated_heap_size(),
            Self::Largest => 0,
        }
    }
}
