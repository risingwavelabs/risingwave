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

use enum_as_inner::EnumAsInner;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_expr::window_function::StateKey;

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub(super) enum KeyWithSentinel<T> {
    Smallest,
    Normal(T),
    Largest,
}

impl<T> KeyWithSentinel<T> {
    pub fn as_normal_expect(&self) -> &T {
        self.as_normal().expect("expect normal key")
    }

    pub fn is_sentinel(&self) -> bool {
        matches!(self, Self::Smallest | Self::Largest)
    }
}

impl<T> PartialOrd for KeyWithSentinel<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for KeyWithSentinel<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use KeyWithSentinel::*;
        match (self, other) {
            (Smallest, Smallest) => std::cmp::Ordering::Equal,
            (Smallest, _) => std::cmp::Ordering::Less,
            (_, Smallest) => std::cmp::Ordering::Greater,
            (Largest, Largest) => std::cmp::Ordering::Equal,
            (Largest, _) => std::cmp::Ordering::Greater,
            (_, Largest) => std::cmp::Ordering::Less,
            (Normal(a), Normal(b)) => a.cmp(b),
        }
    }
}

impl<T: EstimateSize> EstimateSize for KeyWithSentinel<T> {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Self::Smallest => 0,
            Self::Normal(inner) => inner.estimated_heap_size(),
            Self::Largest => 0,
        }
    }
}

impl From<StateKey> for KeyWithSentinel<StateKey> {
    fn from(key: StateKey) -> Self {
        Self::Normal(key)
    }
}
