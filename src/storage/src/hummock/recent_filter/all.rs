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
use std::hash::Hash;
use std::marker::PhantomData;

use crate::hummock::RecentFilterTrait;

pub struct AllRecentFilter<T>(PhantomData<T>);

impl<T> Debug for AllRecentFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllRecentFilter").finish()
    }
}

impl<T> Clone for AllRecentFilter<T> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for AllRecentFilter<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> RecentFilterTrait for AllRecentFilter<T> {
    type Item = T;

    fn insert(&self, _: Self::Item)
    where
        Self::Item: Eq + Hash,
    {
    }

    fn extend(&self, _: impl IntoIterator<Item = Self::Item>)
    where
        Self::Item: Eq + Hash,
    {
    }

    fn contains<Q>(&self, _: &Q) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        true
    }

    fn contains_any<'a, Q>(&self, _: impl IntoIterator<Item = &'a Q>) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + 'a,
    {
        true
    }
}
