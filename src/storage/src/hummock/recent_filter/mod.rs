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
use std::sync::LazyLock;

use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    IntGauge, Registry, register_int_counter_vec_with_registry, register_int_gauge_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

pub trait RecentFilterTrait: Debug + Clone {
    type Item;

    fn insert(&self, item: Self::Item)
    where
        Self::Item: Eq + Hash;
    fn extend(&self, iter: impl IntoIterator<Item = Self::Item>)
    where
        Self::Item: Eq + Hash;
    fn contains<Q>(&self, item: &Q) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq;
    fn contains_any<'a, Q>(&self, iter: impl IntoIterator<Item = &'a Q>) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + 'a;
}

pub static GLOBAL_RECENT_FILTER_METRICS: LazyLock<RecentFilterMetrics> =
    LazyLock::new(|| RecentFilterMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct RecentFilterMetrics {
    pub recent_filter_items: IntGauge,
    pub recent_filter_ops: GenericCounterVec<AtomicU64>,
    pub recent_filter_inserts: GenericCounter<AtomicU64>,
    pub recent_filter_hit: GenericCounter<AtomicU64>,
    pub recent_filter_miss: GenericCounter<AtomicU64>,
}

impl RecentFilterMetrics {
    fn new(registry: &Registry) -> Self {
        let recent_filter_items = register_int_gauge_with_registry!(
            "recent_filter_items",
            "Item numbers of the recent filter.",
            registry
        )
        .unwrap();

        let recent_filter_ops = register_int_counter_vec_with_registry!(
            "recent_filter_ops",
            "Ops of the recent filter.",
            &["op"],
            registry
        )
        .unwrap();

        let recent_filter_inserts = recent_filter_ops.with_label_values(&["insert"]);
        let recent_filter_hit = recent_filter_ops.with_label_values(&["hit"]);
        let recent_filter_miss = recent_filter_ops.with_label_values(&["miss"]);

        Self {
            recent_filter_items,
            recent_filter_ops,
            recent_filter_inserts,
            recent_filter_hit,
            recent_filter_miss,
        }
    }
}

pub mod all;
pub mod none;
pub mod sharded;
pub mod simple;

pub enum RecentFilter<T> {
    All(all::AllRecentFilter<T>),
    Simple(simple::SimpleRecentFilter<T>),
    None(none::NoneRecentFilter<T>),
    Sharded(sharded::ShardedRecentFilter<T>),
}

impl<T> Debug for RecentFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecentFilter::All(filter) => filter.fmt(f),
            RecentFilter::Simple(filter) => filter.fmt(f),
            RecentFilter::None(filter) => filter.fmt(f),
            RecentFilter::Sharded(filter) => filter.fmt(f),
        }
    }
}

impl<T> Clone for RecentFilter<T> {
    fn clone(&self) -> Self {
        match self {
            RecentFilter::All(filter) => RecentFilter::All(filter.clone()),
            RecentFilter::Simple(filter) => RecentFilter::Simple(filter.clone()),
            RecentFilter::None(filter) => RecentFilter::None(filter.clone()),
            RecentFilter::Sharded(filter) => RecentFilter::Sharded(filter.clone()),
        }
    }
}

impl<T> From<all::AllRecentFilter<T>> for RecentFilter<T> {
    fn from(filter: all::AllRecentFilter<T>) -> Self {
        RecentFilter::All(filter)
    }
}

impl<T> From<simple::SimpleRecentFilter<T>> for RecentFilter<T> {
    fn from(filter: simple::SimpleRecentFilter<T>) -> Self {
        RecentFilter::Simple(filter)
    }
}

impl<T> From<none::NoneRecentFilter<T>> for RecentFilter<T> {
    fn from(filter: none::NoneRecentFilter<T>) -> Self {
        RecentFilter::None(filter)
    }
}

impl<T> From<sharded::ShardedRecentFilter<T>> for RecentFilter<T> {
    fn from(filter: sharded::ShardedRecentFilter<T>) -> Self {
        RecentFilter::Sharded(filter)
    }
}

impl<T> RecentFilterTrait for RecentFilter<T>
where
    T: Hash + Eq,
{
    type Item = T;

    fn insert(&self, item: Self::Item)
    where
        Self::Item: Eq + Hash,
    {
        match self {
            RecentFilter::All(filter) => filter.insert(item),
            RecentFilter::Simple(filter) => filter.insert(item),
            RecentFilter::None(filter) => filter.insert(item),
            RecentFilter::Sharded(filter) => filter.insert(item),
        }
    }

    fn extend(&self, iter: impl IntoIterator<Item = Self::Item>)
    where
        Self::Item: Eq + Hash,
    {
        match self {
            RecentFilter::All(filter) => filter.extend(iter),
            RecentFilter::Simple(filter) => filter.extend(iter),
            RecentFilter::None(filter) => filter.extend(iter),
            RecentFilter::Sharded(filter) => filter.extend(iter),
        }
    }

    fn contains<Q>(&self, item: &Q) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            RecentFilter::All(filter) => filter.contains(item),
            RecentFilter::Simple(filter) => filter.contains(item),
            RecentFilter::None(filter) => filter.contains(item),
            RecentFilter::Sharded(filter) => filter.contains(item),
        }
    }

    fn contains_any<'a, Q>(&self, iter: impl IntoIterator<Item = &'a Q>) -> bool
    where
        Self::Item: Borrow<Q>,
        Q: Hash + Eq + 'a,
    {
        match self {
            RecentFilter::All(filter) => filter.contains_any(iter),
            RecentFilter::Simple(filter) => filter.contains_any(iter),
            RecentFilter::None(filter) => filter.contains_any(iter),
            RecentFilter::Sharded(filter) => filter.contains_any(iter),
        }
    }
}
