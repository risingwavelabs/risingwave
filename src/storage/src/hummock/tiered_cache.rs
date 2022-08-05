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

use std::hash::{BuildHasher, Hash};

pub trait HashBuilder = BuildHasher + Clone + Send + Sync + 'static;

pub trait TieredCacheKey: Eq + Send + Sync + Hash + Clone + 'static + std::fmt::Debug {
    fn encoded_len() -> usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(buf: &[u8]) -> Self;
}

#[expect(clippy::len_without_is_empty)]
pub trait TieredCacheValue: Send + Sync + Clone + 'static {
    fn len(&self) -> usize;

    fn encoded_len(&self) -> usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(buf: Vec<u8>) -> Self;
}

pub enum TieredCacheEntry<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    Cache(CachableEntry<K, V>),
    Owned(Box<V>),
}

pub struct TieredCacheEntryHolder<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    handle: TieredCacheEntry<K, V>,
    value: *const V,
}

impl<K, V> TieredCacheEntryHolder<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    pub fn from_cached_value(entry: CachableEntry<K, V>) -> Self {
        let ptr = entry.value() as *const _;
        Self {
            handle: TieredCacheEntry::Cache(entry),
            value: ptr,
        }
    }

    pub fn from_owned_value(value: V) -> Self {
        let value = Box::new(value);
        let ptr = value.as_ref() as *const _;
        Self {
            handle: TieredCacheEntry::Owned(value),
            value: ptr,
        }
    }

    pub fn into_inner(self) -> TieredCacheEntry<K, V> {
        self.handle
    }

    pub fn into_owned(self) -> V {
        match self.handle {
            // TODO(MrCroxx): There is a copy here, eliminate it by erase the entry later.
            TieredCacheEntry::Cache(entry) => entry.value().clone(),
            TieredCacheEntry::Owned(value) => *value,
        }
    }
}

impl<K, V> std::ops::Deref for TieredCacheEntryHolder<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.value) }
    }
}

use std::marker::PhantomData;

use risingwave_common::cache::CachableEntry;

#[cfg(target_os = "linux")]
pub use super::file_cache;

#[derive(thiserror::Error, Debug)]
pub enum TieredCacheError {
    #[cfg(target_os = "linux")]
    #[error("file cache error: {0}")]
    FileCache(#[from] file_cache::error::Error),
}

pub type Result<T> = core::result::Result<T, TieredCacheError>;

pub enum TieredCacheOptions {
    NoneCache,
    #[cfg(target_os = "linux")]
    FileCache(file_cache::cache::FileCacheOptions),
}

pub enum TieredCache<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    NoneCache(PhantomData<(K, V)>),
    #[cfg(target_os = "linux")]
    FileCache(file_cache::cache::FileCache<K, V>),
}

impl<K, V> Clone for TieredCache<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    fn clone(&self) -> Self {
        match self {
            TieredCache::NoneCache(_) => TieredCache::NoneCache(PhantomData::default()),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => TieredCache::FileCache(file_cache.clone()),
        }
    }
}

impl<K, V> TieredCache<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    #[allow(clippy::unused_async)]
    pub async fn open(options: TieredCacheOptions) -> Result<Self> {
        match options {
            TieredCacheOptions::NoneCache => Ok(Self::NoneCache(PhantomData::default())),
            #[cfg(target_os = "linux")]
            TieredCacheOptions::FileCache(options) => {
                let file_cache = file_cache::cache::FileCache::open(options).await?;
                Ok(Self::FileCache(file_cache))
            }
        }
    }

    #[allow(unused_variables)]
    pub fn insert(&self, key: K, value: V) -> Result<()> {
        match self {
            TieredCache::NoneCache(_) => Ok(()),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => {
                file_cache.insert(key, value)?;
                Ok(())
            }
        }
    }

    #[allow(unused_variables)]
    pub fn erase(&self, key: &K) -> Result<()> {
        match self {
            TieredCache::NoneCache(_) => Ok(()),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => {
                file_cache.erase(key)?;
                Ok(())
            }
        }
    }

    #[allow(unused_variables, clippy::unused_async)]
    pub async fn get(&self, key: &K) -> Result<Option<TieredCacheEntryHolder<K, V>>> {
        match self {
            TieredCache::NoneCache(_) => Ok(None),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => {
                let holder = file_cache.get(key).await?;
                Ok(holder)
            }
        }
    }
}
