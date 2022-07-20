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

use std::hash::Hash;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::hummock::HummockResult;

pub trait TieredCacheKey: Eq + Send + Sync + Hash + Clone + 'static + std::fmt::Debug {
    fn encoded_len() -> usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(buf: &[u8]) -> Self;
}

#[async_trait]
pub trait TieredCache<K: TieredCacheKey>: Send + Sync + Clone {
    async fn insert(&self, key: K, value: Vec<u8>) -> HummockResult<()>;
    async fn get(&self, key: &K) -> HummockResult<Option<Vec<u8>>>;
    async fn erase(&self, key: &K) -> HummockResult<()>;
}

#[cfg(target_os = "linux")]
pub mod file_cache;

#[derive(Clone)]
pub struct NoneTieredCache<K: TieredCacheKey>(PhantomData<K>);

#[async_trait]
impl<K: TieredCacheKey> TieredCache<K> for NoneTieredCache<K> {
    async fn insert(&self, _key: K, _value: Vec<u8>) -> HummockResult<()> {
        Ok(())
    }

    async fn get(&self, _key: &K) -> HummockResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn erase(&self, _key: &K) -> HummockResult<()> {
        Ok(())
    }
}
