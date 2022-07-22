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

pub trait TieredCacheKey: Eq + Send + Sync + Hash + Clone + 'static + std::fmt::Debug {
    fn encoded_len() -> usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(buf: &[u8]) -> Self;
}

#[cfg(target_os = "linux")]
pub mod file_cache;

#[cfg(target_os = "linux")]
pub mod linux {
    use super::file_cache::cache::FileCache;

    pub type TieredCache<K> = FileCache<K>;
}

#[cfg(not(target_os = "linux"))]
pub mod not_linux {
    use std::marker::PhantomData;

    use super::TieredCacheKey;
    use crate::hummock::HummockResult;

    #[derive(Clone)]
    pub struct TieredCache<K: TieredCacheKey>(PhantomData<K>);

    impl<K: TieredCacheKey> TieredCache<K> {
        pub fn insert(&self, _key: K, _value: Vec<u8>) -> HummockResult<()> {
            Ok(())
        }

        pub async fn get(&self, _key: &K) -> HummockResult<Option<Vec<u8>>> {
            Ok(None)
        }

        pub fn erase(&self, _key: &K) -> HummockResult<()> {
            Ok(())
        }
    }
}
