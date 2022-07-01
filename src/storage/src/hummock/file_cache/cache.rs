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

use std::sync::Arc;

use risingwave_common::cache::LruCache;

use super::coding::CacheKey;
use super::error::Result;
use super::filter::Filter;
use super::meta::SlotId;
use super::store::{Store, StoreOptions, StoreRef};

pub struct FileCacheOptions {
    pub dir: String,
    pub capacity: usize,
    pub cache_file_fallocate_unit: usize,
    pub filters: Vec<Arc<dyn Filter>>,
}

#[derive(Clone)]
pub struct FileCache<K>
where
    K: CacheKey,
{
    _filters: Vec<Arc<dyn Filter>>,

    _indices: Arc<LruCache<K, SlotId>>,

    _store: StoreRef<K>,
}

impl<K> FileCache<K>
where
    K: CacheKey,
{
    pub async fn open(options: FileCacheOptions) -> Result<Self> {
        let cache_file_manager = Store::open(StoreOptions {
            dir: options.dir,
            capacity: options.capacity,
            cache_file_fallocate_unit: options.cache_file_fallocate_unit,
        })
        .await?;
        let store = Arc::new(cache_file_manager);

        // TODO: Restore indices.
        let indices = LruCache::with_event_listeners(6, options.capacity, vec![store.clone()]);
        store.restore(&indices).await?;
        let indices = Arc::new(indices);

        Ok(Self {
            _filters: options.filters,

            _indices: indices,

            _store: store,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::super::test_utils::TestCacheKey;
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCache<TestCacheKey>>();
    }

    #[tokio::test]
    async fn test_file_cache_manager() {
        let ci: bool = std::env::var("RISINGWAVE_CI")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .expect("env $RISINGWAVE_CI must be 'true' or 'false'");

        let tempdir = if ci {
            tempfile::Builder::new().tempdir_in("/risingwave").unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };

        let options = FileCacheOptions {
            dir: tempdir.path().to_str().unwrap().to_string(),
            capacity: 256 * 1024 * 1024,
            cache_file_fallocate_unit: 64 * 1024 * 1024,
            filters: vec![],
        };
        let _cache: FileCache<TestCacheKey> = FileCache::open(options).await.unwrap();
    }
}
