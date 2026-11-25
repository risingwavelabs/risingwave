// Copyright 2026 RisingWave Labs
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

//! Shared fixtures, compiled only through the `#[cfg(test)]` module declaration.

use std::sync::Arc;

use risingwave_common::config::ObjectStoreConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreImpl, ObjectStoreRef, build_remote_object_store,
};

use super::PinCache;
use crate::monitor::ObjectStoreMetrics;

pub(super) fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

/// Installs a file fixture directly. This is not a production refill or publication API.
/// Never replaces an existing path: path reuse is introduced only with storage admission.
pub(super) async fn install_file_for_test(
    cache: &PinCache,
    object_id: HummockSstableObjectId,
    bytes: bytes::Bytes,
) {
    let path = format!("{}.sst", object_id.as_raw_id());
    assert!(
        cache
            .store
            .metadata(&path)
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
    cache.store.upload(&path, bytes.clone()).await.unwrap();
    cache
        .shard(object_id)
        .write()
        .objects
        .get_mut(&object_id)
        .unwrap()
        .publish(Arc::new(super::PinCacheFile {
            path,
            size: bytes.len() as u64,
        }));
}

pub(super) fn in_memory_object_store() -> ObjectStoreRef {
    Arc::new(ObjectStoreImpl::InMem(
        InMemObjectStore::for_test().monitored(
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ),
    ))
}

pub(super) async fn local_object_store() -> (tempfile::TempDir, ObjectStoreRef) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ObjectStoreConfig {
        upload_part_size: 1,
        ..Default::default()
    };
    config.set_atomic_write_dir();
    let store = Arc::new(
        build_remote_object_store(
            &format!("fs://{}", dir.path().display()),
            Arc::new(ObjectStoreMetrics::unused()),
            "test pin cache",
            Arc::new(config),
        )
        .await,
    );
    (dir, store)
}
