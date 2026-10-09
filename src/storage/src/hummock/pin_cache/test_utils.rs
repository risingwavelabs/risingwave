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
use crate::hummock::SstableStoreRef;
use crate::monitor::ObjectStoreMetrics;

pub(super) fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

/// Downloads and publishes an already registered object to prepare a test fixture.
/// Admission, scheduling and retry behavior must be exercised through the production caller.
pub(in crate::hummock) async fn download_and_publish_for_test(
    pin_cache: &PinCache,
    remote_store: ObjectStoreRef,
    remote_path: String,
    object_id: HummockSstableObjectId,
    object_size: u64,
) -> Result<(), super::refill::PinCacheDownloadError> {
    let token = pin_cache
        .prepare_refill(object_id)
        .expect("test object must be registered");
    let download = pin_cache
        .download(object_id, object_size, remote_store, remote_path)
        .await?;
    assert!(pin_cache.publish(token, download));
    Ok(())
}

pub(in crate::hummock) async fn publish_pin_cache(
    store: SstableStoreRef,
    object_id: HummockSstableObjectId,
    local_store: ObjectStoreRef,
) -> (SstableStoreRef, Arc<PinCache>) {
    let pin = PinCache::new(local_store, u64::MAX, 1, []).await.unwrap();
    let path = store.get_sst_data_path(object_id);
    let size = store.store().metadata(&path).await.unwrap().total_size as u64;
    pin.register_objects([object_id]);
    download_and_publish_for_test(&pin, store.store(), path, object_id, size)
        .await
        .unwrap();
    let store = Arc::new(Arc::into_inner(store).unwrap().with_pin_cache(pin.clone()));
    (store, pin)
}

pub(in crate::hummock) fn in_memory_object_store() -> ObjectStoreRef {
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
