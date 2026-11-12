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
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectResult, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
    build_remote_object_store,
};
use tokio::sync::{mpsc, oneshot};

use super::PinCache;
use crate::monitor::ObjectStoreMetrics;

type SpaceProbe = Arc<dyn Fn() -> ObjectResult<Option<u64>> + Send + Sync>;
pub(super) type FullScan = (Vec<String>, oneshot::Sender<()>);
pub(super) type UploadFinish = (String, oneshot::Sender<()>);

#[derive(Default)]
pub(super) struct PinCacheTestHooks {
    pub(super) space_pause: Mutex<Option<mpsc::UnboundedSender<oneshot::Sender<()>>>>,
    pub(super) upload_finish: Mutex<Option<mpsc::UnboundedSender<UploadFinish>>>,
    pub(super) panic_before_upload_finish: AtomicBool,
    pub(super) space_probe: Mutex<Option<SpaceProbe>>,
    pub(super) full_scan: Mutex<Option<mpsc::UnboundedSender<FullScan>>>,
    pub(super) delete_pause: Mutex<Option<mpsc::UnboundedSender<oneshot::Sender<()>>>>,
}

impl PinCache {
    pub(in crate::hummock) fn set_available_space_for_test(
        &self,
        probe: impl Fn() -> ObjectResult<Option<u64>> + Send + Sync + 'static,
    ) {
        *self.test_hooks.space_probe.lock() = Some(Arc::new(probe));
    }

    pub(super) fn watch_space_probes_for_test(
        &self,
    ) -> mpsc::UnboundedReceiver<oneshot::Sender<()>> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.test_hooks.space_pause.lock() = Some(tx);
        rx
    }

    pub(super) fn watch_upload_finish_for_test(&self) -> mpsc::UnboundedReceiver<UploadFinish> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.test_hooks.upload_finish.lock() = Some(tx);
        rx
    }

    /// Observe the real full selection and pause the worker before deletion until released.
    pub(super) fn watch_full_scans_for_test(&self) -> mpsc::UnboundedReceiver<FullScan> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.test_hooks.full_scan.lock() = Some(tx);
        rx
    }
}

pub(super) fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

/// Downloads and publishes an already registered object to prepare a test fixture.
/// Admission, scheduling and retry behavior must be exercised through the production caller.
pub(in crate::hummock) async fn download_and_publish_for_test(
    pin_cache: &Arc<PinCache>,
    remote_store: ObjectStoreRef,
    remote_path: String,
    object_id: HummockSstableObjectId,
    object_size: u64,
) -> Result<(), super::PinCacheDownloadError> {
    let token = pin_cache
        .prepare_refill(object_id)
        .expect("test object must be registered");
    let download = pin_cache
        .download(object_id, object_size, remote_store, remote_path)
        .await?;
    assert!(pin_cache.publish(token, download));
    Ok(())
}

pub(in crate::hummock) fn in_memory_object_store() -> ObjectStoreRef {
    Arc::new(ObjectStoreImpl::InMem(
        InMemObjectStore::for_test().monitored(
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ),
    ))
}

pub(in crate::hummock) async fn local_object_store() -> (tempfile::TempDir, ObjectStoreRef) {
    local_object_store_with_config(ObjectStoreConfig {
        upload_part_size: 1,
        ..Default::default()
    })
    .await
}

pub(super) async fn local_object_store_with_config(
    mut config: ObjectStoreConfig,
) -> (tempfile::TempDir, ObjectStoreRef) {
    let dir = tempfile::tempdir().unwrap();
    config.set_atomic_write_dir();
    config.retry.delete_objects_attempt_timeout_ms = 0;
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
