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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    InMemObjectStore, MonitoredStreamingReader, ObjectError, ObjectStore, ObjectStoreImpl,
    ObjectStoreRef, build_remote_object_store,
};

use super::{
    PinCache, PinCacheDownloadGuard, PinCacheDownloadStart, PinCacheRefillOutcome, RecoveryState,
};
use crate::monitor::ObjectStoreMetrics;

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

fn published_bytes(pin_cache: &PinCache) -> u64 {
    pin_cache
        .shards
        .iter()
        .map(|shard| {
            shard
                .read()
                .published
                .values()
                .map(|entry| entry.size)
                .sum::<u64>()
        })
        .sum()
}

async fn wait_for_reclaim(pin_cache: &PinCache) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pin_cache.gc.accounted_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn start_download(
    pin_cache: &Arc<PinCache>,
    object_id: HummockSstableObjectId,
) -> PinCacheDownloadGuard {
    let generation = pin_cache.refill_generation(object_id).unwrap();
    match pin_cache
        .start_download_at_generation(object_id, generation)
        .unwrap()
    {
        PinCacheDownloadStart::Download(download) => download,
        PinCacheDownloadStart::Complete(outcome) => {
            panic!("expected download, got {outcome:?}")
        }
    }
}

fn completed_download_outcome(
    pin_cache: &Arc<PinCache>,
    object_id: HummockSstableObjectId,
) -> PinCacheRefillOutcome {
    let generation = pin_cache.refill_generation(object_id).unwrap();
    match pin_cache
        .start_download_at_generation(object_id, generation)
        .unwrap()
    {
        PinCacheDownloadStart::Download(_) => panic!("expected download to be skipped"),
        PinCacheDownloadStart::Complete(outcome) => outcome,
    }
}

#[test]
fn test_parse_finalized_object_path() {
    assert_eq!(
        PinCache::parse_object_id("1001-42.sst"),
        Some(HummockSstableObjectId::from(1001))
    );
    assert_eq!(PinCache::parse_object_id("1001-recovered.sst"), None);
    assert_eq!(PinCache::parse_object_id("1001-42.tmp"), None);
    assert_eq!(PinCache::parse_object_id("nested/1001-42.sst"), None);
}

#[tokio::test]
async fn test_pin_read_and_unpin_lifecycle() {
    let remote_store = in_memory_object_store();
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store, u64::MAX);
    let object_id = HummockSstableObjectId::from(1001);
    let remote_path = "remote.sst";
    let original = Bytes::from_static(b"complete sst");
    remote_store
        .upload(remote_path, original.clone())
        .await
        .unwrap();

    pin_cache.replace_desired_objects(HashMap::from([(object_id, original.len() as u64)]));
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert!(pin_cache.get(object_id).is_some());
    assert_eq!(published_bytes(&pin_cache), original.len() as u64);
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        original
    );
    remote_store
        .upload(remote_path, Bytes::from_static(b"changed remote"))
        .await
        .unwrap();
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        original
    );

    pin_cache.replace_desired_objects(HashMap::new());
    assert!(pin_cache.get(object_id).is_none());
    assert_eq!(published_bytes(&pin_cache), 0);

    let changed = Bytes::from_static(b"changed remote");
    pin_cache.replace_desired_objects(HashMap::from([(object_id, changed.len() as u64)]));
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        changed
    );

    pin_cache.replace_desired_objects(HashMap::new());
    pin_cache
        .pin_sst(remote_store, remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert!(pin_cache.get(object_id).is_none());
    assert_eq!(published_bytes(&pin_cache), 0);
}

#[tokio::test]
async fn test_revoked_generation_cannot_begin_a_late_download() {
    let cache = PinCache::new(in_memory_object_store(), u64::MAX);
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let object = HummockSstableObjectId::from(911);
    cache.replace_desired_objects([(object, 8)]);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    cache.set_refill_gate_for_test(gate.clone());
    let generation = cache.refill_generation(object).unwrap();
    let old_cache = cache.clone();
    let old_remote = remote.clone();
    let old = tokio::spawn(async move {
        old_cache
            .pin_sst_at_generation(old_remote, "sst".into(), object, generation)
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    cache.revoke_inflight(object);
    gate.add_permits(1);
    assert_eq!(old.await.unwrap(), PinCacheRefillOutcome::Obsolete);
    assert!(cache.get(object).is_none());
    gate.add_permits(1);
    assert_eq!(
        cache.pin_sst(remote, "sst".into(), object).await.unwrap(),
        PinCacheRefillOutcome::Published
    );
}

#[tokio::test]
async fn test_retired_route_waits_for_version_application() {
    let cache = PinCache::new(in_memory_object_store(), u64::MAX);
    let object = HummockSstableObjectId::from(910);
    cache.replace_desired_objects([(object, 8)]);
    cache.wait_for_recovery().await;
    let download = start_download(&cache, object);
    cache
        .store
        .upload(&download.entry.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    assert_eq!(download.publish(), PinCacheRefillOutcome::Published);
    cache.apply_desired_object_delta(2.into(), [object], HashMap::new());
    cache.release_retired(1.into());
    assert!(cache.get(object).is_some());
    cache.release_retired(2.into());
    assert!(cache.get(object).is_none());
}

#[tokio::test]
async fn test_revoke_retired_route_before_version_application() {
    let remote_store = in_memory_object_store();
    let cache = PinCache::new(in_memory_object_store(), u64::MAX);
    let object = HummockSstableObjectId::from(911);
    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    cache.replace_desired_objects([(object, 8)]);
    cache
        .pin_sst(remote_store, "sst".into(), object)
        .await
        .unwrap();
    cache.apply_desired_object_delta(2.into(), [object], HashMap::new());
    assert!(cache.get(object).is_some());

    cache.revoke_retired();
    assert!(cache.get(object).is_none());
}

#[tokio::test]
async fn test_same_delta_replacement_keeps_route() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
    let object_id = HummockSstableObjectId::from(1001);
    remote_store
        .upload("sst", Bytes::from_static(b"12345678"))
        .await
        .unwrap();
    pin_cache.replace_desired_objects([(object_id, 8)]);
    pin_cache
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();

    pin_cache.apply_desired_object_delta(2.into(), [object_id], HashMap::from([(object_id, 8)]));
    assert!(pin_cache.get(object_id).is_some());

    pin_cache.apply_desired_object_delta(3.into(), [object_id], HashMap::new());
    assert!(pin_cache.get(object_id).is_some());
    pin_cache.replace_desired_objects([]);
    assert!(pin_cache.get(object_id).is_none());
}

#[tokio::test]
async fn test_inflight_is_not_routable_and_cancellation_releases_token() {
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));

    let download = start_download(&pin_cache, object_id);
    assert!(pin_cache.get(object_id).is_none());
    assert_eq!(
        completed_download_outcome(&pin_cache, object_id),
        PinCacheRefillOutcome::InProgress
    );
    drop(download);
    assert!(
        pin_cache
            .shards
            .iter()
            .all(|shard| shard.read().inflight.is_empty())
    );

    let retry = start_download(&pin_cache, object_id);
    pin_cache
        .store
        .upload(&retry.entry.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    assert_eq!(retry.publish(), PinCacheRefillOutcome::Published);
    assert!(
        pin_cache
            .shards
            .iter()
            .all(|shard| shard.read().inflight.is_empty())
    );
    assert!(pin_cache.get(object_id).is_some());
    assert_eq!(
        completed_download_outcome(&pin_cache, object_id),
        PinCacheRefillOutcome::AlreadyPublished
    );
}

#[tokio::test]
async fn test_revoked_download_cannot_publish_or_remove_replacement() {
    for revoke_by_unpin in [false, true] {
        let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
        let object_id = HummockSstableObjectId::from(1001);
        let desired = HashMap::from([(object_id, 11)]);
        pin_cache.replace_desired_objects(desired.clone());
        let old = start_download(&pin_cache, object_id);

        if revoke_by_unpin {
            pin_cache.replace_desired_objects(HashMap::new());
        } else {
            pin_cache.replace_desired_objects(HashMap::from([(object_id, 12)]));
        }
        pin_cache.replace_desired_objects(desired);
        let replacement = start_download(&pin_cache, object_id);
        assert_ne!(old.entry.path, replacement.entry.path);

        assert_eq!(old.publish(), PinCacheRefillOutcome::Obsolete);
        assert!(pin_cache.get(object_id).is_none());
        assert!(Arc::ptr_eq(
            &pin_cache.shard(object_id).read().inflight[&object_id],
            &replacement.entry,
        ));
        pin_cache
            .store
            .upload(&replacement.entry.path, Bytes::from_static(b"replacement"))
            .await
            .unwrap();
        assert_eq!(replacement.publish(), PinCacheRefillOutcome::Published);
        assert_eq!(
            pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"replacement")
        );
    }
}

#[tokio::test]
async fn test_failed_download_can_be_retried() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), 8);
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));
    assert!(
        pin_cache
            .pin_sst(remote_store.clone(), "sst".into(), object_id)
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
    assert!(
        pin_cache
            .shards
            .iter()
            .all(|shard| shard.read().inflight.is_empty())
    );
    wait_for_reclaim(&pin_cache).await;

    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    pin_cache
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();
    assert!(pin_cache.get(object_id).is_some());
}

#[tokio::test]
async fn test_interrupted_fs_upload_keeps_capacity_until_recovery() {
    for cancel in [false, true] {
        let (_dir, local_store) = local_object_store().await;
        let pin_cache = PinCache::new(local_store.clone(), 8);
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.replace_desired_objects([(object_id, 8)]);
        pin_cache.wait_for_recovery().await;
        let download = start_download(&pin_cache, object_id);
        let final_path = download.entry.path.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
        let reader = MonitoredStreamingReader::new(
            "test",
            Box::pin(
                stream::iter([
                    Ok(Bytes::from_static(b"half")),
                    Ok(Bytes::from_static(b"x")),
                ])
                .chain(stream::once(async move {
                    // Two writes flush the FS position writer's one-chunk buffer.
                    started_tx.send(()).unwrap();
                    let _ = fail_rx.await;
                    Err(ObjectError::internal("injected remote read failure"))
                })),
            ),
            Arc::new(ObjectStoreMetrics::unused()),
            None,
        );
        let task = tokio::spawn(download.write(reader));
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        // Wait for Tokio's buffered file write to reach the filesystem before cancellation.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let files: Vec<_> = local_store
                    .list("", None, None)
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                if files.iter().any(|file| file.total_size == 4) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            fail_tx.send(()).unwrap();
            assert!(task.await.unwrap().is_err());
        }

        assert!(pin_cache.get(object_id).is_none());
        assert!(
            pin_cache
                .shards
                .iter()
                .all(|shard| shard.read().inflight.is_empty())
        );
        assert!(
            local_store
                .metadata(&final_path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
        assert_eq!(pin_cache.gc.accounted_bytes(), 8);
        assert_eq!(
            completed_download_outcome(&pin_cache, object_id),
            PinCacheRefillOutcome::CapacityRejected
        );
        // Unpin must not release the reservation for the backend-owned temporary file either.
        pin_cache.replace_desired_objects([]);
        assert_eq!(pin_cache.gc.accounted_bytes(), 8);
        drop(pin_cache);

        let recovered = PinCache::new(local_store.clone(), 8);
        recovered.replace_desired_objects([(object_id, 8)]);
        recovered.wait_for_recovery().await;
        wait_for_reclaim(&recovered).await;
        assert!(recovered.get(object_id).is_none());
        let files: Vec<_> = local_store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(files.iter().all(|file| file.key.ends_with('/')));
        let remote_store = in_memory_object_store();
        remote_store
            .upload("sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();
        recovered
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .unwrap();
        assert!(recovered.get(object_id).is_some());
    }
}

#[tokio::test]
async fn test_completed_invalid_fs_upload_reclaims_capacity() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 8);
    let remote_store = in_memory_object_store();
    remote_store
        .upload("sst", Bytes::from_static(b"half"))
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.replace_desired_objects([(object_id, 8)]);
    assert!(
        pin_cache
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
    wait_for_reclaim(&pin_cache).await;
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(files.iter().all(|file| file.key.ends_with('/')));
}

#[tokio::test]
async fn test_failed_recovery_refuses_new_local_writes() {
    for partial_inventory in [false, true] {
        let local_store = in_memory_object_store();
        let pin_cache = PinCache::new(local_store.clone(), 16);
        pin_cache.wait_for_recovery().await;
        let recovered_object = HummockSstableObjectId::from(1001);
        let new_object = HummockSstableObjectId::from(1002);
        pin_cache.replace_desired_objects([(recovered_object, 8), (new_object, 8)]);
        local_store
            .upload("1001-42.sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();

        let error = ObjectError::internal("injected inventory failure");
        let objects = if partial_inventory {
            // Exercise a stream error after real metadata has already been accounted.
            let metadata = local_store.metadata("1001-42.sst").await.unwrap();
            Ok(stream::iter([Ok(metadata), Err(error)]).boxed())
        } else {
            Err(error)
        };
        pin_cache.clone().recover_local_files(objects).await;
        assert!(*pin_cache.recovery_state.read() == RecoveryState::Failed);
        assert_eq!(
            pin_cache.gc.accounted_bytes(),
            if partial_inventory { 8 } else { 0 }
        );
        assert_eq!(pin_cache.get(recovered_object).is_some(), partial_inventory);
        if partial_inventory {
            assert_eq!(
                pin_cache
                    .get(recovered_object)
                    .unwrap()
                    .read(..)
                    .await
                    .unwrap(),
                Bytes::from_static(b"complete")
            );
        }

        let remote_store = in_memory_object_store();
        remote_store
            .upload("new", Bytes::from_static(b"new data"))
            .await
            .unwrap();
        let error = pin_cache
            .pin_sst(remote_store, "new".into(), new_object)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("recovery failed"));
        assert!(
            pin_cache
                .shards
                .iter()
                .all(|shard| shard.read().inflight.is_empty())
        );
        assert!(pin_cache.get(new_object).is_none());
        assert_eq!(
            local_store
                .list("", None, None)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn test_read_failure_only_invalidates_selected_publication() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));
    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    pin_cache
        .pin_sst(remote_store.clone(), "sst".into(), object_id)
        .await
        .unwrap();
    let old = pin_cache.get(object_id).unwrap();
    pin_cache.store.delete(&old.entry.path).await.unwrap();
    assert!(old.read(..).await.is_err());
    assert!(pin_cache.get(object_id).is_none());
    assert_eq!(published_bytes(&pin_cache), 0);

    pin_cache
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();
    // This handle stays on the old path and must not remove the new route.
    assert!(old.read(..).await.is_err());
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[tokio::test]
async fn test_recovery_publishes_only_desired_complete_file() {
    let remote_store = in_memory_object_store();
    let (_dir, local_store) = local_object_store().await;
    let object_id = HummockSstableObjectId::from(1001);
    let data = Bytes::from_static(b"complete");
    remote_store.upload("sst", data.clone()).await.unwrap();

    let first = PinCache::new(local_store.clone(), 1024);
    first.replace_desired_objects(HashMap::from([(object_id, data.len() as u64)]));
    first
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();
    assert!(first.get(object_id).is_some());
    drop(first);

    let recovered = PinCache::new(local_store, 1024);
    recovered.replace_desired_objects(HashMap::from([(object_id, data.len() as u64)]));
    recovered.wait_for_recovery().await;
    assert_eq!(published_bytes(&recovered), data.len() as u64);
    assert_eq!(
        recovered.get(object_id).unwrap().read(..).await.unwrap(),
        data
    );
}

#[tokio::test]
async fn test_recovery_waits_for_initial_desired_snapshot_before_cleanup() {
    let local_store = in_memory_object_store();
    let path = "1001-42.sst";
    local_store
        .upload(path, Bytes::from_static(b"stale"))
        .await
        .unwrap();
    local_store
        .upload("unfinished.tmp", Bytes::from_static(b"partial"))
        .await
        .unwrap();
    let pin_cache = PinCache::new(local_store.clone(), 1024);
    pin_cache.wait_for_recovery().await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while local_store.metadata("unfinished.tmp").await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(local_store.metadata(path).await.is_ok());
    pin_cache.replace_desired_objects(HashMap::new());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while local_store.metadata(path).await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(pin_cache.gc.accounted_bytes(), 0);
}

fn object_in_shard(shard: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id) == shard)
        .unwrap()
}

#[tokio::test]
async fn test_other_shard_and_control_locks_do_not_block_lookup_or_publish() {
    let cache = PinCache::new(in_memory_object_store(), 16);
    cache.wait_for_recovery().await;
    let blocked = object_in_shard(0);
    let available = object_in_shard(super::PIN_CACHE_SHARDS - 1);
    cache.replace_desired_objects([(blocked, 8), (available, 8)]);
    let download = start_download(&cache, available);
    cache
        .store
        .upload(&download.entry.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let runtime = tokio::runtime::Handle::current();

    // Keep the locks held until the other thread reports completion. A regression fails
    // with a bounded timeout, then releases the locks so the worker can still exit.
    let result = std::thread::scope(|scope| {
        let control = cache.membership_update.lock();
        let recovery = cache.recovery_state.write();
        let shard = cache.shard(blocked).write();
        let (tx, rx) = std::sync::mpsc::channel();
        let cache = &cache;
        scope.spawn(move || {
            let _runtime = runtime.enter();
            let before = cache.lookup(available);
            assert!(before.desired && before.route.is_none());
            assert_eq!(download.publish(), PinCacheRefillOutcome::Published);
            let after = cache.lookup(available);
            assert!(after.desired);
            after.route.unwrap().invalidate();
            assert!(cache.lookup(available).route.is_none());
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(5));
        drop(shard);
        drop(recovery);
        drop(control);
        result
    });
    result.expect("an unrelated shard or control lock blocked the object lifecycle");
    wait_for_reclaim(&cache).await;
}

#[tokio::test]
async fn test_cross_shard_version_handoff_and_policy_revocation() {
    let cache = PinCache::new(in_memory_object_store(), 64);
    cache.wait_for_recovery().await;
    let objects = [
        object_in_shard(0),
        object_in_shard(super::PIN_CACHE_SHARDS - 1),
    ];
    cache.replace_version_objects(1.into(), objects.into_iter().map(|id| (id, 8)).collect());
    for id in objects {
        let download = start_download(&cache, id);
        cache
            .store
            .upload(&download.entry.path, Bytes::from_static(b"complete"))
            .await
            .unwrap();
        assert_eq!(download.publish(), PinCacheRefillOutcome::Published);
    }
    assert_eq!(published_bytes(&cache), 16);

    cache.replace_version_objects(2.into(), HashMap::new());
    for id in objects {
        let lookup = cache.lookup(id);
        assert!(!lookup.desired && lookup.route.is_some());
        assert!(cache.is_needed(id));
    }
    cache.release_retired(1.into());
    assert_eq!(published_bytes(&cache), 16);
    // Reintroducing one object before applying version 2 must preserve its route.
    cache.apply_desired_object_delta(3.into(), [], HashMap::from([(objects[1], 8)]));
    cache.release_retired(2.into());
    assert!(cache.get(objects[0]).is_none());
    assert!(cache.lookup(objects[1]).desired);
    assert!(cache.get(objects[1]).is_some());
    assert_eq!(published_bytes(&cache), 8);

    let revoked = object_in_shard(1);
    cache.apply_desired_object_delta(4.into(), [], HashMap::from([(revoked, 8)]));
    let old_generation = cache.refill_generation(revoked).unwrap();
    let old_download = start_download(&cache, revoked);
    cache.replace_desired_objects([]);
    cache.replace_desired_objects([(revoked, 8)]);
    let new_generation = cache.refill_generation(revoked).unwrap();
    assert_ne!(old_generation, new_generation);
    assert!(matches!(
        cache
            .start_download_at_generation(revoked, old_generation)
            .unwrap(),
        PinCacheDownloadStart::Complete(PinCacheRefillOutcome::Obsolete)
    ));
    let replacement = start_download(&cache, revoked);
    assert_eq!(old_download.publish(), PinCacheRefillOutcome::Obsolete);
    assert!(Arc::ptr_eq(
        &cache.shard(revoked).read().inflight[&revoked],
        &replacement.entry
    ));
    drop(replacement);
    assert_eq!(published_bytes(&cache), 0);
    wait_for_reclaim(&cache).await;
}

#[tokio::test]
async fn test_recovery_across_shards_before_and_after_initial_membership() {
    for membership_first in [false, true] {
        let local = in_memory_object_store();
        let objects = [
            object_in_shard(0),
            object_in_shard(super::PIN_CACHE_SHARDS - 1),
        ];
        for id in objects {
            for path_id in [1, 2] {
                local
                    .upload(
                        &format!("{}-{path_id}.sst", id.as_raw_id()),
                        Bytes::from_static(b"complete"),
                    )
                    .await
                    .unwrap();
            }
        }
        let cache = PinCache::new(local.clone(), 32);
        if !membership_first {
            cache.wait_for_recovery().await;
            assert_eq!(published_bytes(&cache), 0);
            assert_eq!(cache.gc.accounted_bytes(), 32);
        }
        cache.replace_desired_objects(objects.into_iter().map(|id| (id, 8)));
        cache.wait_for_recovery().await;
        for id in objects {
            let lookup = cache.lookup(id);
            assert!(lookup.desired);
            assert_eq!(
                lookup.route.unwrap().read(..).await.unwrap(),
                Bytes::from_static(b"complete")
            );
        }
        assert_eq!(published_bytes(&cache), 16);
        // Duplicate recovered paths are reclaimed, with exactly one publication per object.
        tokio::time::timeout(Duration::from_secs(5), async {
            while cache.gc.accounted_bytes() != 16 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cache.replace_desired_objects([]);
        wait_for_reclaim(&cache).await;
    }
}
