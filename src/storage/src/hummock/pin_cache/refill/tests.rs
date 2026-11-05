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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError};

use super::{PinCache, PinCacheFile, write_file};
use crate::hummock::pin_cache::test_utils::{
    download_and_publish_for_test, in_memory_object_store, local_object_store,
};
use crate::monitor::ObjectStoreMetrics;

#[tokio::test]
async fn test_revoked_download_cannot_publish_and_replacement_can_retry() {
    for revoke_by_unregister in [false, true] {
        let pin_cache = PinCache::new(in_memory_object_store(), 1, [])
            .await
            .unwrap();
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.register_objects([object_id]);
        let token = pin_cache.prepare_refill(object_id).unwrap();
        let remote = in_memory_object_store();
        remote
            .upload("sst", Bytes::from_static(b"replacement"))
            .await
            .unwrap();
        let old = pin_cache
            .download(object_id, 11, remote.clone(), "sst".into())
            .await
            .unwrap();

        if revoke_by_unregister {
            pin_cache.unregister_objects([object_id]);
            pin_cache.register_objects([object_id]);
        } else {
            pin_cache.revoke_refill(object_id);
        }
        let old_path = old.file.as_ref().unwrap().path.clone();
        // The executor waits for the revoked attempt to finish before starting its replacement.
        assert!(!pin_cache.publish(token, old));
        assert!(pin_cache.get(object_id).is_none());
        pin_cache.select_minor().delete().await.unwrap();
        assert!(
            pin_cache
                .store
                .metadata(&old_path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );

        download_and_publish_for_test(&pin_cache, remote, "sst".into(), object_id, 11)
            .await
            .unwrap();
        assert_ne!(old_path, pin_cache.get(object_id).unwrap().file.path);
        assert_eq!(
            pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"replacement")
        );
    }
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_live_fs_upload_gc_protection_and_partial_remote_error_cleanup() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 1, []).await.unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([object_id]);
    let file = PinCacheFile {
        path: "1001-0.sst".into(),
        size: 8,
    };
    let final_path = file.path.clone();
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
                started_tx.send(()).unwrap();
                let _ = fail_rx.await;
                Err(ObjectError::internal("injected remote read failure"))
            })),
        ),
        Arc::new(ObjectStoreMetrics::unused()),
        None,
    );
    assert!(pin_cache.admit_upload(&file).await.unwrap());
    // Isolate a partial remote-read failure. Cancellation is covered by the real
    // executor tests, where the physical owner outlives canceled waiters.
    let upload_cache = pin_cache.clone();
    let task = tokio::spawn(async move { write_file(&upload_cache, &file, reader).await });
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    pin_cache.select_minor().delete().await.unwrap();
    pin_cache
        .select_full(std::time::SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        files.iter().any(|file| file.total_size == 4),
        "a live upload's temporary file must survive GC"
    );
    fail_tx.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        local_store.metadata(&final_path).await.unwrap().total_size,
        5
    );
    assert!(pin_cache.get(object_id).is_none());
    assert!(pin_cache.storage.lock().uploads.is_empty());
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    // Leave the failed copy's bytes on disk while admitting the replacement.
    download_and_publish_for_test(&pin_cache, remote, "sst".into(), object_id, 8)
        .await
        .unwrap();
    let published = pin_cache.get(object_id).unwrap();
    pin_cache.select_minor().delete().await.unwrap();
    pin_cache
        .select_full(std::time::SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        files
            .iter()
            .all(|file| file.key.ends_with('/') || file.key == published.file.path)
    );
    assert_eq!(
        published.read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_close_error_preserves_original_error_with_successful_or_failed_abort() {
    use risingwave_common::config::ObjectStoreConfig;

    use crate::hummock::pin_cache::test_utils::local_object_store_with_config;
    for abort_fails in [false, true] {
        let mut config = ObjectStoreConfig {
            upload_part_size: 1,
            ..Default::default()
        };
        config.retry.delete_objects_retry_attempts = 0;
        let (dir, local) = local_object_store_with_config(config).await;
        let cache = PinCache::new(local.clone(), 1, []).await.unwrap();
        cache.register_objects([1001.into(), 1002.into()]);
        cache.set_available_space_for_test(|| Ok(Some(8)));
        let remote = in_memory_object_store();
        remote
            .upload("sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();
        let mut finish = cache.watch_upload_finish_for_test();
        let task = tokio::spawn({
            let cache = cache.clone();
            let remote = remote.clone();
            async move { cache.download(1001.into(), 8, remote, "sst".into()).await }
        });
        let (target, resume) = tokio::time::timeout(Duration::from_secs(5), finish.recv())
            .await
            .unwrap()
            .unwrap();
        let inventory: Vec<_> = local
            .list("atomic_write_dir/", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let prefix = format!("atomic_write_dir/{target}.");
        let actual = inventory
            .iter()
            .find(|m| m.key.starts_with(&prefix) && !m.key.ends_with('/'))
            .unwrap()
            .key
            .clone();
        std::fs::create_dir(dir.path().join(&target)).unwrap();
        if abort_fails {
            // Close and abort fail differently. Use the discovered backend name.
            std::fs::remove_file(dir.path().join(&actual)).unwrap();
            std::fs::create_dir(dir.path().join(&actual)).unwrap();
            for path in [&actual, &target] {
                std::fs::write(dir.path().join(path).join("blocked"), b"owned fault").unwrap();
            }
        }
        resume.send(()).unwrap();
        drop(finish);
        let error = match task.await.unwrap() {
            Err(error) => error,
            Ok(_) => panic!("close must fail"),
        };
        assert!(cache.get(1001.into()).is_none());
        assert_eq!(cache.storage.lock().uploads.values().sum::<u64>(), 8);
        if abort_fails {
            assert!(
                error.to_string().to_lowercase().contains("not empty"),
                "original close error was masked: {error}"
            );
            assert!(cache.select_minor().delete().await.is_err());
            assert!(cache.storage.lock().failed_uploads.contains(&target));
            assert!(matches!(
                cache.prepare_download(1002.into(), 1).await,
                Err(super::PinCacheDownloadError::CapacityRejected)
            ));
            for path in [&actual, &target] {
                std::fs::remove_file(dir.path().join(path).join("blocked")).unwrap();
                std::fs::remove_dir(dir.path().join(path)).unwrap();
            }
            // A later minor rediscovers this actual random name after the fault clears.
            std::fs::write(dir.path().join(&actual), b"leftover").unwrap();
        } else {
            assert!(
                !dir.path().join(&actual).exists(),
                "abort must remove its atomic temp"
            );
            std::fs::remove_dir(dir.path().join(&target)).unwrap();
        }
        cache.select_minor().delete().await.unwrap();
        assert!(cache.storage.lock().failed_uploads.is_empty());
        assert!(cache.storage.lock().uploads.is_empty());
        assert!(!dir.path().join(&actual).exists());
        assert!(!dir.path().join(&target).exists());
        download_and_publish_for_test(&cache, remote, "sst".into(), 1001.into(), 8)
            .await
            .unwrap();
        assert_eq!(
            cache.get(1001.into()).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"complete")
        );
    }
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_shutdown_keeps_physical_permit_and_byte_reservation_until_io_exits() {
    use std::sync::atomic::{AtomicU64, Ordering};

    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_pb::id::TableId;
    use tokio::sync::Semaphore;

    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::pin_cache::test_utils::local_object_store;
    use crate::hummock::pin_cache_refill::PinCacheRefillObject;
    use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;
    let store = mock_sstable_store().await;
    let (_dir, local) = local_object_store().await;
    let cache = PinCache::new(local, 1, []).await.unwrap();
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    let object = 887.into();
    store
        .store()
        .upload(
            &store.get_sst_data_path(object),
            bytes::Bytes::from_static(b"complete"),
        )
        .await
        .unwrap();
    cache.register_objects([object, 888.into()]);
    let free = Arc::new(AtomicU64::new(8));
    cache.set_available_space_for_test({
        let free = free.clone();
        move || Ok(Some(free.load(Ordering::SeqCst)))
    });
    let mut finish = cache.watch_upload_finish_for_test();
    let concurrency = Arc::new(Semaphore::new(1));
    let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
    let candidate = PinCacheRefillObject {
        source_sst: SstableInfoInner {
            object_id: object,
            file_size: 8,
            ..Default::default()
        }
        .into(),
        admitted_tables: [TableId::from(233)].into(),
    };
    let waiter = executor.submit(&candidate, cache.prepare_refill(object).unwrap());
    let (_, resume) = tokio::time::timeout(Duration::from_secs(5), finish.recv())
        .await
        .unwrap()
        .unwrap();
    drop(executor);
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
    );
    assert_eq!(
        concurrency.available_permits(),
        0,
        "waiter shutdown cannot release a physical copy's data permit"
    );
    assert_eq!(cache.storage.lock().uploads.values().sum::<u64>(), 8);
    assert!(matches!(
        cache.prepare_download(888.into(), 1).await,
        Err(super::PinCacheDownloadError::CapacityRejected)
    ));
    free.store(0, Ordering::SeqCst);
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while concurrency.available_permits() != 1 || !cache.storage.lock().uploads.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        cache.get(object).is_none(),
        "shutdown must prevent a late publication"
    );
    assert_eq!(
        cache
            .storage
            .lock()
            .files
            .values()
            .map(|f| f.size)
            .sum::<u64>(),
        8
    );
    cache.select_minor().delete().await.unwrap();
    free.store(8, Ordering::SeqCst);
    drop(cache.prepare_download(888.into(), 8).await.unwrap());
}
#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_panicked_copy_and_cancelled_waiter_retain_cleanup_until_physical_exit() {
    use std::sync::atomic::Ordering;

    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_pb::id::TableId;
    use tokio::sync::Semaphore;

    use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
    use crate::hummock::pin_cache_refill::PinCacheRefillObject;
    use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;

    const SIZE: u64 = 8 * 1024 * 1024;
    for cancel_waiter in [false, true] {
        let (_remote_dir, remote) = local_object_store().await;
        let store = mock_sstable_store_with_object_store(remote).await;
        let (dir, local) = local_object_store().await;
        let cache = PinCache::new(local.clone(), 1, []).await.unwrap();
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let object = 887.into();
        store
            .store()
            .upload(
                &store.get_sst_data_path(object),
                Bytes::from(vec![7; SIZE as usize]),
            )
            .await
            .unwrap();
        cache.register_objects([object, 888.into()]);
        cache.set_available_space_for_test(|| Ok(Some(SIZE)));
        let mut finish = cache.watch_upload_finish_for_test();
        cache
            .test_hooks
            .panic_before_upload_finish
            .store(true, Ordering::Relaxed);
        let concurrency = Arc::new(Semaphore::new(1));
        let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
        let candidate = PinCacheRefillObject {
            source_sst: SstableInfoInner {
                object_id: object,
                file_size: SIZE,
                ..Default::default()
            }
            .into(),
            admitted_tables: [TableId::from(233)].into(),
        };
        let waiter = executor.submit(&candidate, cache.prepare_refill(object).unwrap());
        tokio::pin!(waiter);
        let (target, resume) = tokio::time::timeout(Duration::from_secs(5), finish.recv())
            .await
            .unwrap()
            .unwrap();
        let inventory: Vec<_> = local
            .list("atomic_write_dir/", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let prefix = format!("atomic_write_dir/{target}.");
        let actual = inventory
            .iter()
            .find(|entry| entry.key.starts_with(&prefix) && !entry.key.ends_with('/'))
            .unwrap()
            .key
            .clone();
        assert!(
            dir.path().join(&actual).metadata().unwrap().len() > 0,
            "fixture must have completed actual position writes before the panic"
        );
        if cancel_waiter {
            executor.cancel_objects([object]);
            assert_eq!(waiter.as_mut().now_or_never(), Some(false));
            assert!(
                !executor
                    .submit(&candidate, cache.prepare_refill(object).unwrap())
                    .await,
                "cancellation must not start an overlapping physical copy"
            );
        }
        assert_eq!(concurrency.available_permits(), 0);
        assert_eq!(cache.storage.lock().uploads.values().sum::<u64>(), SIZE);
        assert!(cache.storage.lock().failed_uploads.is_empty());
        assert!(matches!(
            cache.prepare_download(888.into(), 1).await,
            Err(super::PinCacheDownloadError::CapacityRejected)
        ));
        resume.send(()).unwrap();
        drop(finish);
        if !cancel_waiter {
            assert!(
                !tokio::time::timeout(Duration::from_secs(5), waiter)
                    .await
                    .unwrap()
            );
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while executor.has_pending_for_test(object) || concurrency.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !cache
                .test_hooks
                .panic_before_upload_finish
                .load(Ordering::Relaxed),
            "the injected panic must have been reached"
        );
        assert!(cache.get(object).is_none());
        assert_eq!(cache.storage.lock().uploads.values().sum::<u64>(), SIZE);
        assert!(cache.storage.lock().failed_uploads.contains(&target));
        cache.select_minor().delete().await.unwrap();
        assert!(cache.storage.lock().uploads.is_empty());
        assert!(cache.storage.lock().failed_uploads.is_empty());
        assert!(!dir.path().join(&actual).exists());
        assert!(
            tokio::time::timeout(
                Duration::from_secs(5),
                executor.submit(&candidate, cache.prepare_refill(object).unwrap())
            )
            .await
            .unwrap()
        );
        assert!(cache.get(object).is_some());
        assert!(
            !dir.path().join(&actual).exists(),
            "no late write may recreate the old temp"
        );
    }
}

#[cfg(not(madsim))]
#[tokio::test]
#[ignore = "requires an owned bounded filesystem in RW_PIN_ENOSPC_MOUNT"]
async fn test_real_enospc_after_admission_cleans_failed_upload() {
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;

    use risingwave_common::config::ObjectStoreConfig;
    use risingwave_object_store::object::build_remote_object_store;

    use crate::monitor::ObjectStoreMetrics;

    let mount =
        PathBuf::from(std::env::var_os("RW_PIN_ENOSPC_MOUNT").expect("owned mount required"));
    assert_eq!(
        std::fs::read(mount.join("pin-enospc-owned")).unwrap(),
        b"pin-root-fix-test"
    );
    assert_ne!(
        mount.metadata().unwrap().dev(),
        mount.parent().unwrap().metadata().unwrap().dev(),
        "never fill the host filesystem"
    );
    let dir = tempfile::tempdir_in(&mount).unwrap();
    let mut config = ObjectStoreConfig {
        upload_part_size: 1,
        ..Default::default()
    };
    config.set_atomic_write_dir();
    let local = Arc::new(
        build_remote_object_store(
            &format!("fs://{}", dir.path().display()),
            Arc::new(ObjectStoreMetrics::unused()),
            "bounded ENOSPC test",
            Arc::new(config),
        )
        .await,
    );
    let initial = local.available_space().await.unwrap().unwrap();
    assert!(
        (16 * 1024 * 1024..96 * 1024 * 1024).contains(&initial),
        "bounded filesystem required: {initial}"
    );
    let cache = PinCache::new(local.clone(), 1, []).await.unwrap();
    cache.register_objects([1001.into()]);
    let remote = in_memory_object_store();
    let bytes = Bytes::from(vec![7; 8 * 1024 * 1024]);
    remote.upload("sst", bytes.clone()).await.unwrap();
    // Admission succeeds before an independent disk user consumes physical space.
    let prepared = cache
        .prepare_download(1001.into(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let filler_path = mount.join("owned-external-filler");
    let mut filler = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&filler_path)
        .unwrap();
    let block = vec![1; 1024 * 1024];
    let mut filled = 0;
    while local.available_space().await.unwrap().unwrap() > 1024 * 1024 {
        assert!(filled < 96 * 1024 * 1024, "bounded filler limit");
        if let Err(error) = filler.write_all(&block) {
            assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
            break;
        }
        filled += block.len();
    }
    drop(filler);
    println!(
        "bounded ENOSPC: initial_free={initial}, external_filler={filled}, remaining_free={}",
        local.available_space().await.unwrap().unwrap()
    );
    let task = tokio::spawn(prepared.run(remote.clone(), "sst".into()));
    let error = match tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
    {
        Err(error) => error,
        Ok(_) => panic!("external writer must cause ENOSPC"),
    };
    assert!(
        (error.to_string().to_lowercase().contains("no space")
            || error
                .to_string()
                .to_lowercase()
                .contains("no storage space")),
        "original error: {error}"
    );
    assert_eq!(
        cache.storage.lock().uploads.values().sum::<u64>(),
        8 * 1024 * 1024
    );
    cache.select_minor().delete().await.unwrap();
    assert!(cache.storage.lock().uploads.is_empty());
    let files: Vec<_> = local
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        files.iter().all(|f| f.key.ends_with('/')),
        "known failed paths remain: {files:?}"
    );
    std::fs::remove_file(filler_path).unwrap();
    download_and_publish_for_test(
        &cache,
        remote,
        "sst".into(),
        1001.into(),
        bytes.len() as u64,
    )
    .await
    .unwrap();
    assert_eq!(
        cache.get(1001.into()).unwrap().read(..).await.unwrap(),
        bytes
    );
    println!(
        "bounded ENOSPC: original error preserved, minor cleanup confirmed, explicit retry published"
    );
}
