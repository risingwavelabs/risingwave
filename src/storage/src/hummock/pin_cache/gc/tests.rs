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

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::TryStreamExt;

use super::super::test_utils::{in_memory_object_store, local_object_store};
use super::{DELETE_BATCH_SIZE, PinCache, PinCacheGcSelection};

#[tokio::test]
async fn test_gc_worker_reclaims_startup_garbage_and_withdrawn_files() {
    let store = in_memory_object_store();
    for path in ["1001-1.sst", "1002-2.sst", "orphan.tmp"] {
        store
            .upload(path, Bytes::from_static(b"complete"))
            .await
            .unwrap();
    }
    let cache = PinCache::new(store.clone(), 1, [(1001.into(), 8)])
        .await
        .unwrap();
    let reader = cache.get(1001.into()).unwrap();
    assert_eq!(cache.storage.lock().files.len(), 2);
    let _gc = scopeguard::guard(cache.start_gc(), |task| task.abort());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let tracked = cache.storage.lock().files.len();
            if tracked == 1
                && store.metadata("1002-2.sst").await.is_err()
                && store.metadata("orphan.tmp").await.is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup GC must reclaim inventory rejects and temporary files");
    assert!(store.metadata("1002-2.sst").await.is_err());

    cache.unregister_objects([1001.into()]);
    // Let minor GC inspect the withdrawn file while its reader still holds a lease.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(cache.storage.lock().files.contains_key("1001-1.sst"));
    assert_eq!(
        reader.read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
    drop(reader);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !cache.storage.lock().files.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("minor GC must reclaim a withdrawal after its final reader exits");
    assert!(store.metadata("1001-1.sst").await.is_err());
}

#[tokio::test]
async fn test_selection_protects_files_and_can_be_abandoned() {
    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
    let file = cache.track_file("1001-1.sst".into(), 8);
    store
        .upload(&file.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let abandoned = cache.track_file("1002-2.sst".into(), 8);
    store
        .upload(&abandoned.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    drop(abandoned);
    // Even a complete, unreferenced file is not a minor candidate without deletion intent.
    assert!(cache.select_minor().files.is_empty());
    cache.enqueue_delete(file.clone());
    // The explicit candidate still has a reader; enqueueing it must not bypass that lease.
    assert!(cache.select_minor().files.is_empty());
    drop(file);
    let selected = cache.select_minor();
    assert_eq!(selected.files.len(), 1);
    assert!(cache.select_minor().files.is_empty());
    let full = cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(full.files.len(), 1);
    assert_eq!(full.files[0].path, "1002-2.sst");
    drop(full);
    drop(selected);
    assert_eq!(cache.storage.lock().files.len(), 2);
    cache.select_minor().delete().await.unwrap();
    assert_eq!(cache.storage.lock().files.len(), 1);
    assert!(
        store
            .metadata("1001-1.sst")
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
    assert!(store.metadata("1002-2.sst").await.is_ok());
    cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert!(cache.storage.lock().files.is_empty());
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_failed_deletion_stays_tracked_and_full_gc_retries() {
    let (dir, store) = local_object_store().await;
    let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
    let path = "1001-42.sst";
    // A nonempty directory makes deletion fail even when the test runs as root.
    std::fs::create_dir(dir.path().join(path)).unwrap();
    std::fs::write(dir.path().join(path).join("child"), b"complete").unwrap();
    cache.enqueue_delete(cache.track_file(path.into(), 8));
    assert!(cache.select_minor().delete().await.is_err());
    assert!(cache.storage.lock().files.contains_key(path));
    assert_eq!(cache.select_minor().files.len(), 1);
    std::fs::remove_file(dir.path().join(path).join("child")).unwrap();
    std::fs::remove_dir(dir.path().join(path)).unwrap();
    // A full pass forgets a tracked file that has already been removed.
    cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert!(cache.storage.lock().files.is_empty());
    assert!(cache.select_minor().files.is_empty());
}

#[tokio::test]
async fn test_full_gc_respects_retention_and_discovers_zombies() {
    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
    store
        .upload("orphan.tmp", Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    assert!(
        cache
            .select_full(SystemTime::UNIX_EPOCH)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    let selection = cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(selection.files.len(), 1);
    assert!(cache.storage.lock().files.is_empty());
    selection.delete().await.unwrap();
    assert!(cache.storage.lock().files.is_empty());
    assert!(
        store
            .metadata("orphan.tmp")
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
}

#[tokio::test]
async fn test_full_gc_preserves_objects_but_reclaims_stale_temporary_files() {
    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
    let final_path = "1001-1.sst";
    let temporary_path = "atomic_write_dir/1001-1.sst.12345678";
    store
        .upload(final_path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    store
        .upload(temporary_path, Bytes::from_static(b"temp"))
        .await
        .unwrap();
    let published = cache.track_file(final_path.into(), 8);

    // A completed object protects only its own path, not temporary residue from an old upload.
    cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert!(cache.storage.lock().files.contains_key(final_path));
    assert!(store.metadata(&published.path).await.is_ok());
    assert!(
        store
            .metadata(temporary_path)
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_partial_batch_releases_only_deleted_objects() {
    let (dir, store) = local_object_store().await;
    let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
    let mut files: Vec<_> = (0..DELETE_BATCH_SIZE)
        .map(|id| cache.track_file(format!("missing-{id}.sst"), 1))
        .collect();
    let failed = "failed.sst";
    std::fs::create_dir(dir.path().join(failed)).unwrap();
    std::fs::write(dir.path().join(failed).join("child"), b"x").unwrap();
    files.push(cache.track_file(failed.into(), 1));
    // The first batch succeeds; the second fails. Only acknowledged paths leave the index.
    let selection = PinCacheGcSelection {
        failed_uploads: Vec::new(),
        cache: cache.clone(),
        files,
    };
    assert!(selection.delete().await.is_err());
    let state = cache.storage.lock();
    assert_eq!(state.files.len(), 1);
    assert!(state.files.contains_key(failed));
}

#[tokio::test]
async fn test_cancelled_gc_keeps_completed_batches_released() {
    for full in [false, true] {
        let store = in_memory_object_store();
        let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
        for id in 0..=DELETE_BATCH_SIZE {
            let path = format!("1001-{id}.sst");
            store.upload(&path, Bytes::from_static(b"x")).await.unwrap();
            let file = cache.track_file(path, 1);
            if !full {
                cache.enqueue_delete(file);
            }
        }
        let selection = if full {
            cache
                .select_full(SystemTime::now() + Duration::from_secs(1))
                .await
                .unwrap()
        } else {
            cache.select_minor()
        };
        // Start with a fresh cooperative budget after fixture setup. InMem deletion is ready,
        // so the next Pending is the deletion's batch yield, with one file still pending.
        tokio::task::yield_now().await;
        let mut deletion = Box::pin(selection.delete());
        assert!(futures::poll!(deletion.as_mut()).is_pending());
        drop(deletion);
        let remaining: Vec<_> = store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(cache.storage.lock().files.len(), 1);

        // A later pass collects the tracked remainder.
        cache
            .select_full(SystemTime::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .delete()
            .await
            .unwrap();
        assert!(cache.storage.lock().files.is_empty());
        assert!(
            store
                .metadata(&remaining[0].key)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
    }
}

// Keep the paused Tokio clock under the test's control while real filesystem I/O runs.
// Awaiting a timer-based timeout here could advance directly to a later periodic scan.
async fn wait_for_gc(mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "GC did not make progress"
        );
        tokio::task::yield_now().await;
    }
}

async fn next_full_scan(
    scans: &mut tokio::sync::mpsc::UnboundedReceiver<super::super::test_utils::FullScan>,
) -> super::super::test_utils::FullScan {
    let mut scan = None;
    wait_for_gc(|| {
        scan = match scans.try_recv() {
            Ok(scan) => Some(scan),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("GC worker stopped before the expected scan")
            }
        };
        scan.is_some()
    })
    .await;
    scan.unwrap()
}

// The selection owns one cache Arc until deletion has completed or failed.
async fn finish_full_scan(
    cache: &std::sync::Arc<PinCache>,
    resume: tokio::sync::oneshot::Sender<()>,
) {
    let owners = std::sync::Arc::strong_count(cache);
    resume.send(()).unwrap();
    wait_for_gc(|| std::sync::Arc::strong_count(cache) < owners).await;
    tokio::task::yield_now().await;
}

async fn advance_gc(duration: Duration) {
    #[cfg(madsim)]
    tokio::time::advance(duration);
    #[cfg(not(madsim))]
    tokio::time::advance(duration).await;
    tokio::task::yield_now().await;
}

async fn reject_copy(cache: &std::sync::Arc<PinCache>, size: u64) {
    cache.register_objects([1003.into()]);
    assert!(matches!(
        cache
            .download(
                1003.into(),
                size,
                in_memory_object_store(),
                "missing".into()
            )
            .await,
        Err(super::super::PinCacheDownloadError::CapacityRejected)
    ));
}

#[tokio::test(start_paused = true)]
async fn test_gc_worker_coalesces_pressure_and_preserves_arrivals_during_full() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{FULL_GC_COOLDOWN, MINOR_GC_INTERVAL};

    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
    let available = Arc::new(AtomicU64::new(0));
    let probe = available.clone();
    cache.set_available_space_for_test(move || Ok(Some(probe.load(Ordering::Relaxed))));
    let mut scans = cache.watch_full_scans_for_test();
    let _gc = scopeguard::guard(cache.start_gc(), |task| task.abort());
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert!(paths.is_empty());
    finish_full_scan(&cache, resume).await;

    store
        .upload("first.tmp", Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    // The later, smaller rejection must not replace the larger physical requirement.
    reject_copy(&cache, 200).await;
    reject_copy(&cache, 100).await;
    available.store(150, Ordering::Relaxed);
    advance_gc(FULL_GC_COOLDOWN - MINOR_GC_INTERVAL).await;
    assert!(
        scans.try_recv().is_err(),
        "pressure must respect the startup cooldown"
    );
    advance_gc(MINOR_GC_INTERVAL).await;
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert_eq!(paths, ["first.tmp"]);

    // This copy is rejected after the worker took the earlier batch, while its real
    // full selection is still alive. Its signal must survive completion of that scan.
    available.store(0, Ordering::Relaxed);
    reject_copy(&cache, 300).await;
    available.store(250, Ordering::Relaxed);
    store
        .upload("second.tmp", Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    finish_full_scan(&cache, resume).await;
    assert!(store.metadata("first.tmp").await.is_err());
    assert!(store.metadata("second.tmp").await.is_ok());
    advance_gc(FULL_GC_COOLDOWN - MINOR_GC_INTERVAL).await;
    assert!(
        scans.try_recv().is_err(),
        "a new hint must not bypass full-scan cooldown"
    );
    advance_gc(MINOR_GC_INTERVAL).await;
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert_eq!(paths, ["second.tmp"]);
    finish_full_scan(&cache, resume).await;
    assert!(store.metadata("second.tmp").await.is_err());

    // A scan that cannot reclaim anything still consumes this ended attempt's hint.
    reject_copy(&cache, 400).await;
    advance_gc(FULL_GC_COOLDOWN).await;
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert!(paths.is_empty());
    finish_full_scan(&cache, resume).await;
    advance_gc(FULL_GC_COOLDOWN * 2).await;
    assert!(
        scans.try_recv().is_err(),
        "an unsatisfied old hint must not become retry debt"
    );
    assert!(cache.storage.lock().uploads.is_empty());
    assert!(cache.get(1003.into()).is_none());
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test(start_paused = true)]
async fn test_gc_worker_rechecks_space_after_minor_before_full() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::{FULL_GC_COOLDOWN, MINOR_GC_INTERVAL};

    let (dir, store) = local_object_store().await;
    let cache = PinCache::new(store, 1, []).await.unwrap();
    let pending = dir.path().join("1001-1.sst");
    let queries = Arc::new(AtomicUsize::new(0));
    let pending_when_checked = Arc::new(AtomicBool::new(false));
    let probe_queries = queries.clone();
    let probe_pending = pending_when_checked.clone();
    let probe_path = pending.clone();
    cache.set_available_space_for_test(move || {
        let present = probe_path.exists();
        probe_pending.store(present, Ordering::Relaxed);
        probe_queries.fetch_add(1, Ordering::Relaxed);
        Ok(Some(if present { 0 } else { 128 }))
    });
    let mut scans = cache.watch_full_scans_for_test();
    let _gc = scopeguard::guard(cache.start_gc(), |task| task.abort());
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert!(paths.is_empty());
    finish_full_scan(&cache, resume).await;
    // Let the last ineligible tick finish before creating either the file or pressure.
    // The next tick must run minor before its eligible pressure recheck.
    advance_gc(FULL_GC_COOLDOWN - MINOR_GC_INTERVAL).await;
    std::fs::write(&pending, b"discarded").unwrap();
    cache.enqueue_delete(cache.track_file("1001-1.sst".into(), 9));
    reject_copy(&cache, 64).await;
    assert_eq!(queries.load(Ordering::Relaxed), 1);
    assert!(pending_when_checked.load(Ordering::Relaxed));
    advance_gc(MINOR_GC_INTERVAL).await;
    wait_for_gc(|| queries.load(Ordering::Relaxed) >= 2).await;
    assert!(
        !pending_when_checked.load(Ordering::Relaxed),
        "the eligible pressure recheck must observe the preceding minor deletion"
    );
    assert!(!pending.exists());
    assert!(!cache.storage.lock().files.contains_key("1001-1.sst"));
    tokio::task::yield_now().await;
    assert!(
        scans.try_recv().is_err(),
        "minor already made enough space for this hint"
    );
}

#[tokio::test(start_paused = true)]
async fn test_gc_worker_periodic_scan_protects_uploads_and_readers() {
    use tokio::sync::mpsc::error::TryRecvError;

    use super::super::PinCacheFile;
    use super::MINOR_GC_INTERVAL;

    let store = in_memory_object_store();
    store
        .upload("1001-1.sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let cache = PinCache::new(store.clone(), 1, [(1001.into(), 8)])
        .await
        .unwrap();
    let reader = cache.get(1001.into()).unwrap();
    cache.unregister_objects([1001.into()]);
    let mut scans = cache.watch_full_scans_for_test();
    let _gc = scopeguard::guard(cache.start_gc(), |task| task.abort());
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert!(paths.is_empty());
    finish_full_scan(&cache, resume).await;

    let upload = PinCacheFile {
        path: "1002-1.sst".into(),
        size: 8,
    };
    assert!(cache.admit_upload(&upload).await.unwrap());
    let temporary = "atomic_write_dir/1002-1.sst.12345678";
    store
        .upload(&upload.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    store
        .upload(temporary, Bytes::from_static(b"partial"))
        .await
        .unwrap();
    store
        .upload("late.tmp", Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    // No download was rejected. Only the periodic trigger can discover this late orphan.
    let previous_interval = Duration::from_secs(5 * 60);
    let periodic_interval = Duration::from_secs(60 * 60);
    advance_gc(previous_interval).await;
    assert!(
        matches!(scans.try_recv(), Err(TryRecvError::Empty)),
        "unexpected full scan at five minutes"
    );
    advance_gc(periodic_interval - previous_interval - MINOR_GC_INTERVAL).await;
    assert!(
        matches!(scans.try_recv(), Err(TryRecvError::Empty)),
        "unexpected full scan before one hour"
    );
    advance_gc(MINOR_GC_INTERVAL).await;
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert_eq!(paths, ["late.tmp"]);
    store
        .upload("after-scan.tmp", Bytes::from_static(b"newer"))
        .await
        .unwrap();
    finish_full_scan(&cache, resume).await;
    assert!(store.metadata("late.tmp").await.is_err());
    assert!(store.metadata("after-scan.tmp").await.is_ok());
    assert!(store.metadata(temporary).await.is_ok());
    assert!(store.metadata(&upload.path).await.is_ok());
    assert_eq!(
        reader.read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );

    drop(reader);
    cache.finish_upload(&upload.path);
    advance_gc(periodic_interval).await;
    let (mut paths, resume) = next_full_scan(&mut scans).await;
    paths.sort();
    assert_eq!(paths, ["1002-1.sst", "after-scan.tmp", temporary]);
    // The reader's discarded final was eligible for minor before this full pass.
    assert!(store.metadata("1001-1.sst").await.is_err());
    finish_full_scan(&cache, resume).await;
    assert!(
        store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test(start_paused = true)]
async fn test_gc_worker_full_failure_keeps_file_for_next_periodic_pass() {
    use risingwave_common::config::ObjectStoreConfig;

    use super::super::test_utils::local_object_store_with_config;
    use super::FULL_GC_INTERVAL;

    // Observe the worker's handling of the delete error without backend retry timers.
    let mut config = ObjectStoreConfig::default();
    config.retry.delete_objects_retry_attempts = 0;
    let (dir, store) = local_object_store_with_config(config).await;
    let cache = PinCache::new(store, 1, []).await.unwrap();
    let mut scans = cache.watch_full_scans_for_test();
    let _gc = scopeguard::guard(cache.start_gc(), |task| task.abort());
    let (_, resume) = next_full_scan(&mut scans).await;
    finish_full_scan(&cache, resume).await;

    let path = "1001-1.sst";
    let local_path = dir.path().join(path);
    let write_garbage = || {
        std::fs::write(&local_path, b"garbage").unwrap();
        std::fs::File::open(&local_path)
            .unwrap()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1))
            .unwrap();
    };
    write_garbage();
    drop(cache.track_file(path.into(), 7));
    advance_gc(FULL_GC_INTERVAL).await;
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert_eq!(paths, [path]);
    // Change only the selected target after LIST; real FS deletion must now fail.
    std::fs::remove_file(&local_path).unwrap();
    std::fs::create_dir(&local_path).unwrap();
    std::fs::write(local_path.join("child"), b"blocked").unwrap();
    finish_full_scan(&cache, resume).await;
    assert!(local_path.join("child").exists());
    assert!(cache.storage.lock().files.contains_key(path));

    std::fs::remove_file(local_path.join("child")).unwrap();
    std::fs::remove_dir(&local_path).unwrap();
    write_garbage();
    advance_gc(FULL_GC_INTERVAL).await;
    let (paths, resume) = next_full_scan(&mut scans).await;
    assert_eq!(paths, [path]);
    finish_full_scan(&cache, resume).await;
    wait_for_gc(|| !cache.storage.lock().files.contains_key(path)).await;
    assert!(!local_path.exists());
}
