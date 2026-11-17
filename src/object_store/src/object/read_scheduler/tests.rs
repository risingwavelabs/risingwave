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

use std::time::Duration;

use tokio::sync::mpsc;

use super::super::{InMemObjectStore, ObjectErrorInner, ObjectStore, ObjectStoreConfig};
use super::execution::{PhysicalReadReservation, acquire_physical};
use super::*;

#[cfg(not(madsim))]
#[test]
fn dedicated_io_runtime_reads_while_scan_worker_is_blocked() {
    struct SignalOnDrop(Option<oneshot::Sender<()>>);

    impl Drop for SignalOnDrop {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    struct ReportWorkerExit(Option<oneshot::Sender<tokio::runtime::Id>>);

    impl Drop for ReportWorkerExit {
        fn drop(&mut self) {
            let _ = self
                .0
                .take()
                .unwrap()
                .send(tokio::runtime::Handle::current().id());
        }
    }

    // Each worker count is tested in a fresh process because the pool
    // belongs to the process, including between scheduler lifetimes.
    let config = ReadSchedulerConfig {
        dedicated_io_runtime_workers: std::env::var("RW_TEST_DEDICATED_IO_WORKERS").map_or_else(
            |_| ReadSchedulerConfig::default().dedicated_io_runtime_workers,
            |workers| workers.parse().unwrap(),
        ),
        use_dedicated_io_runtime: true,
        max_admitted_groups: 1.try_into().unwrap(),
        max_inflight_reads: 1.try_into().unwrap(),
        ..Default::default()
    };
    let expected_workers = config.dedicated_io_runtime_workers.get();
    let scan = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    scan.block_on(async {
        let scan_id = tokio::runtime::Handle::current().id();
        let (release_worker, blocked) = std::sync::mpsc::channel::<()>();
        let (entered, worker_entered) = oneshot::channel();
        let scan_worker = tokio::spawn(async move {
            entered.send(()).unwrap();
            // Simulate a scan occupying every worker on the caller runtime.
            let _ = blocked.recv();
        });
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (started, mut reads) = mpsc::unbounded_channel();
        let (cancelled, cancellation) = oneshot::channel();
        let cancelled = Arc::new(Mutex::new(Some(cancelled)));
        let (closed, worker_closed) = oneshot::channel();
        let backend_owner = ReportWorkerExit(Some(closed));
        let scheduler = Arc::new(ReadScheduler::new(&metrics, "test", config, {
            move |path, range: Range<usize>| {
                let _ = &backend_owner;
                let started = started.clone();
                let cancelled = cancelled.clone();
                async move {
                    let _cancelled = SignalOnDrop(if path == "cancel" {
                        cancelled.lock().unwrap().take()
                    } else {
                        None
                    });
                    let (reply, reply_open) = oneshot::channel();
                    // Emitted from the backend future's first poll, not from admission or dequeue.
                    let runtime = tokio::runtime::Handle::current();
                    assert_eq!(runtime.metrics().num_workers(), expected_workers);
                    assert_eq!(std::thread::current().name(), Some("rw-read-io"));
                    started.send((runtime.id(), path, range, reply)).unwrap();
                    reply_open.await.unwrap()
                }
            }
        }));
        let io_id = dedicated_io_runtime(expected_workers).handle().id();
        // The timeout must still fire if the only scan worker stays blocked.
        let test = async {
            worker_entered.await.unwrap();
            assert_ne!(io_id, scan_id);
            let check = |call: (
                tokio::runtime::Id,
                String,
                Range<usize>,
                oneshot::Sender<ReadResult>,
            ),
                         path: &str,
                         range: Range<usize>| {
                assert_eq!(call.0, io_id);
                assert_eq!(call.1, path);
                assert_eq!(call.2, range);
                call.3
            };

            // Direct admission runs on this block_on thread, not the parked scan worker.
            let response = scheduler.submit("scan", 0..4, None).await.unwrap();
            check(reads.recv().await.unwrap(), "scan", 0..4)
                .send(Ok(Bytes::from_static(b"abcd")))
                .unwrap();
            assert_eq!(response.await.unwrap().unwrap(), "abcd");

            // A partially populated plug flushes even when the caller
            // worker is parked. No missing cache producer must arrive first.
            let plug = ReadPlug::new(scheduler.clone(), 2);
            let token = plug.token();
            let sealed = token.stage(&scheduler, "sealed", 0..1, None).unwrap();
            drop(plug);
            check(reads.recv().await.unwrap(), "sealed", 0..1)
                .send(Ok(Bytes::from_static(b"a")))
                .unwrap();
            assert_eq!(sealed.await.unwrap().unwrap(), "a");
            assert!(token.stage(&scheduler, "sealed", 1..2, None).is_none());
            drop(token);

            // Sealing with G occupied forces deferred admission. Releasing G
            // must advance the whole group without resuming the caller worker.
            let holder = scheduler.submit("holder", 0..1, None).await.unwrap();
            let release_holder = check(reads.recv().await.unwrap(), "holder", 0..1);
            // Supply a collected group explicitly: the plug may close between
            // two stage calls on different runtimes, which is a legal late read.
            let (left_read, left) = scheduler.stage_read("deferred", 0..2, None);
            let (right_read, right) = scheduler.stage_read("deferred", 2..4, None);
            assert_eq!(scheduler.groups.slots.available_permits(), 0);
            scheduler.submit_batch(vec![left_read, right_read]);
            release_holder.send(Ok(Bytes::from_static(b"a"))).unwrap();
            assert_eq!(holder.await.unwrap().unwrap(), "a");
            check(reads.recv().await.unwrap(), "deferred", 0..4)
                .send(Ok(Bytes::from_static(b"abcd")))
                .unwrap();
            assert_eq!(left.await.unwrap().unwrap(), "ab");
            assert_eq!(right.await.unwrap().unwrap(), "cd");

            // Exercise range-error fallback separately. If sealing defers admission
            // because the queue is busy, it must not depend on the parked scan worker.
            let (valid_read, valid) = scheduler.stage_read("fallback", 0..2, None);
            let (invalid_read, invalid) = scheduler.stage_read("fallback", 2..4, None);
            scheduler.submit_batch(vec![valid_read, invalid_read]);
            check(reads.recv().await.unwrap(), "fallback", 0..4)
                .send(Err(ObjectError::read_length_mismatch(
                    "past EOF".to_owned(),
                )))
                .unwrap();
            check(reads.recv().await.unwrap(), "fallback", 0..2)
                .send(Ok(Bytes::from_static(b"ab")))
                .unwrap();
            check(reads.recv().await.unwrap(), "fallback", 2..4)
                .send(Err(ObjectError::read_length_mismatch(
                    "past EOF".to_owned(),
                )))
                .unwrap();
            assert_eq!(valid.await.unwrap().unwrap(), "ab");
            assert!(invalid.await.unwrap().unwrap_err().is_read_range_error());

            let read = scheduler.submit("cancel", 0..1, None).await.unwrap();
            let cancelled_gate = check(reads.recv().await.unwrap(), "cancel", 0..1);
            let next_read = {
                let next = scheduler.submit("next", 4..6, None);
                tokio::pin!(next);
                assert!(
                    futures::poll!(&mut next).is_pending(),
                    "G must be held until cancellation"
                );
                drop(read);
                cancellation.await.unwrap();
                assert!(cancelled_gate.is_closed());
                next.await.unwrap()
            };
            check(reads.recv().await.unwrap(), "next", 4..6)
                .send(Ok(Bytes::from_static(b"ef")))
                .unwrap();
            assert_eq!(next_read.await.unwrap().unwrap(), "ef");

            let groups = scheduler.groups.clone();
            drop(groups.slots.clone().acquire_owned().await.unwrap());
            assert_eq!(groups.admitted.get(), 0);
            assert_eq!(groups.queued.get(), 0);
            assert_eq!(scheduler.pending.get(), 0);
            assert_eq!(scheduler.waiting.get(), 0);
            assert_eq!(
                metrics
                    .read_scheduler_in_flight
                    .with_label_values(&["test"])
                    .get(),
                0
            );
            let state = groups.pending.lock().unwrap();
            assert!(state.groups.is_empty() && state.waiting.is_empty());
            assert_eq!(state.starting, 0);
            drop(state);
            assert!(matches!(
                reads.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));

            // A second scheduler uses the same runtime even after the first closes.
            drop(scheduler);
            assert_eq!(worker_closed.await.unwrap(), io_id);
            assert!(reads.recv().await.is_none());
            while !groups.slots.is_closed() {
                tokio::task::yield_now().await;
            }
            assert_eq!(groups.slots.available_permits(), 1);

            let (starts, second_start) = oneshot::channel();
            let starts = Mutex::new(Some(starts));
            let second = ReadScheduler::new(&metrics, "test", config, move |path, range| {
                let starts = starts.lock().unwrap().take().unwrap();
                async move {
                    starts
                        .send((tokio::runtime::Handle::current().id(), path, range))
                        .unwrap();
                    Ok(Bytes::from_static(b"a"))
                }
            });
            assert_eq!(
                second
                    .submit("other", 0..1, None)
                    .await
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap(),
                "a"
            );
            assert_eq!(
                second_start.await.unwrap(),
                (io_id, "other".to_owned(), 0..1)
            );
        };
        let timeout = {
            let _enter = dedicated_io_runtime(expected_workers).enter();
            tokio::time::timeout(Duration::from_secs(10), test)
        };
        timeout.await.unwrap();

        drop(release_worker);
        scan_worker.await.unwrap();

        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let different_workers = (expected_workers + 1).try_into().unwrap();
        let read = move |_, _| async move {
            assert_eq!(tokio::runtime::Handle::current().id(), scan_id);
            Ok(Bytes::from_static(b"a"))
        };
        let off = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                use_dedicated_io_runtime: false,
                dedicated_io_runtime_workers: different_workers,
                ..config
            },
            read,
        );
        assert_eq!(
            off.submit("off", 0..1, None)
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap(),
            "a",
        );
        let conflict = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    dedicated_io_runtime_workers: different_workers,
                    ..config
                },
                read,
            )
        }));
        let Err(error) = conflict else {
            panic!("a second worker count must be rejected");
        };
        assert!(
            error
                .downcast_ref::<String>()
                .unwrap()
                .contains("conflicting dedicated_io_runtime_workers"),
        );
        eprintln!(
            "Dedicated reads used {expected_workers} I/O workers; OFF used the caller runtime"
        );
    });
}

#[tokio::test]
async fn read_coalesces_adjacent_ranges_on_the_same_object() {
    let metrics = Arc::new(ObjectStoreMetrics::new(&prometheus::Registry::new()));
    let config = ObjectStoreConfig {
        read_scheduler: ReadSchedulerConfig {
            max_batch_requests: 4.try_into().unwrap(),
            max_read_bytes: 8.try_into().unwrap(),
            ..Default::default()
        },
        ..Default::default()
    };
    let max_bytes = config.read_scheduler.max_read_bytes.get();
    let mut store = InMemObjectStore::default().monitored(metrics.clone(), Arc::new(config));
    store
        .upload("first", Bytes::from_static(b"abcdefghijkl"))
        .await
        .unwrap();
    store
        .upload("second", Bytes::from_static(b"mnopqrstuvwx"))
        .await
        .unwrap();
    assert_eq!(store.read("first", 0..2).await.unwrap(), "ab");
    assert_eq!(
        metrics
            .read_request_duration
            .with_label_values(&[store.media_type(), "direct"])
            .get_sample_count(),
        1
    );
    store.enable_read_coalescing();

    let (a, b, c, d) = tokio::join!(
        store.read("first", 0..2),
        store.read("first", 2..4),
        store.read("first", 8..10),
        store.read("second", 0..2),
    );
    assert_eq!(a.unwrap(), "ab");
    assert_eq!(b.unwrap(), "cd");
    assert_eq!(c.unwrap(), "ij");
    assert_eq!(d.unwrap(), "mn");
    assert_eq!(
        metrics
            .operation_size
            .with_label_values(&["read"])
            .get_sample_count(),
        4
    );
    // Adjacent requests may fill the byte cap exactly, but must not cross it.
    let payload = Bytes::from(vec![7; max_bytes + 1]);
    store.upload("boundary", payload.clone()).await.unwrap();
    let middle = max_bytes / 2;
    let (a, b, c) = tokio::join!(
        store.read("boundary", 0..middle),
        store.read("boundary", middle..max_bytes),
        store.read("boundary", max_bytes..max_bytes + 1),
    );
    assert_eq!(a.unwrap(), payload.slice(0..middle));
    assert_eq!(b.unwrap(), payload.slice(middle..max_bytes));
    assert_eq!(c.unwrap(), payload.slice(max_bytes..));
    assert_eq!(
        metrics
            .operation_size
            .with_label_values(&["read"])
            .get_sample_count(),
        6
    );
    assert_eq!(
        metrics
            .read_request_duration
            .with_label_values(&[store.media_type(), "scheduled"])
            .get_sample_count(),
        7
    );
    assert_eq!(
        metrics
            .read_outcomes
            .with_label_values(&[store.media_type(), "physical", "ok"])
            .get(),
        6
    );
    assert_eq!(
        metrics
            .read_scheduler_events
            .with_label_values(&[store.media_type(), "merged_requests"])
            .get(),
        2
    );
    assert_eq!(
        metrics
            .read_scheduler_pending
            .with_label_values(&[store.media_type()])
            .get(),
        0
    );

    // Overlap and duplicate requests still receive their own exact ranges.
    let (a, b, c) = tokio::join!(
        store.read("first", 0..4),
        store.read("first", 2..6),
        store.read("first", 0..4),
    );
    assert_eq!(a.unwrap(), "abcd");
    assert_eq!(b.unwrap(), "cdef");
    assert_eq!(c.unwrap(), "abcd");
    assert_eq!(
        metrics
            .operation_size
            .with_label_values(&["read"])
            .get_sample_count(),
        7
    );
}

#[tokio::test]
async fn contained_read_shares_a_started_get_until_its_last_waiter_leaves() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let reserved = metrics.read_scheduler_reserved.with_label_values(&["test"]);
        let active = metrics
            .read_scheduler_in_flight
            .with_label_values(&["test"]);
        let dispatch = metrics
            .read_scheduler_physical_duration
            .with_label_values(&["test", "reservation_to_start"]);
        let (calls, mut reads) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                reuse_inflight_reads: true,
                ..Default::default()
            },
            {
                let release = release.clone();
                move |_, range: Range<usize>| {
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.send(range.clone()).unwrap();
                        if range == (0..4) {
                            release.notified().await;
                        }
                        Ok(Bytes::from_static(&b"abcdef"[range]))
                    }
                }
            },
        );
        let leader = scheduler.submit("file", 0..4, None).await.unwrap();
        assert_eq!(reads.recv().await.unwrap(), 0..4);
        assert_eq!(scheduler.groups.slots.available_permits(), 0);
        let follower = scheduler.submit("file", 1..3, None).await.unwrap();
        assert_eq!(reserved.get(), 0);
        assert_eq!(active.get(), 1);
        assert_eq!(dispatch.get_sample_count(), 1);
        let partial = scheduler.submit("file", 3..5, None);
        tokio::pin!(partial);
        assert!(futures::poll!(&mut partial).is_pending());
        drop(leader);
        release.notify_one();
        assert_eq!(follower.await.unwrap().unwrap(), "bc");
        assert_eq!(partial.await.unwrap().await.unwrap().unwrap(), "de");
        assert_eq!(reads.recv().await.unwrap(), 3..5);
        assert!(reads.try_recv().is_err());
        assert_eq!(
            metrics
                .read_scheduler_events
                .with_label_values(&["test", "inflight_reused"])
                .get(),
            1,
        );

        let leader = scheduler.submit("file", 0..4, None).await.unwrap();
        assert_eq!(reads.recv().await.unwrap(), 0..4);
        let follower = scheduler.submit("file", 1..3, None).await.unwrap();
        drop(leader);
        drop(follower);
        drop(
            scheduler
                .groups
                .slots
                .clone()
                .acquire_owned()
                .await
                .unwrap(),
        );
        assert!(scheduler.groups.pending.lock().unwrap().active.is_empty());
        assert_eq!(scheduler.pending.get(), 0);
        assert_eq!(reserved.get(), 0);
        assert_eq!(active.get(), 0);
        assert_eq!(dispatch.get_sample_count(), 3);
        assert_eq!(scheduler.groups.ready_groups.get(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn covered_group_waiter_joins_a_new_flight_without_a_credit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                max_batch_requests: 1.try_into().unwrap(),
                reuse_inflight_reads: true,
                ..Default::default()
            },
            {
                let release = release.clone();
                move |_, range: Range<usize>| {
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.send(range.clone()).unwrap();
                        if range == (0..4) {
                            release.notified().await;
                        }
                        Ok(Bytes::from_static(&b"abcd"[range]))
                    }
                }
            },
        );
        // Queue the follower before the first physical task runs. The pending group
        // is already full, so the follower must initially wait for a group credit.
        let leader = scheduler.submit("file", 0..4, None).await.unwrap();
        let follower = scheduler.submit("file", 1..3, None);
        tokio::pin!(follower);
        struct WaitingTask(Notify);
        impl std::task::Wake for WaitingTask {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }
        let task = Arc::new(WaitingTask(Notify::new()));
        let waker = Waker::from(task.clone());
        assert!(
            follower
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(reads.recv().await.unwrap(), 0..4);
        // The leader still owns the only credit. Joining the flight must
        // wake the waiting task independently of the semaphore.
        task.0.notified().await;
        let follower = follower.await.unwrap();
        assert_eq!(scheduler.groups.slots.available_permits(), 0);
        drop(leader);
        release.notify_one();
        assert_eq!(follower.await.unwrap().unwrap(), "bc");
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(scheduler.groups.slots.available_permits(), 1);
        assert!(reads.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn sealed_batch_waits_for_one_group_credit_without_splitting() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                ..Default::default()
            },
            {
                let release = release.clone();
                move |path, range: Range<usize>| {
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.send((path.clone(), range.clone())).unwrap();
                        if path == "busy" {
                            release.notified().await;
                        }
                        Ok(Bytes::from_static(&b"abcd"[range]))
                    }
                }
            },
        ));
        let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
        assert_eq!(reads.recv().await.unwrap(), ("busy".to_owned(), 0..1));

        let plug = ReadPlug::new(scheduler.clone(), 2);
        let token = plug.token();
        let b = token.stage(&scheduler, "file", 2..4, None).unwrap();
        let a = token.stage(&scheduler, "file", 0..2, None).unwrap();
        drop(plug);
        assert!(token.stage(&scheduler, "file", 0..1, None).is_none());

        let cancelled = ReadPlug::new(scheduler.clone(), 2);
        let cancelled_response = cancelled.token().stage(&scheduler, "cancelled", 0..1, None);
        assert!(cancelled_response.is_some());
        drop(cancelled);
        while scheduler.batch_members.get() < 3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(scheduler.groups.slots.available_permits(), 0);
        assert_eq!(scheduler.waiting.get(), 3);
        assert!(reads.try_recv().is_err());
        drop(cancelled_response);
        while scheduler.waiting.get() != 2 {
            tokio::task::yield_now().await;
        }

        release.notify_one();
        assert_eq!(busy.await.unwrap().unwrap(), "a");
        assert_eq!(a.await.unwrap().unwrap(), "ab");
        assert_eq!(b.await.unwrap().unwrap(), "cd");
        assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..4));
        assert!(reads.try_recv().is_err());
        drop(
            scheduler
                .groups
                .slots
                .clone()
                .acquire_owned()
                .await
                .unwrap(),
        );
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn seal_merges_ready_group_while_another_group_waits_for_credit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 2.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                ..Default::default()
            },
            {
                let release = release.clone();
                move |path, range: Range<usize>| {
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.send((path.clone(), range.clone())).unwrap();
                        if path == "busy" {
                            release.notified().await;
                        }
                        Ok(Bytes::from_static(&b"abcd"[range]))
                    }
                }
            },
        ));

        let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
        assert_eq!(reads.recv().await.unwrap(), ("busy".to_owned(), 0..1));
        let queued = scheduler.submit("file", 0..1, None).await.unwrap();
        assert_eq!(scheduler.groups.slots.available_permits(), 0);

        let plug = ReadPlug::new(scheduler.clone(), 3);
        let token = plug.token();
        let deferred = token.stage(&scheduler, "after", 0..1, None).unwrap();
        let c = token.stage(&scheduler, "file", 2..3, None).unwrap();
        let b = token.stage(&scheduler, "file", 1..2, None).unwrap();
        drop(plug);

        // No task has had a chance to poll since sealing the plug.
        {
            let pending = scheduler.groups.pending.lock().unwrap();
            assert_eq!(pending.groups.len(), 1);
            assert_eq!(pending.groups[0].span, 0..3);
            assert_eq!(pending.groups[0].group.requests.len(), 3);
        }
        assert_eq!(scheduler.waiting.get(), 1);
        drop(deferred);
        release.notify_one();
        assert_eq!(busy.await.unwrap().unwrap(), "a");
        assert_eq!(queued.await.unwrap().unwrap(), "a");
        assert_eq!(b.await.unwrap().unwrap(), "b");
        assert_eq!(c.await.unwrap().unwrap(), "c");
        assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..3));
        assert!(reads.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn group_headroom_admits_demand_but_preserves_prefetch_dispatch_order() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for group_limit in [2, 3] {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (send_call, mut calls) = mpsc::unbounded_channel();
            let release_active = Arc::new(Notify::new());
            let release_queued = Arc::new(Notify::new());
            let scheduler = Arc::new(ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: group_limit.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let release_active = release_active.clone();
                    let release_queued = release_queued.clone();
                    move |path, range: Range<usize>| {
                        let send_call = send_call.clone();
                        let release = match path.as_str() {
                            "active-prefetch" => Some(release_active.clone()),
                            "queued-prefetch" => Some(release_queued.clone()),
                            _ => None,
                        };
                        async move {
                            send_call.send((path, range.clone())).unwrap();
                            if let Some(release) = release {
                                release.notified().await;
                            }
                            Ok(Bytes::from_static(&b"abcd"[range]))
                        }
                    }
                },
            ));

            let active_plug = ReadPlug::new(scheduler.clone(), 1);
            let active = active_plug
                .token()
                .stage(&scheduler, "active-prefetch", 0..1, None)
                .unwrap();
            drop(active_plug);
            assert_eq!(
                calls.recv().await.unwrap(),
                ("active-prefetch".into(), 0..1)
            );

            let queued_plug = ReadPlug::new(scheduler.clone(), 2);
            let token = queued_plug.token();
            let a = token
                .stage(&scheduler, "queued-prefetch", 0..1, None)
                .unwrap();
            let b = token
                .stage(&scheduler, "queued-prefetch", 1..2, None)
                .unwrap();
            drop(queued_plug);
            assert_eq!(scheduler.groups.admitted.get(), 2);
            assert_eq!(scheduler.groups.queued.get(), 1);

            let mut demand = std::pin::pin!(scheduler.submit("demand", 2..3, None));
            let admitted = match futures::poll!(&mut demand) {
                Poll::Pending => None,
                Poll::Ready(result) => Some(result.unwrap()),
            };
            assert_eq!(admitted.is_some(), group_limit == 3);
            assert_eq!(scheduler.groups.slots.available_permits(), 0);
            assert!(calls.try_recv().is_err());

            let waiters = scheduler.waiting.get();
            {
                let mut cancelled = std::pin::pin!(scheduler.submit("cancelled", 3..4, None));
                assert!(futures::poll!(&mut cancelled).is_pending());
                assert_eq!(scheduler.waiting.get(), waiters + 1);
            }
            assert_eq!(scheduler.waiting.get(), waiters);

            release_active.notify_one();
            assert_eq!(active.await.unwrap().unwrap(), "a");
            let demand = match admitted {
                Some(response) => response,
                None => demand.await.unwrap(),
            };
            assert_eq!(
                calls.recv().await.unwrap(),
                ("queued-prefetch".into(), 0..2)
            );
            assert!(calls.try_recv().is_err());

            release_queued.notify_one();
            assert_eq!(a.await.unwrap().unwrap(), "a");
            assert_eq!(b.await.unwrap().unwrap(), "b");
            assert_eq!(calls.recv().await.unwrap(), ("demand".into(), 2..3));
            assert_eq!(demand.await.unwrap().unwrap(), "c");
            assert!(calls.try_recv().is_err());
            drop(
                scheduler
                    .groups
                    .slots
                    .clone()
                    .acquire_many_owned(group_limit as u32)
                    .await
                    .unwrap(),
            );
            assert_eq!(scheduler.groups.admitted.get(), 0);
            assert_eq!(scheduler.groups.queued.get(), 0);
            assert_eq!(scheduler.pending.get(), 0);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn plug_starts_on_first_read_after_handle_drop() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig::default(),
            move |_, range: Range<usize>| {
                calls.send(range.clone()).unwrap();
                std::future::ready(Ok(Bytes::from_static(&b"ab"[range])))
            },
        ));
        let plug = ReadPlug::new(scheduler.clone(), 8);
        let token = plug.token();
        drop(plug);
        // Foyer may start the first producer only after registration has ended.
        let first = token.stage(&scheduler, "file", 0..1, None).unwrap();
        // Keeping the token alive cannot hold a single cold read indefinitely.
        assert_eq!(first.await.unwrap().unwrap(), "a");
        assert_eq!(reads.recv().await.unwrap(), 0..1);
        assert_eq!(scheduler.plug_flushes[0].get(), 1);
        assert_eq!(scheduler.plug_flushes[1].get(), 0);
        assert!(token.stage(&scheduler, "file", 1..2, None).is_none());
        let late = scheduler.submit("file", 1..2, None).await.unwrap();
        assert_eq!(late.await.unwrap().unwrap(), "b");
        assert_eq!(reads.recv().await.unwrap(), 1..2);
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
        assert!(reads.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn plug_staging_is_bounded_and_skips_cancelled_reads() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (calls, mut reads) = mpsc::unbounded_channel();
        let scheduler = Arc::new(ReadScheduler::new(
            &ObjectStoreMetrics::unused(),
            "test",
            ReadSchedulerConfig::default(),
            move |_, range: Range<usize>| {
                calls.send(range.clone()).unwrap();
                std::future::ready(Ok(Bytes::from_static(&b"abc"[range])))
            },
        ));
        let plug = ReadPlug::new(scheduler.clone(), 2);
        let token = plug.token();
        let first = token.stage(&scheduler, "file", 0..1, None).unwrap();
        drop(first);
        let live = token.stage(&scheduler, "file", 1..2, None).unwrap();
        assert!(token.stage(&scheduler, "file", 2..3, None).is_none());
        drop(plug);
        assert_eq!(live.await.unwrap().unwrap(), "b");
        assert_eq!(reads.recv().await.unwrap(), 1..2);
        let empty = ReadPlug::new(scheduler.clone(), 0);
        assert!(
            empty
                .token()
                .stage(&scheduler, "file", 0..1, None)
                .is_none()
        );
        drop(empty);
        tokio::task::yield_now().await;
        assert!(reads.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn queued_group_stays_mergeable_until_physical_task_starts() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for (neighbor, cancel, abort) in [
            (true, false, false),
            (false, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let config = ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                max_inflight_reads: 2.try_into().unwrap(),
                ..Default::default()
            };
            let groups = Arc::new(GroupQueue::new(&metrics, "test", config));
            let (shutdown, shutdown_rx) = oneshot::channel();
            let reserved = metrics.read_scheduler_reserved.with_label_values(&["test"]);
            let active = metrics
                .read_scheduler_in_flight
                .with_label_values(&["test"]);
            let dispatch = metrics
                .read_scheduler_physical_duration
                .with_label_values(&["test", "reservation_to_start"]);
            let (reads, mut physical) = mpsc::unbounded_channel();
            let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
            let submit = |range: Range<usize>| {
                let (response, receiver) = oneshot::channel();
                let mut state = groups.pending.lock().unwrap();
                let request = ReadRequest {
                    path: "file".to_owned(),
                    range: range.clone(),
                    response,
                    queued_at: Instant::now(),
                    trace_id: None,
                    _pending: ReadGauge::new(&pending),
                    _queued: None,
                };
                let admission = groups
                    .admission(&state.groups, "file", &range, 1, &mut None, &config)
                    .expect("adjacent read must join even with no free group credit");
                let group = groups.admit(&mut state.groups, admission, range, [request], &config);
                state.groups.push(group);
                state.update_ready(&groups);
                groups.ready.notify_one();
                receiver
            };
            let a = submit(0..2);
            assert_eq!(groups.ready_groups.get(), 1);
            assert_eq!(reserved.get(), 0);
            let mut dispatcher = Box::pin(run(
                shutdown_rx,
                GroupQueueWorker(groups.clone()),
                {
                    let (reserved, active, dispatch) =
                        (reserved.clone(), active.clone(), dispatch.clone());
                    move |_, range: Range<usize>| {
                        assert_eq!(reserved.get(), 0);
                        assert_eq!(active.get(), 1);
                        assert_eq!(dispatch.get_sample_count(), 1);
                        reads.send(range.clone()).unwrap();
                        std::future::ready(Ok(Bytes::from_static(&b"abcd"[range])))
                    }
                },
                config,
                metrics,
                "test",
            ));
            // Drive the dispatcher to spawn on this current-thread runtime,
            // without yielding to the spawned task.
            assert!(futures::poll!(&mut dispatcher).is_pending());
            assert_eq!(groups.queued.get(), 1);
            assert_eq!(groups.ready_groups.get(), 0);
            assert_eq!(reserved.get(), 1);
            assert_eq!(active.get(), 0);
            assert_eq!(dispatch.get_sample_count(), 0);
            assert_eq!(groups.pending.lock().unwrap().starting, 1);
            let b = neighbor.then(|| submit(2..4));
            drop(shutdown);
            if abort {
                drop(dispatcher);
                assert!(a.await.is_err());
                // Aborting the join schedules cleanup of the unpolled child.
                while reserved.get() != 0 || groups.pending.lock().unwrap().starting != 0 {
                    tokio::task::yield_now().await;
                }
            } else if cancel {
                drop(a);
                dispatcher.await;
            } else {
                dispatcher.await;
                assert_eq!(a.await.unwrap().unwrap(), "ab");
                if let Some(b) = b {
                    assert_eq!(b.await.unwrap().unwrap(), "cd");
                }
                assert_eq!(
                    physical.try_recv().unwrap(),
                    0..if neighbor { 4 } else { 2 }
                );
            }
            assert!(physical.try_recv().is_err());
            assert_eq!(groups.ready_groups.get(), 0);
            assert_eq!(reserved.get(), 0);
            assert_eq!(active.get(), 0);
            assert_eq!(dispatch.get_sample_count(), u64::from(!cancel && !abort));
            assert_eq!(pending.get(), 0);
            assert_eq!(groups.slots.available_permits(), 1);
            assert_eq!(groups.pending.lock().unwrap().starting, 0);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stopping_dispatcher_cancels_physical_reads() {
    // The spawned future must also be cancelled if its owner is dropped
    // before either the child or its join handle has been polled.
    let unstarted_slot = Arc::new(Semaphore::new(1));
    let permit = unstarted_slot.clone().acquire_owned().await.unwrap();
    let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let child_polled = polled.clone();
    let task = ReadTask(tokio::spawn(async move {
        let _permit = permit;
        child_polled.store(true, std::sync::atomic::Ordering::Relaxed);
    }));
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), async {
        let _permit = unstarted_slot.acquire().await.unwrap();
    })
    .await
    .unwrap();
    assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));

    let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
    let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
    let active = metrics
        .read_scheduler_in_flight
        .with_label_values(&["test"]);
    let groups = Arc::new(GroupQueue::new(
        &metrics,
        "test",
        ReadSchedulerConfig {
            max_admitted_groups: 1.try_into().unwrap(),
            ..Default::default()
        },
    ));
    let (_shutdown, shutdown_rx) = oneshot::channel();
    let (response, result) = oneshot::channel();
    {
        let mut pending_groups = groups.pending.lock().unwrap();
        let permit = groups.slots.clone().try_acquire_owned().unwrap();
        pending_groups.groups.push(PendingReadGroup::new(
            &groups,
            permit,
            0..1,
            vec![ReadRequest {
                path: "file".to_owned(),
                range: 0..1,
                response,
                queued_at: Instant::now(),
                trace_id: None,
                _pending: ReadGauge::new(&pending),
                _queued: None,
            }],
        ));
    }
    let started = Arc::new(Notify::new());
    let dispatcher = tokio::spawn(run(
        shutdown_rx,
        GroupQueueWorker(groups.clone()),
        {
            let started = started.clone();
            move |_, _| {
                let started = started.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<ReadResult>().await
                }
            }
        },
        ReadSchedulerConfig::default(),
        metrics,
        "test",
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        started.notified().await;
        assert_eq!(active.get(), 1);
        let waiting = groups.slots.clone().acquire_owned();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        dispatcher.abort();
        assert!(dispatcher.await.unwrap_err().is_cancelled());
        assert!(result.await.is_err());
        assert!(waiting.await.is_err());
        assert_eq!(active.get(), 0);
        assert_eq!(pending.get(), 0);
        assert!(groups.slots.is_closed());
        assert_eq!(groups.slots.available_permits(), 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_after_planning_is_accounted_before_dispatch() {
    for cancel_all in [false, true] {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
        let reserved = metrics.read_scheduler_reserved.with_label_values(&["test"]);
        let dispatch = metrics
            .read_scheduler_physical_duration
            .with_label_values(&["test", "reservation_to_start"]);

        let slots = Arc::new(Semaphore::new(1));
        let groups = metrics
            .read_scheduler_groups
            .with_label_values(&["test", "admitted"]);
        let mut requests = Vec::new();
        let mut responses = Vec::new();
        for start in [0, 4, 8] {
            let (response, receiver) = oneshot::channel();
            requests.push(ReadRequest {
                path: "file".to_owned(),
                range: start..start + 4,
                response,
                queued_at: Instant::now(),
                trace_id: None,
                _pending: ReadGauge::new(&pending),
                _queued: None,
            });
            responses.push(receiver);
        }
        let group = ReadGroup {
            requests,
            _credit: (
                ReadGauge::new(&groups),
                slots.clone().try_acquire_owned().unwrap(),
            ),
        };
        let physical_slots = Arc::new(Semaphore::new(1));
        // Force cancellation after planning, before the group's first poll.
        drop(responses.remove(1));
        if cancel_all {
            responses.clear();
        }
        let read = |_, range| async move {
            assert!(!cancel_all, "a fully cancelled group must not issue IO");
            assert_eq!(range, 0..12);
            Ok(Bytes::from_static(b"abcdefghijkl"))
        };
        group
            .execute(
                &read,
                &physical_slots,
                PhysicalReadReservation {
                    permit: acquire_physical(physical_slots.clone()).await.unwrap(),
                    reserved: ReadGauge::new(&reserved),
                    reserved_at: Instant::now(),
                    dispatch_wait: dispatch.clone(),
                },
                &metrics,
                "test",
                None,
            )
            .await;
        if !cancel_all {
            assert_eq!(responses.remove(0).await.unwrap().unwrap(), "abcd");
            assert_eq!(responses.remove(0).await.unwrap().unwrap(), "ijkl");
        }
        assert_eq!(
            metrics
                .read_scheduler_events
                .with_label_values(&["test", "cancelled_before_dispatch"])
                .get(),
            if cancel_all { 3 } else { 1 }
        );
        assert_eq!(reserved.get(), 0);
        assert_eq!(dispatch.get_sample_count(), u64::from(!cancel_all));
        assert_eq!(physical_slots.available_permits(), 1);
        assert_eq!(pending.get(), 0);
        assert_eq!(slots.available_permits(), 1);
    }
}

#[tokio::test]
async fn non_range_error_is_shared_without_individual_retries() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let scheduler = ReadScheduler::new(
        &ObjectStoreMetrics::unused(),
        "test",
        ReadSchedulerConfig::default(),
        {
            let calls = calls.clone();
            move |_, range| {
                let calls = calls.clone();
                async move {
                    assert_eq!(range, 0..4);
                    calls.fetch_add(1, Ordering::Relaxed);
                    Err(ObjectError::internal("backend unavailable"))
                }
            }
        },
    );
    let first = scheduler.submit("file", 0..2, None).await.unwrap();
    let second = scheduler.submit("file", 2..4, None).await.unwrap();
    for response in [first, second] {
        let error = response.await.unwrap().unwrap_err();
        assert!(matches!(error.inner(), ObjectErrorInner::SharedRead(_)));
        assert!(!error.is_read_range_error());
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn out_of_bounds_read_does_not_block_adjacent_valid_reads() {
    let release_slow_read = Arc::new(Notify::new());
    let scheduler = ReadScheduler::new(
        &ObjectStoreMetrics::unused(),
        "test",
        ReadSchedulerConfig {
            max_inflight_reads: 2.try_into().unwrap(),
            ..Default::default()
        },
        {
            let release_slow_read = release_slow_read.clone();
            move |_, range| {
                let release_slow_read = release_slow_read.clone();
                async move {
                    if range == (0..2) {
                        release_slow_read.notified().await;
                    }
                    b"data".get(range).map(Bytes::from_static).ok_or_else(|| {
                        ObjectError::read_length_mismatch("range exceeds object".to_owned())
                    })
                }
            }
        },
    );
    let slow = scheduler.submit("file", 0..2, None).await.unwrap();
    let valid = scheduler.submit("file", 2..4, None).await.unwrap();
    let invalid = scheduler.submit("file", 4..6, None).await.unwrap();
    // Normal shutdown drains already admitted work, including range fallback.
    drop(scheduler);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), valid)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        "ta"
    );
    assert!(invalid.await.unwrap().unwrap_err().is_read_range_error());
    release_slow_read.notify_one();
    assert_eq!(slow.await.unwrap().unwrap(), "da");
}

#[tokio::test]
async fn bridge_reuses_group_credit_at_capacity() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 3.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                ..Default::default()
            },
            {
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move |path, range| {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.lock().unwrap().push((path.clone(), range.clone()));
                        if path == "busy" {
                            started.notify_one();
                            release.notified().await;
                        }
                        Ok(Bytes::from_static(&b"abcdefgh"[range]))
                    }
                }
            },
        );
        let groups = &scheduler.groups;
        let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
        started.notified().await;
        let a = scheduler.submit("file", 0..2, None).await.unwrap();
        let c = scheduler.submit("file", 4..6, None).await.unwrap();
        assert_eq!(groups.admitted.get(), 3);
        let other = scheduler.submit("aaa", 0..1, None);
        tokio::pin!(other);
        assert!(futures::poll!(&mut other).is_pending());

        let b = scheduler.submit("file", 2..4, None).await.unwrap();
        let other = other.await.unwrap();
        assert_eq!(groups.admitted.get(), 3);
        assert_eq!(groups.queued.get(), 2);
        assert_eq!(groups.ready_groups.get(), 2);

        drop(other);
        let newer = scheduler.submit("newer", 0..1, None).await.unwrap();
        release.notify_one();
        assert_eq!(busy.await.unwrap().unwrap(), "a");
        assert_eq!(a.await.unwrap().unwrap(), "ab");
        assert_eq!(b.await.unwrap().unwrap(), "cd");
        assert_eq!(c.await.unwrap().unwrap(), "ef");
        assert_eq!(newer.await.unwrap().unwrap(), "a");
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("busy".to_owned(), 0..1),
                ("file".to_owned(), 0..6),
                ("newer".to_owned(), 0..1),
            ]
        );
        // Wait for the execution owners to drop their credits after sending results.
        drop(groups.slots.clone().acquire_many_owned(3).await.unwrap());
        assert_eq!(groups.queued.get(), 0);
        assert_eq!(groups.admitted.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pending_group_admits_compatible_g_waiters_before_dispatch() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for deferred in [false, true] {
            // Also cover a cancelled waiter and a G grant that C has not polled.
            for (extend, cancel, grant) in [
                (false, false, false),
                (true, false, false),
                (true, true, false),
                (true, false, true),
            ] {
                let credits = 1 + usize::from(extend) + usize::from(grant);
                let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
                let (calls, mut reads) = mpsc::unbounded_channel();
                let release = Arc::new(Notify::new());
                let scheduler = Arc::new(ReadScheduler::new(
                    &metrics,
                    "test",
                    ReadSchedulerConfig {
                        max_admitted_groups: credits.try_into().unwrap(),
                        max_inflight_reads: 1.try_into().unwrap(),
                        max_batch_requests: 3.try_into().unwrap(),
                        max_gap_bytes: 0,
                        reuse_inflight_reads: true,
                        ..Default::default()
                    },
                    {
                        let release = release.clone();
                        move |path, range: Range<usize>| {
                            let release = release.clone();
                            let calls = calls.clone();
                            async move {
                                calls.send((path.clone(), range.clone())).unwrap();
                                if path == "busy" {
                                    release.notified().await;
                                }
                                Ok(Bytes::from_static(&b"abc"[range]))
                            }
                        }
                    },
                ));
                let groups = &scheduler.groups;
                let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
                assert_eq!(reads.recv().await.unwrap(), ("busy".to_owned(), 0..1));

                let mut a = std::pin::pin!(scheduler.submit("file", 0..1, None));
                let admitted_a = match futures::poll!(&mut a) {
                    Poll::Ready(result) if extend => Some(result.unwrap()),
                    Poll::Pending if !extend => None,
                    _ => panic!("unexpected admission for A"),
                };
                let held = grant.then(|| groups.slots.clone().try_acquire_owned().unwrap());
                let c_range = if extend { 2..3 } else { 1..2 };
                let mut ordinary = None;
                let mut staged = None;
                if deferred {
                    let plug = ReadPlug::new(scheduler.clone(), 1);
                    staged = plug.token().stage(&scheduler, "file", c_range, None);
                    assert!(staged.is_some());
                    drop(plug);
                    while groups
                        .pending
                        .lock()
                        .unwrap()
                        .waiting
                        .get("file")
                        .map_or(0, BTreeMap::len)
                        < if extend { 1 } else { 2 }
                    {
                        tokio::task::yield_now().await;
                    }
                } else {
                    let mut submit = Box::pin(scheduler.submit("file", c_range, None));
                    assert!(futures::poll!(&mut submit).is_pending());
                    ordinary = Some(submit);
                }
                assert_eq!(groups.slots.available_permits(), 0);

                if cancel {
                    drop(ordinary.take());
                    drop(staged.take());
                    while groups.pending.lock().unwrap().waiting.contains_key("file") {
                        tokio::task::yield_now().await;
                    }
                }
                let mut unrelated = std::pin::pin!(scheduler.submit("other", 0..1, None));
                if grant {
                    assert!(futures::poll!(&mut unrelated).is_pending());
                    // The semaphore assigns the returned credit to C, which is
                    // still suspended. B must return it without waiting for C.
                    drop(held);
                    assert_eq!(groups.slots.available_permits(), 0);
                }
                let b = if extend {
                    let b = scheduler.submit("file", 1..2, None).await.unwrap();
                    assert_eq!(groups.slots.available_permits(), 0);
                    assert!(!groups.pending.lock().unwrap().waiting.contains_key("file"));
                    Some(b)
                } else {
                    None
                };
                let other = if grant {
                    let Poll::Ready(result) = futures::poll!(&mut unrelated) else {
                        panic!("merging C must release its assigned credit");
                    };
                    Some(result.unwrap())
                } else {
                    None
                };
                assert!(reads.try_recv().is_err());
                release.notify_one();
                let a = match admitted_a {
                    Some(a) => a,
                    None => a.await.unwrap(),
                };
                // In the ordinary case C has still never been polled since registering.
                let end = if extend && !cancel { 3 } else { 2 };
                assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..end));
                assert_eq!(busy.await.unwrap().unwrap(), "a");
                assert_eq!(a.await.unwrap().unwrap(), "a");
                if let Some(b) = b {
                    assert_eq!(b.await.unwrap().unwrap(), "b");
                }
                if !cancel {
                    let c = match ordinary {
                        Some(c) => c.await.unwrap(),
                        None => staged.unwrap(),
                    };
                    assert_eq!(c.await.unwrap().unwrap(), if extend { "c" } else { "b" });
                }
                if let Some(other) = other {
                    assert_eq!(reads.recv().await.unwrap(), ("other".to_owned(), 0..1));
                    assert_eq!(other.await.unwrap().unwrap(), "a");
                }
                drop(
                    groups
                        .slots
                        .clone()
                        .acquire_many_owned(credits as u32)
                        .await
                        .unwrap(),
                );
                assert!(reads.try_recv().is_err());
                assert!(groups.pending.lock().unwrap().waiting.is_empty());
                assert_eq!(groups.admitted.get(), 0);
                assert_eq!(scheduler.waiting.get(), 0);
                assert_eq!(scheduler.pending.get(), 0);
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_batch_prefix_does_not_hide_a_mergeable_g_waiter() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 2.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                max_batch_requests: 4.try_into().unwrap(),
                max_read_bytes: 16.try_into().unwrap(),
                max_gap_bytes: 4,
                ..Default::default()
            },
            {
                let release = release.clone();
                move |path, range: Range<usize>| {
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.send(range.clone()).unwrap();
                        if path == "busy" {
                            release.notified().await;
                        }
                        Ok(Bytes::from_iter(range.map(|offset| offset as u8)))
                    }
                }
            },
        ));
        let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
        assert_eq!(reads.recv().await.unwrap(), 0..1);
        let a = scheduler.submit("file", 100..101, None).await.unwrap();
        let plug = ReadPlug::new(scheduler.clone(), 2);
        let token = plug.token();
        let cancelled = token.stage(&scheduler, "file", 81..92, None).unwrap();
        let c = token.stage(&scheduler, "file", 92..97, None).unwrap();
        drop(plug);
        let groups = &scheduler.groups;
        while !groups.pending.lock().unwrap().waiting.contains_key("file") {
            tokio::task::yield_now().await;
        }

        drop(cancelled);
        let b = scheduler.submit("file", 101..102, None).await.unwrap();
        assert_eq!(groups.slots.available_permits(), 0);
        assert!(groups.pending.lock().unwrap().waiting.is_empty());
        release.notify_one();
        busy.await.unwrap().unwrap();
        assert_eq!(reads.recv().await.unwrap(), 92..102);
        assert_eq!(c.await.unwrap().unwrap().as_ref(), &[92, 93, 94, 95, 96]);
        assert_eq!(a.await.unwrap().unwrap().as_ref(), &[100]);
        assert_eq!(b.await.unwrap().unwrap().as_ref(), &[101]);
        drop(groups.slots.clone().acquire_many_owned(2).await.unwrap());
        assert!(reads.try_recv().is_err());
        assert_eq!(groups.admitted.get(), 0);
        assert_eq!(scheduler.waiting.get(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn in_memory_range_fallback_preserves_valid_reads() {
    let metrics = Arc::new(ObjectStoreMetrics::new(&prometheus::Registry::new()));
    let mut store =
        InMemObjectStore::for_test().monitored(metrics.clone(), Arc::new(Default::default()));
    store.enable_read_coalescing();
    store
        .upload("file", Bytes::from_static(b"data"))
        .await
        .unwrap();

    // On the current-thread runtime both adjacent reads reach the scheduler before dispatch.
    let (valid, invalid) = tokio::join!(store.read("file", 0..4), store.read("file", 4..6));
    assert_eq!(valid.unwrap(), "data");
    assert!(invalid.unwrap_err().is_read_range_error());
    assert_eq!(
        metrics
            .read_scheduler_events
            .with_label_values(&[store.media_type(), "range_fallback_requests"])
            .get(),
        2
    );
}

#[tokio::test]
async fn range_fallback_shares_the_physical_limit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let active = metrics
            .read_scheduler_in_flight
            .with_label_values(&["test"]);
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_inflight_reads: 1.try_into().unwrap(),
                max_admitted_groups: 1.try_into().unwrap(),
                ..Default::default()
            },
            {
                let active = active.clone();
                let calls = calls.clone();
                move |_, range| {
                    let active = active.clone();
                    let calls = calls.clone();
                    async move {
                        calls.lock().unwrap().push(range.clone());
                        assert_eq!(active.get(), 1);
                        // Let other fallback children poll; an unbounded join_all
                        // would raise the physical gauge above one here.
                        tokio::task::yield_now().await;
                        assert_eq!(active.get(), 1);
                        b"data"
                            .get(range)
                            .map(Bytes::from_static)
                            .ok_or_else(|| ObjectError::read_length_mismatch("past EOF".to_owned()))
                    }
                }
            },
        );
        let a = scheduler.submit("file", 0..2, None).await.unwrap();
        let b = scheduler.submit("file", 2..4, None).await.unwrap();
        let c = scheduler.submit("file", 4..6, None).await.unwrap();
        assert_eq!(a.await.unwrap().unwrap(), "da");
        assert_eq!(b.await.unwrap().unwrap(), "ta");
        assert!(c.await.unwrap().unwrap_err().is_read_range_error());
        assert_eq!(*calls.lock().unwrap(), vec![0..6, 0..2, 2..4, 4..6]);
        assert_eq!(active.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
        assert_eq!(scheduler.queued.get(), 0);
        let next = scheduler.submit("next", 0..1, None).await.unwrap();
        assert_eq!(next.await.unwrap().unwrap(), "d");
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn fallback_holds_group_credit_while_physical_capacity_is_available() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (send_call, mut calls) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                max_inflight_reads: 2.try_into().unwrap(),
                ..Default::default()
            },
            {
                let release = release.clone();
                move |path, range| {
                    let send_call = send_call.clone();
                    let release = release.clone();
                    async move {
                        send_call.send((path.clone(), range.clone())).unwrap();
                        if path == "file" && range == (0..2) {
                            release.notified().await;
                        }
                        b"ab"
                            .get(range)
                            .map(Bytes::from_static)
                            .ok_or_else(|| ObjectError::read_length_mismatch("past EOF".to_owned()))
                    }
                }
            },
        );
        let a = scheduler.submit("file", 0..2, None).await.unwrap();
        let b = scheduler.submit("file", 2..4, None).await.unwrap();
        assert_eq!(calls.recv().await.unwrap(), ("file".to_owned(), 0..4));
        assert_eq!(calls.recv().await.unwrap(), ("file".to_owned(), 0..2));
        assert_eq!(calls.recv().await.unwrap(), ("file".to_owned(), 2..4));
        assert!(b.await.unwrap().unwrap_err().is_read_range_error());

        let other = scheduler.submit("other", 0..1, None);
        tokio::pin!(other);
        assert!(futures::poll!(&mut other).is_pending());
        assert_eq!(
            metrics
                .read_scheduler_in_flight
                .with_label_values(&["test"])
                .get(),
            1
        );

        release.notify_one();
        assert_eq!(a.await.unwrap().unwrap(), "ab");
        assert_eq!(other.await.unwrap().await.unwrap().unwrap(), "a");
        assert_eq!(calls.recv().await.unwrap(), ("other".to_owned(), 0..1));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn merged_read_is_cancelled_after_both_waiters_drop() {
    struct NotifyOnDrop(Arc<Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
    let scheduler = ReadScheduler::new(
        &metrics,
        "test",
        ReadSchedulerConfig {
            max_admitted_groups: 1.try_into().unwrap(),
            ..Default::default()
        },
        {
            let started = started.clone();
            let cancelled = cancelled.clone();
            move |_, range| {
                let started = started.clone();
                let cancelled = cancelled.clone();
                async move {
                    assert_eq!(range, 0..4);
                    let _guard = NotifyOnDrop(cancelled);
                    started.notify_one();
                    std::future::pending::<ReadResult>().await
                }
            }
        },
    );
    let first = scheduler.submit("file", 0..2, None).await.unwrap();
    let second = scheduler.submit("file", 2..4, None).await.unwrap();
    started.notified().await;
    {
        let blocked = scheduler.submit("file", 4..6, None);
        tokio::pin!(blocked);
        assert!(futures::poll!(&mut blocked).is_pending());
        assert_eq!(scheduler.waiting.get(), 1);
    }
    assert_eq!(scheduler.waiting.get(), 0);
    assert_eq!(
        metrics
            .read_scheduler_pending
            .with_label_values(&["test"])
            .get(),
        2
    );
    drop(first);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), cancelled.notified())
            .await
            .is_err()
    );
    drop(second);
    tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(
        metrics
            .read_scheduler_pending
            .with_label_values(&["test"])
            .get(),
        0
    );
    assert_eq!(
        metrics
            .read_scheduler_events
            .with_label_values(&["test", "group_cancelled"])
            .get(),
        1
    );
}

#[cfg(not(madsim))]
#[tokio::test]
async fn plug_flush_runtime_shutdown_releases_readers() {
    let scheduler = Arc::new(ReadScheduler::new(
        &ObjectStoreMetrics::unused(),
        "test",
        ReadSchedulerConfig::default(),
        |_, _| std::future::ready(Ok(Bytes::from_static(b"a"))),
    ));
    let plug = ReadPlug::new(scheduler.clone(), 8);
    let token = plug.token();
    drop(plug);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let response = {
        let _enter = runtime.enter();
        token.stage(&scheduler, "file", 0..1, None).unwrap()
    };
    // The flush has not even been polled; another runtime still owns the reader.
    runtime.shutdown_background();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), response)
            .await
            .unwrap()
            .is_err()
    );
    assert!(token.stage(&scheduler, "file", 0..1, None).is_none());
}

#[cfg(not(madsim))]
#[tokio::test]
async fn concurrent_plug_capacity_flush_submits_once() {
    for byte_limit in [false, true] {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_read_bytes: (if byte_limit { 2 } else { 1024 }).try_into().unwrap(),
                max_batch_requests: (if byte_limit { 16 } else { 2 }).try_into().unwrap(),
                ..Default::default()
            },
            move |_, range: Range<usize>| {
                calls.send(range.clone()).unwrap();
                std::future::ready(Ok(Bytes::from_static(&b"ab"[range])))
            },
        ));
        let plug = ReadPlug::new(scheduler.clone(), 8);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let runtime = tokio::runtime::Handle::current();
        let threads: Vec<_> = (0..2)
            .map(|i| {
                let token = plug.token();
                let scheduler = scheduler.clone();
                let barrier = barrier.clone();
                let runtime = runtime.clone();
                std::thread::spawn(move || {
                    let _enter = runtime.enter();
                    barrier.wait();
                    token.stage(&scheduler, "file", i..i + 1, None).unwrap()
                })
            })
            .collect();
        // The queued flush cannot run on this current-thread runtime yet.
        let responses: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(plug.token().stage(&scheduler, "file", 0..1, None).is_none());
        for (i, response) in responses.into_iter().enumerate() {
            assert_eq!(response.await.unwrap().unwrap(), &b"ab"[i..i + 1]);
        }
        tokio::task::yield_now().await;
        assert_eq!(reads.recv().await.unwrap(), 0..2);
        assert!(reads.try_recv().is_err());
        assert_eq!(scheduler.plug_flushes[1].get(), 1);
        assert_eq!(scheduler.batch_members.get(), 2);
        assert_eq!(scheduler.waiting.get(), 0);
        drop(plug);
    }
}

#[tokio::test]
async fn collecting_plug_does_not_reserve_point_read_capacity() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (calls, mut reads) = mpsc::unbounded_channel();
        let release_point = Arc::new(Notify::new());
        let scheduler = Arc::new(ReadScheduler::new(
            &ObjectStoreMetrics::new(&prometheus::Registry::new()),
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                ..Default::default()
            },
            {
                let release_point = release_point.clone();
                move |path, _| {
                    let calls = calls.clone();
                    let release_point = release_point.clone();
                    async move {
                        calls.send(path.clone()).unwrap();
                        if path == "point" {
                            release_point.notified().await;
                        }
                        Ok(Bytes::from_static(b"a"))
                    }
                }
            },
        ));
        let plug = ReadPlug::new(scheduler.clone(), 8);
        let response = plug
            .token()
            .stage(&scheduler, "prefetch", 0..1, None)
            .unwrap();
        assert_eq!(scheduler.groups.slots.available_permits(), 1);
        // Ordinary demand can take G while the plug is collecting. Keep
        // the plug handle alive throughout this check.
        let point = scheduler.submit("point", 0..1, None).await.unwrap();
        assert_eq!(reads.recv().await.unwrap(), "point");
        assert!(reads.try_recv().is_err());
        release_point.notify_one();
        assert_eq!(point.await.unwrap().unwrap(), "a");
        assert_eq!(response.await.unwrap().unwrap(), "a");
        assert_eq!(reads.recv().await.unwrap(), "prefetch");
        drop(plug);
    })
    .await
    .unwrap();
}
