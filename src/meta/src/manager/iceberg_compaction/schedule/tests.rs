// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use prometheus::Registry;

use super::*;
use crate::controller::catalog::CatalogController;
use crate::controller::cluster::ClusterController;
use crate::hummock::IcebergCompactorManager;
use crate::rpc::metrics::MetaMetrics;

async fn build_test_manager() -> Arc<IcebergCompactionManager> {
    let env = MetaSrvEnv::for_test().await;
    let cluster_ctl = Arc::new(
        ClusterController::new(env.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
    );
    let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
    let metadata_manager = MetadataManager::new(cluster_ctl, catalog_ctl);
    let iceberg_compactor_manager = Arc::new(IcebergCompactorManager::new());
    let metrics = Arc::new(MetaMetrics::for_test(&Registry::new()));
    let (manager, _) =
        IcebergCompactionManager::build(env, metadata_manager, iceberg_compactor_manager, metrics);
    manager
}

fn new_track(
    now: Instant,
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    pending_commit_count: usize,
) -> CompactionTrack {
    CompactionTrack {
        task_type: TaskType::Full,
        trigger_interval_sec,
        trigger_snapshot_count,
        report_timeout: Duration::from_secs(30 * 60),
        last_config_refresh_at: now,
        pending_commit_count,
        state: CompactionTrackState::Idle {
            next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
        },
    }
}

fn start_in_flight(track: &mut CompactionTrack, task_id: u64, now: Instant) {
    track.start_processing();
    track.mark_dispatched(task_id, now);
}

fn record_commits(track: &mut CompactionTrack, n: usize) {
    for _ in 0..n {
        track.record_commit();
    }
}

fn new_test_iceberg_config(
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    compaction_type: CompactionType,
) -> IcebergConfig {
    let mut values = BTreeMap::from([
        ("connector".to_owned(), "iceberg".to_owned()),
        ("type".to_owned(), "append-only".to_owned()),
        ("force_append_only".to_owned(), "true".to_owned()),
        ("catalog.name".to_owned(), "test-catalog".to_owned()),
        ("catalog.type".to_owned(), "storage".to_owned()),
        ("warehouse.path".to_owned(), "s3://iceberg".to_owned()),
        ("database.name".to_owned(), "test_db".to_owned()),
        ("table.name".to_owned(), "test_table".to_owned()),
    ]);
    values.insert(
        "compaction_interval_sec".to_owned(),
        trigger_interval_sec.to_string(),
    );
    values.insert(
        "compaction.trigger_snapshot_count".to_owned(),
        trigger_snapshot_count.to_string(),
    );
    values.insert(
        "compaction.type".to_owned(),
        match compaction_type {
            CompactionType::Full => "full",
            CompactionType::SmallFiles => "small-files",
            CompactionType::FilesWithDelete => "files-with-delete",
        }
        .to_owned(),
    );

    IcebergConfig::from_btreemap(values).unwrap()
}

#[test]
fn test_should_trigger_by_pending_commit_threshold() {
    let now = Instant::now();
    let track = new_track(now, 300, 3, 3);

    assert!(track.should_trigger(now));
}

#[test]
fn test_should_trigger_by_interval_only_with_pending_commits() {
    let now = Instant::now();
    let mut track = new_track(now, 60, 10, 1);
    track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
    };
    assert!(track.should_trigger(now));

    let mut empty_track = new_track(now, 60, 10, 0);
    empty_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
    };
    assert!(!empty_track.should_trigger(now));
}

#[test]
fn test_record_force_compaction_bootstraps_or_preserves_backlog() {
    let now = Instant::now();

    for (initial_backlog, expected_backlog) in [(0, 1), (4, 4)] {
        let mut track = new_track(now, 300, 10, initial_backlog);

        track.record_force_compaction(now);

        assert_eq!(track.pending_commit_count, expected_backlog);
        assert!(track.should_trigger(now));
    }
}

#[test]
fn test_finish_success_clears_dispatched_baseline_and_starts_cooldown() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 12);
    start_in_flight(&mut track, 1, now);

    track.finish_success(now);

    assert_eq!(track.pending_commit_count, 0);
    assert!(!track.should_trigger(now));
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
        } => assert!(next_compaction_time >= now + Duration::from_secs(120)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should be idle")
        }
    }
}

#[test]
fn test_finish_failed_preserves_backlog_and_allows_retry() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 4);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now);

    assert_eq!(track.pending_commit_count, 4);
    assert!(track.should_trigger(now));
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
        } => assert!(next_compaction_time <= now),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should be idle")
        }
    }
}

#[test]
fn test_report_timeout_is_based_on_processing_deadline() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 5);
    track.report_timeout = Duration::from_secs(17);
    start_in_flight(&mut track, 1, now);

    match track.state {
        CompactionTrackState::InFlight { .. } => {}
        CompactionTrackState::Idle { .. } => panic!("track should remain pending"),
        CompactionTrackState::PendingDispatch { .. } => {
            panic!("track should have been dispatched")
        }
    }
    assert!(!track.is_report_timed_out(now + track.report_timeout - Duration::from_secs(1)));
    assert!(track.is_report_timed_out(now + track.report_timeout));
}

#[test]
fn test_revert_pre_dispatch_failure_restores_idle_without_losing_backlog() {
    let now = Instant::now();
    for additional_commits in [0, 3] {
        let mut track = new_track(now, 120, 10, 5);
        track.start_processing();
        record_commits(&mut track, additional_commits);

        track.revert_pre_dispatch_failure();

        assert_eq!(track.pending_commit_count, 5 + additional_commits);
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert_eq!(next_compaction_time, now + Duration::from_secs(120)),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should be restored to idle")
            }
        }
    }
}

#[test]
fn test_mark_dispatched_records_task_id_for_stale_report_filtering() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 5);
    track.start_processing();
    assert!(track.is_pending_dispatch());
    track.mark_dispatched(42, now);

    assert!(track.is_processing_task(42));
    assert!(!track.is_processing_task(43));
}

#[test]
fn test_force_compaction_does_not_make_processing_track_triggerable() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.start_processing();

    track.record_force_compaction(now);

    assert!(!track.should_trigger(now));
}

#[test]
fn test_finish_failed_after_force_keeps_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now);

    assert_eq!(track.pending_commit_count, 1);
    assert!(track.should_trigger(now));
}

#[test]
fn test_finish_success_after_force_consumes_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now);
    start_in_flight(&mut track, 1, now);

    track.finish_success(now);

    assert_eq!(track.pending_commit_count, 0);
    assert!(!track.should_trigger(now));
}

#[test]
fn test_record_commit_during_processing_is_preserved_after_success() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 1, now);
    record_commits(&mut track, 5);

    track.finish_success(now);

    assert_eq!(track.pending_commit_count, 5);
}

#[test]
fn test_update_interval_resets_idle_deadline() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);

    track.update_interval(300, now);

    assert_eq!(track.trigger_interval_sec, 300);
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
        } => assert_eq!(next_compaction_time, now + Duration::from_secs(300)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[test]
fn test_update_interval_same_value_keeps_existing_idle_deadline() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    let original_deadline = match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
        } => next_compaction_time,
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should start idle")
        }
    };

    track.update_interval(120, now + Duration::from_secs(30));

    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
        } => assert_eq!(next_compaction_time, original_deadline),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[test]
fn test_update_interval_does_not_interrupt_processing() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.start_processing();

    track.update_interval(300, now);

    assert_eq!(track.trigger_interval_sec, 300);
    match track.state {
        CompactionTrackState::PendingDispatch { .. } => {}
        CompactionTrackState::Idle { .. } => panic!("processing state should be preserved"),
        CompactionTrackState::InFlight { .. } => {
            panic!("track should remain pending dispatch")
        }
    }
}

#[test]
fn test_needs_config_refresh_respects_ttl() {
    let now = Instant::now();
    let track = new_track(now, 120, 10, 1);

    let refresh_interval = Duration::from_secs(60);

    assert!(!track.needs_config_refresh(
        now + refresh_interval - Duration::from_secs(1),
        refresh_interval,
    ));
    assert!(track.needs_config_refresh(now + refresh_interval, refresh_interval));
}

#[test]
fn test_should_refresh_config_requires_idle_state() {
    let now = Instant::now();
    let refresh_interval = Duration::from_secs(60);
    let mut track = new_track(now, 120, 10, 1);

    assert!(track.should_refresh_config(now + refresh_interval, refresh_interval));

    track.start_processing();

    assert!(!track.should_refresh_config(now + refresh_interval, refresh_interval));
}

#[tokio::test]
async fn test_apply_sink_update_refreshes_existing_idle_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(41);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.last_config_refresh_at = now - manager.config_refresh_interval();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let refresh_at = now + Duration::from_secs(30);
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = manager.inner.write();

    manager.apply_sink_update(
        &mut guard,
        sink_id,
        SinkUpdateKind::Commit,
        refresh_at,
        manager.config_refresh_interval(),
        PreparedSinkUpdate {
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.task_type, TaskType::SmallFiles);
    assert_eq!(track.trigger_interval_sec, 300);
    assert_eq!(track.trigger_snapshot_count, 3);
    assert_eq!(track.last_config_refresh_at, refresh_at);
    assert_eq!(track.pending_commit_count, 2);
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
        } => assert_eq!(next_compaction_time, refresh_at + Duration::from_secs(300)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[tokio::test]
async fn test_update_iceberg_commit_info_skips_missing_track_on_load_failure() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);

    manager
        .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction: false,
        })
        .await;

    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_update_iceberg_commit_info_refresh_failure_keeps_refresh_deadline_stale() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(43);
    let now = Instant::now();
    let stale_refresh_at = now - manager.config_refresh_interval();
    let mut track = new_track(now, 120, 7, 3);
    track.last_config_refresh_at = stale_refresh_at;
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager
        .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction: false,
        })
        .await;

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.trigger_interval_sec, 120);
    assert_eq!(track.trigger_snapshot_count, 7);
    assert_eq!(track.pending_commit_count, 4);
    assert_eq!(track.last_config_refresh_at, stale_refresh_at);
}

#[tokio::test]
async fn test_apply_sink_update_creates_missing_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(44);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::new(),
        manual_task_waiters: HashMap::new(),
    };

    manager.apply_sink_update(
        &mut guard,
        sink_id,
        SinkUpdateKind::Commit,
        now,
        manager.config_refresh_interval(),
        PreparedSinkUpdate {
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.task_type, TaskType::SmallFiles);
    assert_eq!(track.trigger_interval_sec, 300);
    assert_eq!(track.trigger_snapshot_count, 3);
    assert_eq!(track.pending_commit_count, 1);
    assert_eq!(track.last_config_refresh_at, now);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_apply_sink_update_does_not_resurrect_disappeared_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(45);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::new(),
        manual_task_waiters: HashMap::new(),
    };

    manager.apply_sink_update(
        &mut guard,
        sink_id,
        SinkUpdateKind::Commit,
        now,
        manager.config_refresh_interval(),
        PreparedSinkUpdate {
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_get_top_n_creates_pending_dispatch_handle_and_drop_restores_idle() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(46);
    let now = Instant::now();
    let mut track = new_track(now, 120, 3, 3);
    track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
    };
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 1);
    {
        let guard = manager.inner.read();
        assert!(matches!(
            guard.sink_schedules.get(&sink_id).unwrap().state,
            CompactionTrackState::PendingDispatch { .. }
        ));
    }

    drop(handles);

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 3);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_success_consumes_backlog_and_resets_to_idle() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(47);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    record_commits(&mut track, 3);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 9,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 3);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_completes_manual_waiter_on_success() {
    let manager = build_test_manager().await;
    let task_id = 42;
    let (tx, rx) = tokio::sync::oneshot::channel();
    manager
        .inner
        .write()
        .manual_task_waiters
        .insert(task_id, tx);

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: SinkId::new(47).as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });

    assert!(rx.await.unwrap().is_ok());
    assert!(
        !manager
            .inner
            .read()
            .manual_task_waiters
            .contains_key(&task_id)
    );
}

#[tokio::test]
async fn test_handle_report_task_completes_manual_waiter_on_failure() {
    let manager = build_test_manager().await;
    let task_id = 43;
    let (tx, rx) = tokio::sync::oneshot::channel();
    manager
        .inner
        .write()
        .manual_task_waiters
        .insert(task_id, tx);

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: SinkId::new(48).as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
    });

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("boom"));
    assert!(
        !manager
            .inner
            .read()
            .manual_task_waiters
            .contains_key(&task_id)
    );
}

#[tokio::test]
async fn test_handle_report_task_failure_preserves_backlog_and_resets_to_idle() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(471);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    record_commits(&mut track, 3);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 9,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 5);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_ignores_stale_task_id() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(48);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 10,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 2);
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id: 9, .. }
    ));
}

#[tokio::test]
async fn test_get_top_n_retries_timed_out_inflight_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(49);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.report_timeout = Duration::from_secs(1);
    start_in_flight(&mut track, 7, now - Duration::from_secs(5));
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 1);
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(
        track.state,
        CompactionTrackState::PendingDispatch { .. }
    ));
}
