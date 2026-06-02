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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use prometheus::Registry;

use super::*;
use crate::controller::catalog::CatalogController;
use crate::controller::cluster::ClusterController;
use crate::hummock::IcebergCompactorManager;
use crate::manager::MetaOpts;
use crate::rpc::metrics::MetaMetrics;

async fn build_test_manager() -> Arc<IcebergCompactionManager> {
    let mut opts = MetaOpts::test(false);
    opts.iceberg_compaction_report_timeout_sec = 30 * 60;
    let env = MetaSrvEnv::for_test_opts(opts, |_| ()).await;
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
        finish_action: CompactionTrackFinishAction::KeepTrack,
        state: CompactionTrackState::Idle {
            next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
            next_task_type_override: None,
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
    values.insert("enable_compaction".to_owned(), "true".to_owned());

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
        next_task_type_override: None,
    };
    assert!(track.should_trigger(now));

    let mut empty_track = new_track(now, 60, 10, 0);
    empty_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
        next_task_type_override: None,
    };
    assert!(!empty_track.should_trigger(now));
}

#[test]
fn test_record_force_compaction_bootstraps_or_preserves_backlog() {
    let now = Instant::now();

    for (initial_backlog, expected_backlog) in [(0, 1), (4, 4)] {
        let mut track = new_track(now, 300, 10, initial_backlog);

        track.record_force_compaction(now, None);

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
            ..
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
            ..
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
                ..
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

    track.record_force_compaction(now, None);

    assert!(!track.should_trigger(now));
}

#[test]
fn test_finish_failed_after_force_keeps_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now, None);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now);

    assert_eq!(track.pending_commit_count, 1);
    assert!(track.should_trigger(now));
}

#[test]
fn test_finish_success_after_force_consumes_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now, None);
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
            ..
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
            ..
        } => next_compaction_time,
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should start idle")
        }
    };

    track.update_interval(120, now + Duration::from_secs(30));

    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
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
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now: refresh_at,
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
            ..
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
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    };

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now,
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
async fn test_apply_sink_update_tracks_snapshot_expiration_without_compaction() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(144);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    config.enable_snapshot_expiration = true;
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::new(),
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    };

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(guard.snapshot_expiration_sink_ids.contains(&sink_id));
}

#[tokio::test]
async fn test_apply_manual_force_update_allows_disabled_compaction() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(146);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::new(),
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    };

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::ManualForceCompaction {
                task_type: TaskType::Full,
            },
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 1);
    assert!(track.should_trigger(now));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
}

#[tokio::test]
async fn test_manual_force_update_uses_selected_task_type_once() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(147);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    {
        let mut guard = manager.inner.write();
        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: SinkUpdateKind::ManualForceCompaction {
                    task_type: TaskType::FilesWithDelete,
                },
                now,
                allow_track_initialization: true,
                loaded_config: Some(config),
            },
        );

        assert!(applied);
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.task_type, TaskType::SmallFiles);

        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: SinkUpdateKind::ForceCompaction,
                now,
                allow_track_initialization: false,
                loaded_config: None,
            },
        );
        assert!(applied);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].task_type, TaskType::FilesWithDelete);
    drop(handles);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].task_type, TaskType::SmallFiles);
}

#[tokio::test]
async fn test_apply_sink_update_keeps_processing_track_when_compaction_is_disabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(145);
    let task_id = 8;
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut track = new_track(now, 300, 3, 0);
    track.start_processing();
    track.mark_dispatched(task_id, now);
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::from([(sink_id, track)]),
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    };

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now,
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    assert!(matches!(
        guard.sink_schedules.get(&sink_id).unwrap().state,
        CompactionTrackState::InFlight { task_id: 8, .. }
    ));
}

#[tokio::test]
async fn test_apply_sink_update_keeps_temporary_manual_track_when_compaction_is_disabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(148);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut track = new_track(now, 300, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::from([(sink_id, track)]),
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::from([(sink_id, tx)]),
    };

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now,
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    assert!(!applied);
    assert!(guard.sink_schedules.contains_key(&sink_id));
    assert!(guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_apply_sink_update_rejects_temporary_manual_processing_track_without_config() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(149);
    let task_id = 8;
    let now = Instant::now();
    let mut track = new_track(now, 300, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let mut guard = IcebergCompactionManagerInner {
        sink_schedules: HashMap::from([(sink_id, track)]),
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    };

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now,
            allow_track_initialization: false,
            loaded_config: None,
        },
    );

    assert!(!applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 1);
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id: 8, .. }
    ));
}

#[tokio::test]
async fn test_apply_sink_update_promotes_temporary_manual_track_when_compaction_enabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(150);
    let task_id = 9;
    let now = Instant::now();
    let mut track = new_track(now, 300, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);

        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: SinkUpdateKind::Commit,
                now,
                allow_track_initialization: false,
                loaded_config: Some(config),
            },
        );

        assert!(applied);
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.pending_commit_count, 2);
        assert_eq!(track.finish_action, CompactionTrackFinishAction::KeepTrack);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 1);
    assert_eq!(track.finish_action, CompactionTrackFinishAction::KeepTrack);
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
        snapshot_expiration_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    };

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit,
            now,
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
        next_task_type_override: None,
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
async fn test_pre_dispatch_failure_notifies_manual_waiter_and_removes_temporary_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(461);
    let now = Instant::now();
    let mut track = new_track(now, 120, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    track.start_processing();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    drop(IcebergCompactionHandle::new(
        sink_id,
        TaskType::Full,
        manager.inner.clone(),
        manager.metadata_manager.clone(),
    ));

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("failed before dispatch"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
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
    let sink_id = SinkId::new(47);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.start_processing();
    track.mark_dispatched(task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });

    assert_eq!(rx.await.unwrap().unwrap(), task_id);
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 0);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_completes_manual_waiter_on_failure() {
    let manager = build_test_manager().await;
    let task_id = 43;
    let sink_id = SinkId::new(48);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.start_processing();
    track.mark_dispatched(task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
    });

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("boom"));
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 2);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_removes_temporary_manual_track_on_success() {
    let manager = build_test_manager().await;
    let task_id = 44;
    let sink_id = SinkId::new(49);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    track.start_processing();
    track.mark_dispatched(task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });

    assert_eq!(rx.await.unwrap().unwrap(), task_id);
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_removes_temporary_manual_track_on_failure() {
    let manager = build_test_manager().await;
    let task_id = 45;
    let sink_id = SinkId::new(149);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    track.start_processing();
    track.mark_dispatched(task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
    });

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("boom"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_cancel_manual_compaction_waiter_keeps_pending_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(51);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.start_processing();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.cancel_manual_compaction_waiter(sink_id);

    assert!(rx.await.is_err());
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 2);
    assert!(matches!(
        track.state,
        CompactionTrackState::PendingDispatch { .. }
    ));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_trigger_manual_compaction_does_not_register_waiter_during_config_load() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(511);
    let _compactor_rx = manager.iceberg_compactor_manager.add_compactor(1.into());
    let catalog_write_guard = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;

    let join_handle = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.trigger_manual_compaction(sink_id).await })
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    {
        let guard = manager.inner.read();
        assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
        assert!(!guard.sink_schedules.contains_key(&sink_id));
    }

    join_handle.abort();
    assert!(join_handle.await.unwrap_err().is_cancelled());
    drop(catalog_write_guard);

    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_manual_compaction_waiter_is_not_stolen_during_config_load() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(512);
    let task_id = 48;
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.last_config_refresh_at = now - manager.config_refresh_interval();
    manager.inner.write().sink_schedules.insert(sink_id, track);
    let catalog_write_guard = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;

    let join_handle = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.start_manual_compaction(sink_id).await })
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        track.start_processing();
        track.mark_dispatched(task_id, now);
    }
    manager.handle_report_task(IcebergReportTask {
        task_id,
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
    });
    assert!(
        !manager
            .inner
            .read()
            .manual_compaction_waiters
            .contains_key(&sink_id)
    );

    drop(catalog_write_guard);
    let waiter = join_handle.await.unwrap().unwrap();
    assert!(
        manager
            .inner
            .read()
            .manual_compaction_waiters
            .contains_key(&sink_id)
    );
    manager.cancel_manual_compaction_waiter(sink_id);
    assert!(waiter.await.is_err());
}

#[tokio::test]
async fn test_get_top_n_notifies_manual_waiter_on_report_timeout() {
    let manager = build_test_manager().await;
    let task_id = 46;
    let sink_id = SinkId::new(52);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.report_timeout = Duration::from_secs(1);
    start_in_flight(&mut track, task_id, now - Duration::from_secs(5));
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 1);
    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("timed out"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_get_top_n_removes_temporary_manual_track_on_report_timeout() {
    let manager = build_test_manager().await;
    let task_id = 47;
    let sink_id = SinkId::new(53);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    track.report_timeout = Duration::from_secs(1);
    start_in_flight(&mut track, task_id, now - Duration::from_secs(5));
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert!(handles.is_empty());
    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("timed out"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_clear_iceberg_maintenance_notifies_manual_waiter() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(54);
    let now = Instant::now();
    let track = new_track(now, 120, 10, 1);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.snapshot_expiration_sink_ids.insert(sink_id);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.clear_iceberg_maintenance_by_sink_id(sink_id);

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("was cleared"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.snapshot_expiration_sink_ids.contains(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_trigger_manual_compaction_rejects_existing_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(50);
    let now = Instant::now();
    let _compactor_rx = manager.iceberg_compactor_manager.add_compactor(1.into());
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let error = manager
        .trigger_manual_compaction(sink_id)
        .await
        .unwrap_err();

    let error = error.to_string();
    assert!(error.contains("already running"));
    assert!(error.contains("state=in_flight"));
    assert!(error.contains("task_id=9"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(matches!(
        guard.sink_schedules.get(&sink_id).unwrap().state,
        CompactionTrackState::InFlight { task_id: 9, .. }
    ));
}

#[tokio::test]
async fn test_trigger_manual_compaction_rejects_without_compactor() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(502);

    let error = manager
        .trigger_manual_compaction(sink_id)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("No iceberg compactor available"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_start_manual_compaction_rejects_existing_pending_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(501);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.start_processing();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let error = manager.start_manual_compaction(sink_id).await.unwrap_err();

    let error = error.to_string();
    assert!(error.contains("already running"));
    assert!(error.contains("state=pending_dispatch"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(matches!(
        guard.sink_schedules.get(&sink_id).unwrap().state,
        CompactionTrackState::PendingDispatch { .. }
    ));
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
    let (tx, _rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

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
    assert!(guard.manual_compaction_waiters.contains_key(&sink_id));
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
