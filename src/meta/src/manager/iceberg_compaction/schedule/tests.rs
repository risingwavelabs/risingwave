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
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
use risingwave_pb::id::IcebergCompactionTaskId;

use super::*;
use crate::barrier::BarrierScheduler;
use crate::controller::catalog::CatalogController;
use crate::controller::cluster::ClusterController;
use crate::hummock::{CompactorManager, HummockManager, IcebergCompactorManager};
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
    let hummock_manager = HummockManager::new(
        env.clone(),
        metadata_manager.clone(),
        metrics.clone(),
        Arc::new(CompactorManager::for_test()),
        tokio::sync::mpsc::unbounded_channel().0,
    )
    .await
    .unwrap();
    let (barrier_scheduler, _) = BarrierScheduler::new_pair(hummock_manager);
    let (manager, _) = IcebergCompactionManager::build(
        env,
        metadata_manager,
        iceberg_compactor_manager,
        metrics,
        barrier_scheduler,
    );
    manager
}

/// Builds a single-task track. Tests for sequence-bounded rounds must use
/// `new_round_track` instead.
fn new_track(
    now: Instant,
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    pending_commit_count: usize,
) -> CompactionTrack {
    let mut track = CompactionTrack::new(
        CompactionScheduleConfig {
            policy: CompactionPolicy {
                task_type: TaskType::Full,
                small_files_threshold_mb: 32,
                target_file_size_mb: 128,
                delete_ratio_percent: 30,
            },
            write_mode: IcebergWriteMode::CopyOnWrite,
            pk_index_coordinated: false,
            automatic_enabled: true,
            trigger_interval_sec,
            trigger_snapshot_count,
            report_timeout: Duration::from_secs(30 * 60),
            refreshed_at: now,
        },
        now,
    );
    track.demand = CompactionDemand {
        commit_count: pending_commit_count,
        since: (pending_commit_count > 0).then_some(now),
        reconciliation_pending: false,
    };
    track
}

fn new_round_track(
    now: Instant,
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    pending_commit_count: usize,
) -> CompactionTrack {
    let mut track = new_track(
        now,
        trigger_interval_sec,
        trigger_snapshot_count,
        pending_commit_count,
    );
    track.config.write_mode = IcebergWriteMode::MergeOnRead;
    track
}

fn start_in_flight_on(
    track: &mut CompactionTrack,
    task_id: impl Into<IcebergCompactionTaskId>,
    compactor_context_id: HummockContextId,
    now: Instant,
) {
    track.start_attempt();
    track.mark_dispatched(task_id.into(), compactor_context_id, now);
}

fn start_in_flight(
    track: &mut CompactionTrack,
    task_id: impl Into<IcebergCompactionTaskId>,
    now: Instant,
) {
    start_in_flight_on(track, task_id, 1.into(), now);
}

fn record_commits(track: &mut CompactionTrack, n: usize) {
    for _ in 0..n {
        track.record_commit();
    }
}

fn committed_snapshot(snapshot_id: i64, timestamp_ms: i64) -> IcebergCommittedSnapshot {
    IcebergCommittedSnapshot {
        branch: "main".to_owned(),
        snapshot_id,
        timestamp_ms,
        max_file_sequence_number: Some(snapshot_id),
    }
}

fn commit_update(snapshot_id: i64, timestamp_ms: i64) -> SinkUpdateKind {
    SinkUpdateKind::Commit {
        observed_snapshot: committed_snapshot(snapshot_id, timestamp_ms),
    }
}

fn force_update(snapshot_id: i64, timestamp_ms: i64) -> SinkUpdateKind {
    SinkUpdateKind::ForceCompaction {
        observed_snapshot: committed_snapshot(snapshot_id, timestamp_ms),
    }
}

fn empty_inner() -> IcebergCompactionManagerInner {
    IcebergCompactionManagerInner {
        sink_schedules: HashMap::new(),
        sink_recovery: HashMap::new(),
        snapshot_expiration_sink_ids: HashSet::new(),
        manifest_rewrite_sink_ids: HashSet::new(),
        orphan_file_cleanup_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
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
            CompactionType::Auto => "auto",
            CompactionType::Full => "full",
            CompactionType::SmallFiles => "small-files",
            CompactionType::FilesWithDelete => "files-with-delete",
        }
        .to_owned(),
    );
    values.insert("enable_compaction".to_owned(), "true".to_owned());

    IcebergConfig::from_btreemap(values).unwrap()
}

#[tokio::test]
async fn test_schedule_resolves_licensed_compaction_type_policy() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    assert!(
        risingwave_common::license::Feature::IcebergCompaction
            .check_available()
            .is_ok()
    );

    for (write_mode, configured_type, expected_task_type) in [
        (IcebergWriteMode::CopyOnWrite, None, TaskType::Auto),
        (
            IcebergWriteMode::CopyOnWrite,
            Some(CompactionType::Full),
            TaskType::Auto,
        ),
        (IcebergWriteMode::MergeOnRead, None, TaskType::Auto),
        (
            IcebergWriteMode::MergeOnRead,
            Some(CompactionType::Full),
            TaskType::Full,
        ),
    ] {
        let mut config = new_test_iceberg_config(300, 3, CompactionType::Auto);
        config.r#type = "upsert".to_owned();
        config.write_mode = write_mode;
        config.compaction_type = configured_type;

        let mut track = manager.create_compaction_track(&config, now);
        assert_eq!(track.config.policy.task_type, expected_task_type);
        assert_eq!(track.config.write_mode, write_mode);

        track.record_observed_snapshot(committed_snapshot(10, 1000));
        track.record_commit();
        let attempt = track.start_attempt();
        let expected_boundary = (write_mode == IcebergWriteMode::MergeOnRead).then_some(10);
        assert_eq!(track.round_boundary(), expected_boundary);
        assert_eq!(attempt.max_file_sequence_number, expected_boundary);
    }
}

#[test]
fn test_automatic_compaction_without_file_sequence_bound_stays_unbounded() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 0);
    let mut observed_snapshot = committed_snapshot(10, 1000);
    observed_snapshot.max_file_sequence_number = None;
    track.record_observed_snapshot(observed_snapshot);
    track.record_commit();

    let attempt = track.start_attempt();

    assert_eq!(attempt.max_file_sequence_number, None);
    assert_eq!(track.round_boundary(), None);
    assert_eq!(track.demand.commit_count, 1);
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
        manual_task_type: None,
    };
    assert!(track.should_trigger(now));

    let mut empty_track = new_track(now, 60, 10, 0);
    empty_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
        manual_task_type: None,
    };
    assert!(!empty_track.should_trigger(now));
}

#[test]
fn test_record_force_compaction_bootstraps_or_preserves_backlog() {
    let now = Instant::now();

    for (initial_backlog, expected_backlog) in [(0, 1), (4, 4)] {
        let mut track = new_track(now, 300, 10, initial_backlog);

        track.record_force_compaction(now, None);

        assert_eq!(track.demand.commit_count, expected_backlog);
        assert!(track.should_trigger(now));
    }
}

#[tokio::test]
async fn test_commit_update_freezes_gc_watermark_after_attempt_start() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(404);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 10, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(10, 1000),
            now,
            allow_track_initialization: true,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    guard
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .start_attempt();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(11, 2000),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: None,
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(
        track
            .latest_observed_snapshot
            .as_ref()
            .map(|s| s.snapshot_id),
        Some(11)
    );
    match track.active_attempt_gc_watermark_snapshot() {
        Some(Some(snapshot)) => assert_eq!(snapshot.snapshot_id, 10),
        protection => panic!("unexpected gc watermark: {protection:?}"),
    }

    guard
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .mark_dispatched(1.into(), 1.into(), now);
    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(12, 3000),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: None,
        },
    );
    assert!(applied);
    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: force_update(13, 4000),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: None,
        },
    );
    assert!(applied);

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(
        track
            .latest_observed_snapshot
            .as_ref()
            .map(|s| s.snapshot_id),
        Some(13)
    );
    match track.active_attempt_gc_watermark_snapshot() {
        Some(Some(snapshot)) => assert_eq!(snapshot.snapshot_id, 10),
        protection => panic!("unexpected gc watermark: {protection:?}"),
    }
}

#[tokio::test]
async fn test_force_update_initializes_observed_snapshot() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(405);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 10, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: force_update(10, 1000),
            now,
            allow_track_initialization: true,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
    track.start_attempt();
    match track.active_attempt_gc_watermark_snapshot() {
        Some(Some(snapshot)) => assert_eq!(snapshot.snapshot_id, 10),
        protection => panic!("unexpected gc watermark: {protection:?}"),
    }
}

#[test]
fn test_manual_task_without_observed_snapshot_has_no_gc_watermark() {
    let now = Instant::now();
    let skip_sink_id = SinkId::new(42);
    let mut skip_track = new_track(now, 300, 10, 0);
    skip_track.record_force_compaction(now, Some(TaskType::Full));
    skip_track.start_attempt();

    let mut inner = empty_inner();
    inner.sink_schedules.insert(skip_sink_id, skip_track);

    assert!(matches!(
        inner
            .sink_schedules
            .get(&skip_sink_id)
            .unwrap()
            .active_attempt_gc_watermark_snapshot(),
        Some(None)
    ));
}

#[test]
fn test_finish_success_clears_dispatched_baseline_and_starts_cooldown() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 12);
    start_in_flight(&mut track, 1, now);

    track.finish_success(now);

    assert_eq!(track.demand.commit_count, 0);
    assert!(!track.should_trigger(now));
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert!(next_compaction_time >= now + Duration::from_secs(120)),
        CompactionTrackState::PendingDispatch { .. }
        | CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
            panic!("track should be idle")
        }
    }
}

#[test]
fn test_active_round_keeps_sequence_boundary_until_drained() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);

    assert_eq!(track.round_boundary(), Some(10));
    track.record_observed_snapshot(committed_snapshot(11, 2000));
    track.record_commit();
    track.finish_success(now);

    assert_eq!(track.round_boundary(), Some(10));
    assert!(track.should_trigger(now));
    track.start_attempt();
    track.mark_dispatched(2.into(), 1.into(), now);
    track.finish_drained(now);

    assert_eq!(track.round_boundary(), None);
    assert!(!track.should_trigger(now));
    assert_eq!(track.demand.commit_count, 1);
}

#[test]
fn test_force_advances_active_round_retry_without_adding_backlog() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    track.finish_failed(now, "boom".to_owned());

    let force_at = now + Duration::from_millis(500);
    assert!(!track.should_trigger(force_at));
    track.record_force_compaction(force_at, None);

    assert_eq!(track.demand.commit_count, 0);
    assert_eq!(track.round_boundary(), Some(10));
    assert!(track.should_trigger(force_at));
}

#[test]
fn test_active_round_failure_backoff_ignores_commit_threshold() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 3, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    record_commits(&mut track, 3);

    track.finish_failed(now, "boom".to_owned());

    assert_eq!(track.round_boundary(), Some(10));
    assert_eq!(track.demand.commit_count, 3);
    assert!(!track.should_trigger(now));
    assert!(track.should_trigger(now + Duration::from_secs(1)));
}

#[test]
fn test_finish_failed_preserves_backlog_and_allows_retry() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 4);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now, "boom".to_owned());

    assert_eq!(track.demand.commit_count, 4);
    assert!(!track.should_trigger(now));
    assert!(track.should_trigger(now + Duration::from_secs(1)));
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, now + Duration::from_secs(1)),
        CompactionTrackState::PendingDispatch { .. }
        | CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
            panic!("track should be idle")
        }
    }
}

#[test]
fn test_report_timeout_starts_at_dispatch() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 5);
    track.config.report_timeout = Duration::from_secs(17);
    start_in_flight(&mut track, 1, now);

    match track.state {
        CompactionTrackState::InFlight { .. } => {}
        CompactionTrackState::Idle { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
            panic!("track should remain in flight")
        }
        CompactionTrackState::PendingDispatch { .. } => {
            panic!("track should have been dispatched")
        }
    }
    assert!(!track.is_report_timed_out(now + track.config.report_timeout - Duration::from_secs(1)));
    assert!(track.is_report_timed_out(now + track.config.report_timeout));
}

#[test]
fn test_revert_pre_dispatch_failure_requeues_after_backoff_without_losing_backlog() {
    let now = Instant::now();
    for additional_commits in [0, 3] {
        let mut track = new_track(now, 120, 10, 5);
        track.start_attempt();
        record_commits(&mut track, additional_commits);

        let revert_at = now + Duration::from_secs(30);
        assert_eq!(
            track.revert_pre_dispatch_failure(revert_at),
            CompactionTrackFinishAction::KeepTrack
        );

        assert_eq!(track.demand.commit_count, 5 + additional_commits);
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                ..
            } => assert_eq!(next_compaction_time, revert_at + Duration::from_secs(1)),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. }
            | CompactionTrackState::Applying { .. }
            | CompactionTrackState::NeedsRecovery { .. } => {
                panic!("track should be restored to idle")
            }
        }
    }
}

#[test]
fn test_mark_dispatched_records_task_id_for_stale_report_filtering() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 5);
    track.start_attempt();
    track.mark_dispatched(42.into(), 1.into(), now);

    assert!(track.is_in_flight_task(42.into()));
    assert!(!track.is_in_flight_task(43.into()));
}

#[test]
fn test_force_compaction_does_not_make_active_attempt_triggerable() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.start_attempt();

    track.record_force_compaction(now, None);

    assert!(!track.should_trigger(now));
}

#[test]
fn test_finish_failed_after_force_keeps_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now, None);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now, "boom".to_owned());

    assert_eq!(track.demand.commit_count, 1);
    assert!(!track.should_trigger(now));
    assert!(track.should_trigger(now + Duration::from_secs(1)));
}

#[test]
fn test_finish_success_after_force_consumes_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now, None);
    start_in_flight(&mut track, 1, now);

    track.finish_success(now);

    assert_eq!(track.demand.commit_count, 0);
    assert!(!track.should_trigger(now));
}

#[test]
fn test_record_commit_during_active_attempt_is_preserved_after_success() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 1, now);
    record_commits(&mut track, 5);

    track.finish_success(now);

    assert_eq!(track.demand.commit_count, 5);
}

#[test]
fn test_update_interval_resets_idle_deadline() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);

    track.update_interval(300, now);

    assert_eq!(track.config.trigger_interval_sec, 300);
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, now + Duration::from_secs(300)),
        CompactionTrackState::PendingDispatch { .. }
        | CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
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
        CompactionTrackState::PendingDispatch { .. }
        | CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
            panic!("track should start idle")
        }
    };

    track.update_interval(120, now + Duration::from_secs(30));

    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, original_deadline),
        CompactionTrackState::PendingDispatch { .. }
        | CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[test]
fn test_update_interval_does_not_interrupt_active_attempt() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.start_attempt();

    track.update_interval(300, now);

    assert_eq!(track.config.trigger_interval_sec, 300);
    match track.state {
        CompactionTrackState::PendingDispatch { .. } => {}
        CompactionTrackState::Idle { .. } => panic!("active attempt should be preserved"),
        CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
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

#[tokio::test]
async fn test_commit_update_refreshes_config_while_task_is_in_flight() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(240);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.config.refreshed_at = now - manager.config_refresh_interval();
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    // Holding the catalog write lock makes an attempted config refresh observable:
    // the commit update must wait here even though the track is in flight.
    let catalog_write_guard = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;
    let join_handle = {
        let manager = manager.clone();
        tokio::spawn(async move {
            manager
                .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
                    sink_id,
                    force_compaction: false,
                    has_new_data: true,
                    observed_snapshot: committed_snapshot(11, 2000),
                })
                .await;
        })
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    assert!(!join_handle.is_finished());

    join_handle.abort();
    assert!(join_handle.await.unwrap_err().is_cancelled());
    drop(catalog_write_guard);
}

#[tokio::test]
async fn test_apply_sink_update_refreshes_existing_idle_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(41);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.config.refreshed_at = now - manager.config_refresh_interval();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let refresh_at = now + Duration::from_secs(30);
    let config = new_test_iceberg_config(300, 3, CompactionType::Auto);
    let mut guard = manager.inner.write();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now: refresh_at,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.config.policy.task_type, TaskType::Auto);
    assert_eq!(track.config.trigger_interval_sec, 300);
    assert_eq!(track.config.trigger_snapshot_count, 3);
    assert_eq!(track.config.refreshed_at, refresh_at);
    assert_eq!(track.demand.commit_count, 2);
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, refresh_at + Duration::from_secs(300)),
        CompactionTrackState::PendingDispatch { .. }
        | CompactionTrackState::InFlight { .. }
        | CompactionTrackState::Applying { .. }
        | CompactionTrackState::NeedsRecovery { .. } => {
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
            has_new_data: true,
            observed_snapshot: committed_snapshot(1, 1),
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
    track.config.refreshed_at = stale_refresh_at;
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager
        .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction: false,
            has_new_data: true,
            observed_snapshot: committed_snapshot(1, 1),
        })
        .await;

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.config.trigger_interval_sec, 120);
    assert_eq!(track.config.trigger_snapshot_count, 7);
    assert_eq!(track.demand.commit_count, 4);
    assert_eq!(track.config.refreshed_at, stale_refresh_at);
}

#[tokio::test]
async fn test_apply_sink_update_creates_missing_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(44);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: true,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.config.policy.task_type, TaskType::SmallFiles);
    assert_eq!(track.config.trigger_interval_sec, 300);
    assert_eq!(track.config.trigger_snapshot_count, 3);
    assert_eq!(track.demand.commit_count, 1);
    assert_eq!(track.config.refreshed_at, now);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_apply_sink_update_tracks_metadata_maintenance_without_compaction() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(144);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    config.enable_snapshot_expiration = true;
    config.enable_manifest_rewrite = true;
    config.enable_orphan_file_cleanup = true;
    let mut guard = empty_inner();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: true,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(guard.snapshot_expiration_sink_ids.contains(&sink_id));
    assert!(guard.manifest_rewrite_sink_ids.contains(&sink_id));
    assert!(guard.orphan_file_cleanup_sink_ids.contains(&sink_id));
}

#[tokio::test]
async fn test_apply_manual_force_update_allows_disabled_compaction() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(146);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut guard = empty_inner();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::ManualForceCompaction {
                task_type: TaskType::Full,
            },
            now,
            allow_track_initialization: true,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 1);
    assert!(track.should_trigger(now));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
}

#[tokio::test]
async fn test_start_manual_compaction_rejects_active_automatic_round() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(407);
    let now = Instant::now();
    let mut track = new_round_track(now, 300, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    track.finish_success(now);

    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert_eq!(track.round_boundary(), Some(10));

    manager.inner.write().sink_schedules.insert(sink_id, track);

    let error = manager.start_manual_compaction(sink_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("manual Full compaction is rejected")
    );

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.round_boundary(), Some(10));
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_refresh_schedule_config_updates_next_attempt_without_changing_active_attempt() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let mut track = new_round_track(now, 300, 3, 1);
    track.config.policy.task_type = TaskType::SmallFiles;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);

    let refresh_at = now + Duration::from_secs(5);
    let refreshed_config = new_test_iceberg_config(42, 7, CompactionType::FilesWithDelete);
    manager.refresh_schedule_config(&mut track, &refreshed_config, refresh_at);

    assert_eq!(track.config.policy.task_type, TaskType::FilesWithDelete);
    assert_eq!(track.effective_task_type(), TaskType::SmallFiles);
    assert_eq!(track.config.trigger_interval_sec, 42);
    assert_eq!(track.config.trigger_snapshot_count, 7);
    assert_eq!(track.round_boundary(), Some(10));

    track.finish_success(refresh_at);
    assert!(track.should_trigger(refresh_at));
    let next_attempt = track.start_attempt();
    assert_eq!(next_attempt.policy.task_type, TaskType::FilesWithDelete);
    assert_eq!(next_attempt.max_file_sequence_number, Some(10));
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
                recovery_generation: 0,
                loaded_config: Some(config),
            },
        );

        assert!(applied);
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.config.policy.task_type, TaskType::SmallFiles);

        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: force_update(1, 1),
                now,
                allow_track_initialization: false,
                recovery_generation: 0,
                loaded_config: None,
            },
        );
        assert!(applied);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(
        handles[0].attempt.policy.task_type,
        TaskType::FilesWithDelete
    );
    assert_eq!(handles[0].attempt.max_file_sequence_number, None);
    drop(handles);

    manager
        .inner
        .write()
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .record_force_compaction(Instant::now(), None);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.policy.task_type, TaskType::SmallFiles);
    drop(handles);
}

#[tokio::test]
async fn test_apply_sink_update_keeps_in_flight_attempt_when_compaction_is_disabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(145);
    let task_id = 8;
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut track = new_round_track(now, 300, 3, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, task_id, now);
    let mut guard = empty_inner();
    guard.sink_schedules.insert(sink_id, track);

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 8
    ));
    assert_eq!(track.round_boundary(), Some(10));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
}

#[tokio::test]
async fn test_apply_sink_update_keeps_queued_manual_task_when_compaction_is_disabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(248);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut track = new_track(now, 300, 3, 1);
    track.record_force_compaction(now, Some(TaskType::Full));
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let mut guard = empty_inner();
    guard.sink_schedules.insert(sink_id, track);
    guard.manual_compaction_waiters.insert(sink_id, tx);

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    assert!(!applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(
        track.state,
        CompactionTrackState::Idle {
            manual_task_type: Some(TaskType::Full),
            ..
        }
    ));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
    assert!(guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_apply_sink_update_rejects_temporary_manual_in_flight_attempt_without_config() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(149);
    let task_id = 8;
    let now = Instant::now();
    let mut track = new_track(now, 300, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let mut guard = empty_inner();
    guard.sink_schedules.insert(sink_id, track);

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: None,
        },
    );

    assert!(!applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 1);
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 8
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
                kind: commit_update(1, 1),
                now,
                allow_track_initialization: false,
                recovery_generation: 0,
                loaded_config: Some(config),
            },
        );

        assert!(applied);
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.demand.commit_count, 2);
        assert_eq!(track.finish_action, CompactionTrackFinishAction::KeepTrack);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 1);
    assert_eq!(track.finish_action, CompactionTrackFinishAction::KeepTrack);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_apply_sink_update_does_not_resurrect_disappeared_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(45);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            recovery_generation: 0,
            loaded_config: Some(config),
        },
    );

    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_cleared_pending_handle_does_not_dispatch_or_restore_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(246);
    let now = Instant::now();
    let mut receiver = manager.iceberg_compactor_manager.add_compactor(1.into());
    let compactor = manager
        .iceberg_compactor_manager
        .get_compactor(1.into())
        .unwrap();
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, new_track(now, 120, 3, 3));

    let mut handle = manager.get_top_n_iceberg_commit_sink_ids(1).pop().unwrap();
    manager.clear_iceberg_maintenance_by_sink_id(sink_id);

    handle
        .try_dispatch_task(
            &compactor,
            IcebergCompactionTask {
                task_id: 99.into(),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    drop(handle);

    assert!(!manager.inner.read().sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_clear_iceberg_maintenance_cancels_in_flight_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(467);
    let task_id = 88;
    let compactor_context_id = 1000.into();
    let mut receiver = manager
        .iceberg_compactor_manager
        .add_compactor(compactor_context_id);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight_on(&mut track, task_id, compactor_context_id, now);
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.snapshot_expiration_sink_ids.insert(sink_id);
        guard.manifest_rewrite_sink_ids.insert(sink_id);
        guard.orphan_file_cleanup_sink_ids.insert(sink_id);
    }

    manager.clear_iceberg_maintenance_by_sink_id(sink_id);

    {
        let guard = manager.inner.read();
        assert!(!guard.sink_schedules.contains_key(&sink_id));
        assert!(!guard.snapshot_expiration_sink_ids.contains(&sink_id));
        assert!(!guard.manifest_rewrite_sink_ids.contains(&sink_id));
        assert!(!guard.orphan_file_cleanup_sink_ids.contains(&sink_id));
    }

    let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
    match event {
        IcebergResponseEvent::CancelCompactTask(cancel_task) => {
            assert_eq!(cancel_task.task_id, task_id);
        }
        other => panic!("unexpected iceberg compactor event: {other:?}"),
    }
}

#[tokio::test]
async fn test_pre_dispatch_failure_notifies_manual_waiter_and_removes_temporary_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(461);
    let now = Instant::now();
    let mut track = new_track(now, 120, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    let attempt = track.start_attempt();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    drop(IcebergCompactionHandle::new(
        sink_id,
        attempt,
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
        task_id: 9.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 3);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_drained_finishes_active_round() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(470);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 9, now);
    track.record_observed_snapshot(committed_snapshot(11, 2000));
    track.record_commit();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 9.into(),
        sink_id,
        status: IcebergReportTaskStatus::Drained as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.round_boundary(), None);
    assert_eq!(track.demand.commit_count, 1);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_completes_manual_waiter_on_success() {
    let manager = build_test_manager().await;
    let task_id = 42;
    let sink_id = SinkId::new(47);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    assert_eq!(rx.await.unwrap().unwrap(), task_id);
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 0);
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
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
    });

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("boom"));
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 2);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_treats_unbounded_drained_as_failure() {
    let manager = build_test_manager().await;
    let task_id = 431;
    let sink_id = SinkId::new(481);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, task_id, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Drained as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 2);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_compaction_status_reports_failures_until_success() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(482);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.config.report_timeout = Duration::from_secs(1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);
    let status = || {
        manager
            .list_compaction_statuses()
            .into_iter()
            .find(|status| status.sink_id == sink_id)
            .unwrap()
    };

    manager.handle_report_task(IcebergReportTask {
        task_id: 1.into(),
        sink_id,
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
    });
    let failed = status();
    assert_eq!(failed.consecutive_failures, 1);
    assert_eq!(failed.last_error.as_deref(), Some("boom"));
    assert!(failed.last_failure_at.is_some());
    assert!(failed.last_success_at.is_none());
    assert_eq!(failed.compaction_lag, None);

    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        track.record_observed_snapshot(committed_snapshot(11, 4000));
        track.record_commit();
        start_in_flight(track, 2, now - Duration::from_secs(5));
    }
    assert!(manager.get_top_n_iceberg_commit_sink_ids(0).is_empty());
    let timed_out = status();
    assert_eq!(timed_out.consecutive_failures, 2);
    assert!(timed_out.last_error.unwrap().contains("timed out"));

    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        start_in_flight(track, 3, now);
        track.record_observed_snapshot(committed_snapshot(12, 9000));
        track.record_commit();
    }
    manager.handle_report_task(IcebergReportTask {
        task_id: 3.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });
    let recovered = status();
    assert_eq!(recovered.consecutive_failures, 0);
    assert!(recovered.last_success_at.is_some());
    assert!(recovered.last_error.unwrap().contains("timed out"));
    // The task started from snapshot 11, so snapshot 12 is still uncompacted.
    assert_eq!(recovered.compaction_lag, Some(Duration::from_secs(5)));
}

#[test]
fn test_compaction_lag_uses_round_start_snapshot() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    track.record_observed_snapshot(committed_snapshot(11, 4000));
    track.record_commit();

    track.finish_success(now);
    assert!(track.health.last_success_at.is_some());
    assert_eq!(track.compaction_lag(), None);

    // A later attempt in the same round observes snapshot 11, but draining the
    // round only proves that commits up to the round start are compacted.
    start_in_flight(&mut track, 2, now);
    track.finish_drained(now);
    assert_eq!(track.compaction_lag(), Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn test_handle_report_task_removes_temporary_manual_track_on_success() {
    let manager = build_test_manager().await;
    let task_id = 44;
    let sink_id = SinkId::new(49);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
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
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
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
    track.start_attempt();
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
    assert_eq!(track.demand.commit_count, 2);
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
    track.config.refreshed_at = now - manager.config_refresh_interval();
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
        start_in_flight(track, task_id, now);
    }
    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
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
    track.config.report_timeout = Duration::from_secs(1);
    start_in_flight(&mut track, task_id, now - Duration::from_secs(5));
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 0);
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
    track.config.report_timeout = Duration::from_secs(1);
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
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 9
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
    track.start_attempt();
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
        task_id: 9.into(),
        sink_id,
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 5);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_ignores_stale_task_id() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(48);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 2);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 9, now);
    let (tx, _rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: 10.into(),
        sink_id,
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.demand.commit_count, 0);
    assert_eq!(track.round_boundary(), Some(10));
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 9
    ));
    assert!(guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_get_top_n_backs_off_after_timed_out_in_flight_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(49);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 2);
    track.config.report_timeout = Duration::from_secs(1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 7, now - Duration::from_secs(5));
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 0);
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert_eq!(track.round_boundary(), Some(10));
}

#[tokio::test]
async fn test_pre_dispatch_failure_requeues_track_behind_overdue_candidates() {
    let manager = build_test_manager().await;
    let failing = SinkId::new(480);
    let healthy = SinkId::new(481);
    let now = Instant::now();
    // The failing track is the most overdue, so it wins the only slot first.
    let mut failing_track = new_round_track(now, 120, 10, 1);
    failing_track.record_observed_snapshot(committed_snapshot(10, 1000));
    failing_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(100),
        manual_task_type: None,
    };
    let mut healthy_track = new_track(now, 120, 10, 1);
    healthy_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(50),
        manual_task_type: None,
    };
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(failing, failing_track);
        guard.sink_schedules.insert(healthy, healthy_track);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].sink_id, failing);
    // Dropping the handle simulates a pre-dispatch failure.
    drop(handles);

    let guard = manager.inner.read();
    assert_eq!(
        guard
            .sink_schedules
            .get(&failing)
            .and_then(|track| track.round_boundary()),
        Some(10)
    );
    drop(guard);

    // The failing track is re-queued at the revert time, so the still-overdue
    // healthy track wins the next slot instead of being starved.
    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].sink_id, healthy);
}

fn pk_index_report(status: IcebergReportTaskStatus) -> IcebergReportTask {
    use risingwave_connector::sink::iceberg::IcebergCommitResult;
    use risingwave_pb::connector_service::SinkMetadata;
    use risingwave_pb::iceberg_compaction::PkIndexCompactionResult;

    IcebergReportTask {
        task_id: 9.into(),
        sink_id: SinkId::new(42),
        status: status as i32,
        error_message: None,
        pk_index_result: Some(PkIndexCompactionResult {
            output_files: Some(
                SinkMetadata::try_from(&IcebergCommitResult {
                    schema_id: 3,
                    partition_spec_id: 4,
                    data_files: vec![
                        serde_json::from_value(serde_json::json!({
                            "content": 0,
                            "file_path": "s3://bucket/output.parquet",
                            "file_format": "PARQUET",
                            "partition": {},
                            "record_count": 1,
                            "file_size_in_bytes": 1
                        }))
                        .unwrap(),
                    ],
                })
                .unwrap(),
            ),
            input_file_paths: vec!["s3://bucket/input.parquet".to_owned()],
            read_snapshot_id: 10,
        }),
    }
}

fn pk_index_track(now: Instant, bounded: bool) -> CompactionTrack {
    let mut track = new_round_track(now, 120, 10, 2);
    track.config.pk_index_coordinated = true;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    if !bounded {
        track.record_force_compaction(now, Some(TaskType::Full));
    }
    start_in_flight(&mut track, 9, now);
    track
}

#[test]
fn test_pk_index_commit_events_ignore_duplicates_and_maintenance() {
    let now = Instant::now();
    let mut track = new_round_track(now, 60, 2, 0);
    track.config.pk_index_coordinated = true;
    commit_update(10, 1000).apply_to_track(&mut track, now);
    commit_update(10, 1000).apply_to_track(&mut track, now);
    commit_update(9, 900).apply_to_track(&mut track, now);
    assert_eq!(track.demand.commit_count, 1);
    assert_eq!(
        track.latest_observed_snapshot.as_ref().unwrap().snapshot_id,
        10
    );
    SinkUpdateKind::Maintenance {
        observed_snapshot: committed_snapshot(11, 1100),
    }
    .apply_to_track(&mut track, now);
    assert_eq!(track.demand.commit_count, 1);
    assert_eq!(
        track.latest_observed_snapshot.as_ref().unwrap().snapshot_id,
        11
    );
    assert!(!track.should_trigger(now));
    commit_update(12, 1200).apply_to_track(&mut track, now);
    assert_eq!(track.demand.commit_count, 2);
    assert!(track.should_trigger(now));
}

#[test]
fn test_pk_index_retry_gate_cannot_be_bypassed_by_commits_or_force() {
    let now = Instant::now();
    let mut track = pk_index_track(now, false);
    track.config.trigger_snapshot_count = 1;
    track.finish_failed(now, "boom".to_owned());
    record_commits(&mut track, 10);
    track.record_force_compaction(now, None);
    assert!(!track.should_trigger(now));
    let first_retry = track.retry.not_before.unwrap();
    assert!(
        (Duration::from_secs(1)..=Duration::from_millis(1250))
            .contains(&first_retry.duration_since(now))
    );
    assert!(track.should_trigger(first_retry));
    start_in_flight(&mut track, 10, first_retry);
    track.finish_failed(first_retry, "boom".to_owned());
    assert!(!track.should_trigger(first_retry + Duration::from_secs(1)));
    let second_retry = track.retry.not_before.unwrap();
    assert!(
        (Duration::from_secs(2)..=Duration::from_millis(2500))
            .contains(&second_retry.duration_since(first_retry))
    );
    assert!(track.should_trigger(second_retry));
}

#[tokio::test]
async fn test_pk_index_selection_uses_worker_pull_count_without_global_quota() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let sink_count = 12;
    {
        let mut guard = manager.inner.write();
        for id in 0..sink_count {
            let mut track = new_round_track(now, 120, 1, 1);
            track.config.pk_index_coordinated = true;
            track.record_observed_snapshot(committed_snapshot(10, 1000));
            guard.sink_schedules.insert(SinkId::new(id as u32), track);
        }
        let legacy = new_track(now, 120, 1, 1);
        guard.sink_schedules.insert(SinkId::new(100), legacy);
    }
    let handles = manager.get_top_n_iceberg_commit_sink_ids(4);
    assert_eq!(handles.len(), 4);
    assert_eq!(
        handles
            .iter()
            .filter(|h| h.attempt.pk_index_coordinated)
            .count(),
        4
    );
    {
        let mut guard = manager.inner.write();
        guard
            .sink_schedules
            .get_mut(&SinkId::new(1))
            .unwrap()
            .mark_dispatched(9.into(), 1.into(), now);
        for id in [2, 3] {
            let track = guard.sink_schedules.get_mut(&SinkId::new(id)).unwrap();
            let CompactionTrackState::PendingDispatch { attempt } = &track.state else {
                unreachable!()
            };
            track.state = CompactionTrackState::Applying {
                task_id: u64::from(id).into(),
                attempt: attempt.clone(),
            };
        }
    }
    manager.mark_apply_unknown(
        IcebergReportTask {
            task_id: 3.into(),
            sink_id: SinkId::new(3),
            ..Default::default()
        },
        anyhow!("lost collection").into(),
    );
    // Pending, InFlight, Applying, and NeedsRecovery only exclude their own sinks.
    let remaining = manager.get_top_n_iceberg_commit_sink_ids(sink_count + 1);
    assert_eq!(remaining.len(), sink_count - 4 + 1);
    assert_eq!(
        remaining
            .iter()
            .filter(|h| h.attempt.pk_index_coordinated)
            .count(),
        sink_count - 4
    );
    assert!(remaining.iter().all(|h| h.sink_id.as_raw_id() >= 4));
    assert!(manager.get_top_n_iceberg_commit_sink_ids(1).is_empty());
}

#[tokio::test]
async fn test_pk_index_reports_start_applying_independently() {
    let manager = build_test_manager().await;
    let mut waiters = Vec::new();
    for id in [42, 43] {
        let sink_id = SinkId::new(id);
        let (tx, rx) = oneshot::channel();
        let mut guard = manager.inner.write();
        guard
            .sink_schedules
            .insert(sink_id, pk_index_track(Instant::now(), false));
        guard.manual_compaction_waiters.insert(sink_id, tx);
        waiters.push(rx);
        drop(guard);
        let mut report = pk_index_report(IcebergReportTaskStatus::Success);
        report.sink_id = sink_id;
        manager.handle_report_task(report);
    }
    let statuses = manager.list_compaction_statuses();
    assert_eq!(statuses.len(), 2);
    assert!(
        statuses
            .iter()
            .all(|status| status.schedule_state == "applying")
    );
    // Both handoffs run independently and fail on the missing catalog sinks.
    for rx in waiters {
        let error = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to apply pk-index compaction")
        );
    }
}

#[tokio::test]
async fn test_pk_index_attempts_enable_coordinated_dispatch() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    for manual in [false, true] {
        let mut track = manager.create_compaction_track(&config, now);
        track.record_observed_snapshot(committed_snapshot(10, 1000));
        if manual {
            track.record_force_compaction(now, Some(TaskType::Full));
        }
        let attempt = track.start_attempt();
        assert!(attempt.pk_index_coordinated);
        assert_eq!(attempt.max_file_sequence_number, (!manual).then_some(10));
        assert_eq!(
            attempt.policy.task_type,
            if manual {
                TaskType::Full
            } else {
                TaskType::Auto
            }
        );
    }
}

#[tokio::test]
async fn test_pk_index_apply_holds_waiter_and_gc_watermark() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let now = Instant::now();
    let (tx, mut rx) = oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard
            .sink_schedules
            .insert(sink_id, pk_index_track(now, false));
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }
    let report = pk_index_report(IcebergReportTaskStatus::Success);
    let apply = manager.prepare_report_task(report.clone()).unwrap();
    assert_eq!(apply.overwrite.output_result.schema_id, 3);
    assert_eq!(apply.overwrite.output_result.partition_spec_id, 4);
    assert_eq!(apply.overwrite.output_result.data_files.len(), 1);
    assert_eq!(apply.overwrite.read_snapshot_id, 10);
    assert_eq!(
        apply.overwrite.input_file_paths,
        vec!["s3://bucket/input.parquet"]
    );

    // Duplicate success and failure reports cannot finish the accepted result.
    manager.handle_report_task(report);
    manager.handle_report_task(pk_index_report(IcebergReportTaskStatus::Failed));
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    {
        let mut guard = manager.inner.write();
        assert!(
            IcebergCompactionManager::finish_timed_out_compaction_tasks(
                &mut guard,
                now + Duration::from_secs(3600),
            )
            .is_empty()
        );
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        track.record_observed_snapshot(committed_snapshot(11, 2000));
        track.record_commit();
        assert!(!track.should_trigger(now + Duration::from_secs(3600)));
        assert_eq!(
            track
                .active_attempt_gc_watermark_snapshot()
                .flatten()
                .unwrap()
                .snapshot_id,
            10
        );
        // The compactor has already finished; clearing need not send a cancel.
        assert!(track.scheduled_task().is_none());
    }
    assert_eq!(
        manager.list_compaction_statuses()[0].schedule_state,
        "applying"
    );
    assert!(manager.get_top_n_iceberg_commit_sink_ids(1).is_empty());

    manager.finish_report_task(apply.report, true);
    assert_eq!(rx.await.unwrap().unwrap(), IcebergCompactionTaskId::new(9));
    let guard = manager.inner.read();
    let track = &guard.sink_schedules[&sink_id];
    assert_eq!(track.demand.commit_count, 1);
    assert!(track.active_attempt_gc_watermark_snapshot().is_none());
}

#[tokio::test]
async fn test_pk_index_bounded_result_finishes_only_after_apply() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let now = Instant::now();
    for (reported_status, apply_failed) in [
        (IcebergReportTaskStatus::Success, false),
        (IcebergReportTaskStatus::Drained, false),
        (IcebergReportTaskStatus::Drained, true),
    ] {
        manager
            .inner
            .write()
            .sink_schedules
            .insert(sink_id, pk_index_track(now, true));
        let mut report = pk_index_report(reported_status);
        report.pk_index_result.as_mut().unwrap().input_file_paths =
            vec!["group-a.parquet".to_owned(), "group-b.parquet".to_owned()];
        let mut apply = manager.prepare_report_task(report).unwrap();
        assert_eq!(
            apply.overwrite.input_file_paths,
            ["group-a.parquet", "group-b.parquet"]
        );
        {
            let mut guard = manager.inner.write();
            let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
            assert_eq!(track.round_boundary(), Some(10));
            track.record_commit();
        }
        if apply_failed {
            apply.report.status = IcebergReportTaskStatus::Failed as i32;
        }
        manager.finish_report_task(apply.report, true);
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert_eq!(track.round_boundary(), Some(10));
        assert_eq!(track.demand.commit_count, 1);
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
        assert_eq!(track.should_trigger(Instant::now()), !apply_failed);
    }
}

#[tokio::test]
async fn test_pk_index_rejects_invalid_report_payloads() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    for case in 0..6 {
        let mut track = pk_index_track(Instant::now(), false);
        let mut report = pk_index_report(IcebergReportTaskStatus::Success);
        let payload = report.pk_index_result.as_mut().unwrap();
        match case {
            0 => payload.output_files = None,
            1 => payload.output_files.as_mut().unwrap().metadata = None,
            2 => payload.input_file_paths.clear(),
            3 => payload.read_snapshot_id = 0,
            4 => {
                // Rebuild the attempt with the uncoordinated flag.
                track = new_track(Instant::now(), 120, 10, 2);
                start_in_flight(&mut track, 9, Instant::now());
            }
            5 => report.status = IcebergReportTaskStatus::Drained as i32,
            _ => unreachable!(),
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = manager.inner.write();
            guard.sink_schedules.insert(sink_id, track);
            guard.manual_compaction_waiters.insert(sink_id, tx);
        }
        assert!(manager.prepare_report_task(report).is_none());
        let error = rx.await.unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid pk-index compaction result")
        );
        assert!(matches!(
            manager.inner.read().sink_schedules[&sink_id].state,
            CompactionTrackState::Idle { .. }
        ));
    }
}

#[tokio::test]
async fn test_pk_index_no_plan_reports_need_no_barrier() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    for bounded in [false, true] {
        manager
            .inner
            .write()
            .sink_schedules
            .insert(sink_id, pk_index_track(Instant::now(), bounded));
        let mut report = pk_index_report(if bounded {
            IcebergReportTaskStatus::Drained
        } else {
            IcebergReportTaskStatus::Success
        });
        report.pk_index_result = None;
        assert!(manager.prepare_report_task(report).is_none());
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert_eq!(track.round_boundary(), None);
        assert_eq!(track.demand.commit_count, 0);
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    }
}

#[tokio::test]
async fn test_pk_index_empty_manual_full_waits_for_observed_snapshot() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    let mut track = manager.create_compaction_track(&config, now);
    assert!(track.latest_observed_snapshot.is_none());
    SinkUpdateKind::ManualForceCompaction {
        task_type: TaskType::Full,
    }
    .apply_to_track(&mut track, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.policy.task_type, TaskType::Full);
    assert_eq!(handles[0].attempt.max_file_sequence_number, None);
    manager
        .inner
        .write()
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .mark_dispatched(9.into(), 1.into(), now);
    drop(handles);

    let mut report = pk_index_report(IcebergReportTaskStatus::Success);
    report.pk_index_result = None;
    manager.handle_report_task(report);
    {
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert_eq!(track.demand.commit_count, 0);
        assert!(track.demand.reconciliation_pending);
        assert!(track.latest_observed_snapshot.is_none());
    }
    assert!(manager.get_top_n_iceberg_commit_sink_ids(1).is_empty());
    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        assert!(!track.should_trigger(now + Duration::from_secs(3600)));
        commit_update(10, 1000).apply_to_track(track, now);
    }
    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.policy.task_type, TaskType::Auto);
    assert_eq!(handles[0].attempt.max_file_sequence_number, Some(10));
}

#[tokio::test]
async fn test_pk_index_apply_failure_notifies_manual_waiter() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut track = pk_index_track(Instant::now(), false);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    let (tx, rx) = oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }
    // The missing catalog sink causes the asynchronous application to fail.
    manager.handle_report_task(pk_index_report(IcebergReportTaskStatus::Success));
    let error = tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to apply pk-index compaction")
    );
    assert!(!manager.inner.read().sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_pk_index_stale_apply_completion_preserves_replacement() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, pk_index_track(Instant::now(), false));
    let apply = manager
        .prepare_report_task(pk_index_report(IcebergReportTaskStatus::Success))
        .unwrap();
    manager.clear_iceberg_maintenance_by_sink_id(sink_id);
    let mut replacement = new_track(Instant::now(), 120, 10, 2);
    start_in_flight(&mut replacement, 10, Instant::now());
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, replacement);
    manager.finish_report_task(apply.report, true);
    assert!(manager.inner.read().sink_schedules[&sink_id].is_in_flight_task(10.into()));
}

#[tokio::test]
async fn test_pk_index_all_deleted_input_still_requires_apply() {
    use risingwave_connector::sink::iceberg::IcebergCommitResult;
    use risingwave_pb::connector_service::SinkMetadata;

    let manager = build_test_manager().await;
    manager
        .inner
        .write()
        .sink_schedules
        .insert(SinkId::new(42), pk_index_track(Instant::now(), true));
    let mut report = pk_index_report(IcebergReportTaskStatus::Drained);
    report.pk_index_result.as_mut().unwrap().output_files =
        Some(SinkMetadata::try_from(&IcebergCommitResult::default()).unwrap());
    let apply = manager.prepare_report_task(report).unwrap();
    assert!(apply.overwrite.output_result.data_files.is_empty());
    assert!(!apply.overwrite.input_file_paths.is_empty());
    assert!(matches!(
        manager.inner.read().sink_schedules[&SinkId::new(42)].state,
        CompactionTrackState::Applying { .. }
    ));
}

#[tokio::test]
async fn test_stale_dispatch_handle_cannot_send_or_revert_replacement() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.record_force_compaction(now, Some(TaskType::Full));
    manager.inner.write().sink_schedules.insert(sink_id, track);
    let mut stale_handle = manager.get_top_n_iceberg_commit_sink_ids(1).pop().unwrap();
    manager.clear_iceberg_maintenance_by_sink_id(sink_id);
    let mut replacement = new_track(now, 120, 10, 1);
    replacement.start_attempt();
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, replacement);
    let mut events = manager.iceberg_compactor_manager.add_compactor(1.into());
    let compactor = manager
        .iceberg_compactor_manager
        .get_compactor(1.into())
        .unwrap();
    stale_handle
        .try_dispatch_task(
            &compactor,
            IcebergCompactionTask {
                task_id: 9.into(),
                sink_id: sink_id.as_raw_id(),
                ..Default::default()
            },
        )
        .unwrap();
    drop(stale_handle);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        manager.inner.read().sink_schedules[&sink_id].state,
        CompactionTrackState::PendingDispatch { .. }
    ));
}

#[tokio::test]
async fn test_disabling_pending_compaction_allows_manual_full_after_dispatch_failure() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(10, 1000)));
    let mut handle = manager.get_top_n_iceberg_commit_sink_ids(1).pop().unwrap();

    config.enable_compaction = false;
    manager.on_sink_config_changed(sink_id, config);
    assert!(
        manager.inner.read().sink_schedules[&sink_id]
            .round
            .is_some()
    );

    let events = manager.iceberg_compactor_manager.add_compactor(1.into());
    let compactor = manager
        .iceberg_compactor_manager
        .get_compactor(1.into())
        .unwrap();
    drop(events);
    assert!(
        handle
            .try_dispatch_task(
                &compactor,
                IcebergCompactionTask {
                    task_id: 9.into(),
                    sink_id: sink_id.as_raw_id(),
                    ..Default::default()
                },
            )
            .is_err()
    );
    drop(handle);

    {
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
        assert!(track.round.is_none());
        assert!(track.demand.reconciliation_pending);
        assert!(!track.should_trigger(Instant::now() + Duration::from_secs(3600)));
    }
    let _waiter = manager.start_manual_compaction(sink_id).await.unwrap();
    assert!(matches!(
        manager.inner.read().sink_schedules[&sink_id].state,
        CompactionTrackState::Idle {
            manual_task_type: Some(TaskType::Full),
            ..
        }
    ));
}

#[tokio::test]
async fn test_disabling_compaction_retains_apply_until_completion() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, pk_index_track(Instant::now(), false));
    let apply = manager
        .prepare_report_task(pk_index_report(IcebergReportTaskStatus::Success))
        .unwrap();
    let error = manager.start_manual_compaction(sink_id).await.unwrap_err();
    assert!(error.to_string().contains("already applying"));
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_compaction = false;
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    {
        let mut guard = manager.inner.write();
        assert!(manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: commit_update(11, 2000),
                now: Instant::now(),
                allow_track_initialization: false,
                recovery_generation: 0,
                loaded_config: Some(config),
            },
        ));
        let track = &guard.sink_schedules[&sink_id];
        assert!(!track.config.automatic_enabled);
        assert!(!track.removes_track_after_finish());
        assert!(track.active_attempt_gc_watermark_snapshot().is_some());
    }
    manager.finish_report_task(apply.report, true);
    let guard = manager.inner.read();
    let track = &guard.sink_schedules[&sink_id];
    assert_eq!(track.demand.commit_count, 1);
    assert!(track.demand.reconciliation_pending);
    assert!(!track.should_trigger(Instant::now() + Duration::from_secs(3600)));
}

#[tokio::test]
async fn test_pk_index_batch_refreshes_policy_and_reconciles_changes() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    let mut track = manager.create_compaction_track(&config, now);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    let first = track.start_attempt();
    track.mark_dispatched(9.into(), 1.into(), now);
    let mut updated = config;
    updated.compaction_type = Some(CompactionType::SmallFiles);
    updated.delete_ratio_percent = Some(50);
    manager.refresh_schedule_config(&mut track, &updated, now);
    assert!(track.demand.reconciliation_pending);
    track.finish_success(now);
    let second = track.start_attempt();
    assert_eq!(first.policy.task_type, TaskType::Auto);
    assert_eq!(second.policy.task_type, TaskType::SmallFiles);
    assert_eq!(second.policy.delete_ratio_percent, 50);
    assert!(!track.demand.reconciliation_pending);
    track.mark_dispatched(10.into(), 1.into(), now);
    track.finish_drained(now);
    let next = track.start_attempt();
    assert_eq!(next.policy.task_type, TaskType::SmallFiles);
    assert_eq!(next.policy.delete_ratio_percent, 50);
}

#[test]
fn test_pk_index_batches_include_new_writes_without_changing_active_attempt() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.config.pk_index_coordinated = true;
    track.config.policy.task_type = TaskType::Auto;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    let first = track.start_attempt();
    track.mark_dispatched(9.into(), 1.into(), now);
    commit_update(20, 2000).apply_to_track(&mut track, now);
    assert_eq!(first.max_file_sequence_number, Some(10));
    assert_eq!(track.round_boundary(), Some(10));
    assert!(!track.should_trigger(now));
    track.finish_success(now);
    assert!(track.should_trigger(now));
    let second = track.start_attempt();
    assert_eq!(second.max_file_sequence_number, Some(20));
    assert_eq!(
        second.gc_watermark_snapshot.as_ref().unwrap().snapshot_id,
        20
    );
    assert_eq!(track.round_boundary(), Some(20));
    assert_eq!(track.demand.commit_count, 0);
    track.mark_dispatched(10.into(), 1.into(), now);
    commit_update(30, 3000).apply_to_track(&mut track, now);
    track.finish_drained(now);
    assert_eq!(track.round_boundary(), None);
    assert_eq!(track.demand.commit_count, 1);
}

#[test]
fn test_pk_index_maintenance_does_not_reselect_rewrite_outputs() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.config.pk_index_coordinated = true;
    track.config.policy.task_type = TaskType::Auto;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 9, now);
    SinkUpdateKind::Maintenance {
        observed_snapshot: committed_snapshot(20, 2000),
    }
    .apply_to_track(&mut track, now);
    assert_eq!(track.demand.commit_count, 0);
    track.finish_success(now);
    let next = track.start_attempt();
    assert_eq!(next.max_file_sequence_number, Some(10));
    assert_eq!(next.gc_watermark_snapshot.as_ref().unwrap().snapshot_id, 20);
}

#[test]
fn test_pk_index_full_keeps_scope_until_drained() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.config.pk_index_coordinated = true;
    track.config.policy.task_type = TaskType::Full;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    let first = track.start_attempt();
    track.mark_dispatched(9.into(), 1.into(), now);
    commit_update(20, 2000).apply_to_track(&mut track, now);
    track.config.policy.task_type = TaskType::SmallFiles;
    track.finish_success(now);
    let second = track.start_attempt();
    assert_eq!(second.max_file_sequence_number, Some(10));
    assert_eq!(second.policy, first.policy);
    assert_eq!(track.demand.commit_count, 1);
    track.mark_dispatched(10.into(), 1.into(), now);
    track.finish_drained(now);
    let next = track.start_attempt();
    assert_eq!(next.max_file_sequence_number, Some(20));
    assert_eq!(next.policy.task_type, TaskType::SmallFiles);
}

#[tokio::test]
async fn test_pk_index_blocked_is_not_failure_or_drained() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    for status in [
        IcebergReportTaskStatus::BlockedCapacity,
        IcebergReportTaskStatus::BlockedStatistics,
        IcebergReportTaskStatus::Busy,
    ] {
        manager
            .inner
            .write()
            .sink_schedules
            .insert(sink_id, pk_index_track(Instant::now(), true));
        let mut report = pk_index_report(status);
        report.pk_index_result = None;
        manager.handle_report_task(report.clone());
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        assert_eq!(track.round_boundary(), Some(10));
        assert_eq!(track.retry.consecutive_failures, 0);
        let gate = track.retry.not_before.unwrap();
        track.record_commit();
        track.record_force_compaction(Instant::now(), None);
        assert!(!track.should_trigger(gate - Duration::from_millis(1)));
        assert!(track.should_trigger(gate));
        if status == IcebergReportTaskStatus::Busy {
            assert!(track.retry.blocked.is_none());
        } else {
            assert_eq!(track.retry.blocked.as_ref().unwrap().reason, status);
        }
    }
}

#[tokio::test]
async fn test_pk_index_unknown_is_discarded_by_recovery_without_blocking_other_sinks() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, pk_index_track(Instant::now(), true));
    let apply = manager
        .prepare_report_task(pk_index_report(IcebergReportTaskStatus::Success))
        .unwrap();
    manager.mark_apply_unknown(apply.report.clone(), anyhow!("lost collection").into());
    {
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert!(matches!(
            track.state,
            CompactionTrackState::NeedsRecovery { .. }
        ));
        assert!(!track.should_trigger(Instant::now() + Duration::from_secs(3600)));
        assert_eq!(
            track
                .active_attempt_gc_watermark_snapshot()
                .flatten()
                .unwrap()
                .snapshot_id,
            10
        );
    }
    manager.on_recovery_start(Some(&[sink_id]));
    assert!(!manager.inner.read().sink_schedules.contains_key(&sink_id));
    assert!(manager.inner.read().is_recovering(sink_id));
    for id in [43, 44] {
        manager
            .inner
            .write()
            .sink_schedules
            .insert(SinkId::new(id), pk_index_track(Instant::now(), true));
        let mut report = pk_index_report(IcebergReportTaskStatus::Success);
        report.sink_id = id.into();
        let other = manager.prepare_report_task(report).unwrap();
        assert!(matches!(
            manager.inner.read().sink_schedules[&SinkId::new(id)].state,
            CompactionTrackState::Applying { .. }
        ));
        manager.finish_report_task(other.report, true);
    }
    manager.finish_report_task(apply.report.clone(), true);
    assert!(!manager.inner.read().sink_schedules.contains_key(&sink_id));
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(11, 2000)));
    manager.finish_report_task(apply.report, true);
    assert!(!manager.inner.read().is_recovering(sink_id));
    assert!(matches!(
        manager.inner.read().sink_schedules[&SinkId::new(42)].state,
        CompactionTrackState::Idle { .. }
    ));
}

#[tokio::test]
async fn test_pk_index_recovery_discards_each_attempt_phase() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut events = manager.iceberg_compactor_manager.add_compactor(1.into());
    for phase in ["idle", "pending", "in_flight", "applying", "needs_recovery"] {
        let mut track = pk_index_track(Instant::now(), true);
        let attempt = match &track.state {
            CompactionTrackState::InFlight { attempt, .. } => attempt.clone(),
            _ => unreachable!(),
        };
        track.state = match phase {
            "idle" => CompactionTrackState::Idle {
                next_compaction_time: Instant::now(),
                manual_task_type: Some(TaskType::Full),
            },
            "pending" => CompactionTrackState::PendingDispatch { attempt },
            "in_flight" => track.state,
            "applying" => CompactionTrackState::Applying {
                task_id: 9.into(),
                attempt,
            },
            "needs_recovery" => CompactionTrackState::NeedsRecovery {
                task_id: 9.into(),
                attempt,
            },
            _ => unreachable!(),
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = manager.inner.write();
            guard.sink_schedules.insert(sink_id, track);
            guard.manual_compaction_waiters.insert(sink_id, tx);
        }
        manager.on_recovery_start(Some(&[sink_id]));
        {
            let guard = manager.inner.read();
            assert!(guard.is_recovering(sink_id), "{phase}");
            assert!(!guard.sink_schedules.contains_key(&sink_id), "{phase}");
            assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
        }
        assert!(
            rx.await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("recovery")
        );
        if phase == "in_flight" {
            assert!(matches!(
                events.try_recv().unwrap().unwrap().event.unwrap(),
                IcebergResponseEvent::CancelCompactTask(_)
            ));
        } else {
            assert!(events.try_recv().is_err());
        }
        assert!(manager.get_top_n_iceberg_commit_sink_ids(10).is_empty());
        assert!(
            manager
                .start_manual_compaction(sink_id)
                .await
                .unwrap_err()
                .to_string()
                .contains("during recovery")
        );
        assert!(
            manager
                .prepare_report_task(pk_index_report(IcebergReportTaskStatus::Success))
                .is_none()
        );
    }
}

#[tokio::test]
async fn test_pk_index_recovery_scope_preserves_other_sinks_and_legacy_compaction() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let first = SinkId::new(42);
    let second = SinkId::new(43);
    let legacy = SinkId::new(44);
    {
        let mut guard = manager.inner.write();
        guard
            .sink_schedules
            .insert(first, pk_index_track(now, true));
        guard
            .sink_schedules
            .insert(second, pk_index_track(now, true));
        guard
            .sink_schedules
            .insert(legacy, new_track(now, 120, 10, 1));
    }
    manager.on_recovery_start(Some(&[first, legacy]));
    let generation = {
        let guard = manager.inner.read();
        assert!(guard.is_recovering(first));
        assert!(!guard.is_recovering(second));
        assert!(!guard.is_recovering(legacy));
        assert!(guard.sink_schedules.contains_key(&second));
        assert!(guard.sink_schedules.contains_key(&legacy));
        guard.recovery_generation(first)
    };
    manager.on_recovery_start(None);
    let guard = manager.inner.read();
    assert!(guard.is_recovering(first));
    assert!(guard.recovery_generation(first) > generation);
    assert!(guard.is_recovering(second));
    assert!(!guard.is_recovering(legacy));
    assert_eq!(guard.sink_schedules.len(), 1);
    assert!(guard.sink_schedules.contains_key(&legacy));
}

#[tokio::test]
async fn test_pk_index_ready_registration_reconciles_existing_snapshot_without_new_commits() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(3600, 10, CompactionType::Full);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;

    assert!(manager.get_top_n_iceberg_commit_sink_ids(1).is_empty());
    // CREATE's post-collect phase publishes scheduling only after the job is ready.
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(10, 1000)));
    {
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert_eq!(track.demand.commit_count, 0);
        assert!(track.demand.reconciliation_pending);
        assert!(track.should_trigger(Instant::now()));
    }
    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.max_file_sequence_number, Some(10));
    assert_eq!(
        handles[0]
            .attempt
            .gc_watermark_snapshot
            .as_ref()
            .unwrap()
            .snapshot_id,
        10
    );
}

#[tokio::test]
async fn test_pk_index_empty_registration_waits_for_commit_threshold() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(120, 2, CompactionType::Full);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    manager.on_sink_registered(sink_id, &config, None);

    let first_commit_at = Instant::now();
    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        commit_update(10, 1000).apply_to_track(track, first_commit_at);
        assert_eq!(track.demand.commit_count, 1);
        assert!(!track.should_trigger(first_commit_at));
        assert!(!track.should_trigger(first_commit_at + Duration::from_secs(119)));
        assert!(track.should_trigger(first_commit_at + Duration::from_secs(120)));
    }
    assert!(manager.get_top_n_iceberg_commit_sink_ids(1).is_empty());

    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        commit_update(11, 2000).apply_to_track(track, Instant::now());
    }
    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.policy.task_type, TaskType::Full);
    assert_eq!(handles[0].attempt.max_file_sequence_number, Some(11));
}

#[tokio::test]
async fn test_pk_index_config_change_refreshes_idle_sink_without_a_commit() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(3600, 10, CompactionType::Full);
    config.enable_pk_index = true;
    config.enable_compaction = false;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    config.enable_snapshot_expiration = false;
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(10, 1000)));
    // Save an older config load that completes after ALTER.
    let stale_update = PreparedSinkUpdate {
        sink_id,
        kind: commit_update(11, 2000),
        now: Instant::now(),
        allow_track_initialization: false,
        recovery_generation: manager.inner.read().recovery_generation(sink_id),
        loaded_config: Some(config.clone()),
    };
    config.enable_compaction = true;
    config.trigger_snapshot_count = Some(2);
    config.compaction_interval_sec = Some(120);
    config.enable_snapshot_expiration = true;
    config.enable_manifest_rewrite = true;
    manager.on_sink_config_changed(sink_id, config.clone());
    {
        let mut guard = manager.inner.write();
        assert!(manager.apply_sink_update(&mut guard, stale_update));
        let track = &guard.sink_schedules[&sink_id];
        assert!(track.config.automatic_enabled);
        assert_eq!(track.config.trigger_snapshot_count, 2);
        assert_eq!(track.config.trigger_interval_sec, 120);
        assert_eq!(track.demand.commit_count, 1);
        assert_eq!(
            track.latest_observed_snapshot.as_ref().unwrap().snapshot_id,
            11
        );
        assert!(track.should_trigger(Instant::now()));
        assert!(guard.snapshot_expiration_sink_ids.contains(&sink_id));
        assert!(guard.manifest_rewrite_sink_ids.contains(&sink_id));
    }
    config.enable_compaction = false;
    config.enable_snapshot_expiration = false;
    config.enable_manifest_rewrite = false;
    manager.on_sink_config_changed(sink_id, config);
    let guard = manager.inner.read();
    let track = &guard.sink_schedules[&sink_id];
    assert!(!track.should_trigger(Instant::now() + Duration::from_secs(7200)));
    assert_eq!(track.demand.commit_count, 1);
    assert!(!guard.snapshot_expiration_sink_ids.contains(&sink_id));
    assert!(!guard.manifest_rewrite_sink_ids.contains(&sink_id));
}

#[tokio::test]
async fn test_pk_index_config_change_preserves_empty_registration_and_recovery() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(3600, 2, CompactionType::Full);
    config.enable_pk_index = true;
    config.enable_compaction = false;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    manager.on_sink_registered(sink_id, &config, None);
    config.enable_compaction = true;
    manager.on_sink_config_changed(sink_id, config.clone());
    {
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert!(track.config.automatic_enabled);
        assert!(!track.demand.reconciliation_pending);
        assert_eq!(track.demand.commit_count, 0);
        assert!(!track.should_trigger(Instant::now() + Duration::from_secs(7200)));
    }
    manager.on_recovery_start(Some(&[sink_id]));
    manager.on_sink_config_changed(sink_id, config);
    let guard = manager.inner.read();
    assert!(guard.is_recovering(sink_id));
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_pk_index_config_change_keeps_active_attempt() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(3600, 2, CompactionType::Full);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    config.target_file_size_mb = Some(128);
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(10, 1000)));
    {
        let mut guard = manager.inner.write();
        start_in_flight(guard.sink_schedules.get_mut(&sink_id).unwrap(), 7, now);
    }
    config.enable_compaction = false;
    config.target_file_size_mb = Some(64);
    manager.on_sink_config_changed(sink_id, config);
    let guard = manager.inner.read();
    let track = &guard.sink_schedules[&sink_id];
    assert!(!track.config.automatic_enabled);
    assert_eq!(track.config.policy.target_file_size_mb, 64);
    assert_eq!(track.round_boundary(), Some(10));
    let CompactionTrackState::InFlight {
        task_id, attempt, ..
    } = &track.state
    else {
        panic!("configuration change must retain the active attempt");
    };
    assert_eq!(task_id.as_raw_id(), 7);
    assert_eq!(attempt.policy.target_file_size_mb, 128);
}

#[tokio::test]
async fn test_pk_index_registration_rebuilds_empty_and_recovered_tables() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    config.enable_snapshot_expiration = true;
    config.enable_manifest_rewrite = true;
    manager.on_sink_registered(sink_id, &config, None);
    manager.on_recovery_start(Some(&[sink_id]));
    manager.on_sink_registered(sink_id, &config, None);
    {
        let guard = manager.inner.read();
        assert!(!guard.is_recovering(sink_id));
        assert!(guard.snapshot_expiration_sink_ids.contains(&sink_id));
        assert!(guard.manifest_rewrite_sink_ids.contains(&sink_id));
        let track = &guard.sink_schedules[&sink_id];
        assert!(track.latest_observed_snapshot.is_none());
        assert!(!track.demand.reconciliation_pending);
        assert!(!track.should_trigger(Instant::now() + Duration::from_secs(3600)));
    }
    manager.on_recovery_start(Some(&[sink_id]));
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(20, 2000)));
    {
        let guard = manager.inner.read();
        let track = &guard.sink_schedules[&sink_id];
        assert!(!guard.is_recovering(sink_id));
        assert!(track.round.is_none());
        assert_eq!(track.demand.commit_count, 0);
        assert!(track.demand.reconciliation_pending);
        assert!(track.should_trigger(Instant::now()));
    }
    // An old commit queued before recovery cannot create fresh demand or replace the settled head.
    manager
        .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction: false,
            has_new_data: true,
            observed_snapshot: committed_snapshot(10, 1000),
        })
        .await;
    let guard = manager.inner.read();
    let track = &guard.sink_schedules[&sink_id];
    assert_eq!(
        track.latest_observed_snapshot.as_ref().unwrap().snapshot_id,
        20
    );
    assert_eq!(track.demand.commit_count, 0);
}

#[tokio::test]
async fn test_pk_index_recovery_fences_prepared_updates_across_track_replacement() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.enable_pk_index = true;
    config.write_mode = IcebergWriteMode::MergeOnRead;
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(10, 1000)));
    let before_recovery = manager
        .prepare_sink_update(sink_id, commit_update(30, 3000), Instant::now())
        .await;
    let before_recovery_after = manager
        .prepare_sink_update(sink_id, commit_update(30, 3000), Instant::now())
        .await;
    manager.on_recovery_start(Some(&[sink_id]));
    let during_recovery = PreparedSinkUpdate {
        sink_id,
        kind: commit_update(30, 3000),
        now: Instant::now(),
        allow_track_initialization: true,
        recovery_generation: manager.inner.read().recovery_generation(sink_id),
        loaded_config: Some(config.clone()),
    };
    {
        let mut guard = manager.inner.write();
        assert!(!manager.apply_sink_update(&mut guard, before_recovery));
        assert!(!guard.sink_schedules.contains_key(&sink_id));
    }
    manager.on_sink_registered(sink_id, &config, Some(committed_snapshot(20, 2000)));
    let mut guard = manager.inner.write();
    assert!(!manager.apply_sink_update(&mut guard, before_recovery_after));
    assert!(!manager.apply_sink_update(&mut guard, during_recovery));
    let track = &guard.sink_schedules[&sink_id];
    assert_eq!(
        track.latest_observed_snapshot.as_ref().unwrap().snapshot_id,
        20
    );
    assert_eq!(track.demand.commit_count, 0);
}

#[tokio::test]
async fn test_obsolete_manual_cleanup_preserves_live_replacement_waiter() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);
    let (old_tx, old_rx) = oneshot::channel();
    manager
        .inner
        .write()
        .manual_compaction_waiters
        .insert(sink_id, old_tx);
    let (new_tx, new_rx) = oneshot::channel();
    manager
        .inner
        .write()
        .manual_compaction_waiters
        .insert(sink_id, new_tx);
    drop(old_rx);
    manager.cancel_closed_manual_compaction_waiter(sink_id);
    assert!(
        manager
            .inner
            .read()
            .manual_compaction_waiters
            .contains_key(&sink_id)
    );
    drop(new_rx);
    manager.cancel_closed_manual_compaction_waiter(sink_id);
    assert!(
        !manager
            .inner
            .read()
            .manual_compaction_waiters
            .contains_key(&sink_id)
    );
}

#[test]
fn test_pk_index_dirty_wait_is_not_postponed_by_new_commits() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 1000, 1);
    track.config.pk_index_coordinated = true;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    let first_dirty = track.demand.since.unwrap();
    track.record_commit();
    assert_eq!(track.demand.since, Some(first_dirty));
    assert!(!track.should_trigger(first_dirty + Duration::from_secs(119)));
    assert!(track.should_trigger(first_dirty + Duration::from_secs(120)));
}

#[test]
fn test_pk_index_new_round_minimum_gate_does_not_gate_continuation() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 1, 1);
    track.config.pk_index_coordinated = true;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 9, now);
    let gate = track.next_check_not_before.unwrap();
    track.finish_success(now);
    assert!(track.should_trigger(now));
    start_in_flight(&mut track, 10, now);
    track.record_commit();
    track.finish_drained(now);
    assert!(!track.should_trigger(gate - Duration::from_millis(1)));
    assert!(track.should_trigger(gate));
}

#[test]
fn test_pk_index_retry_delay_is_seedable_and_capped_with_bounded_jitter() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let mut replay = rand::rngs::StdRng::seed_from_u64(7);
    for failures in (0..32).chain(std::iter::once(u32::MAX)) {
        let delay = CompactionRetry::delay(failures, &mut rng);
        assert_eq!(delay, CompactionRetry::delay(failures, &mut replay));
        let base = Duration::from_secs(1_u64 << failures.min(6));
        assert!(delay >= base.min(Duration::from_secs(60)));
        assert!(delay <= (base + base / 4).min(Duration::from_secs(60)));
    }
}

#[tokio::test]
async fn test_compaction_selection_orders_pk_index_and_legacy_by_deadline() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    for (id, pk_index_coordinated, interval_sec) in [(3, true, 30), (2, true, 10), (1, false, 10)] {
        let mut track = new_round_track(now, interval_sec, 1, 1);
        track.config.pk_index_coordinated = pk_index_coordinated;
        track.record_observed_snapshot(committed_snapshot(10, 1000));
        manager
            .inner
            .write()
            .sink_schedules
            .insert(SinkId::new(id), track);
    }
    let first = manager.get_top_n_iceberg_commit_sink_ids(2);
    assert_eq!(
        first
            .iter()
            .map(|handle| handle.sink_id)
            .collect::<Vec<_>>(),
        vec![SinkId::new(1), SinkId::new(2)]
    );
    let next = manager.get_top_n_iceberg_commit_sink_ids(2);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].sink_id, SinkId::new(3));
}

#[test]
fn test_compaction_policy_serializes_selection_properties_at_dispatch() {
    let policy = CompactionPolicy {
        task_type: TaskType::Auto,
        small_files_threshold_mb: 32,
        target_file_size_mb: 128,
        delete_ratio_percent: 50,
    };
    assert_eq!(
        policy.properties(),
        BTreeMap::from([
            (
                "compaction.small_files_threshold_mb".to_owned(),
                "32".to_owned()
            ),
            (
                "compaction.target_file_size_mb".to_owned(),
                "128".to_owned()
            ),
            (
                "compaction.delete_ratio_percent".to_owned(),
                "50".to_owned()
            ),
        ])
    );
}

#[tokio::test]
async fn test_legacy_round_keeps_boundary_but_does_not_freeze_policy() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let mut config = new_test_iceberg_config(120, 10, CompactionType::Auto);
    config.write_mode = IcebergWriteMode::MergeOnRead;
    let mut track = manager.create_compaction_track(&config, now);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    let first = track.start_attempt();
    assert!(track.round.as_ref().unwrap().policy.is_none());
    track.mark_dispatched(9.into(), 1.into(), now);
    config.compaction_type = Some(CompactionType::SmallFiles);
    config.delete_ratio_percent = Some(50);
    manager.refresh_schedule_config(&mut track, &config, now);
    track.finish_success(now);
    let second = track.start_attempt();
    assert_eq!(
        second.max_file_sequence_number,
        first.max_file_sequence_number
    );
    assert_eq!(second.policy, track.config.policy);
    assert_eq!(second.policy.task_type, TaskType::SmallFiles);
    assert_eq!(second.policy.delete_ratio_percent, 50);
    assert_ne!(second.policy, first.policy);
}
