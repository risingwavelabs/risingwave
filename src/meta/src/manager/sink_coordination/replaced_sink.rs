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

//! Committing what an exactly-once sink left behind when `REPLACE SINK` removed it.
//!
//! An exactly-once sink commits an epoch to the external system only after Hummock has committed
//! that epoch. When `REPLACE SINK` stops the old sink at a barrier, the last epochs it pre-committed
//! are therefore still waiting for their external commit. The barrier's post-collect step moves
//! those epochs from `pending_sink_state` to `replaced_sink_pending_state` in the transaction that
//! deletes the old sink, and does not wait for them, so checkpoints keep completing.
//!
//! This module commits the moved epochs in the background, and keeps the replacement sink from
//! committing to the external system until they are done: a commit of the replacement sink that
//! lands first would, for an upsert sink, let the old sink's later deletes remove newer rows.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_connector::sink::SinkCommitCoordinator;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_pb::catalog::PbSink;
use sea_orm::DatabaseConnection;
use thiserror_ext::AsReport;
use tokio::sync::Notify;
use tokio_retry::strategy::jitter;
use tracing::{error, info, warn};

use crate::manager::exactly_once_util::{
    delete_replaced_sink_pending_epoch, load_replaced_sink_pending_epochs,
};
use crate::manager::sink_coordination::coordinator_worker::{
    commit_epoch_to_external, run_future_with_periodic_fn,
};

/// Builds the commit coordinator of a replaced sink from the catalog it had when it was replaced.
pub(super) type BuildReplacedSinkCoordinatorFn =
    Arc<dyn Fn(PbSink) -> BoxFuture<'static, anyhow::Result<SinkCommitCoordinator>> + Send + Sync>;

/// Sinks that must not commit to the external system yet, because the sink they replaced still
/// has epochs to commit.
#[derive(Clone, Default)]
pub(super) struct ReplacedSinkFences {
    inner: Arc<ReplacedSinkFencesInner>,
}

#[derive(Default)]
struct ReplacedSinkFencesInner {
    /// Successor sink -> replaced sinks with uncommitted epochs.
    fenced: Mutex<HashMap<SinkId, HashSet<SinkId>>>,
    released: Notify,
}

impl ReplacedSinkFences {
    pub(super) fn fence(&self, successor_sink_id: SinkId, replaced_sink_id: SinkId) {
        self.inner
            .fenced
            .lock()
            .entry(successor_sink_id)
            .or_default()
            .insert(replaced_sink_id);
    }

    pub(super) fn release(&self, successor_sink_id: SinkId, replaced_sink_id: SinkId) {
        {
            let mut fenced = self.inner.fenced.lock();
            if let Some(replaced) = fenced.get_mut(&successor_sink_id) {
                replaced.remove(&replaced_sink_id);
                if replaced.is_empty() {
                    fenced.remove(&successor_sink_id);
                }
            }
        }
        self.inner.released.notify_waiters();
    }

    pub(super) fn is_fenced(&self, sink_id: SinkId) -> bool {
        self.inner.fenced.lock().contains_key(&sink_id)
    }

    /// Wait until every sink that `sink_id` replaced has committed its epochs. Returns at once
    /// for a sink that replaced nothing.
    pub(super) async fn wait_released(&self, sink_id: SinkId) {
        loop {
            // Register before checking, so a release between the check and the await is not lost.
            let released = self.inner.released.notified();
            if !self.is_fenced(sink_id) {
                return;
            }
            released.await;
        }
    }
}

/// Commit the epochs left by the replaced sink `sink_id` in ascending order, removing each from
/// `replaced_sink_pending_state` after its commit.
///
/// A failure is retried with backoff for as long as epochs remain: giving up would lose data the
/// old sink has already acknowledged. The function returns when no epoch is left, which is also
/// the case once the successor sink is dropped and its rows are deleted with it.
pub(super) async fn commit_replaced_sink_pending_epochs(
    db: DatabaseConnection,
    sink_id: SinkId,
    build_coordinator: BuildReplacedSinkCoordinatorFn,
) {
    let start_time = Instant::now();
    let mut backoff = new_retry_backoff();
    let mut total_committed = 0;
    loop {
        let mut committed = 0;
        let result = try_commit_replaced_sink_pending_epochs(
            &db,
            sink_id,
            &build_coordinator,
            &mut committed,
        )
        .await;
        total_committed += committed;
        match result {
            Ok(()) => {
                info!(
                    %sink_id,
                    committed_epochs = total_committed,
                    elapsed = ?start_time.elapsed(),
                    "finished committing the epochs left by a replaced sink"
                );
                return;
            }
            Err(e) => {
                if committed > 0 {
                    backoff = new_retry_backoff();
                }
                let delay = backoff.next().expect("infinite");
                error!(
                    %sink_id,
                    error = %e.as_report(),
                    elapsed = ?start_time.elapsed(),
                    retry_after = ?delay,
                    "failed to commit the epochs left by a replaced sink, its replacement sink \
                     cannot commit until they are committed"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn new_retry_backoff() -> impl Iterator<Item = Duration> {
    exponential_backoff(Duration::from_millis(10), 10, Duration::from_secs(60)).map(jitter)
}

async fn try_commit_replaced_sink_pending_epochs(
    db: &DatabaseConnection,
    sink_id: SinkId,
    build_coordinator: &BuildReplacedSinkCoordinatorFn,
    committed: &mut usize,
) -> anyhow::Result<()> {
    let Some((sink, epochs)) = load_replaced_sink_pending_epochs(db, sink_id).await? else {
        return Ok(());
    };
    let mut coordinator = build_coordinator(sink).await?;
    match &mut coordinator {
        SinkCommitCoordinator::SinglePhase(coordinator) => coordinator.init().await?,
        SinkCommitCoordinator::TwoPhase(coordinator) => coordinator.init().await?,
    }
    for pending in epochs {
        let epoch = pending.epoch;
        let commit_start_time = Instant::now();
        // The same epoch may already have been committed by the old coordinator, or by an earlier
        // attempt whose row removal failed. Coordinators are required to commit idempotently.
        run_future_with_periodic_fn(
            commit_epoch_to_external(
                &mut coordinator,
                sink_id,
                epoch,
                pending.metadata,
                pending.schema_change,
            ),
            Duration::from_secs(5),
            || {
                warn!(
                    elapsed = ?commit_start_time.elapsed(),
                    %sink_id,
                    epoch,
                    "committing an epoch left by a replaced sink"
                );
            },
        )
        .await?;
        if !delete_replaced_sink_pending_epoch(db, sink_id, epoch).await? {
            info!(
                %sink_id,
                epoch,
                "the replacement sink was dropped, discard the remaining epochs of the replaced sink"
            );
            return Ok(());
        }
        *committed += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_connector::sink::catalog::SinkId;
    use tokio::time::timeout;

    use super::ReplacedSinkFences;

    #[tokio::test]
    async fn test_fence_released_after_every_replaced_sink() {
        let fences = ReplacedSinkFences::default();
        let successor = SinkId::from(3);

        // A sink that replaced nothing is never blocked.
        fences.wait_released(successor).await;

        fences.fence(successor, SinkId::from(1));
        fences.fence(successor, SinkId::from(2));
        let waiter = tokio::spawn({
            let fences = fences.clone();
            async move { fences.wait_released(successor).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        // Other sinks are not affected.
        fences.wait_released(SinkId::from(4)).await;

        fences.release(successor, SinkId::from(1));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        assert!(fences.is_fenced(successor));

        fences.release(successor, SinkId::from(2));
        timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(!fences.is_fenced(successor));
    }
}
