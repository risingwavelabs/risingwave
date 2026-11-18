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

use std::collections::VecDeque;
use std::future::poll_fn;
use std::task::Poll;

use futures::FutureExt;
use futures::future::BoxFuture;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};

use super::HummockVersionUpdate;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::monitor::GLOBAL_CACHE_REFILL_METRICS;

/// Completion permits this event to be published after earlier events.
/// Refillers start their work independently of polling this future and define whether
/// dropping it cancels or detaches the underlying work. It may be first-polled late.
pub(super) type VersionUpdateCondition = BoxFuture<'static, ()>;

/// One received payload's version transition, possibly containing multiple raw deltas.
pub(super) struct VersionUpdateEvent {
    /// Base ID for chain checks; published holders or the preceding event keep it pinned.
    pub prev_version_id: HummockVersionId,
    /// Constructed target awaiting publication.
    pub new_pinned_version: PinnedVersion,
}

struct PendingVersion {
    condition: VersionUpdateCondition,
    event: VersionUpdateEvent,
}

/// Constructs received versions and orders their publication after cache refill completes.
/// The refiller starts the work; this queue only waits for its completion.
pub(super) struct VersionUpdateQueue {
    /// Advanced by enqueue; may lead the version currently published to readers.
    latest_received: PinnedVersion,
    queue: VecDeque<PendingVersion>,
}

impl VersionUpdateQueue {
    pub fn new(version: PinnedVersion) -> Self {
        Self {
            latest_received: version,
            queue: VecDeque::new(),
        }
    }

    /// Builds and pins the target version without advancing `latest_received` or publishing it.
    /// Creating the pin sends a request to the pinned-version manager, even though this takes `&self`.
    /// `before_apply` observes each intermediate version immediately before SDK delta application.
    /// Returns `None` for an unchanged version, including an empty delta payload.
    /// Enqueue the returned event before resolving another payload so its base is up to date.
    ///
    /// # Panics
    /// Panics if the delta chain does not extend `latest_received`, or a snapshot is older than it.
    pub fn resolve_update(
        &self,
        payload: HummockVersionUpdate,
        mut before_apply: impl FnMut(&HummockVersion, &HummockVersionDelta),
    ) -> Option<VersionUpdateEvent> {
        let pinned_version = &self.latest_received;
        let new_pinned_version = match payload {
            HummockVersionUpdate::VersionDeltas(deltas) => {
                let mut version = (**pinned_version).clone();
                for delta in &deltas {
                    assert_eq!(version.id, delta.prev_id);
                    before_apply(&version, delta);
                    version.apply_version_delta(delta);
                }
                pinned_version.new_with_local_version(version)?
            }
            HummockVersionUpdate::PinnedVersion(version) => {
                pinned_version.new_pin_version(*version)?
            }
        };
        Some(VersionUpdateEvent {
            prev_version_id: pinned_version.id(),
            new_pinned_version,
        })
    }

    /// Advances `latest_received` and queues the transition with its publication condition.
    /// Work startup and cancellation follow the [`VersionUpdateCondition`] contract.
    ///
    /// # Panics
    /// Panics if the event's previous version does not match `latest_received`.
    pub fn enqueue(&mut self, event: VersionUpdateEvent, condition: VersionUpdateCondition) {
        assert_eq!(event.prev_version_id, self.latest_received.id());
        self.latest_received = event.new_pinned_version.clone();
        self.queue.push_back(PendingVersion { condition, event });
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.inc();
    }

    /// Removes up to 16 ready events from the front for the caller to publish in order.
    /// Cancellation while pending preserves queued events and their condition futures.
    /// An empty queue has no wake source: the caller must enqueue and repoll on incoming updates.
    pub async fn next_events(&mut self) -> Vec<VersionUpdateEvent> {
        poll_fn(|cx| {
            const MAX_BATCH_SIZE: usize = 16;
            let mut events = None;
            while let Some(pending) = self.queue.front_mut()
                && pending.condition.poll_unpin(cx).is_ready()
            {
                let pending = self.queue.pop_front().unwrap();
                GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.dec();
                let events = events.get_or_insert_with(|| Vec::with_capacity(MAX_BATCH_SIZE));
                events.push(pending.event);
                if events.len() >= MAX_BATCH_SIZE {
                    break;
                }
            }
            match events {
                Some(events) => Poll::Ready(events),
                None => Poll::Pending,
            }
        })
        .await
    }
}

impl Drop for VersionUpdateQueue {
    fn drop(&mut self) {
        GLOBAL_CACHE_REFILL_METRICS
            .refill_queue_total
            .sub(self.queue.len() as i64);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[tokio::test]
    async fn test_ready_prefix_limit_and_delta_observation() {
        let version = PinnedVersion::new(HummockVersion::default(), unbounded_channel().0);
        let mut queue = VersionUpdateQueue::new(version);
        for id in 1..=17 {
            let deltas = (id * 2 - 1..=id * 2)
                .map(|raw_id| {
                    let mut delta = HummockVersionDelta::default();
                    delta.prev_id = (raw_id - 1).into();
                    delta.id = raw_id.into();
                    delta
                })
                .collect();
            let mut observed = Vec::new();
            let event = queue
                .resolve_update(
                    HummockVersionUpdate::VersionDeltas(deltas),
                    |before, delta| {
                        observed.push((before.id.as_raw_id(), delta.id.as_raw_id()));
                    },
                )
                .unwrap();
            assert_eq!(
                observed,
                vec![(id * 2 - 2, id * 2 - 1), (id * 2 - 1, id * 2)]
            );
            queue.enqueue(event, std::future::ready(()).boxed());
        }
        let first = queue.next_events().await;
        assert_eq!(first.len(), 16);
        assert_eq!(
            first
                .iter()
                .map(|event| event.new_pinned_version.id().as_raw_id())
                .collect::<Vec<_>>(),
            (1..=16).map(|id| id * 2).collect::<Vec<_>>()
        );
        let last = queue.next_events().await;
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].new_pinned_version.id().as_raw_id(), 34);
    }
}
