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

/// Waits for this event's refill attempts, including backend fallback, then returns whether
/// its target may be published. False means a batch subsumed this target, not refill failure;
/// retire it only with a later true endpoint. Pending attempts must not be bypassed.
/// Work starts independently of polling this future; refillers own cancellation semantics.
pub(super) type VersionUpdateCondition = BoxFuture<'static, bool>;

/// One received payload's version transition, possibly containing multiple raw deltas.
pub(super) struct VersionUpdateEvent {
    /// Base ID for chain checks; published holders or the preceding event keep it pinned.
    pub prev_version_id: HummockVersionId,
    /// Constructed target awaiting publication.
    pub new_pinned_version: PinnedVersion,
}

/// Keeps raw inputs only until the refiller has planned this received payload.
pub(super) struct ResolvedVersionUpdate {
    pub event: VersionUpdateEvent,
    pub version_deltas: Option<Vec<HummockVersionDelta>>,
}

struct PendingVersion {
    condition: VersionUpdateCondition,
    event: VersionUpdateEvent,
    /// A subsumed target can remain queued after its condition has completed.
    publishable: Option<bool>,
}

/// Constructs received versions and orders their publication after refill conditions complete.
/// Refillers start the work and bound their own attempts; this queue only waits for completion.
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
    ) -> Option<ResolvedVersionUpdate> {
        let pinned_version = &self.latest_received;
        let (new_pinned_version, version_deltas) = match payload {
            HummockVersionUpdate::VersionDeltas(deltas) => {
                let mut version = (**pinned_version).clone();
                for delta in &deltas {
                    assert_eq!(version.id, delta.prev_id);
                    before_apply(&version, delta);
                    version.apply_version_delta(delta);
                }
                (
                    pinned_version.new_with_local_version(version)?,
                    Some(deltas),
                )
            }
            HummockVersionUpdate::PinnedVersion(version) => {
                (pinned_version.new_pin_version(*version)?, None)
            }
        };
        Some(ResolvedVersionUpdate {
            event: VersionUpdateEvent {
                prev_version_id: pinned_version.id(),
                new_pinned_version,
            },
            version_deltas,
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
        self.queue.push_back(PendingVersion {
            condition,
            event,
            publishable: None,
        });
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.inc();
    }

    /// Returns the settled FIFO prefix through its latest publishable target.
    /// Subsumed targets may retire with a later endpoint, but unfinished attempts cannot be skipped.
    /// Cancellation while pending preserves queued events and completed condition results.
    /// An empty queue must be repolled after the caller enqueues an incoming update.
    pub async fn next_events(&mut self) -> Vec<VersionUpdateEvent> {
        poll_fn(|cx| {
            const MAX_BATCH_SIZE: usize = 16;
            let mut count = 0;
            for (index, pending) in self.queue.iter_mut().enumerate() {
                let publishable = match pending.publishable {
                    Some(publishable) => publishable,
                    None => match pending.condition.poll_unpin(cx) {
                        Poll::Pending => break,
                        Poll::Ready(publishable) => {
                            pending.publishable = Some(publishable);
                            publishable
                        }
                    },
                };
                if publishable {
                    count = index + 1;
                    // Keep main's publication bound, extending only to a safe batch endpoint.
                    if count >= MAX_BATCH_SIZE {
                        break;
                    }
                }
            }
            if count == 0 {
                Poll::Pending
            } else {
                let events = self
                    .queue
                    .drain(..count)
                    .map(|pending| pending.event)
                    .collect();
                GLOBAL_CACHE_REFILL_METRICS
                    .refill_queue_total
                    .sub(count as i64);
                Poll::Ready(events)
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
    async fn test_latest_ready_target_and_delta_observation() {
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
                .unwrap()
                .event;
            assert_eq!(
                observed,
                vec![(id * 2 - 2, id * 2 - 1), (id * 2 - 1, id * 2)]
            );
            queue.enqueue(event, std::future::ready(true).boxed());
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
        assert_eq!(last[0].new_pinned_version.id(), 34);
        assert!(queue.next_events().now_or_never().is_none());
    }

    #[tokio::test]
    async fn test_subsumed_targets_wait_for_endpoint_and_survive_pending_polls() {
        let version = PinnedVersion::new(HummockVersion::default(), unbounded_channel().0);
        let mut queue = VersionUpdateQueue::new(version);
        let (release, receiver) = tokio::sync::oneshot::channel();
        let mut receiver = Some(receiver);
        for id in 1..=20 {
            let mut delta = HummockVersionDelta::default();
            delta.prev_id = (id - 1).into();
            delta.id = id.into();
            let event = queue
                .resolve_update(HummockVersionUpdate::VersionDeltas(vec![delta]), |_, _| {})
                .unwrap()
                .event;
            // A merged refill subsumes targets 2..19. Even a large batch must wait
            // for its publishable endpoint, including that event's Foyer attempt.
            let condition: VersionUpdateCondition = if id == 20 {
                let receiver = receiver.take().unwrap();
                async move {
                    receiver.await.unwrap();
                    true
                }
                .boxed()
            } else {
                async move { !(2..=19).contains(&id) }.boxed()
            };
            queue.enqueue(event, condition);
        }
        let events = queue.next_events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].new_pinned_version.id(), 1);
        // Event 2's completed async future must not be polled again after a canceled wait.
        for _ in 0..2 {
            assert!(queue.next_events().now_or_never().is_none());
        }
        release.send(()).unwrap();
        let events = queue.next_events().await;
        assert_eq!(events.len(), 19);
        assert_eq!(events.first().unwrap().prev_version_id, 1);
        assert_eq!(events.last().unwrap().new_pinned_version.id(), 20);
        assert!(queue.next_events().now_or_never().is_none());
    }
}
