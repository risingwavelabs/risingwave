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

//! Applies Pin-cache membership changes at version receipt and publication boundaries.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_hummock_sdk::{HummockSstableObjectId, HummockVersionId};
use risingwave_pb::id::TableId;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::membership::{PinCacheMembershipChanges, PinCacheMembershipIndex};

struct MembershipUpdate {
    after: PinnedVersion,
    changes: PinCacheMembershipChanges,
}

/// Handler-owned version membership, independent of refill selection or success.
/// Refill workers do not share this state.
pub(crate) struct PinCacheMembership {
    index: PinCacheMembershipIndex,
    published: PinnedVersion,
    received: VecDeque<MembershipUpdate>,
    cache: Arc<PinCache>,
}

impl PinCacheMembership {
    pub(crate) fn new(
        cache: Arc<PinCache>,
        version: PinnedVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> Self {
        Self {
            index: PinCacheMembershipIndex::new(version.clone(), is_pinned),
            published: version,
            received: VecDeque::new(),
            cache,
        }
    }

    /// Registers additions from an already-applied target and defers removals until publication.
    pub(crate) fn on_version_received(
        &mut self,
        deltas: &[SstDeltaInfo],
        target: PinnedVersion,
        raw: Option<&[HummockVersionDelta]>,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) {
        let changes = self
            .index
            .advance_version(deltas, target.clone(), raw, is_pinned);
        // Membership is independent of batch selection and download admission.
        self.cache
            .register_objects(changes.inserted.iter().copied());
        self.received.push_back(MembershipUpdate {
            after: target,
            changes,
        });
    }

    pub(crate) fn on_version_published(
        &mut self,
        version: HummockVersionId,
    ) -> HashSet<HummockSstableObjectId> {
        let mut removed = HashSet::new();
        while self
            .received
            .front()
            .is_some_and(|update| update.after.id() <= version)
        {
            let update = self.received.pop_front().unwrap();
            for object in update.changes.inserted {
                removed.remove(&object);
            }
            removed.extend(update.changes.removed);
            self.published = update.after;
        }
        removed.retain(|object| {
            !self
                .received
                .iter()
                .any(|update| update.changes.inserted.contains(object))
        });
        self.cache.unregister_objects(removed.iter().copied());
        removed
    }

    /// Rebuilds protected membership under a policy transition; this creates no refill intent.
    pub(crate) fn replace_policy(
        &mut self,
        old: impl Fn(&TableId) -> bool + Copy,
        new: impl Fn(&TableId) -> bool + Copy,
    ) {
        let versions: Vec<_> = std::iter::once(self.published.clone())
            .chain(self.received.iter().map(|update| update.after.clone()))
            .collect();
        let changes = PinCacheMembershipChanges::between_policies(&versions, old, new);
        self.cache.register_objects(changes.inserted);
        self.cache.unregister_objects(changes.removed);
        self.index.rebuild_for_policy(new);
        let mut before = &self.published;
        for update in &mut self.received {
            update.changes =
                PinCacheMembershipChanges::between_versions(before, &update.after, new);
            before = &update.after;
        }
    }
}
