# PR 26470 cherry-pick conflict log

Source PR: https://github.com/risingwavelabs/risingwave/pull/26470
Target branch: `patch/v2.8.5-refill`

Commits cherry-picked, in PR order:

1. `f13f5130b9` feat(hummock): support table refill runtime state refresh (#24452)
2. `759e37d56d` fix: adapt table refill backport to release-2.8
3. `b420ae5c92` fix: adapt simulation refill test to release-2.8
4. `90a2201248` refactor(meta): remove unused fragment serving actor count

## Conflicts and resolutions

### Commit `f13f5130b9` feat(hummock): support table refill runtime state refresh (#24452)

Conflicted files:

- `src/storage/src/hummock/event_handler/mod.rs`
- `src/storage/src/hummock/observer_manager.rs`
- `src/storage/src/hummock/store/hummock_storage.rs`

Resolution:

- Kept the target branch's `HummockVersion` based `HummockVersionUpdate::PinnedVersion` type instead of the PR branch's `LocalHummockVersion` type, because `patch/v2.8.5-refill` still represents pinned versions as `HummockVersion`.
- Kept the PR's new `HummockObserverEvent` wrapper and `TableRefillRuntimeConfig` observer event so table refill runtime config notifications are delivered through the new observer event channel.
- In `HummockObserverNode::handle_initialization_notification`, sent the initial pinned version through `observer_event_sender` as `HummockObserverEvent::VersionUpdate(HummockVersionUpdate::PinnedVersion(Box<HummockVersion>))`.
- In the test-only `HummockStorage::update_version_and_wait`, sent the provided `HummockVersion` through `_observer_event_sender` using the same `HummockObserverEvent::VersionUpdate` wrapper, without converting it to `LocalHummockVersion`.
- Kept `HummockVersion` imported in non-test builds in `hummock_storage.rs`, because `get_time_travel_version` still constructs a `PinnedVersion` from an RPC `HummockVersion` outside test-only code.

### Commit `759e37d56d` fix: adapt table refill backport to release-2.8

No conflicts.

### Commit `b420ae5c92` fix: adapt simulation refill test to release-2.8

No conflicts.

### Commit `90a2201248` refactor(meta): remove unused fragment serving actor count

No conflicts.
