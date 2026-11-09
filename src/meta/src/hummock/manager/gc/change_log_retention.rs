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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_hummock_sdk::HummockObjectId;
use risingwave_hummock_sdk::change_log::TableChangeLog;

/// Retains removed changelog references across backup publication and GC scans.
///
/// This state is independent of versioning. Callers record removals before releasing
/// their existing versioning write lock, and check retained objects under their existing
/// GC versioning read lock. Registration and release never acquire versioning.
#[derive(Clone, Default)]
pub(crate) struct ChangeLogObjectRetention(Arc<Mutex<RetentionState>>);

#[derive(Default)]
struct RetentionState {
    next_epoch: u64,
    pins: BTreeMap<u64, usize>,
    retired: BTreeMap<u64, Vec<RetainedChangeLogBatch>>,
}

/// Own the removed collection without enumerating or copying its object IDs. Cloning
/// a batch only clones its Arc, so GC can snapshot handles under the tracker mutex.
#[derive(Clone)]
pub(crate) enum RetainedChangeLogBatch {
    ObjectIds(Arc<HashSet<HummockObjectId>>),
    ChangeLog(Arc<TableChangeLog>),
}

pub(crate) struct ChangeLogObjectGuard {
    retention: ChangeLogObjectRetention,
    epoch: u64,
}

impl ChangeLogObjectRetention {
    /// Backup pins protect subsequent removals; GC additionally inherits all active pins
    /// before scanning manifests, so publication cannot release references behind its scan.
    pub(crate) fn pin(&self, inherit: bool) -> ChangeLogObjectGuard {
        let mut state = self.0.lock();
        state.next_epoch += 1;
        let epoch = if inherit {
            state
                .pins
                .keys()
                .next()
                .copied()
                .unwrap_or(state.next_epoch)
        } else {
            state.next_epoch
        };
        *state.pins.entry(epoch).or_default() += 1;
        ChangeLogObjectGuard {
            retention: self.clone(),
            epoch,
        }
    }

    /// Publish an immutable batch while the caller still holds the versioning write
    /// lock that protects the removed references. Only batch bookkeeping runs under
    /// the tracker mutex; object IDs are enumerated later by GC.
    pub(crate) fn retain_batch(&self, batch: RetainedChangeLogBatch) {
        let mut state = self.0.lock();
        if let Some(epoch) = state.pins.keys().next_back().copied() {
            state.retired.entry(epoch).or_default().push(batch);
        }
        // If no pin needs the batch, release the mutex before destroying its contents.
        drop(state);
    }

    pub(crate) fn objects(&self) -> HashSet<HummockObjectId> {
        let batches: Vec<_> = self.0.lock().retired.values().flatten().cloned().collect();
        let mut objects = HashSet::new();
        for batch in batches {
            match batch {
                RetainedChangeLogBatch::ObjectIds(ids) => objects.extend(ids.iter().copied()),
                RetainedChangeLogBatch::ChangeLog(log) => objects.extend(log.get_object_ids()),
            }
        }
        objects
    }
}

impl Drop for ChangeLogObjectGuard {
    fn drop(&mut self) {
        let mut state = self.retention.0.lock();
        let count = state.pins.get_mut(&self.epoch).unwrap();
        *count -= 1;
        if *count == 0 {
            state.pins.remove(&self.epoch);
        }
        let min_epoch = state.pins.keys().next().copied();
        // Drop potentially large batches outside the mutex. Full GC can reclaim these
        // objects even if minor GC already consumed their original candidate entries.
        let mut released = Vec::new();
        while let Some(entry) = state.retired.first_entry() {
            if min_epoch.is_some_and(|epoch| epoch <= *entry.key()) {
                break;
            }
            released.push(entry.remove());
        }
        drop(state);
        drop(released);
    }
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;

    use super::*;

    #[test]
    fn retention_survives_publication_and_overlapping_gc() {
        let retention = ChangeLogObjectRetention::default();
        let first = HummockObjectId::Sstable(1.into());
        let second = HummockObjectId::Sstable(2.into());
        retention.retain_batch(RetainedChangeLogBatch::ObjectIds(Arc::new(HashSet::from(
            [first],
        ))));
        assert!(retention.objects().is_empty());
        let backup = retention.pin(false);
        retention.retain_batch(RetainedChangeLogBatch::ObjectIds(Arc::new(HashSet::from(
            [first],
        ))));
        let full_gc = retention.pin(true);
        let minor_gc = retention.pin(true);
        drop(backup);
        assert_eq!(retention.objects(), HashSet::from([first]));
        let next_backup = retention.pin(false);
        retention.retain_batch(RetainedChangeLogBatch::ObjectIds(Arc::new(HashSet::from(
            [second],
        ))));
        drop(full_gc);
        assert_eq!(retention.objects(), HashSet::from([first, second]));
        drop(minor_gc);
        assert_eq!(retention.objects(), HashSet::from([second]));
        drop(next_backup);
        assert!(retention.objects().is_empty());
    }

    #[test]
    fn mixed_batches_preserve_all_references_until_last_inherited_pin_drops() {
        let retention = ChangeLogObjectRetention::default();
        let first = HummockObjectId::Sstable(1.into());
        let second = HummockObjectId::Sstable(2.into());
        let third = HummockObjectId::Sstable(3.into());
        let ids = Arc::new(HashSet::from([first, second]));
        let log = Arc::new(TableChangeLog::new([EpochNewChangeLog {
            new_value: vec![
                SstableInfoInner {
                    object_id: 3.into(),
                    ..Default::default()
                }
                .into(),
            ],
            old_value: vec![
                SstableInfoInner {
                    object_id: 2.into(),
                    ..Default::default()
                }
                .into(),
            ],
            non_checkpoint_epochs: vec![],
            checkpoint_epoch: 1,
        }]));
        let weak_ids = Arc::downgrade(&ids);
        let weak_log = Arc::downgrade(&log);
        let backup = retention.pin(false);
        // Both batches have the same retention epoch, and one ID occurs in both.
        retention.retain_batch(RetainedChangeLogBatch::ObjectIds(ids));
        retention.retain_batch(RetainedChangeLogBatch::ChangeLog(log));
        let gc = retention.pin(true);
        drop(backup);
        assert_eq!(retention.objects(), HashSet::from([first, second, third]));
        assert!(weak_ids.upgrade().is_some());
        assert!(weak_log.upgrade().is_some());
        drop(gc);
        assert!(retention.objects().is_empty());
        assert!(weak_ids.upgrade().is_none());
        assert!(weak_log.upgrade().is_none());

        let unpinned = Arc::new(HashSet::from([first]));
        let weak_unpinned = Arc::downgrade(&unpinned);
        retention.retain_batch(RetainedChangeLogBatch::ObjectIds(unpinned));
        assert!(weak_unpinned.upgrade().is_none());
    }
}
