// Copyright 2023 RisingWave Labs
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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::key::UserKey;
use risingwave_hummock_sdk::HummockEpoch;

use super::{DeleteRangeTombstone, MonotonicDeleteEvent};
use crate::hummock::iterator::DeleteRangeIterator;
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::Sstable;

pub struct SortedBoundary {
    sequence: HummockEpoch,
    user_key: UserKey<Vec<u8>>,
}

impl PartialEq<Self> for SortedBoundary {
    fn eq(&self, other: &Self) -> bool {
        self.user_key.eq(&other.user_key) && self.sequence == other.sequence
    }
}

impl PartialOrd for SortedBoundary {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ret = other
            .user_key
            .cmp(&self.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence));
        Some(ret)
    }
}

impl Eq for SortedBoundary {}

impl Ord for SortedBoundary {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[derive(Default)]
pub struct CompactionDeleteRangesBuilder {
    events: Vec<Vec<MonotonicDeleteEvent>>,
}

#[derive(Clone)]
pub(crate) struct TombstoneEnterExitEvent {
    pub(crate) tombstone_epoch: HummockEpoch,
}

pub(crate) type CompactionDeleteRangeEvent = (
    // event key
    UserKey<Vec<u8>>,
    // Old tombstones which exits at the event key
    Vec<TombstoneEnterExitEvent>,
    // New tombstones which enters at the event key
    Vec<TombstoneEnterExitEvent>,
);
/// We introduce `event` to avoid storing excessive range tombstones after compaction if there are
/// overlaps among range tombstones among different SSTs/batchs in compaction.
/// The core idea contains two parts:
/// 1) we only need to keep the smallest epoch of the overlapping
/// range tomstone intervals since the key covered by the range tombstone in lower level must have
/// smaller epoches;
/// 2) due to 1), we lose the information to delete a key by tombstone in a single
/// SST so we add a tombstone key in the data block.
/// We leverage `events` to calculate the epoch information mentioned above.
/// e.g. Delete range [1, 5) at epoch1, delete range [3, 7) at epoch2 and delete range [10, 12) at
/// epoch3 will first be transformed into `events` below:
/// `<1, +epoch1> <5, -epoch1> <3, +epoch2> <7, -epoch2> <10, +epoch3> <12, -epoch3>`
/// Then `events` are sorted by user key:
/// `<1, +epoch1> <3, +epoch2> <5, -epoch1> <7, -epoch2> <10, +epoch3> <12, -epoch3>`
/// We rely on the fact that keys met in compaction are in order.
/// When user key 0 comes, no events have happened yet so no range delete epoch. (will be
/// represented as range delete epoch `HummockEpoch::MAX`)
/// When user key 1 comes, event `<1, +epoch1>` happens so there is currently one range delete
/// epoch: epoch1.
/// When user key 2 comes, no more events happen so the set remains `{epoch1}`.
/// When user key 3 comes, event `<3, +epoch2>` happens so the range delete epoch set is now
/// `{epoch1, epoch2}`.
/// When user key 5 comes, event `<5, -epoch1>` happens so epoch1 exits the set,
/// therefore the current range delete epoch set is `{epoch2}`.
/// When user key 11 comes, events `<7, -epoch2>` and `<10, +epoch3>`
/// both happen, one after another. The set changes to `{epoch3}` from `{epoch2}`.
pub(crate) fn apply_event(epochs: &mut BTreeSet<HummockEpoch>, event: &CompactionDeleteRangeEvent) {
    let (_, exit, enter) = event;
    // Correct because ranges in an epoch won't intersect.
    for TombstoneEnterExitEvent { tombstone_epoch } in exit {
        epochs.remove(tombstone_epoch);
    }
    for TombstoneEnterExitEvent { tombstone_epoch } in enter {
        epochs.insert(*tombstone_epoch);
    }
}

#[derive(Clone)]
pub struct CompactionDeleteRanges {
    events: Vec<CompactionDeleteRangeEvent>,
    gc_delete_keys: bool,
}

impl CompactionDeleteRangesBuilder {
    pub fn add_delete_events(&mut self, data: Vec<MonotonicDeleteEvent>) {
        self.events.push(data);
    }

    /// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
    /// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
    /// can be transformed into events below:
    /// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
    /// -epoch3> }`
    pub(crate) fn build_events(
        delete_tombstones: &Vec<DeleteRangeTombstone>,
    ) -> Vec<CompactionDeleteRangeEvent> {
        let tombstone_len = delete_tombstones.len();
        let mut events = Vec::with_capacity(tombstone_len * 2);
        for DeleteRangeTombstone {
            start_user_key,
            end_user_key,
            sequence,
        } in delete_tombstones
        {
            events.push((start_user_key, 1, *sequence));
            events.push((end_user_key, 0, *sequence));
        }
        events.sort();

        let mut result = Vec::with_capacity(events.len());
        for (user_key, group) in &events.into_iter().group_by(|(user_key, _, _)| *user_key) {
            let (mut exit, mut enter) = (vec![], vec![]);
            for (_, op, sequence) in group {
                match op {
                    0 => exit.push(TombstoneEnterExitEvent {
                        tombstone_epoch: sequence,
                    }),
                    1 => {
                        enter.push(TombstoneEnterExitEvent {
                            tombstone_epoch: sequence,
                        });
                    }
                    _ => unreachable!(),
                }
            }
            result.push((user_key.clone(), exit, enter));
        }

        result
    }

    pub(crate) fn build_for_compaction(self, gc_delete_keys: bool) -> Arc<CompactionDeleteRanges> {
        let mut ret = BTreeMap::<
            UserKey<Vec<u8>>,
            (Vec<TombstoneEnterExitEvent>, Vec<TombstoneEnterExitEvent>),
        >::default();
        for monotonic_deletes in self.events {
            let mut last_exit_epoch = HummockEpoch::MAX;
            for delete_event in monotonic_deletes {
                if last_exit_epoch != HummockEpoch::MAX {
                    let entry = ret.entry(delete_event.event_key.clone()).or_default();
                    entry.0.push(TombstoneEnterExitEvent {
                        tombstone_epoch: last_exit_epoch,
                    });
                }
                if delete_event.new_epoch != HummockEpoch::MAX {
                    let entry = ret.entry(delete_event.event_key).or_default();
                    entry.1.push(TombstoneEnterExitEvent {
                        tombstone_epoch: delete_event.new_epoch,
                    });
                }
                last_exit_epoch = delete_event.new_epoch;
            }
        }

        Arc::new(CompactionDeleteRanges {
            events: ret
                .into_iter()
                .map(|(k, (exits, enters))| (k, exits, enters))
                .collect_vec(),
            gc_delete_keys,
        })
    }
}

impl CompactionDeleteRanges {
    pub(crate) fn for_test() -> Self {
        Self {
            events: vec![],
            gc_delete_keys: false,
        }
    }

    pub(crate) fn iter(self: &Arc<Self>) -> CompactionDeleteRangeIterator {
        CompactionDeleteRangeIterator {
            events: self.clone(),
            seek_idx: 0,
            epochs: BTreeSet::default(),
        }
    }

    /// the `largest_user_key` always mean that
    pub(crate) fn get_tombstone_between(
        &self,
        smallest_user_key: UserKey<&[u8]>,
        largest_user_key: UserKey<&[u8]>,
    ) -> Vec<MonotonicDeleteEvent> {
        if self.gc_delete_keys {
            return vec![];
        }

        let mut monotonic_events = Vec::with_capacity(self.events.len());
        let mut epochs = BTreeSet::new();
        let mut idx = 0;
        while idx < self.events.len() {
            if self.events[idx].0.as_ref().gt(&smallest_user_key) {
                if let Some(epoch) = epochs.first() {
                    monotonic_events.push(MonotonicDeleteEvent {
                        event_key: smallest_user_key.to_vec(),
                        new_epoch: *epoch,
                        is_exclusive: false,
                    });
                }
                break;
            }
            apply_event(&mut epochs, &self.events[idx]);
            idx += 1;
        }
        while idx < self.events.len() {
            // TODO: replace it with Bound
            if !largest_user_key.is_empty() && self.events[idx].0.as_ref().gt(&largest_user_key) {
                monotonic_events.push(MonotonicDeleteEvent {
                    event_key: largest_user_key.to_vec(),
                    new_epoch: HummockEpoch::MAX,
                    is_exclusive: false,
                });
                break;
            }
            apply_event(&mut epochs, &self.events[idx]);
            monotonic_events.push(MonotonicDeleteEvent {
                event_key: self.events[idx].0.clone(),
                is_exclusive: false,
                new_epoch: epochs.first().map_or(HummockEpoch::MAX, |epoch| *epoch),
            });
            idx += 1;
        }
        monotonic_events
    }

    pub(crate) fn into_events(self) -> Vec<CompactionDeleteRangeEvent> {
        self.events
    }
}

pub(crate) struct CompactionDeleteRangeIterator {
    events: Arc<CompactionDeleteRanges>,
    seek_idx: usize,
    /// The correctness of the algorithm needs to be guaranteed by "the epoch of the
    /// intervals covering each other must be different".
    epochs: BTreeSet<HummockEpoch>,
}

impl CompactionDeleteRangeIterator {
    fn apply(&mut self, idx: usize) {
        apply_event(&mut self.epochs, &self.events.events[idx]);
    }

    /// Return the earliest range-tombstone which deletes target-key.
    /// Target-key must be given in order.
    pub(crate) fn earliest_delete_which_can_see_key(
        &mut self,
        target_user_key: UserKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> HummockEpoch {
        while let Some((user_key, ..)) = self.events.events.get(self.seek_idx) && user_key.as_ref().le(&target_user_key) {
            self.apply(self.seek_idx);
            self.seek_idx += 1;
        }
        self.earliest_delete_since(epoch)
    }

    pub(crate) fn earliest_delete_since(&self, epoch: HummockEpoch) -> HummockEpoch {
        self.epochs
            .range(epoch..)
            .next()
            .map_or(HummockEpoch::MAX, |ret| *ret)
    }

    pub(crate) fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.seek_idx = self
            .events
            .events
            .partition_point(|(user_key, ..)| user_key.as_ref().le(&target_user_key));
        self.epochs.clear();
        for idx in 0..self.seek_idx {
            self.apply(idx);
        }
    }

    pub(crate) fn rewind(&mut self) {
        self.seek_idx = 0;
        self.epochs.clear();
    }
}

pub struct SstableDeleteRangeIterator {
    table: TableHolder,
    next_idx: usize,
}

impl SstableDeleteRangeIterator {
    pub fn new(table: TableHolder) -> Self {
        Self { table, next_idx: 0 }
    }
}

impl DeleteRangeIterator for SstableDeleteRangeIterator {
    fn next_user_key(&self) -> UserKey<&[u8]> {
        self.table.value().meta.monotonic_tombstone_events[self.next_idx]
            .event_key
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        if self.next_idx > 0 {
            self.table.value().meta.monotonic_tombstone_events[self.next_idx - 1].new_epoch
        } else {
            HummockEpoch::MAX
        }
    }

    fn next(&mut self) {
        self.next_idx += 1;
    }

    fn rewind(&mut self) {
        self.next_idx = 0;
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.next_idx = self
            .table
            .value()
            .meta
            .monotonic_tombstone_events
            .partition_point(|MonotonicDeleteEvent { event_key, .. }| {
                event_key.as_ref().le(&target_user_key)
            });
    }

    fn is_valid(&self) -> bool {
        self.next_idx < self.table.value().meta.monotonic_tombstone_events.len()
    }
}

pub fn get_min_delete_range_epoch_from_sstable(
    table: &Sstable,
    query_user_key: UserKey<&[u8]>,
) -> HummockEpoch {
    let idx = table.meta.monotonic_tombstone_events.partition_point(
        |MonotonicDeleteEvent { event_key, .. }| event_key.as_ref().le(&query_user_key),
    );
    if idx == 0 {
        HummockEpoch::MAX
    } else {
        table.meta.monotonic_tombstone_events[idx - 1].new_epoch
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;

    use super::*;
    use crate::hummock::create_monotonic_events;
    use crate::hummock::iterator::test_utils::{
        gen_iterator_test_sstable_with_range_tombstones, iterator_test_user_key_of,
        mock_sstable_store,
    };
    use crate::hummock::test_utils::test_user_key;

    #[test]
    pub fn test_compaction_delete_range_iterator() {
        let mut builder = CompactionDeleteRangesBuilder::default();
        let table_id = TableId::default();
        builder.add_delete_events(create_monotonic_events(&vec![
            DeleteRangeTombstone::new(table_id, b"aaaaaa".to_vec(), b"bbbccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaaaa".to_vec(), b"bbbddd".to_vec(), 9),
            DeleteRangeTombstone::new(table_id, b"bbbaab".to_vec(), b"bbbdddf".to_vec(), 6),
            DeleteRangeTombstone::new(table_id, b"bbbeee".to_vec(), b"eeeeee".to_vec(), 8),
            DeleteRangeTombstone::new(table_id, b"bbbfff".to_vec(), b"ffffff".to_vec(), 9),
            DeleteRangeTombstone::new(table_id, b"gggggg".to_vec(), b"hhhhhh".to_vec(), 9),
        ]));
        let compaction_delete_ranges = builder.build_for_compaction(false);
        let mut iter = compaction_delete_ranges.iter();

        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbb").as_ref(), 13),
            HummockEpoch::MAX
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbb").as_ref(), 11),
            12
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbb").as_ref(), 8),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbaaa").as_ref(), 8),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbccd").as_ref(), 8),
            9
        );

        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbddd").as_ref(), 8),
            HummockEpoch::MAX
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbeee").as_ref(), 8),
            8
        );

        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbeef").as_ref(), 10),
            HummockEpoch::MAX
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"eeeeee").as_ref(), 9),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"gggggg").as_ref(), 8),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"hhhhhh").as_ref(), 8),
            HummockEpoch::MAX
        );
    }

    #[test]
    pub fn test_delete_range_split() {
        let table_id = TableId::default();
        let mut builder = CompactionDeleteRangesBuilder::default();
        builder.add_delete_events(create_monotonic_events(&vec![
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"bbbb".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"cccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"dddd".to_vec(), 10),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"eeee".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"eeee".to_vec(), b"ffff".to_vec(), 12),
        ]));
        let compaction_delete_range = builder.build_for_compaction(false);
        let split_ranges = compaction_delete_range.get_tombstone_between(
            test_user_key(b"bbbb").as_ref(),
            test_user_key(b"eeeeee").as_ref(),
        );
        assert_eq!(4, split_ranges.len());
        assert_eq!(test_user_key(b"bbbb"), split_ranges[0].event_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[1].event_key);
        assert_eq!(test_user_key(b"dddd"), split_ranges[2].event_key);
        assert_eq!(test_user_key(b"eeeeee"), split_ranges[3].event_key);
    }

    #[tokio::test]
    async fn test_delete_range_get() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let sstable = gen_iterator_test_sstable_with_range_tombstones(
            0,
            vec![],
            vec![(0, 2, 300), (1, 4, 150), (3, 6, 50), (5, 8, 150)],
            sstable_store,
        )
        .await;
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            iterator_test_user_key_of(0).as_ref(),
        );
        assert_eq!(ret, 300);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            iterator_test_user_key_of(1).as_ref(),
        );
        assert_eq!(ret, 150);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            iterator_test_user_key_of(3).as_ref(),
        );
        assert_eq!(ret, 50);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            iterator_test_user_key_of(6).as_ref(),
        );
        assert_eq!(ret, 150);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            iterator_test_user_key_of(8).as_ref(),
        );
        assert_eq!(ret, HummockEpoch::MAX);
    }
}
