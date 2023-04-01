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

use super::DeleteRangeTombstone;
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
pub struct DeleteRangeAggregatorBuilder {
    delete_tombstones: Vec<DeleteRangeTombstone>,
}

#[derive(Clone)]
pub(crate) struct TombstoneEnterExitEvent {
    original_position_in_delete_tombstones: usize,
    tombstone_epoch: HummockEpoch,
}

type CompactionDeleteRangeEvent = (
    // event key
    UserKey<Vec<u8>>,
    // tombstones which exits at the event key
    Vec<TombstoneEnterExitEvent>,
    // tombstones which enters at the event key
    Vec<TombstoneEnterExitEvent>,
);
pub(crate) fn apply_event(epochs: &mut BTreeSet<HummockEpoch>, event: &CompactionDeleteRangeEvent) {
    let (_, exit, enter) = event;
    // Correct because ranges in an epoch won't intersect.
    for TombstoneEnterExitEvent {
        tombstone_epoch, ..
    } in exit
    {
        epochs.remove(tombstone_epoch);
    }
    for TombstoneEnterExitEvent {
        tombstone_epoch, ..
    } in enter
    {
        epochs.insert(*tombstone_epoch);
    }
}

pub(crate) fn build_monotonic_tombstone_events(events: Vec<CompactionDeleteRangeEvent>)  {
    let mut epochs = BTreeSet::new();
        let mut monotonic_tombstone_events = Vec::with_capacity(events.len());
        for event in events {
            apply_event(&mut epochs, &event);
            monotonic_tombstone_events.push((
                event.0,
                epochs.first().map_or(HummockEpoch::MAX, |epoch| *epoch),
            ));
        }
        monotonic_tombstone_events.dedup_by_key(|(_, epoch)| *epoch);
}

#[derive(Clone)]
pub struct CompactionDeleteRanges {
    delete_tombstones: Vec<DeleteRangeTombstone>,
    events: Vec<CompactionDeleteRangeEvent>,
    event_seek_mapping: Vec<usize>,
    watermark: HummockEpoch,
    gc_delete_keys: bool,
}

impl DeleteRangeAggregatorBuilder {
    pub fn add_tombstone(&mut self, data: Vec<DeleteRangeTombstone>) {
        self.delete_tombstones.extend(data);
    }

    /// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
    /// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
    /// can be transformed into events below:
    /// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
    /// -epoch3> }`
    pub(crate) fn build_events(
        delete_tombstones: &Vec<DeleteRangeTombstone>,
    ) -> (Vec<CompactionDeleteRangeEvent>, Vec<usize>) {
        let tombstone_len = delete_tombstones.len();
        let mut events = Vec::with_capacity(tombstone_len * 2);
        for (
            index,
            DeleteRangeTombstone {
                start_user_key,
                end_user_key,
                ..
            },
        ) in delete_tombstones.iter().enumerate()
        {
            events.push((start_user_key, 1, index));
            events.push((end_user_key, 0, index));
        }
        events.sort();

        let mut result = Vec::with_capacity(events.len());
        let mut enter_pos = vec![0; tombstone_len];
        for (user_key, group) in &events.into_iter().group_by(|(user_key, _, _)| *user_key) {
            let (mut exit, mut enter) = (vec![], vec![]);
            for (_, op, index) in group {
                match op {
                    0 => exit.push(TombstoneEnterExitEvent {
                        original_position_in_delete_tombstones: index,
                        tombstone_epoch: delete_tombstones[index].sequence,
                    }),
                    1 => {
                        enter.push(TombstoneEnterExitEvent {
                            original_position_in_delete_tombstones: index,
                            tombstone_epoch: delete_tombstones[index].sequence,
                        });
                        enter_pos[index] = result.len();
                    }
                    _ => unreachable!(),
                }
            }
            result.push((user_key.clone(), exit, enter));
        }

        (result, enter_pos)
    }

    pub(crate) fn build_for_compaction(
        self,
        watermark: HummockEpoch,
        gc_delete_keys: bool,
    ) -> Arc<CompactionDeleteRanges> {
        let (result, enter_pos) = Self::build_events(&self.delete_tombstones);

        let result_len = result.len();
        // `event_seek_mapping` and `hook` are unnecessary so you can ignore them.
        let mut event_seek_mapping = vec![0; result_len + 1];
        let mut hook = result_len;
        event_seek_mapping[result_len] = hook;
        for (result_idx, (_, exit, _enter)) in result.iter().enumerate().rev() {
            if result_idx < hook {
                hook = result_idx;
            }
            for TombstoneEnterExitEvent {
                original_position_in_delete_tombstones,
                ..
            } in exit
            {
                if enter_pos[*original_position_in_delete_tombstones] < hook {
                    hook = enter_pos[*original_position_in_delete_tombstones];
                }
            }
            event_seek_mapping[result_idx] = hook;
        }

        Arc::new(CompactionDeleteRanges {
            delete_tombstones: self.delete_tombstones,
            events: result,
            event_seek_mapping,
            watermark,
            gc_delete_keys,
        })
    }
}

impl CompactionDeleteRanges {
    pub(crate) fn for_test() -> Self {
        Self {
            delete_tombstones: vec![],
            events: vec![],
            event_seek_mapping: vec![0],
            gc_delete_keys: false,
            watermark: 0,
        }
    }

    pub(crate) fn iter(self: &Arc<Self>) -> CompactionDeleteRangeIterator {
        CompactionDeleteRangeIterator {
            events: self.clone(),
            seek_idx: 0,
            epochs: BTreeSet::default(),
        }
    }

    pub(crate) fn get_tombstone_between(
        &self,
        smallest_user_key: &UserKey<&[u8]>,
        largest_user_key: &UserKey<&[u8]>,
    ) -> Vec<DeleteRangeTombstone> {
        let (mut tombstones_above_watermark, mut tombstones_within_watermark) = (vec![], vec![]);
        for tombstone in &self.delete_tombstones {
            let mut candidate = tombstone.clone();
            if !smallest_user_key.is_empty()
                && smallest_user_key.gt(&candidate.start_user_key.as_ref())
            {
                candidate.start_user_key = smallest_user_key.to_vec();
            }
            if !largest_user_key.is_empty() && largest_user_key.lt(&candidate.end_user_key.as_ref())
            {
                candidate.end_user_key = largest_user_key.to_vec();
            }
            if candidate.start_user_key < candidate.end_user_key {
                if candidate.sequence > self.watermark {
                    tombstones_above_watermark.push(candidate);
                } else {
                    tombstones_within_watermark.push(candidate);
                }
            }
        }

        let mut ret = tombstones_above_watermark;
        if self.gc_delete_keys {
            return ret;
        }

        let (events, _) = DeleteRangeAggregatorBuilder::build_events(&tombstones_within_watermark);
        let mut epoch2index = BTreeMap::new();
        let mut is_useful = vec![false; tombstones_within_watermark.len()];
        for (_, exit, enter) in events {
            // Correct because ranges in an epoch won't intersect.
            for TombstoneEnterExitEvent {
                tombstone_epoch, ..
            } in exit
            {
                epoch2index.remove(&tombstone_epoch);
            }
            for TombstoneEnterExitEvent {
                original_position_in_delete_tombstones,
                tombstone_epoch,
            } in enter
            {
                epoch2index.insert(tombstone_epoch, original_position_in_delete_tombstones);
            }
            if let Some((_, index)) = epoch2index.last_key_value() {
                is_useful[*index] = true;
            }
        }

        ret.extend(
            tombstones_within_watermark
                .into_iter()
                .enumerate()
                .filter(|(index, _tombstone)| is_useful[*index])
                .map(|(_index, tombstone)| tombstone),
        );
        ret
    }

    pub(crate) fn events(self) -> Vec<CompactionDeleteRangeEvent> {
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
        target_user_key: &UserKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> HummockEpoch {
        while let Some((user_key, ..)) = self.events.events.get(self.seek_idx) && user_key.as_ref().le(target_user_key) {
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
        let hook = self.events.event_seek_mapping[self.seek_idx];
        for idx in hook..self.seek_idx {
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
        self.table.value().monotonic_tombstone_events[self.next_idx]
            .0
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        if self.next_idx > 0 {
            self.table.value().monotonic_tombstone_events[self.next_idx - 1].1
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
            .monotonic_tombstone_events
            .partition_point(|(user_key, _)| user_key.as_ref().le(&target_user_key));
    }

    fn is_valid(&self) -> bool {
        self.next_idx < self.table.value().monotonic_tombstone_events.len()
    }
}

pub fn get_min_delete_range_epoch_from_sstable(
    table: &Sstable,
    query_user_key: &UserKey<&[u8]>,
) -> HummockEpoch {
    let idx = table
        .monotonic_tombstone_events
        .partition_point(|(user_key, _)| user_key.as_ref().le(query_user_key));
    if idx == 0 {
        HummockEpoch::MAX
    } else {
        table.monotonic_tombstone_events[idx - 1].1
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        gen_iterator_test_sstable_with_range_tombstones, iterator_test_user_key_of,
        mock_sstable_store,
    };
    use crate::hummock::test_utils::test_user_key;

    #[test]
    pub fn test_compaction_delete_range_iterator() {
        let mut builder = DeleteRangeAggregatorBuilder::default();
        let table_id = TableId::default();
        builder.add_tombstone(vec![
            DeleteRangeTombstone::new(table_id, b"aaaaaa".to_vec(), b"bbbccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaaaa".to_vec(), b"bbbddd".to_vec(), 9),
            DeleteRangeTombstone::new(table_id, b"bbbaab".to_vec(), b"bbbdddf".to_vec(), 6),
            DeleteRangeTombstone::new(table_id, b"bbbeee".to_vec(), b"eeeeee".to_vec(), 8),
            DeleteRangeTombstone::new(table_id, b"bbbfff".to_vec(), b"ffffff".to_vec(), 9),
            DeleteRangeTombstone::new(table_id, b"gggggg".to_vec(), b"hhhhhh".to_vec(), 9),
        ]);
        let compaction_delete_ranges = builder.build_for_compaction(10, false);
        let mut iter = compaction_delete_ranges.iter();

        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbb").as_ref(), 13),
            HummockEpoch::MAX
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbb").as_ref(), 11),
            12
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbb").as_ref(), 8),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbbaaa").as_ref(), 8),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbbccd").as_ref(), 8),
            9
        );

        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbbddd").as_ref(), 8),
            HummockEpoch::MAX
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbbeee").as_ref(), 8),
            8
        );

        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"bbbeef").as_ref(), 10),
            HummockEpoch::MAX
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"eeeeee").as_ref(), 9),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"gggggg").as_ref(), 8),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(&test_user_key(b"hhhhhh").as_ref(), 8),
            HummockEpoch::MAX
        );
    }

    #[test]
    pub fn test_delete_range_split() {
        let table_id = TableId::default();
        let mut builder = DeleteRangeAggregatorBuilder::default();
        builder.add_tombstone(vec![
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"bbbb".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"cccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"dddd".to_vec(), 10),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"eeee".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"eeee".to_vec(), b"ffff".to_vec(), 12),
        ]);
        let compaction_delete_range = builder.build_for_compaction(10, true);
        let split_ranges = compaction_delete_range.get_tombstone_between(
            &test_user_key(b"bbbb").as_ref(),
            &test_user_key(b"eeeeee").as_ref(),
        );
        assert_eq!(3, split_ranges.len());
        assert_eq!(test_user_key(b"bbbb"), split_ranges[0].start_user_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[0].end_user_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[1].start_user_key);
        assert_eq!(test_user_key(b"eeee"), split_ranges[1].end_user_key);
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
            &iterator_test_user_key_of(0).as_ref(),
        );
        assert_eq!(ret, 300);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_user_key_of(1).as_ref(),
        );
        assert_eq!(ret, 150);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_user_key_of(3).as_ref(),
        );
        assert_eq!(ret, 50);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_user_key_of(6).as_ref(),
        );
        assert_eq!(ret, 150);
        let ret = get_min_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_user_key_of(8).as_ref(),
        );
        assert_eq!(ret, HummockEpoch::MAX);
    }
}
