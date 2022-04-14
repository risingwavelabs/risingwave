// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This mod implements a `ConflictDetector` that  detect write key conflict in each epoch

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use dashmap::DashMap;

use crate::hummock::value::HummockValue;
use crate::hummock::HummockEpoch;
use crate::store::StorageTableId;

pub struct ConflictDetector {
    // epoch -> table_id -> key-sets
    epoch_history: DashMap<HummockEpoch, HashMap<StorageTableId, HashSet<Vec<u8>>>>,
    table_epoch_watermark: DashMap<StorageTableId, HummockEpoch>,
}

impl ConflictDetector {
    pub fn new() -> ConflictDetector {
        ConflictDetector {
            epoch_history: DashMap::new(),
            table_epoch_watermark: DashMap::new(),
        }
    }

    pub fn get_epoch_watermark(&self, table_id: StorageTableId) -> HummockEpoch {
        self.table_epoch_watermark
            .get(&table_id)
            .map(|entry| *entry.value())
            .unwrap_or(HummockEpoch::MIN)
    }

    pub fn set_single_table_watermark(&self, epoch: HummockEpoch, table_id: StorageTableId) {
        let mut table_watermark = self
            .table_epoch_watermark
            .entry(table_id)
            .or_insert(HummockEpoch::MIN);

        assert!(
            epoch > *table_watermark.value(),
            "not allowed to set epoch watermark to equal to or lower than current watermark: current is {}, epoch to set {}",
            *table_watermark.value(),
            epoch
        );

        *table_watermark = epoch;
    }

    pub fn set_watermark(&self, epoch: HummockEpoch, table_ids: Option<&Vec<StorageTableId>>) {
        let table_ids = table_ids.cloned().unwrap_or_else(|| {
            self.table_epoch_watermark
                .iter()
                .map(|entry| *entry.key())
                .collect()
        });

        table_ids.iter().for_each(|table_id| {
            self.set_single_table_watermark(epoch, *table_id);
        });
    }

    /// Checks whether there is key conflict for the given `kv_pairs` and adds the key in `kv_pairs`
    /// to the tracking history. Besides, whether the `epoch` has been archived will also be checked
    /// to avoid writing to a stale epoch
    pub fn check_conflict_and_track_write_batch(
        &self,
        kv_pairs: &[(Bytes, HummockValue<Bytes>)],
        epoch: HummockEpoch,
        table_id: StorageTableId,
    ) {
        assert!(
            epoch > self.get_epoch_watermark(table_id),
            "write to an archived epoch: {}",
            epoch
        );

        let mut epoch_written_key = self.epoch_history.entry(epoch).or_insert(HashMap::new());
        // check whether the key has been written in the epoch in any table
        epoch_written_key.values().for_each(|table_written_key| {
            for (key, value) in kv_pairs {
                assert!(
                    !table_written_key.contains(&key.to_vec()),
                    "key {:?} is written again after previously written, value is {:?}",
                    key,
                    value,
                );
            }
        });

        let table_written_key = epoch_written_key
            .entry(table_id)
            .or_insert_with(HashSet::new);

        // add the keys to history
        for (key, value) in kv_pairs.iter() {
            assert!(
                table_written_key.insert(key.to_vec()),
                "key {:?} is written again after previously written, value is {:?}",
                key,
                value,
            );
        }
    }

    /// Archives an epoch for a storage table. An archived epoch cannot be written anymore in the
    /// storage table.
    ///
    /// `table_ids` is an optional parameter that specifies which storage tables to archive. If
    /// `None`, all tables are archived.
    pub fn archive_epoch(&self, epoch: HummockEpoch, table_ids: Option<&Vec<StorageTableId>>) {
        if let Some(mut epoch_history) = self.epoch_history.get_mut(&epoch) {
            if let Some(table_ids) = table_ids {
                for table_id in table_ids {
                    epoch_history.remove(table_id);
                }
            } else {
                epoch_history.clear();
            }
        }
        self.epoch_history
            .remove_if(&epoch, |_, epoch_history| epoch_history.is_empty());
        self.set_watermark(epoch, table_ids);
    }
}

#[cfg(test)]
mod test {
    use std::iter::once;

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::hummock::conflict_detector::ConflictDetector;
    use crate::hummock::value::HummockValue;
    use crate::store::GLOBAL_STORAGE_TABLE_ID;

    #[test]
    #[should_panic]
    fn test_write_conflict_in_one_batch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            (0..2)
                .map(|_| {
                    (
                        Bytes::from("conflicted-key"),
                        HummockValue::Delete(Default::default()),
                    )
                })
                .into_iter()
                .collect_vec()
                .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
    }

    #[test]
    #[should_panic]
    fn test_write_conflict_in_multi_batch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("conflicted-key"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("conflicted-key"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
    }

    #[test]
    fn test_valid_write_in_multi_batch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key2"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
        detector.archive_epoch(233, Some(&vec![GLOBAL_STORAGE_TABLE_ID]));
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            234,
            GLOBAL_STORAGE_TABLE_ID,
        );
    }

    #[test]
    #[should_panic]
    fn test_write_to_archived_epoch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
        detector.archive_epoch(233, Some(&vec![GLOBAL_STORAGE_TABLE_ID]));
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
    }

    #[test]
    fn test_clear_key_after_epoch_archive() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
        assert!(!detector.epoch_history.get(&233).unwrap().is_empty());
        detector.archive_epoch(233, Some(&vec![GLOBAL_STORAGE_TABLE_ID]));
        assert!(detector.epoch_history.get(&233).is_none());
    }

    #[test]
    #[should_panic]
    fn test_write_below_epoch_watermark() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            233,
            GLOBAL_STORAGE_TABLE_ID,
        );
        detector.archive_epoch(233, Some(&vec![GLOBAL_STORAGE_TABLE_ID]));
        detector.check_conflict_and_track_write_batch(
            once((
                Bytes::from("key1"),
                HummockValue::Delete(Default::default()),
            ))
            .collect_vec()
            .as_slice(),
            232,
            GLOBAL_STORAGE_TABLE_ID,
        );
    }
}
