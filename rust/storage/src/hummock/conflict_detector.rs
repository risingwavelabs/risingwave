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
//
//! This mod implements a `ConflictDetector` that  detect write key conflict in each epoch

use std::collections::HashSet;

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};

use crate::hummock::value::HummockValue;
use crate::hummock::HummockEpoch;

pub struct ConflictDetector {
    // epoch -> key-sets
    epoch_history: DashMap<HummockEpoch, HashSet<Bytes>>,
    epoch_watermark: AtomicCell<HummockEpoch>,
}

impl ConflictDetector {
    pub fn new() -> ConflictDetector {
        ConflictDetector {
            epoch_history: DashMap::new(),
            epoch_watermark: AtomicCell::new(HummockEpoch::MIN),
        }
    }

    pub fn get_epoch_watermark(&self) -> HummockEpoch {
        self.epoch_watermark.load()
    }

    pub fn set_watermark(&self, epoch: HummockEpoch) {
        // set the new watermark with CAS to enable detection in concurrent update
        loop {
            let current_watermark = self.get_epoch_watermark();
            assert!(
                epoch > current_watermark,
                "not allowed to set epoch watermark to equal to or lower than current watermark: current is {}, epoch to set {}",
                current_watermark,
                epoch
            );
            if self
                .epoch_watermark
                .compare_exchange(current_watermark, epoch)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Check whether there is key conflict for the given `kv_pairs` and add the key in `kv_pairs`
    /// to the tracking history. Besides, whether the `epoch` has been archived will also be checked
    /// to avoid writing to a stale epoch
    pub fn check_conflict_and_track_write_batch(
        &self,
        kv_pairs: &[(Bytes, HummockValue<Bytes>)],
        epoch: HummockEpoch,
    ) {
        assert!(
            epoch > self.get_epoch_watermark(),
            "write to an archived epoch: {}",
            epoch
        );

        let mut written_key = self.epoch_history.entry(epoch).or_insert(HashSet::new());

        for (key, value) in kv_pairs.iter() {
            assert!(
                written_key.insert(key.clone()),
                "key {:?} is written again after previously written, value is {:?}",
                key,
                value,
            );
        }
    }

    /// Archive an epoch. An archived epoch cannot be written anymore.
    pub fn archive_epoch(&self, epoch: HummockEpoch) {
        self.epoch_history.remove(&epoch);
        self.set_watermark(epoch);
    }
}

#[cfg(test)]
mod test {
    use std::iter::once;

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::hummock::conflict_detector::ConflictDetector;
    use crate::hummock::value::HummockValue;

    #[test]
    #[should_panic]
    fn test_write_conflict_in_one_batch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            (0..2)
                .map(|_| (Bytes::from("conflicted-key"), HummockValue::Delete))
                .into_iter()
                .collect_vec()
                .as_slice(),
            233,
        );
    }

    #[test]
    #[should_panic]
    fn test_write_conflict_in_multi_batch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("conflicted-key"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("conflicted-key"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
    }

    #[test]
    fn test_valid_write_in_multi_batch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key2"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
        detector.archive_epoch(233);
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            234,
        );
    }

    #[test]
    #[should_panic]
    fn test_write_to_archived_epoch() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
        detector.archive_epoch(233);
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
    }

    #[test]
    fn test_clear_key_after_epoch_archive() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
        assert!(!detector.epoch_history.get(&233).unwrap().is_empty());
        detector.archive_epoch(233);
        assert!(detector.epoch_history.get(&233).is_none());
    }

    #[test]
    #[should_panic]
    fn test_write_below_epoch_watermark() {
        let detector = ConflictDetector::new();
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            233,
        );
        detector.archive_epoch(233);
        detector.check_conflict_and_track_write_batch(
            once((Bytes::from("key1"), HummockValue::Delete))
                .collect_vec()
                .as_slice(),
            232,
        );
    }
}
