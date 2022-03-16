use bytes::Bytes;
/// This mod implements a `ConflictDetector` that  detect write key conflict in each epoch
use dashmap::{DashMap, DashSet};

use crate::hummock::value::HummockValue;
use crate::hummock::HummockEpoch;

struct EpochStatus {
    written_key: DashSet<Bytes>,
    deleted: bool,
}

impl EpochStatus {
    fn new() -> EpochStatus {
        EpochStatus {
            written_key: DashSet::new(),
            deleted: false,
        }
    }
}

pub struct ConflictDetector {
    // epoch -> (key-sets, epoch-deleted)
    epoch_history: DashMap<HummockEpoch, EpochStatus>,
}

impl ConflictDetector {
    pub fn new() -> ConflictDetector {
        ConflictDetector {
            epoch_history: DashMap::new(),
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
        let entry = self
            .epoch_history
            .entry(epoch)
            .or_insert(EpochStatus::new());

        assert!(
            !entry.value().deleted,
            "write to an archived epoch: {}",
            epoch
        );

        for (key, _) in kv_pairs.iter() {
            assert!(
                entry.written_key.insert(key.clone()),
                "key {:?} is written again after previously written",
                key
            );
        }
    }

    /// Archive an epoch. An archived epoch cannot be written anymore.
    pub fn archive_epoch(&self, epoch: HummockEpoch) {
        let mut entry = self
            .epoch_history
            .entry(epoch)
            .or_insert(EpochStatus::new());
        assert!(!entry.deleted, "archive an archived epoch: {}", epoch);
        entry.written_key.clear();
        entry.deleted = true;
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
        assert!(!detector
            .epoch_history
            .get(&233)
            .unwrap()
            .written_key
            .is_empty());
        detector.archive_epoch(233);
        assert!(detector
            .epoch_history
            .get(&233)
            .unwrap()
            .written_key
            .is_empty());
    }
}
