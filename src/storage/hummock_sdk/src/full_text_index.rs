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

//! Hummock version state of a full-text index.
//!
//! A full-text index is a list of immutable segments plus delete files whose markers have not
//! been resolved into per-segment deletion vectors yet. All changes are committed through
//! [`FullTextIndexDelta`] together with the epoch's SSTs, so the Hummock version acts as the
//! manifest of the index.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_pb::catalog::PbFullTextIndexInfo;
use risingwave_pb::hummock::full_text_index_delta::{PbFullTextIndexAdds, PbFullTextIndexInit};
use risingwave_pb::hummock::{
    PbFullTextDeleteFileInfo, PbFullTextDeletionVectorInfo, PbFullTextDeletionVectorUpdate,
    PbFullTextIndex, PbFullTextIndexAdd, PbFullTextIndexDelta, PbFullTextSegmentInfo,
    full_text_index_delta,
};

use crate::{
    HummockFullTextDeleteFileId, HummockFullTextDeletionVectorId, HummockFullTextSegmentId,
    HummockObjectId,
};

#[derive(Clone, Debug, PartialEq)]
pub struct FullTextDeletionVectorInfo {
    pub object_id: HummockFullTextDeletionVectorId,
    pub file_size: u64,
    pub num_deleted: u64,
}

impl From<PbFullTextDeletionVectorInfo> for FullTextDeletionVectorInfo {
    fn from(pb: PbFullTextDeletionVectorInfo) -> Self {
        Self {
            object_id: pb.object_id,
            file_size: pb.file_size,
            num_deleted: pb.num_deleted,
        }
    }
}

impl From<FullTextDeletionVectorInfo> for PbFullTextDeletionVectorInfo {
    fn from(info: FullTextDeletionVectorInfo) -> Self {
        Self {
            object_id: info.object_id,
            file_size: info.file_size,
            num_deleted: info.num_deleted,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FullTextSegmentInfo {
    pub object_id: HummockFullTextSegmentId,
    pub file_size: u64,
    pub segment_meta: Bytes,
    pub num_docs: u64,
    pub min_epoch: u64,
    pub max_epoch: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub deletion_vector: Option<FullTextDeletionVectorInfo>,
}

impl FullTextSegmentInfo {
    pub fn num_deleted(&self) -> u64 {
        self.deletion_vector.as_ref().map_or(0, |dv| dv.num_deleted)
    }

    pub fn num_alive(&self) -> u64 {
        self.num_docs - self.num_deleted()
    }

    /// Whether a delete marker of `key` written at `marker_epoch` may delete a document in this
    /// segment.
    pub fn may_be_deleted_by(&self, key: &[u8], marker_epoch: u64) -> bool {
        self.min_epoch < marker_epoch
            && self.min_key.as_ref() <= key
            && key <= self.max_key.as_ref()
    }
}

impl From<PbFullTextSegmentInfo> for FullTextSegmentInfo {
    fn from(pb: PbFullTextSegmentInfo) -> Self {
        Self {
            object_id: pb.object_id,
            file_size: pb.file_size,
            segment_meta: pb.segment_meta.into(),
            num_docs: pb.num_docs,
            min_epoch: pb.min_epoch,
            max_epoch: pb.max_epoch,
            min_key: pb.min_key.into(),
            max_key: pb.max_key.into(),
            deletion_vector: pb.deletion_vector.map(Into::into),
        }
    }
}

impl From<FullTextSegmentInfo> for PbFullTextSegmentInfo {
    fn from(info: FullTextSegmentInfo) -> Self {
        Self {
            object_id: info.object_id,
            file_size: info.file_size,
            segment_meta: info.segment_meta.into(),
            num_docs: info.num_docs,
            min_epoch: info.min_epoch,
            max_epoch: info.max_epoch,
            min_key: info.min_key.into(),
            max_key: info.max_key.into(),
            deletion_vector: info.deletion_vector.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FullTextDeleteFileInfo {
    pub object_id: HummockFullTextDeleteFileId,
    pub file_size: u64,
    /// Markers in this file delete documents written before this epoch.
    pub epoch: u64,
    pub num_keys: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
}

impl From<PbFullTextDeleteFileInfo> for FullTextDeleteFileInfo {
    fn from(pb: PbFullTextDeleteFileInfo) -> Self {
        Self {
            object_id: pb.object_id,
            file_size: pb.file_size,
            epoch: pb.epoch,
            num_keys: pb.num_keys,
            min_key: pb.min_key.into(),
            max_key: pb.max_key.into(),
        }
    }
}

impl From<FullTextDeleteFileInfo> for PbFullTextDeleteFileInfo {
    fn from(info: FullTextDeleteFileInfo) -> Self {
        Self {
            object_id: info.object_id,
            file_size: info.file_size,
            epoch: info.epoch,
            num_keys: info.num_keys,
            min_key: info.min_key.into(),
            max_key: info.max_key.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FullTextIndex {
    pub info: PbFullTextIndexInfo,
    pub segments: Vec<FullTextSegmentInfo>,
    pub pending_delete_files: Vec<FullTextDeleteFileInfo>,
}

impl FullTextIndex {
    pub fn new(info: &PbFullTextIndexInfo) -> Self {
        Self {
            info: info.clone(),
            segments: vec![],
            pending_delete_files: vec![],
        }
    }

    pub fn get_objects(&self) -> impl Iterator<Item = (HummockObjectId, u64)> + '_ {
        self.segments
            .iter()
            .flat_map(|segment| {
                std::iter::once((
                    HummockObjectId::FullTextSegment(segment.object_id),
                    segment.file_size,
                ))
                .chain(segment.deletion_vector.as_ref().map(|dv| {
                    (
                        HummockObjectId::FullTextDeletionVector(dv.object_id),
                        dv.file_size,
                    )
                }))
            })
            .chain(self.pending_delete_files.iter().map(|file| {
                (
                    HummockObjectId::FullTextDeleteFile(file.object_id),
                    file.file_size,
                )
            }))
    }

    pub fn apply_add(&mut self, add: &FullTextIndexAdd) {
        if !add.removed_segments.is_empty() {
            let removed: HashSet<_> = add.removed_segments.iter().copied().collect();
            let prev_len = self.segments.len();
            self.segments
                .retain(|segment| !removed.contains(&segment.object_id));
            assert_eq!(
                prev_len - self.segments.len(),
                removed.len(),
                "removed full-text segments {:?} not all exist",
                add.removed_segments
            );
        }
        for segment in &add.added_segments {
            assert!(
                self.segments
                    .iter()
                    .all(|existing| existing.object_id != segment.object_id),
                "full-text segment {} already exists",
                segment.object_id
            );
            self.segments.push(segment.clone());
        }
        for (segment_id, dv) in &add.deletion_vector_updates {
            let segment = self
                .segments
                .iter_mut()
                .find(|segment| segment.object_id == *segment_id)
                .unwrap_or_else(|| {
                    panic!("full-text segment {segment_id} of deletion vector update not found")
                });
            assert!(dv.num_deleted <= segment.num_docs);
            segment.deletion_vector = Some(dv.clone());
        }
        if !add.consumed_delete_files.is_empty() {
            let consumed: HashSet<_> = add.consumed_delete_files.iter().copied().collect();
            let prev_len = self.pending_delete_files.len();
            self.pending_delete_files
                .retain(|file| !consumed.contains(&file.object_id));
            assert_eq!(
                prev_len - self.pending_delete_files.len(),
                consumed.len(),
                "consumed full-text delete files {:?} not all exist",
                add.consumed_delete_files
            );
        }
        self.pending_delete_files
            .extend(add.added_delete_files.iter().cloned());
    }
}

impl From<PbFullTextIndex> for FullTextIndex {
    fn from(pb: PbFullTextIndex) -> Self {
        Self {
            info: pb.info.unwrap(),
            segments: pb.segments.into_iter().map(Into::into).collect(),
            pending_delete_files: pb
                .pending_delete_files
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<FullTextIndex> for PbFullTextIndex {
    fn from(index: FullTextIndex) -> Self {
        Self {
            info: Some(index.info),
            segments: index.segments.into_iter().map(Into::into).collect(),
            pending_delete_files: index
                .pending_delete_files
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

/// Changes to a full-text index committed in one epoch. They are applied in field order.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FullTextIndexAdd {
    pub removed_segments: Vec<HummockFullTextSegmentId>,
    pub added_segments: Vec<FullTextSegmentInfo>,
    pub deletion_vector_updates: Vec<(HummockFullTextSegmentId, FullTextDeletionVectorInfo)>,
    pub consumed_delete_files: Vec<HummockFullTextDeleteFileId>,
    pub added_delete_files: Vec<FullTextDeleteFileInfo>,
}

impl FullTextIndexAdd {
    pub fn is_empty(&self) -> bool {
        self.removed_segments.is_empty()
            && self.added_segments.is_empty()
            && self.deletion_vector_updates.is_empty()
            && self.consumed_delete_files.is_empty()
            && self.added_delete_files.is_empty()
    }

    pub fn newly_added_objects(&self) -> impl Iterator<Item = (HummockObjectId, u64)> + '_ {
        self.added_segments
            .iter()
            .flat_map(|segment| {
                std::iter::once((
                    HummockObjectId::FullTextSegment(segment.object_id),
                    segment.file_size,
                ))
                .chain(segment.deletion_vector.as_ref().map(|dv| {
                    (
                        HummockObjectId::FullTextDeletionVector(dv.object_id),
                        dv.file_size,
                    )
                }))
            })
            .chain(self.deletion_vector_updates.iter().map(|(_, dv)| {
                (
                    HummockObjectId::FullTextDeletionVector(dv.object_id),
                    dv.file_size,
                )
            }))
            .chain(self.added_delete_files.iter().map(|file| {
                (
                    HummockObjectId::FullTextDeleteFile(file.object_id),
                    file.file_size,
                )
            }))
    }
}

impl From<PbFullTextIndexAdd> for FullTextIndexAdd {
    fn from(pb: PbFullTextIndexAdd) -> Self {
        Self {
            removed_segments: pb.removed_segments,
            added_segments: pb.added_segments.into_iter().map(Into::into).collect(),
            deletion_vector_updates: pb
                .deletion_vector_updates
                .into_iter()
                .map(|update| {
                    (
                        update.segment_object_id,
                        update.deletion_vector.unwrap().into(),
                    )
                })
                .collect(),
            consumed_delete_files: pb.consumed_delete_files,
            added_delete_files: pb.added_delete_files.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<FullTextIndexAdd> for PbFullTextIndexAdd {
    fn from(add: FullTextIndexAdd) -> Self {
        Self {
            removed_segments: add.removed_segments,
            added_segments: add.added_segments.into_iter().map(Into::into).collect(),
            deletion_vector_updates: add
                .deletion_vector_updates
                .into_iter()
                .map(|(segment_object_id, dv)| PbFullTextDeletionVectorUpdate {
                    segment_object_id,
                    deletion_vector: Some(dv.into()),
                })
                .collect(),
            consumed_delete_files: add.consumed_delete_files,
            added_delete_files: add.added_delete_files.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FullTextIndexDelta {
    /// Creates the index at the first barrier of `CREATE INDEX`.
    Init(PbFullTextIndexInfo),
    /// Changes committed by the index writer, applied in order.
    Adds(Vec<FullTextIndexAdd>),
}

impl FullTextIndexDelta {
    pub fn newly_added_objects(&self) -> impl Iterator<Item = (HummockObjectId, u64)> + '_ {
        match self {
            FullTextIndexDelta::Init(_) => None,
            FullTextIndexDelta::Adds(adds) => {
                Some(adds.iter().flat_map(|add| add.newly_added_objects()))
            }
        }
        .into_iter()
        .flatten()
    }
}

impl From<PbFullTextIndexDelta> for FullTextIndexDelta {
    fn from(pb: PbFullTextIndexDelta) -> Self {
        match pb.delta.unwrap() {
            full_text_index_delta::Delta::Init(init) => Self::Init(init.info.unwrap()),
            full_text_index_delta::Delta::Adds(adds) => {
                Self::Adds(adds.adds.into_iter().map(Into::into).collect())
            }
        }
    }
}

impl From<FullTextIndexDelta> for PbFullTextIndexDelta {
    fn from(delta: FullTextIndexDelta) -> Self {
        let delta = match delta {
            FullTextIndexDelta::Init(info) => {
                full_text_index_delta::Delta::Init(PbFullTextIndexInit { info: Some(info) })
            }
            FullTextIndexDelta::Adds(adds) => {
                full_text_index_delta::Delta::Adds(PbFullTextIndexAdds {
                    adds: adds.into_iter().map(Into::into).collect(),
                })
            }
        };
        Self { delta: Some(delta) }
    }
}

pub fn apply_full_text_index_delta(
    indexes: &mut HashMap<TableId, FullTextIndex>,
    deltas: &HashMap<TableId, FullTextIndexDelta>,
    removed_table_ids: &HashSet<TableId>,
) {
    for (table_id, delta) in deltas {
        match delta {
            FullTextIndexDelta::Init(info) => {
                indexes
                    .try_insert(*table_id, FullTextIndex::new(info))
                    .unwrap();
            }
            FullTextIndexDelta::Adds(adds) => {
                let index = indexes.get_mut(table_id).unwrap_or_else(|| {
                    panic!("full-text index {table_id} not found when applying adds")
                });
                for add in adds {
                    index.apply_add(add);
                }
            }
        }
    }
    indexes.retain(|table_id, _| !removed_table_ids.contains(table_id));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(id: u64, num_docs: u64) -> FullTextSegmentInfo {
        FullTextSegmentInfo {
            object_id: id.into(),
            file_size: 100,
            segment_meta: Bytes::new(),
            num_docs,
            min_epoch: 1,
            max_epoch: 2,
            min_key: Bytes::from_static(b"a"),
            max_key: Bytes::from_static(b"m"),
            deletion_vector: None,
        }
    }

    fn delete_file(id: u64) -> FullTextDeleteFileInfo {
        FullTextDeleteFileInfo {
            object_id: id.into(),
            file_size: 10,
            epoch: 3,
            num_keys: 1,
            min_key: Bytes::from_static(b"b"),
            max_key: Bytes::from_static(b"b"),
        }
    }

    #[test]
    fn test_apply_full_text_index_add() {
        let mut index = FullTextIndex::new(&PbFullTextIndexInfo::default());
        index.apply_add(&FullTextIndexAdd {
            added_segments: vec![segment(1, 10), segment(2, 10)],
            added_delete_files: vec![delete_file(3)],
            ..Default::default()
        });
        assert_eq!(index.get_objects().count(), 3);

        let dv = FullTextDeletionVectorInfo {
            object_id: 4.into(),
            file_size: 5,
            num_deleted: 1,
        };
        let add = FullTextIndexAdd {
            deletion_vector_updates: vec![(1.into(), dv)],
            consumed_delete_files: vec![3.into()],
            ..Default::default()
        };
        assert_eq!(
            add.newly_added_objects().collect::<Vec<_>>(),
            vec![(HummockObjectId::FullTextDeletionVector(4.into()), 5)]
        );
        let pb: PbFullTextIndexAdd = add.clone().into();
        assert_eq!(FullTextIndexAdd::from(pb), add);
        index.apply_add(&add);
        assert!(index.pending_delete_files.is_empty());
        assert_eq!(index.segments[0].num_alive(), 9);

        index.apply_add(&FullTextIndexAdd {
            removed_segments: vec![1.into(), 2.into()],
            added_segments: vec![segment(5, 19)],
            ..Default::default()
        });
        assert_eq!(
            index.get_objects().collect::<Vec<_>>(),
            vec![(HummockObjectId::FullTextSegment(5.into()), 100)]
        );
        let pb: PbFullTextIndex = index.clone().into();
        assert_eq!(FullTextIndex::from(pb), index);
    }

    #[test]
    #[should_panic]
    fn test_remove_missing_segment() {
        let mut index = FullTextIndex::new(&PbFullTextIndexInfo::default());
        index.apply_add(&FullTextIndexAdd {
            removed_segments: vec![1.into()],
            ..Default::default()
        });
    }

    #[test]
    fn test_apply_full_text_index_delta() {
        let table_id = TableId::new(1);
        let info = PbFullTextIndexInfo {
            analyzer: "simple".to_owned(),
            analyzer_version: 1,
            is_jsonb: false,
            format_version: 1,
        };
        let mut indexes = HashMap::new();
        let init = FullTextIndexDelta::Init(info.clone());
        assert_eq!(init.newly_added_objects().count(), 0);
        apply_full_text_index_delta(
            &mut indexes,
            &HashMap::from_iter([(table_id, init.clone())]),
            &HashSet::new(),
        );
        assert_eq!(indexes[&table_id].info, info);

        let adds = FullTextIndexDelta::Adds(vec![
            FullTextIndexAdd {
                added_segments: vec![segment(1, 10)],
                ..Default::default()
            },
            FullTextIndexAdd {
                added_delete_files: vec![delete_file(2)],
                ..Default::default()
            },
        ]);
        assert_eq!(adds.newly_added_objects().count(), 2);
        for delta in [&init, &adds] {
            let pb: PbFullTextIndexDelta = delta.clone().into();
            assert_eq!(&FullTextIndexDelta::from(pb), delta);
        }
        apply_full_text_index_delta(
            &mut indexes,
            &HashMap::from_iter([(table_id, adds)]),
            &HashSet::new(),
        );
        assert_eq!(indexes[&table_id].get_objects().count(), 2);

        apply_full_text_index_delta(
            &mut indexes,
            &HashMap::new(),
            &HashSet::from_iter([table_id]),
        );
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_may_be_deleted_by() {
        let segment = segment(1, 10);
        assert!(segment.may_be_deleted_by(b"b", 2));
        assert!(!segment.may_be_deleted_by(b"b", 1));
        assert!(!segment.may_be_deleted_by(b"z", 2));
    }
}
