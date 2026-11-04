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

use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqDebug;
use tantivy::directory::{Directory, ManagedDirectory, RamDirectory, TerminatingWrite};
use tantivy::fastfield::{AliveBitSet, write_alive_bitset};
use tantivy::index::{Segment, SegmentComponent};
use tantivy::indexer::merge_filtered_segments;
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::{BitSet, DocId, DocSet, Index, SegmentReader, TERMINATED};

use super::bundle::{
    BundleDirectory, DELETE_OPSTAMP, collect_segment_files, decode_bundle, decode_segment_meta,
    encode_bundle, encode_segment_meta,
};
use super::{
    BuiltSegment, EPOCH_FIELD, KEY_FIELD, full_text_error, full_text_schema, index_settings,
};
use crate::hummock::{HummockError, HummockResult};

/// Serializes an alive bitset as a Tantivy delete file, including Tantivy's footer, so that it
/// can be placed next to the segment files as is.
pub(super) fn encode_alive_bitset(alive: &BitSet) -> HummockResult<Bytes> {
    let ram = RamDirectory::create();
    let directory = ManagedDirectory::wrap(Box::new(ram.clone())).map_err(full_text_error)?;
    let path = Path::new("alive.del");
    let mut writer = directory.open_write(path).map_err(full_text_error)?;
    write_alive_bitset(alive, &mut writer).map_err(full_text_error)?;
    writer.terminate().map_err(full_text_error)?;
    let data = ram
        .open_read(path)
        .map_err(full_text_error)?
        .read_bytes()
        .map_err(full_text_error)?;
    Ok(Bytes::copy_from_slice(data.as_slice()))
}

/// Reads and verifies a deletion vector placed in the directory of `index`.
fn read_alive_bitset(index: &Index, path: &Path, max_doc: u32) -> HummockResult<AliveBitSet> {
    let corrupted = || HummockError::decode_error("corrupted full-text deletion vector");
    let directory = index.directory();
    if !directory.validate_checksum(path).map_err(full_text_error)? {
        return Err(corrupted());
    }
    let data = directory
        .open_read(path)
        .map_err(full_text_error)?
        .read_bytes()
        .map_err(full_text_error)?;
    // Layout of a serialized `BitSet`: max_value: u32, then one u64 per 64 values.
    let expected_len = 4 + max_doc.div_ceil(64) as usize * 8;
    if data.len() != expected_len || data.as_slice()[..4] != max_doc.to_le_bytes() {
        return Err(corrupted());
    }
    Ok(AliveBitSet::open(data))
}

/// Tells whether the doc of a key written at an epoch is deleted.
pub type IsDeletedFn<'a> = &'a dyn Fn(&[u8], u64) -> bool;

/// A segment opened for reading.
pub struct OpenedSegment {
    segment: Segment,
    reader: SegmentReader,
}

impl OpenedSegment {
    pub fn open(
        segment_meta: &[u8],
        bundle: Bytes,
        deletion_vector: Option<Bytes>,
    ) -> HummockResult<Self> {
        let meta = decode_segment_meta(segment_meta)?;
        let files = decode_bundle(bundle)?;
        let directory = BundleDirectory::new(files);
        let delete_path = PathBuf::from(format!(
            "{}.{}.del",
            meta.segment_id.uuid_string(),
            DELETE_OPSTAMP
        ));
        if let Some(deletion_vector) = &deletion_vector {
            directory
                .overlay()
                .atomic_write(&delete_path, deletion_vector)
                .map_err(full_text_error)?;
        }
        let index = Index::create(
            directory,
            full_text_schema().schema.clone(),
            index_settings(),
        )
        .map_err(full_text_error)?;

        let mut segment_meta = index.new_segment_meta(meta.segment_id, meta.max_doc);
        if deletion_vector.is_some() {
            let alive = read_alive_bitset(&index, &delete_path, meta.max_doc)?;
            let num_deleted = meta.max_doc - alive.num_alive_docs() as u32;
            if num_deleted > 0 {
                segment_meta = segment_meta.with_delete_meta(num_deleted, DELETE_OPSTAMP);
                debug_assert_eq!(
                    segment_meta.relative_path(SegmentComponent::Delete),
                    delete_path
                );
            }
        }
        let segment = index.segment(segment_meta);
        let reader = SegmentReader::open(&segment).map_err(full_text_error)?;
        let opened = Self { segment, reader };
        let num_rows = opened.key_column()?.num_rows();
        if num_rows != meta.max_doc {
            return Err(HummockError::other(format!(
                "full-text segment meta has {} docs, but the bundle has {num_rows}",
                meta.max_doc
            )));
        }
        Ok(opened)
    }

    /// Number of documents in the segment, including deleted ones.
    pub fn num_docs(&self) -> u64 {
        self.reader.max_doc() as u64
    }

    pub fn num_deleted(&self) -> u64 {
        self.reader.num_deleted_docs() as u64
    }

    /// Keys of alive documents that contain ALL tokens. `tokens` is non-empty.
    ///
    /// Keys are returned sorted and de-duplicated.
    pub fn search(&self, tokens: &[String]) -> HummockResult<Vec<Bytes>> {
        self.search_filtered(tokens, None, None)
    }

    /// Like [`Self::search`], but also drops a matched doc when `is_deleted(key, doc_epoch)`
    /// returns true, and returns at most `limit` keys.
    pub fn search_filtered(
        &self,
        tokens: &[String],
        is_deleted: Option<IsDeletedFn<'_>>,
        limit: Option<usize>,
    ) -> HummockResult<Vec<Bytes>> {
        if tokens.is_empty() {
            return Err(HummockError::other(
                "full-text search requires at least one token",
            ));
        }
        let schema = full_text_schema();
        let inverted_index = self
            .reader
            .inverted_index(schema.text)
            .map_err(full_text_error)?;

        let mut unique_tokens: Vec<&str> = tokens.iter().map(String::as_str).collect();
        unique_tokens.sort_unstable();
        unique_tokens.dedup();
        let mut term_infos = Vec::with_capacity(unique_tokens.len());
        for token in unique_tokens {
            let term = Term::from_field_text(schema.text, token);
            match inverted_index
                .get_term_info(&term)
                .map_err(full_text_error)?
            {
                Some(term_info) => term_infos.push(term_info),
                None => return Ok(vec![]),
            }
        }
        // Drive the intersection with the rarest token.
        term_infos.sort_by_key(|term_info| term_info.doc_freq);
        let mut postings = term_infos
            .iter()
            .map(|term_info| {
                inverted_index.read_postings_from_terminfo(term_info, IndexRecordOption::Basic)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(full_text_error)?;

        let (lead, others) = postings.split_first_mut().expect("tokens are non-empty");
        let mut matched: Vec<DocId> = Vec::new();
        let mut candidate = lead.doc();
        'outer: while candidate != TERMINATED {
            for other in others.iter_mut() {
                // `seek` requires a target no smaller than the current doc, but another
                // posting list may already be past the candidate, e.g. at its first doc.
                let doc = if other.doc() < candidate {
                    other.seek(candidate)
                } else {
                    other.doc()
                };
                if doc > candidate {
                    candidate = lead.seek(doc);
                    continue 'outer;
                }
            }
            if !self.reader.is_deleted(candidate) {
                matched.push(candidate);
            }
            candidate = lead.advance();
        }
        let mut keys = match is_deleted {
            None => self.keys_of_docs(&matched)?,
            Some(is_deleted) => self.keys_of_docs_filtered(&matched, is_deleted)?,
        };
        if let Some(limit) = limit {
            keys.truncate(limit);
        }
        Ok(keys)
    }

    /// Returns the sorted, de-duplicated keys of the given docs that are not deleted by
    /// `is_deleted(key, doc_epoch)`. A key is kept if any of its docs is kept.
    fn keys_of_docs_filtered(
        &self,
        docs: &[DocId],
        is_deleted: IsDeletedFn<'_>,
    ) -> HummockResult<Vec<Bytes>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }
        let key_column = self.key_column()?;
        let epoch_column = self.epoch_column()?;
        let mut ord_epochs = docs
            .iter()
            .map(|&doc| {
                let ord = key_column
                    .ords()
                    .first(doc)
                    .ok_or_else(|| missing_key(doc))?;
                let epoch = epoch_column.first(doc).ok_or_else(|| missing_epoch(doc))?;
                Ok((ord, epoch))
            })
            .collect::<HummockResult<Vec<_>>>()?;
        ord_epochs.sort_unstable();
        let ords: Vec<u64> = ord_epochs.iter().map(|(ord, _)| *ord).dedup().collect();
        let mut all_keys = Vec::with_capacity(ords.len());
        let all_found = key_column
            .dictionary()
            .sorted_ords_to_term_cb(&ords, |key| all_keys.push(Bytes::copy_from_slice(key)))
            .map_err(full_text_error)?;
        if !all_found || all_keys.len() != ords.len() {
            return Err(HummockError::other(
                "full-text segment key dictionary is inconsistent",
            ));
        }
        let keys = ord_epochs
            .chunk_by(|(a, _), (b, _)| a == b)
            .zip_eq_debug(all_keys)
            .filter(|(group, key)| group.iter().any(|(_, epoch)| !is_deleted(key, *epoch)))
            .map(|(_, key)| key)
            .collect();
        Ok(keys)
    }

    /// Returns the sorted, de-duplicated keys of the given docs.
    fn keys_of_docs(&self, docs: &[DocId]) -> HummockResult<Vec<Bytes>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }
        let key_column = self.key_column()?;
        let mut ords = docs
            .iter()
            .map(|&doc| key_column.ords().first(doc).ok_or_else(|| missing_key(doc)))
            .collect::<HummockResult<Vec<_>>>()?;
        // Term ordinals follow key order, so sorting them sorts the keys.
        ords.sort_unstable();
        ords.dedup();
        let mut keys = Vec::with_capacity(ords.len());
        let all_found = key_column
            .dictionary()
            .sorted_ords_to_term_cb(&ords, |key| keys.push(Bytes::copy_from_slice(key)))
            .map_err(full_text_error)?;
        if !all_found {
            return Err(HummockError::other(
                "full-text segment key dictionary is inconsistent",
            ));
        }
        Ok(keys)
    }

    fn key_column(&self) -> HummockResult<tantivy::columnar::BytesColumn> {
        self.reader
            .fast_fields()
            .bytes(KEY_FIELD)
            .map_err(full_text_error)?
            .ok_or_else(|| HummockError::other("full-text segment has no key column"))
    }

    fn epoch_column(&self) -> HummockResult<tantivy::columnar::Column<u64>> {
        self.reader
            .fast_fields()
            .u64(EPOCH_FIELD)
            .map_err(full_text_error)
    }

    /// `markers` are `(key, marker_epoch)` pairs sorted by key (keys may repeat with different
    /// epochs). A marker deletes every alive doc with the same key whose epoch is
    /// `< marker_epoch`.
    ///
    /// Returns the new full deletion vector (old deletions + new ones) and the new
    /// `num_deleted`, or `None` if no doc is newly deleted.
    pub fn resolve_deletes(&self, markers: &[(Bytes, u64)]) -> HummockResult<Option<(Bytes, u64)>> {
        let schema = full_text_schema();
        let key_index = self
            .reader
            .inverted_index(schema.key)
            .map_err(full_text_error)?;
        let epoch_column = self.epoch_column()?;
        let max_doc = self.reader.max_doc();

        // Built lazily: most delete files do not touch most segments.
        let mut alive: Option<BitSet> = None;
        let mut newly_deleted = 0u32;
        // Only the largest marker epoch of a key matters.
        for group in markers.chunk_by(|(a, _), (b, _)| a == b) {
            let key = &group[0].0;
            let marker_epoch = group.iter().map(|(_, epoch)| *epoch).max().unwrap_or(0);
            let term = Term::from_field_bytes(schema.key, key);
            let Some(mut postings) = key_index
                .read_postings(&term, IndexRecordOption::Basic)
                .map_err(full_text_error)?
            else {
                continue;
            };
            let mut doc = postings.doc();
            while doc != TERMINATED {
                let is_alive = match &alive {
                    Some(alive) => alive.contains(doc),
                    None => !self.reader.is_deleted(doc),
                };
                if is_alive {
                    let epoch = epoch_column.first(doc).ok_or_else(|| missing_epoch(doc))?;
                    if epoch < marker_epoch {
                        alive
                            .get_or_insert_with(|| self.current_alive_bitset(max_doc))
                            .remove(doc);
                        newly_deleted += 1;
                    }
                }
                doc = postings.advance();
            }
        }

        match alive {
            None => Ok(None),
            Some(alive) => {
                debug_assert!(newly_deleted > 0);
                let num_deleted = max_doc as u64 - alive.len() as u64;
                Ok(Some((encode_alive_bitset(&alive)?, num_deleted)))
            }
        }
    }

    fn current_alive_bitset(&self, max_doc: u32) -> BitSet {
        let mut alive = BitSet::with_max_value_and_full(max_doc);
        if let Some(current) = self.reader.alive_bitset() {
            alive.intersect_update(current.bitset());
        }
        alive
    }

    /// Key and epoch ranges of the alive docs, or `None` if no doc is alive.
    fn alive_ranges(&self) -> HummockResult<Option<AliveRanges>> {
        let key_column = self.key_column()?;
        let epoch_column = self.epoch_column()?;
        let mut ranges: Option<(u64, u64, u64, u64)> = None;
        for doc in self.reader.doc_ids_alive() {
            let ord = key_column
                .ords()
                .first(doc)
                .ok_or_else(|| missing_key(doc))?;
            let epoch = epoch_column.first(doc).ok_or_else(|| missing_epoch(doc))?;
            let (min_ord, max_ord, min_epoch, max_epoch) =
                ranges.get_or_insert((ord, ord, epoch, epoch));
            *min_ord = (*min_ord).min(ord);
            *max_ord = (*max_ord).max(ord);
            *min_epoch = (*min_epoch).min(epoch);
            *max_epoch = (*max_epoch).max(epoch);
        }
        let Some((min_ord, max_ord, min_epoch, max_epoch)) = ranges else {
            return Ok(None);
        };
        let mut keys = Vec::with_capacity(2);
        let ords = if min_ord == max_ord {
            vec![min_ord]
        } else {
            vec![min_ord, max_ord]
        };
        key_column
            .dictionary()
            .sorted_ords_to_term_cb(&ords, |key| keys.push(Bytes::copy_from_slice(key)))
            .map_err(full_text_error)?;
        let (Some(min_key), Some(max_key)) = (keys.first(), keys.last()) else {
            return Err(HummockError::other(
                "full-text segment key dictionary is inconsistent",
            ));
        };
        Ok(Some(AliveRanges {
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            min_epoch,
            max_epoch,
        }))
    }

    /// Alive documents as (key, epoch), in doc id order.
    #[cfg(test)]
    pub(super) fn alive_docs(&self) -> HummockResult<Vec<(Bytes, u64)>> {
        let key_column = self.key_column()?;
        let epoch_column = self.epoch_column()?;
        let mut docs = Vec::new();
        for doc in self.reader.doc_ids_alive() {
            let ord = key_column
                .ords()
                .first(doc)
                .ok_or_else(|| missing_key(doc))?;
            let mut key = Vec::new();
            key_column
                .ord_to_bytes(ord, &mut key)
                .map_err(full_text_error)?;
            let epoch = epoch_column.first(doc).ok_or_else(|| missing_epoch(doc))?;
            docs.push((Bytes::from(key), epoch));
        }
        Ok(docs)
    }
}

struct AliveRanges {
    min_key: Bytes,
    max_key: Bytes,
    min_epoch: u64,
    max_epoch: u64,
}

fn missing_key(doc: DocId) -> HummockError {
    HummockError::other(format!("full-text doc {doc} has no key"))
}

fn missing_epoch(doc: DocId) -> HummockError {
    HummockError::other(format!("full-text doc {doc} has no epoch"))
}

/// Merges segments into one, dropping deleted docs. Returns `None` if no alive doc remains.
/// Keeps each doc's key and epoch.
///
/// Alive docs keep their relative order: the docs of `inputs[0]` come first, then those of
/// `inputs[1]`, and so on.
pub fn merge_segments<S: Borrow<OpenedSegment>>(
    inputs: &[S],
) -> HummockResult<Option<BuiltSegment>> {
    let mut ranges: Option<AliveRanges> = None;
    for input in inputs {
        let Some(input_ranges) = input.borrow().alive_ranges()? else {
            continue;
        };
        match &mut ranges {
            None => ranges = Some(input_ranges),
            Some(ranges) => {
                if input_ranges.min_key < ranges.min_key {
                    ranges.min_key = input_ranges.min_key;
                }
                if input_ranges.max_key > ranges.max_key {
                    ranges.max_key = input_ranges.max_key;
                }
                ranges.min_epoch = ranges.min_epoch.min(input_ranges.min_epoch);
                ranges.max_epoch = ranges.max_epoch.max(input_ranges.max_epoch);
            }
        }
    }
    let Some(ranges) = ranges else {
        return Ok(None);
    };

    let segments: Vec<Segment> = inputs
        .iter()
        .map(|input| input.borrow().segment.clone())
        .collect();
    let output = RamDirectory::create();
    // Each input's own deletion vector is applied by the merger, so no extra filter is needed.
    let index = merge_filtered_segments(
        &segments,
        index_settings(),
        segments.iter().map(|_| None).collect(),
        output.clone(),
    )
    .map_err(full_text_error)?;
    let index_meta = index.load_metas().map_err(full_text_error)?;
    let [segment_meta] = index_meta.segments.as_slice() else {
        return Err(HummockError::other(format!(
            "expected exactly one merged full-text segment, got {}",
            index_meta.segments.len()
        )));
    };
    let files = collect_segment_files(&output, &index_meta, segment_meta)?;
    Ok(Some(BuiltSegment {
        bundle: encode_bundle(&files),
        segment_meta: encode_segment_meta(segment_meta.id(), segment_meta.max_doc()),
        num_docs: segment_meta.max_doc() as u64,
        min_epoch: ranges.min_epoch,
        max_epoch: ranges.max_epoch,
        min_key: ranges.min_key,
        max_key: ranges.max_key,
        deletion_vector: None,
    }))
}
