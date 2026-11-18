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
use std::collections::BTreeSet;
use std::path::PathBuf;

use bytes::Bytes;
use rw_loser_tree::LoserTree;
use tantivy::directory::{Directory, RamDirectory};
use tantivy::index::{Segment, SegmentComponent};
use tantivy::indexer::{merge_filtered_segments, merge_filtered_segments_with_doc_id_order};
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::{BitSet, DocAddress, DocSet, Index, SegmentReader, TERMINATED};

use crate::bundle::{
    BundleDirectory, DELETE_OPSTAMP, SegmentBundle, collect_segment_files, decode_bundle,
    decode_segment_meta, encode_segment_meta,
};
use crate::deletion_vector::{
    encode_deletion_vector, materialize_deletion_vector, read_alive_bitset,
};
use crate::keys::{KeyArray, KeyCursor, SortedKeyWriter};
use crate::{
    BuiltSegment, FullTextError, FullTextResult, full_text_error, full_text_schema, index_settings,
};

/// Tells whether a key is deleted by a pending marker newer than this segment.
pub type IsDeletedFn<'a> = &'a dyn Fn(&[u8]) -> bool;

/// A segment opened for reading.
pub struct OpenedSegment {
    segment: Segment,
    reader: SegmentReader,
    keys: KeyArray,
    deletion_vector_size: usize,
}

impl OpenedSegment {
    pub fn open(
        segment_meta: &[u8],
        bundle: Bytes,
        deletion_vector: Option<Bytes>,
    ) -> FullTextResult<Self> {
        let meta = decode_segment_meta(segment_meta)?;
        let mut files = decode_bundle(bundle)?;
        let keys = KeyArray::open(&mut files, meta.max_doc)?;
        let prefix = format!("{}.", meta.segment_id.uuid_string());
        if files.keys().any(|name| !name.starts_with(&prefix)) {
            return Err(FullTextError::decode(
                "unexpected file in full-text segment bundle",
            ));
        }
        let directory = BundleDirectory::new(files);
        let delete_path = PathBuf::from(format!(
            "{}.{}.del",
            meta.segment_id.uuid_string(),
            DELETE_OPSTAMP
        ));
        let deletion_vector_size = if let Some(deletion_vector) = deletion_vector {
            let native = materialize_deletion_vector(deletion_vector, meta.max_doc)?;
            directory
                .overlay()
                .atomic_write(&delete_path, &native)
                .map_err(full_text_error)?;
            Some(native.len())
        } else {
            None
        };
        let index = Index::create(
            directory,
            full_text_schema().schema.clone(),
            index_settings(),
        )
        .map_err(full_text_error)?;

        let mut segment_meta = index.new_segment_meta(meta.segment_id, meta.max_doc);
        if deletion_vector_size.is_some() {
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
        Ok(Self {
            segment,
            reader,
            keys,
            deletion_vector_size: deletion_vector_size.unwrap_or_default(),
        })
    }

    /// Number of documents in the segment, including deleted ones.
    pub fn num_docs(&self) -> u64 {
        self.reader.max_doc() as u64
    }

    pub fn num_deleted(&self) -> u64 {
        self.reader.num_deleted_docs() as u64
    }

    pub fn pk_directory_heap_size(&self) -> usize {
        self.keys.heap_size()
    }

    pub fn deletion_vector_size(&self) -> usize {
        self.deletion_vector_size
    }

    /// Keys of alive documents that contain ALL tokens. `tokens` is non-empty.
    ///
    /// Keys are returned sorted and de-duplicated.
    pub fn search(&self, tokens: &[String]) -> FullTextResult<Vec<Bytes>> {
        self.search_filtered(tokens, None, None)
    }

    /// Like [`Self::search`], but also drops a matched doc when `is_deleted(key)`
    /// returns true, and returns at most `limit` keys.
    pub fn search_filtered(
        &self,
        tokens: &[String],
        is_deleted: Option<IsDeletedFn<'_>>,
        limit: Option<usize>,
    ) -> FullTextResult<Vec<Bytes>> {
        if tokens.is_empty() {
            return Err(FullTextError::other(
                "full-text search requires at least one token",
            ));
        }
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let schema = full_text_schema();
        let inverted_index = self
            .reader
            .inverted_index(schema.tokens)
            .map_err(full_text_error)?;

        let mut unique_tokens: Vec<&str> = tokens.iter().map(String::as_str).collect();
        unique_tokens.sort_unstable();
        unique_tokens.dedup();
        let mut term_infos = Vec::with_capacity(unique_tokens.len());
        for token in unique_tokens {
            let term = Term::from_field_bytes(schema.tokens, token.as_bytes());
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
        let mut keys = BTreeSet::new();
        let mut pk_reader = self.keys.reader();
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
                let key = pk_reader.get(candidate)?;
                if !is_deleted.is_some_and(|is_deleted| is_deleted(key)) {
                    // Do not return slices that keep the whole bundle alive past cache eviction.
                    keys.insert(Bytes::copy_from_slice(key));
                }
                if limit.is_some_and(|limit| keys.len() >= limit) {
                    break;
                }
            }
            candidate = lead.advance();
        }
        Ok(keys.into_iter().collect())
    }

    /// Deletes every live document matching a key. The caller filters markers by segment epoch.
    ///
    /// Returns the new full deletion vector (old deletions + new ones) and the new
    /// `num_deleted`, or `None` if no doc is newly deleted.
    pub fn resolve_deletes(&self, keys: &[Bytes]) -> FullTextResult<Option<(Bytes, u64)>> {
        let max_doc = self.reader.max_doc();

        // Built lazily: most delete files do not touch most segments.
        let mut alive: Option<BitSet> = None;
        let mut pk_reader = self.keys.reader();
        for key in keys {
            for doc in pk_reader.find(key)? {
                if !self.reader.is_deleted(doc) {
                    alive
                        .get_or_insert_with(|| self.current_alive_bitset(max_doc))
                        .remove(doc);
                }
            }
        }

        match alive {
            None => Ok(None),
            Some(alive) => {
                let num_deleted = max_doc as u64 - alive.len() as u64;
                Ok(Some((encode_deletion_vector(&alive)?, num_deleted)))
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

    #[cfg(test)]
    pub fn alive_keys(&self) -> Vec<Bytes> {
        let mut pk_reader = self.keys.reader();
        self.reader
            .doc_ids_alive()
            .map(|doc| Bytes::copy_from_slice(pk_reader.get(doc).unwrap()))
            .collect()
    }
}

/// Merges segments in PK order, dropping deleted docs. Returns `None` if no alive doc remains.
pub fn merge_segments<S: Borrow<OpenedSegment>>(
    inputs: &[S],
) -> FullTextResult<Option<BuiltSegment>> {
    let inputs: Vec<_> = inputs
        .iter()
        .map(Borrow::borrow)
        .filter(|input| input.reader.num_docs() != 0)
        .collect();
    let num_docs = inputs
        .iter()
        .map(|input| input.reader.num_docs() as usize)
        .sum::<usize>();
    if num_docs == 0 {
        return Ok(None);
    }
    if num_docs >= TERMINATED as usize {
        return Err(FullTextError::other("merged full-text segment is full"));
    }

    let mut keys = SortedKeyWriter::new(num_docs)?;
    let output = RamDirectory::create();
    // Tantivy reads each segment's own deletion vector.
    let index = if let Some(inputs) = disjoint_pk_order(&inputs)? {
        for input in &inputs {
            let mut docs = input.reader.doc_ids_alive();
            let mut cursor = input
                .keys
                .cursor(docs.next().expect("merge inputs have live docs"))?;
            keys.push(cursor.key())?;
            for doc in docs {
                cursor.seek(doc)?;
                keys.push(cursor.key())?;
            }
        }
        let segments: Vec<_> = inputs.iter().map(|input| input.segment.clone()).collect();
        merge_filtered_segments(
            &segments,
            index_settings(),
            vec![None; segments.len()],
            output.clone(),
        )
    } else {
        let order = merge_pk_order(&inputs, &mut keys, num_docs)?;
        let segments: Vec<_> = inputs.iter().map(|input| input.segment.clone()).collect();
        merge_filtered_segments_with_doc_id_order(
            &segments,
            index_settings(),
            vec![None; segments.len()],
            order,
            output.clone(),
        )
    }
    .map_err(full_text_error)?;
    let index_meta = index.load_metas().map_err(full_text_error)?;
    let [segment_meta] = index_meta.segments.as_slice() else {
        return Err(FullTextError::other(format!(
            "expected exactly one merged full-text segment, got {}",
            index_meta.segments.len()
        )));
    };
    if segment_meta.max_doc() as usize != num_docs {
        return Err(FullTextError::other(
            "full-text merge must produce exactly one key per output document",
        ));
    }
    let mut files = collect_segment_files(&output, &index_meta, segment_meta)?;
    let (min_key, max_key) = keys.finish(&mut files)?;
    Ok(Some(BuiltSegment {
        bundle: SegmentBundle::new(files),
        segment_meta: encode_segment_meta(segment_meta.id(), segment_meta.max_doc()),
        num_docs: segment_meta.max_doc() as u64,
        min_key,
        max_key,
        deletion_vector: None,
    }))
}

/// Tantivy concatenates live docs in input order without a sort field. This gives the desired
/// output order when the inputs' PK ranges are strictly disjoint. Bounds include deleted docs,
/// so the test can be conservative. Touching ranges must preserve the usual input-index tie break.
fn disjoint_pk_order<'a>(
    inputs: &[&'a OpenedSegment],
) -> FullTextResult<Option<Vec<&'a OpenedSegment>>> {
    if inputs.len() <= 1 {
        return Ok(Some(inputs.to_vec()));
    }
    let mut ranges = inputs
        .iter()
        .map(|&input| {
            Ok((
                input,
                input.keys.cursor(0)?,
                input.keys.cursor(input.reader.max_doc() - 1)?,
            ))
        })
        .collect::<FullTextResult<Vec<_>>>()?;
    ranges.sort_unstable_by(|(_, a, _), (_, b, _)| a.key().cmp(b.key()));
    if !ranges
        .windows(2)
        .all(|pair| pair[0].2.key() < pair[1].1.key())
    {
        return Ok(None);
    }
    Ok(Some(
        ranges.into_iter().map(|(input, _, _)| input).collect(),
    ))
}

/// Emits output PKs and the corresponding source addresses in the same order.
fn merge_pk_order(
    inputs: &[&OpenedSegment],
    keys: &mut SortedKeyWriter,
    num_docs: usize,
) -> FullTextResult<Vec<DocAddress>> {
    let mut docs = Vec::with_capacity(inputs.len());
    let mut heads = Vec::with_capacity(inputs.len());
    for input in inputs {
        let mut alive_docs = input.reader.doc_ids_alive();
        let doc = alive_docs.next().expect("merge inputs have live docs");
        heads.push(Some((input.keys.cursor(doc)?, doc)));
        docs.push(alive_docs);
    }
    let mut tree = LoserTree::new(heads, |a: &(KeyCursor<'_>, _), b| a.0.key().cmp(b.0.key()));
    let mut order = Vec::with_capacity(num_docs);
    while tree.advance(|source, (cursor, doc)| -> FullTextResult<_> {
        keys.push(cursor.key())?;
        order.push(DocAddress::new(source as u32, *doc));
        let Some(next) = docs[source].next() else {
            return Ok(false);
        };
        cursor.seek(next)?;
        *doc = next;
        Ok(true)
    })? {}
    Ok(order)
}
