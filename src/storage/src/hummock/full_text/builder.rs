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

use std::collections::HashSet;

use bytes::Bytes;
use parking_lot::Mutex;
use tantivy::directory::RamDirectory;
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{BitSet, Index, SingleSegmentIndexWriter, TantivyDocument};

use super::bundle::{collect_segment_files, encode_bundle, encode_segment_meta};
use super::segment::encode_alive_bitset;
use super::{BuiltSegment, full_text_error, full_text_schema, index_settings};
use crate::hummock::{HummockError, HummockResult};

/// Only sizes the initial term hash table of the Tantivy writer; it is not a hard limit.
const WRITER_MEMORY_BUDGET: usize = 16 << 20;

/// Tantivy doc ids must stay below `TERMINATED` (`i32::MAX`).
const MAX_DOCS_PER_SEGMENT: u32 = i32::MAX as u32 - 1;

/// Builds one segment in memory.
pub struct SegmentBuilder {
    directory: RamDirectory,
    /// Tantivy's writer is `Send` but not `Sync`. The mutex makes the builder `Sync`, so that
    /// async callers can hold a reference to it across an await point. It is only locked in
    /// `estimated_size`; other methods take `&mut self` and use `get_mut`.
    writer: Mutex<SingleSegmentIndexWriter<TantivyDocument>>,
    num_docs: u32,
    deleted: HashSet<u32>,
    key_range: Option<(Bytes, Bytes)>,
    min_epoch: u64,
    max_epoch: u64,
}

impl SegmentBuilder {
    pub fn new() -> HummockResult<Self> {
        let directory = RamDirectory::create();
        let writer = Index::builder()
            .schema(full_text_schema().schema.clone())
            .settings(index_settings())
            .single_segment_index_writer(directory.clone(), WRITER_MEMORY_BUDGET)
            .map_err(full_text_error)?;
        Ok(Self {
            directory,
            writer: Mutex::new(writer),
            num_docs: 0,
            deleted: HashSet::new(),
            key_range: None,
            min_epoch: u64::MAX,
            max_epoch: 0,
        })
    }

    /// Adds a document. Returns its doc id within this segment.
    ///
    /// `tokens` are indexed verbatim, without any further analysis.
    pub fn add(&mut self, key: &[u8], epoch: u64, tokens: &[String]) -> HummockResult<u32> {
        if self.num_docs >= MAX_DOCS_PER_SEGMENT {
            return Err(HummockError::other("full-text segment is full"));
        }
        let schema = full_text_schema();
        let mut doc = TantivyDocument::new();
        doc.add_bytes(schema.key, key);
        doc.add_u64(schema.epoch, epoch);
        let tokens = tokens
            .iter()
            .enumerate()
            .map(|(position, text)| Token {
                offset_from: 0,
                offset_to: 0,
                position,
                text: text.clone(),
                position_length: 1,
            })
            .collect();
        doc.add_pre_tokenized_text(
            schema.text,
            PreTokenizedString {
                text: String::new(),
                tokens,
            },
        );
        self.writer
            .get_mut()
            .add_document(doc)
            .map_err(full_text_error)?;

        let doc_id = self.num_docs;
        self.num_docs += 1;
        match &mut self.key_range {
            None => {
                let key = Bytes::copy_from_slice(key);
                self.key_range = Some((key.clone(), key));
            }
            Some((min_key, max_key)) => {
                if key < min_key.as_ref() {
                    *min_key = Bytes::copy_from_slice(key);
                } else if key > max_key.as_ref() {
                    *max_key = Bytes::copy_from_slice(key);
                }
            }
        }
        self.min_epoch = self.min_epoch.min(epoch);
        self.max_epoch = self.max_epoch.max(epoch);
        Ok(doc_id)
    }

    /// Marks a previously added doc (in this builder) as deleted.
    pub fn delete(&mut self, doc_id: u32) {
        debug_assert!(doc_id < self.num_docs, "doc {doc_id} not in the builder");
        if doc_id < self.num_docs {
            self.deleted.insert(doc_id);
        }
    }

    /// Number of added documents, including deleted ones.
    pub fn num_docs(&self) -> usize {
        self.num_docs as usize
    }

    pub fn is_empty(&self) -> bool {
        self.num_docs == 0
    }

    /// Rough memory usage, used by the caller to decide when to seal.
    pub fn estimated_size(&self) -> usize {
        let key_range_size = self
            .key_range
            .as_ref()
            .map_or(0, |(min_key, max_key)| min_key.len() + max_key.len());
        self.writer.lock().mem_usage()
            + self.directory.total_mem_usage()
            + self.deleted.capacity() * size_of::<u32>() * 2
            + key_range_size
    }

    /// Seals the segment. Returns `None` if no documents were added. If every doc was deleted,
    /// the segment is still returned (with a deletion vector); the caller may drop it.
    pub fn finish(self) -> HummockResult<Option<BuiltSegment>> {
        let Some((min_key, max_key)) = self.key_range else {
            return Ok(None);
        };
        let index = self
            .writer
            .into_inner()
            .finalize()
            .map_err(full_text_error)?;
        let index_meta = index.load_metas().map_err(full_text_error)?;
        let [segment_meta] = index_meta.segments.as_slice() else {
            return Err(HummockError::other(format!(
                "expected exactly one segment in a new full-text segment, got {}",
                index_meta.segments.len()
            )));
        };
        let max_doc = segment_meta.max_doc();
        if max_doc != self.num_docs {
            return Err(HummockError::other(format!(
                "full-text segment has {max_doc} docs, expected {}",
                self.num_docs
            )));
        }

        let files = collect_segment_files(&self.directory, &index_meta, segment_meta)?;
        let bundle = encode_bundle(&files);
        let segment_meta_bytes = encode_segment_meta(segment_meta.id(), max_doc);

        let deletion_vector = if self.deleted.is_empty() {
            None
        } else {
            let mut alive = BitSet::with_max_value_and_full(max_doc);
            for &doc_id in &self.deleted {
                alive.remove(doc_id);
            }
            Some((encode_alive_bitset(&alive)?, self.deleted.len() as u64))
        };

        Ok(Some(BuiltSegment {
            bundle,
            segment_meta: segment_meta_bytes,
            num_docs: max_doc as u64,
            min_epoch: self.min_epoch,
            max_epoch: self.max_epoch,
            min_key,
            max_key,
            deletion_vector,
        }))
    }
}
