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

use parking_lot::Mutex;
use tantivy::directory::RamDirectory;
use tantivy::indexer::DocIdMapping;
use tantivy::{BitSet, Index, SingleSegmentIndexWriter, TantivyDocument};

use crate::bundle::{SegmentBundle, collect_segment_files, encode_segment_meta};
use crate::deletion_vector::encode_deletion_vector;
use crate::keys::KeyArrayBuilder;
use crate::{
    BuiltSegment, FullTextError, FullTextResult, FullTextTokenSource, full_text_error,
    full_text_schema, index_settings,
};

/// Only sizes the initial term hash table of the Tantivy writer; it is not a hard limit.
const WRITER_MEMORY_BUDGET: usize = 16 << 20;

/// Tantivy doc ids must stay below `TERMINATED` (`i32::MAX`).
const MAX_DOCS_PER_SEGMENT: usize = i32::MAX as usize - 1;

/// Builds one segment in memory.
pub struct SegmentBuilder {
    directory: RamDirectory,
    /// Tantivy's writer is `Send` but not `Sync`. The mutex makes the builder `Sync`, so that
    /// async callers can hold a reference to it across an await point. It is only locked in
    /// `estimated_size`; other methods take `&mut self` and use `get_mut`.
    writer: Mutex<SingleSegmentIndexWriter<TantivyDocument>>,
    deleted: HashSet<u32>,
    keys: KeyArrayBuilder,
}

impl SegmentBuilder {
    pub fn new() -> FullTextResult<Self> {
        let directory = RamDirectory::create();
        let writer = Index::builder()
            .schema(full_text_schema().schema.clone())
            .settings(tantivy::IndexSettings {
                manual_doc_id_mapping: true,
                ..index_settings()
            })
            .single_segment_index_writer(directory.clone(), WRITER_MEMORY_BUDGET)
            .map_err(full_text_error)?;
        Ok(Self {
            directory,
            writer: Mutex::new(writer),
            deleted: HashSet::new(),
            keys: KeyArrayBuilder::default(),
        })
    }

    /// Adds a document. Returns a temporary doc ID for use with [`Self::delete`].
    ///
    /// `tokens` are indexed verbatim, without any further analysis.
    pub fn add(&mut self, key: &[u8], tokens: &mut FullTextTokenSource<'_>) -> FullTextResult<u32> {
        if self.num_docs() >= MAX_DOCS_PER_SEGMENT {
            return Err(FullTextError::other("full-text segment is full"));
        }
        let schema = full_text_schema();
        let mut doc = TantivyDocument::new();
        tokens(&mut |token| {
            doc.add_bytes(schema.tokens, token.as_bytes());
        });
        self.writer
            .get_mut()
            .add_document(doc)
            .map_err(full_text_error)?;

        let doc_id = self.num_docs() as u32;
        self.keys.push(key);
        Ok(doc_id)
    }

    /// Marks a previously added doc (in this builder) as deleted.
    pub fn delete(&mut self, doc_id: u32) {
        debug_assert!(
            (doc_id as usize) < self.num_docs(),
            "doc {doc_id} not in the builder"
        );
        if (doc_id as usize) < self.num_docs() {
            self.deleted.insert(doc_id);
        }
    }

    /// Number of added documents, including deleted ones.
    pub fn num_docs(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.num_docs() == 0
    }

    /// Rough memory usage, used by the caller to decide when to seal.
    pub fn estimated_size(&self) -> usize {
        self.writer.lock().mem_usage()
            + self.directory.total_mem_usage()
            + self.deleted.capacity() * size_of::<u32>() * 2
            + self.keys.estimated_size()
    }

    /// Seals the segment. Returns `None` if no documents were added. If every doc was deleted,
    /// the segment is still returned (with a deletion vector); the caller may drop it.
    pub fn finish(self) -> FullTextResult<Option<BuiltSegment>> {
        if self.is_empty() {
            return Ok(None);
        }
        let order = self.keys.pk_order();
        let index = {
            let mapping = DocIdMapping::new_permutation(order.clone()).map_err(full_text_error)?;
            self.writer
                .into_inner()
                .finalize_with_doc_id_mapping(&mapping)
                .map_err(full_text_error)?
        };
        let index_meta = index.load_metas().map_err(full_text_error)?;
        let [segment_meta] = index_meta.segments.as_slice() else {
            return Err(FullTextError::other(format!(
                "expected exactly one segment in a new full-text segment, got {}",
                index_meta.segments.len()
            )));
        };
        let max_doc = segment_meta.max_doc();
        if max_doc as usize != self.keys.len() {
            return Err(FullTextError::other(format!(
                "full-text segment has {max_doc} docs, expected {}",
                self.keys.len()
            )));
        }

        let mut files = collect_segment_files(&self.directory, &index_meta, segment_meta)?;
        let (min_key, max_key) = self.keys.finish(&mut files, Some(&order))?;
        let bundle = SegmentBundle::new(files);
        let segment_meta_bytes = encode_segment_meta(segment_meta.id(), max_doc);

        let deletion_vector = if self.deleted.is_empty() {
            None
        } else {
            let mut alive = BitSet::with_max_value_and_full(max_doc);
            for (doc, old_doc) in order.into_iter().enumerate() {
                if self.deleted.contains(&old_doc) {
                    alive.remove(doc as u32);
                }
            }
            Some((encode_deletion_vector(&alive)?, self.deleted.len() as u64))
        };

        Ok(Some(BuiltSegment {
            bundle,
            segment_meta: segment_meta_bytes,
            num_docs: max_doc as u64,
            min_key,
            max_key,
            deletion_vector,
        }))
    }
}
