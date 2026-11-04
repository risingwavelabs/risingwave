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

//! Native full-text index, built on Tantivy.
//!
//! [`objects`] reads and writes the objects of the index, and [`writer`] is the singleton index
//! writer. Everything else in this module is the segment layer, which is synchronous and
//! in-memory. It never touches object storage, Hummock versions or an async runtime: callers
//! upload and download the produced objects and run these functions on blocking threads.
//!
//! Objects produced by the segment layer:
//! - **Segment bundle**: all files of one Tantivy segment packed into one blob (see
//!   [`bundle`](self::bundle) for the layout), plus a small opaque `segment_meta`.
//! - **Deletion vector**: the alive-bitset (`.del`) file of a segment, in Tantivy's own format,
//!   so that Tantivy's readers and merger skip deleted documents natively.
//! - **Delete file**: the sorted keys deleted at one epoch.

mod builder;
mod bundle;
mod delete_file;
pub(crate) mod objects;
mod segment;
#[cfg(test)]
mod tests;
pub(crate) mod writer;

use std::sync::LazyLock;

use bytes::Bytes;
use tantivy::IndexSettings;
use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::store::Compressor;
use thiserror_ext::AsReport;

pub use self::builder::SegmentBuilder;
pub use self::delete_file::{decode_delete_file, encode_delete_file};
pub use self::segment::{OpenedSegment, merge_segments};
use crate::hummock::HummockError;

/// Version of the on-object formats (bundle, segment meta, delete file) and of the Tantivy
/// schema and settings used to write segments.
pub const FULL_TEXT_FORMAT_VERSION: u32 = 1;

/// A sealed segment, ready to be uploaded.
#[derive(Debug, Clone)]
pub struct BuiltSegment {
    /// Content of the segment bundle object.
    pub bundle: Bytes,
    /// Opaque, small metadata needed to open the segment later (stored in the Hummock version
    /// as bytes).
    pub segment_meta: Bytes,
    /// Number of documents in the segment, including deleted ones.
    pub num_docs: u64,
    pub min_epoch: u64,
    pub max_epoch: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    /// Present when some docs are already deleted: (deletion vector object content,
    /// `num_deleted`).
    pub deletion_vector: Option<(Bytes, u64)>,
}

const KEY_FIELD: &str = "key";
const EPOCH_FIELD: &str = "epoch";
const TEXT_FIELD: &str = "text";

/// The Tantivy schema shared by all segments. Merging requires identical schemas, so it must
/// only be defined here.
struct FullTextSchema {
    schema: Schema,
    /// Memcomparable-encoded stream key. Indexed to find a document by key, and fast to emit
    /// keys from search results.
    key: Field,
    /// Epoch in which the document was written.
    epoch: Field,
    /// Pre-analyzed tokens. Term frequencies and field norms are kept so that BM25 can be added
    /// later without reindexing.
    text: Field,
}

static FULL_TEXT_SCHEMA: LazyLock<FullTextSchema> = LazyLock::new(|| {
    let mut builder = Schema::builder();
    let key = builder.add_bytes_field(KEY_FIELD, BytesOptions::default().set_indexed().set_fast());
    let epoch = builder.add_u64_field(EPOCH_FIELD, NumericOptions::default().set_fast());
    // Tokens are fed as `PreTokenizedString`, so the tokenizer is never used. "raw" is
    // registered by default and keeps the writer from failing on an unknown tokenizer.
    let text = builder.add_text_field(
        TEXT_FIELD,
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::WithFreqs)
                .set_fieldnorms(true),
        ),
    );
    FullTextSchema {
        schema: builder.build(),
        key,
        epoch,
        text,
    }
});

fn full_text_schema() -> &'static FullTextSchema {
    &FULL_TEXT_SCHEMA
}

fn index_settings() -> IndexSettings {
    IndexSettings {
        // Nothing is stored in the doc store; keep its format independent of Tantivy features.
        docstore_compression: Compressor::None,
        // Avoid spawning a compression thread per segment writer.
        docstore_compress_dedicated_thread: false,
        ..Default::default()
    }
}

fn full_text_error(e: impl std::error::Error) -> HummockError {
    HummockError::other(format!("full-text index: {}", e.as_report()))
}
