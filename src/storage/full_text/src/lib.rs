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
//! This crate builds, reads, and merges segments synchronously in memory. Callers handle object
//! storage, versioning, and scheduling CPU-intensive work off the async runtime.
//!
//! Objects produced by the segment layer:
//! - **Segment bundle**: all files of one Tantivy segment and its PK array packed into one blob,
//!   with separate opaque `segment_meta` bytes.
//! - **Deletion vector**: a dense Tantivy alive bitset or sparse deleted doc IDs. Tantivy
//!   readers and the merger use the native alive bitset after opening the segment.
//! - **Delete file**: the sorted keys deleted at one epoch.

mod builder;
mod bundle;
mod delete_file;
mod deletion_vector;
mod error;
mod keys;
mod segment;
#[cfg(test)]
mod tests;

use std::sync::LazyLock;

use bytes::Bytes;
use tantivy::IndexSettings;
use tantivy::schema::{BytesOptions, Field, Schema};
use tantivy::store::Compressor;

pub use self::builder::SegmentBuilder;
pub use self::bundle::SegmentBundle;
pub use self::delete_file::{decode_delete_file, encode_delete_file};
use self::error::full_text_error;
pub use self::error::{FullTextError, FullTextResult};
pub use self::segment::{IsDeletedFn, OpenedSegment, merge_segments};

/// Version of the on-object formats and of the Tantivy schema and settings used to write segments.
pub const FULL_TEXT_FORMAT_VERSION: u32 = 1;

/// Produces a document's analyzed tokens synchronously. A token need only live until its
/// call to the sink returns.
pub type FullTextTokenSource<'a> = dyn FnMut(&mut dyn FnMut(&str)) + 'a;

/// A sealed segment, ready to be uploaded.
#[derive(Debug, Clone)]
pub struct BuiltSegment {
    /// Content of the segment bundle object.
    pub bundle: SegmentBundle,
    /// Opaque metadata that the caller must retain to open the segment later.
    pub segment_meta: Bytes,
    /// Number of documents in the segment, including deleted ones.
    pub num_docs: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    /// Present when some docs are already deleted: (deletion vector object content,
    /// `num_deleted`).
    pub deletion_vector: Option<(Bytes, u64)>,
}

/// The Tantivy schema shared by all segments. Merging requires identical schemas, so it must
/// only be defined here.
struct FullTextSchema {
    schema: Schema,
    /// Pre-analyzed tokens, indexed for membership only.
    tokens: Field,
}

static FULL_TEXT_SCHEMA: LazyLock<FullTextSchema> = LazyLock::new(|| {
    let mut builder = Schema::builder();
    let tokens = builder.add_bytes_field("tokens", BytesOptions::default().set_indexed());
    FullTextSchema {
        schema: builder.build(),
        tokens,
    }
});

fn full_text_schema() -> &'static FullTextSchema {
    &FULL_TEXT_SCHEMA
}

fn index_settings() -> IndexSettings {
    IndexSettings {
        // Nothing is stored. Keep the format independent of Tantivy compression features.
        docstore_compression: Compressor::None,
        // Avoid spawning a compression thread per segment writer.
        docstore_compress_dedicated_thread: false,
        ..Default::default()
    }
}
