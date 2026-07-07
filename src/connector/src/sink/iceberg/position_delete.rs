// Copyright 2025 RisingWave Labs
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

//! Shared iceberg position-delete (Puffin deletion vector) helpers.
//!
//! These used to live in `src/stream`'s `PositionDeleteHandlerImpl` and were private to the stream
//! crate. They are hoisted here so BOTH the streaming sink (steady-state DV authoring) and meta's
//! pk-index compaction coordinator (dead-row masking) can share the exact same Puffin read/write
//! logic instead of duplicating the on-disk blob format in two places.

use std::collections::HashMap;

use anyhow::{Context, Result};
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use serde::Deserialize;

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// Decode-side mirror of the RW-local `RowProvenanceEntry` serialized by the compactor in
/// `src/storage/.../iceberg_compaction/report.rs`. One entry per input→output row mapping, spilled
/// as newline-delimited JSON. Field names and types MUST stay in sync with that serialize struct
/// and with the fork's `RowProvenance`.
#[derive(Debug, Clone, Deserialize)]
pub struct RowProvenanceEntry {
    pub input_file: String,
    pub input_pos: u64,
    pub output_file: String,
    pub output_pos: u64,
}

/// Reads the deletion-vector positions of a single Puffin DV `DataFile`.
///
/// The `DataFile` must carry a `content_offset` / `content_size_in_bytes` locating its blob inside
/// the Puffin container (as written by [`write_dv_puffin_file`]); the blob body decodes into a
/// [`DeleteVector`] (roaring bitmap) of deleted row positions in the referenced data file.
pub async fn read_dv_positions_from_data_file(
    file_io: &FileIO,
    data_file: &DataFile,
) -> Result<DeleteVector> {
    let blob_offset = data_file.content_offset().with_context(|| {
        format!(
            "DV file {} missing content_offset for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        )
    })?;
    let blob_length = data_file.content_size_in_bytes().with_context(|| {
        format!(
            "DV file {} missing content_size_in_bytes for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        )
    })?;

    let input_file = file_io.new_input(data_file.file_path())?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader.file_metadata().await?;
    let blob_metadata = file_metadata
        .blobs()
        .iter()
        .find(|blob| blob.offset() == blob_offset as u64 && blob.length() == blob_length as u64)
        .with_context(|| {
            format!(
                "DV blob metadata not found in {} at offset={} length={}",
                data_file.file_path(),
                blob_offset,
                blob_length
            )
        })?;
    let blob = puffin_reader.blob(blob_metadata).await?;

    let delete_vector = DeleteVector::from_puffin_blob(blob)?;
    Ok(delete_vector)
}

/// Writes `delete_vector` as a single V3 Puffin deletion-vector blob referencing `data_file_path`,
/// and returns its [`DataFile`] metadata (content `PositionDeletes`, format `Puffin`) with
/// `referenced_data_file` set.
///
/// The blob location is derived from `location_generator` + `file_name_generator` (both borrow-only
/// so callers keep ownership); `partition_key` is attached to the produced `DataFile` for
/// partitioned tables. Callers that need to merge with an existing DV must fold it into
/// `delete_vector` before calling — this writes exactly the positions it is given.
pub async fn write_dv_puffin_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    data_file_path: String,
    delete_vector: DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key, &file_name);
    let output_file = table.file_io().new_output(&location)?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;

    let cardinality = delete_vector.len();
    let properties = HashMap::from([
        (
            DELETION_VECTOR_PROPERTY_CARDINALITY.to_owned(),
            cardinality.to_string(),
        ),
        (
            DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_owned(),
            data_file_path.clone(),
        ),
    ]);
    let blob = delete_vector.to_puffin_blob(properties)?;
    writer.add(blob, CompressionCodec::None).await?;

    let result = writer.close_with_metadata().await?;
    let blob_metadata = result
        .blobs_metadata
        .first()
        .context("blob metadata should be present")?;

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::PositionDeletes)
        .file_path(location)
        .file_format(DataFileFormat::Puffin)
        .record_count(cardinality)
        .file_size_in_bytes(result.file_size_in_bytes)
        .referenced_data_file(Some(data_file_path))
        .content_offset(Some(blob_metadata.offset() as i64))
        .content_size_in_bytes(Some(blob_metadata.length() as i64));
    if let Some(partition_key) = partition_key {
        builder
            .partition(partition_key.data().clone())
            .partition_spec_id(partition_key.spec().spec_id());
    }
    builder
        .build()
        .context("Failed to build deletion vector file metadata")
}
