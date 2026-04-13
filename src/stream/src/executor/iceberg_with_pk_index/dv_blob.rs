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

//! Helpers for reading Iceberg deletion-vector blobs from Puffin files.

use std::collections::BTreeSet;
use std::io::Cursor;

use anyhow::Context;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinReader;
use iceberg::spec::DataFile;
use risingwave_pb::id::SinkId;
use roaring::RoaringTreemap;

use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// Read deleted row positions from a Puffin deletion-vector `DataFile`.
pub async fn read_dv_positions_from_data_file(
    file_io: &FileIO,
    data_file: &DataFile,
    sink_id: SinkId,
) -> StreamExecutorResult<BTreeSet<i64>> {
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

    let input_file = file_io
        .new_input(data_file.file_path())
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader
        .file_metadata()
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;
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
    let blob = puffin_reader
        .blob(blob_metadata)
        .await
        .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

    // TODO: make DeleteVector public in iceberg-rust crate, then replace it with upstream code
    deserialize_puffin_dv_blob(blob.data())
}

/// DV blob magic bytes defined by the Iceberg Puffin deletion-vector format.
const DELETION_VECTOR_MAGIC_BYTES: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Deserialize deleted row positions from a Puffin deletion-vector blob.
pub fn deserialize_puffin_dv_blob(data: &[u8]) -> StreamExecutorResult<BTreeSet<i64>> {
    if data.len() < 12 {
        return Err(
            anyhow::anyhow!("serialized deletion vector blob too small: {}", data.len()).into(),
        );
    }

    if data[4..8] != DELETION_VECTOR_MAGIC_BYTES {
        return Err(anyhow::anyhow!("invalid deletion vector magic bytes").into());
    }

    let bitmap_data = &data[8..data.len() - 4];
    let roaring = RoaringTreemap::deserialize_from(&mut Cursor::new(bitmap_data))
        .context("failed to deserialize deletion vector bitmap")?;

    roaring
        .iter()
        .map(|pos| i64::try_from(pos).context("deletion vector position exceeds i64 range"))
        .collect::<anyhow::Result<BTreeSet<_>>>()
        .map_err(Into::into)
}
