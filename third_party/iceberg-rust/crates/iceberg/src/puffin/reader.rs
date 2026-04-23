// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use tokio::sync::OnceCell;

use super::validate_puffin_compression;
use crate::Result;
use crate::io::InputFile;
use crate::puffin::blob::Blob;
use crate::puffin::metadata::{BlobMetadata, FileMetadata};

/// Puffin reader
pub struct PuffinReader {
    input_file: InputFile,
    file_metadata: OnceCell<FileMetadata>,
}

impl PuffinReader {
    /// Returns a new Puffin reader
    pub fn new(input_file: InputFile) -> Self {
        Self {
            input_file,
            file_metadata: OnceCell::new(),
        }
    }

    /// Returns file metadata
    pub async fn file_metadata(&self) -> Result<&FileMetadata> {
        self.file_metadata
            .get_or_try_init(|| FileMetadata::read(&self.input_file))
            .await
    }

    /// Returns blob
    pub async fn blob(&self, blob_metadata: &BlobMetadata) -> Result<Blob> {
        validate_puffin_compression(blob_metadata.compression_codec)?;

        let file_read = self.input_file.reader().await?;
        let start = blob_metadata.offset;
        let end = start + blob_metadata.length;
        let bytes = file_read.read(start..end).await?;
        let data = blob_metadata.compression_codec.decompress(bytes.to_vec())?;

        Ok(Blob {
            r#type: blob_metadata.r#type.clone(),
            fields: blob_metadata.fields.clone(),
            snapshot_id: blob_metadata.snapshot_id,
            sequence_number: blob_metadata.sequence_number,
            data,
            properties: blob_metadata.properties.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::ErrorKind;
    use crate::compression::CompressionCodec;
    use crate::puffin::metadata::BlobMetadata;
    use crate::puffin::reader::PuffinReader;
    use crate::puffin::test_utils::{
        blob_0, blob_1, java_uncompressed_metric_input_file,
        java_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };

    #[tokio::test]
    async fn test_puffin_reader_uncompressed_metric_data() {
        let input_file = java_uncompressed_metric_input_file();
        let puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, uncompressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_puffin_reader_zstd_compressed_metric_data() {
        let input_file = java_zstd_compressed_metric_input_file();
        let puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_gzip_compression_rejected_on_blob_access() {
        // Use a real puffin file
        let input_file = java_uncompressed_metric_input_file();
        let reader = PuffinReader::new(input_file);

        // Create a BlobMetadata with Gzip compression
        let gzip_blob_metadata = BlobMetadata {
            r#type: "test-type".to_string(),
            fields: vec![1],
            snapshot_id: 1,
            sequence_number: 1,
            offset: 4,
            length: 10,
            compression_codec: CompressionCodec::Gzip,
            properties: HashMap::new(),
        };

        // Attempting to access the blob should fail
        let result = reader.blob(&gzip_blob_metadata).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("Gzip"));
        assert!(
            err.to_string()
                .contains("is not supported for Puffin files")
        );
    }
}
