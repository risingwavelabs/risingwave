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

//! Iceberg Puffin implementation.

#![deny(missing_docs)]

use crate::{Error, ErrorKind, Result};

mod blob;
pub use blob::{APACHE_DATASKETCHES_THETA_V1, Blob, DELETION_VECTOR_V1};

pub use crate::compression::CompressionCodec;

/// Compression codecs supported by the Puffin spec.
const SUPPORTED_PUFFIN_CODECS: &[CompressionCodec] = &[
    CompressionCodec::None,
    CompressionCodec::Lz4,
    CompressionCodec::Zstd,
];

/// Validates that the compression codec is supported for Puffin files.
/// Returns an error if the codec is not supported.
fn validate_puffin_compression(codec: CompressionCodec) -> Result<()> {
    if !SUPPORTED_PUFFIN_CODECS.contains(&codec) {
        let supported_names: Vec<String> = SUPPORTED_PUFFIN_CODECS
            .iter()
            .map(|c| format!("{c:?}"))
            .collect();
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Compression codec {codec:?} is not supported for Puffin files. Only {} are supported.",
                supported_names.join(", ")
            ),
        ));
    }
    Ok(())
}

mod metadata;
pub use metadata::{BlobMetadata, CREATED_BY_PROPERTY, FileMetadata};

mod reader;
pub use reader::PuffinReader;

mod writer;
pub use writer::{PuffinWriteResult, PuffinWriter};

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_puffin_codec_validation() {
        // All codecs in SUPPORTED_PUFFIN_CODECS should be valid
        for codec in SUPPORTED_PUFFIN_CODECS {
            assert!(validate_puffin_compression(*codec).is_ok());
        }

        // Gzip should not be supported for Puffin files
        assert!(validate_puffin_compression(CompressionCodec::Gzip).is_err());
    }
}
