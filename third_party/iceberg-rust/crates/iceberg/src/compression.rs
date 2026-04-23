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

//! Compression codec support for data compression and decompression.

use std::io::{Read, Write};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};

use crate::{Error, ErrorKind, Result};

/// Data compression formats
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    #[default]
    /// No compression
    None,
    /// LZ4 single compression frame with content size present
    Lz4,
    /// Zstandard single compression frame with content size present
    Zstd,
    /// Gzip compression
    Gzip,
}

impl CompressionCodec {
    pub(crate) fn decompress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "LZ4 decompression is not supported currently",
            )),
            CompressionCodec::Zstd => Ok(zstd::stream::decode_all(&bytes[..])?),
            CompressionCodec::Gzip => {
                let mut decoder = GzDecoder::new(&bytes[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                Ok(decompressed)
            }
        }
    }

    pub(crate) fn compress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "LZ4 compression is not supported currently",
            )),
            CompressionCodec::Zstd => {
                let writer = Vec::<u8>::new();
                let mut encoder = zstd::stream::Encoder::new(writer, 3)?;
                encoder.include_checksum(true)?;
                encoder.set_pledged_src_size(Some(bytes.len().try_into()?))?;
                std::io::copy(&mut &bytes[..], &mut encoder)?;
                Ok(encoder.finish()?)
            }
            CompressionCodec::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
        }
    }

    pub(crate) fn is_none(&self) -> bool {
        matches!(self, CompressionCodec::None)
    }
}

#[cfg(test)]
mod tests {
    use super::CompressionCodec;

    #[tokio::test]
    async fn test_compression_codec_none() {
        let bytes_vec = [0_u8; 100].to_vec();

        let codec = CompressionCodec::None;
        let compressed = codec.compress(bytes_vec.clone()).unwrap();
        assert_eq!(bytes_vec, compressed);
        let decompressed = codec.decompress(compressed).unwrap();
        assert_eq!(bytes_vec, decompressed);
    }

    #[tokio::test]
    async fn test_compression_codec_compress() {
        let bytes_vec = [0_u8; 100].to_vec();

        let compression_codecs = [CompressionCodec::Zstd, CompressionCodec::Gzip];

        for codec in compression_codecs {
            let compressed = codec.compress(bytes_vec.clone()).unwrap();
            assert!(compressed.len() < bytes_vec.len());
            let decompressed = codec.decompress(compressed).unwrap();
            assert_eq!(decompressed, bytes_vec);
        }
    }

    #[tokio::test]
    async fn test_compression_codec_unsupported() {
        let unsupported_codecs = [(CompressionCodec::Lz4, "LZ4")];
        let bytes_vec = [0_u8; 100].to_vec();

        for (codec, name) in unsupported_codecs {
            assert_eq!(
                codec.compress(bytes_vec.clone()).unwrap_err().to_string(),
                format!("FeatureUnsupported => {name} compression is not supported currently"),
            );

            assert_eq!(
                codec.decompress(bytes_vec.clone()).unwrap_err().to_string(),
                format!("FeatureUnsupported => {name} decompression is not supported currently"),
            );
        }
    }
}
