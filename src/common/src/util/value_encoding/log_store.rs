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

//! The explicit schema-version header of a KV log-store value.
//!
//! Legacy values begin with a non-null epoch (basic encoding tag 1). Tag 2 identifies the new
//! header, followed by a little-endian `u32` schema version and the original basic-encoded row.
//! The version is independent of both the row operation and the primary key.

use super::{Result, ValueEncodingError};

const HEADER_TAG: u8 = 2;
const HEADER_LEN: usize = 1 + size_of::<u32>();

/// The position of the version column in the value row, if it is requested by the reader.
#[derive(Clone)]
pub struct LogStoreHeader {
    pub version_index: Option<usize>,
}

pub fn serialize_header(version: u32, buf: &mut Vec<u8>) {
    buf.push(HEADER_TAG);
    buf.extend_from_slice(&version.to_le_bytes());
}

pub fn deserialize_header(bytes: &mut &[u8]) -> Result<u32> {
    match bytes.first() {
        Some(&HEADER_TAG) if bytes.len() >= HEADER_LEN => {
            let version = u32::from_le_bytes(bytes[1..HEADER_LEN].try_into().unwrap());
            *bytes = &bytes[HEADER_LEN..];
            Ok(version)
        }
        Some(1) => Ok(0),
        _ => Err(ValueEncodingError::InvalidLogStoreHeader),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_schema_version_header() {
        for version in [0, 1, 4095, 4096, 65536, u32::MAX] {
            let mut bytes = Vec::new();
            serialize_header(version, &mut bytes);
            bytes.extend_from_slice(&[1, 42]);
            let mut input = bytes.as_slice();
            assert_eq!(deserialize_header(&mut input).unwrap(), version);
            assert_eq!(input, &[1, 42]);
        }
        let mut bytes = Vec::new();
        serialize_header(0x12345678, &mut bytes);
        assert_eq!(bytes, [2, 0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_legacy_and_invalid_headers() {
        let legacy = &[1, 42][..];
        let mut input = legacy;
        assert_eq!(deserialize_header(&mut input).unwrap(), 0);
        assert_eq!(input, legacy);
        for len in 0..HEADER_LEN {
            let bytes = [HEADER_TAG; HEADER_LEN];
            assert!(deserialize_header(&mut &bytes[..len]).is_err());
        }
        for tag in [0, 3, 255] {
            assert!(deserialize_header(&mut &[tag][..]).is_err());
        }
    }
}
