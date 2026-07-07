// Copyright 2022 RisingWave Labs
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
use std::fmt::{Display, Formatter};

use bytes::{Buf, BufMut};
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::version::HummockVersion;

use crate::error::{BackupError, BackupResult};
use crate::{MetaSnapshotId, xxhash64_checksum, xxhash64_verify};

pub trait Metadata: Display + Send + Sync {
    fn encode_to(&self, buf: &mut Vec<u8>) -> BackupResult<()>;

    fn decode(buf: &[u8]) -> BackupResult<Self>
    where
        Self: Sized;

    fn hummock_version_ref(&self) -> &HummockVersion;

    fn hummock_version(self) -> HummockVersion;

    fn storage_url(&self) -> BackupResult<String>;

    fn storage_directory(&self) -> BackupResult<String>;

    fn table_change_log_object_ids(&self) -> HashSet<HummockRawObjectId>;
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MetaSnapshot<T: Metadata> {
    pub format_version: u32,
    pub id: MetaSnapshotId,
    /// Snapshot of meta store.
    pub metadata: T,
}

impl<T: Metadata> MetaSnapshot<T> {
    pub fn encode(&self) -> BackupResult<Vec<u8>> {
        let mut buf = vec![];
        buf.put_u32_le(self.format_version);
        buf.put_u64_le(self.id);
        self.metadata.encode_to(&mut buf)?;
        let checksum = xxhash64_checksum(&buf);
        buf.put_u64_le(checksum);
        Ok(buf)
    }

    pub fn decode(buf: &[u8]) -> BackupResult<Self> {
        let payload = verify_checksum_and_strip(buf)?;
        let mut payload = payload;
        let format_version = read_u32_le(&mut payload)?;
        let id = read_u64_le(&mut payload)?;
        let metadata = T::decode(payload)?;
        Ok(Self {
            format_version,
            id,
            metadata,
        })
    }

    pub fn decode_format_version(buf: &[u8]) -> BackupResult<u32> {
        let mut payload = verify_checksum_and_strip(buf)?;
        read_u32_le(&mut payload)
    }
}

pub(crate) fn verify_checksum_and_strip(snapshot: &[u8]) -> BackupResult<&[u8]> {
    let payload_len = snapshot.len().checked_sub(8).ok_or_else(|| {
        BackupError::Other(anyhow::anyhow!(
            "metadata snapshot is too short: {} bytes",
            snapshot.len()
        ))
    })?;
    let mut checksum_buf = &snapshot[payload_len..];
    let checksum = read_u64_le(&mut checksum_buf)?;
    xxhash64_verify(&snapshot[..payload_len], checksum)?;
    Ok(&snapshot[..payload_len])
}

pub(crate) fn read_u32_le(buf: &mut &[u8]) -> BackupResult<u32> {
    if buf.remaining() < 4 {
        return Err(BackupError::Other(anyhow::anyhow!(
            "metadata snapshot is truncated while reading u32"
        )));
    }
    Ok(buf.get_u32_le())
}

pub(crate) fn read_u64_le(buf: &mut &[u8]) -> BackupResult<u64> {
    if buf.remaining() < 8 {
        return Err(BackupError::Other(anyhow::anyhow!(
            "metadata snapshot is truncated while reading u64"
        )));
    }
    Ok(buf.get_u64_le())
}

impl<T: Metadata> Display for MetaSnapshot<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "format_version: {}", self.format_version)?;
        writeln!(f, "id: {}", self.id)?;
        writeln!(f, "{}", self.metadata)?;
        Ok(())
    }
}
