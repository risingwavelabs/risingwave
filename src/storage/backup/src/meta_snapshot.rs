// Copyright 2023 RisingWave Labs
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

use std::fmt::{Display, Formatter};

use bytes::{Buf, BufMut};
use risingwave_pb::hummock::HummockVersion;

use crate::error::BackupResult;
use crate::{xxhash64_checksum, xxhash64_verify, MetaSnapshotId};

pub trait Metadata: Display + Send + Sync {
    fn encode_to(&self, buf: &mut Vec<u8>) -> BackupResult<()>;

    fn decode(buf: &[u8]) -> BackupResult<Self>
    where
        Self: Sized;

    fn hummock_version_ref(&self) -> &HummockVersion;

    fn hummock_version(self) -> HummockVersion;
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

    pub fn decode(mut buf: &[u8]) -> BackupResult<Self> {
        let checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], checksum)?;
        let format_version = buf.get_u32_le();
        let id = buf.get_u64_le();
        let metadata = T::decode(buf)?;
        Ok(Self {
            format_version,
            id,
            metadata,
        })
    }

    pub fn decode_format_version(mut buf: &[u8]) -> BackupResult<u32> {
        let checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], checksum)?;
        let format_version = buf.get_u32_le();
        Ok(format_version)
    }
}

impl<T: Metadata> Display for MetaSnapshot<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "format_version: {}", self.format_version)?;
        writeln!(f, "id: {}", self.id)?;
        writeln!(f, "{}", self.metadata)?;
        Ok(())
    }
}
