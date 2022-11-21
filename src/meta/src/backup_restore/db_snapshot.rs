// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use bytes::{Buf, BufMut};
use risingwave_pb::hummock::{CompactionGroup, HummockVersion, HummockVersionStats};

use crate::backup_restore::error::{BackupError, BackupResult};
use crate::backup_restore::utils::{xxhash64_checksum, xxhash64_verify};
use crate::backup_restore::DbSnapshotId;
use crate::storage::MetaStore;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct DbSnapshot {
    pub id: DbSnapshotId,
    /// Snapshot of meta store.
    pub metadata: MetadataSnapshot,
}

impl DbSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.put_u64_le(self.id);
        self.metadata.encode_to(&mut buf);
        let checksum = xxhash64_checksum(&buf);
        buf.put_u64_le(checksum);
        buf
    }

    pub fn decode(mut buf: &[u8]) -> BackupResult<Self> {
        let checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], checksum)?;
        let id = buf.get_u64_le();
        let metadata = MetadataSnapshot::decode(buf)?;
        Ok(Self { id, metadata })
    }
}

pub struct DbSnapshotBuilder<S> {
    db_snapshot: DbSnapshot,
    meta_store: Arc<S>,
}

impl<S: MetaStore> DbSnapshotBuilder<S> {
    pub fn new(meta_store: Arc<S>) -> Self {
        Self {
            db_snapshot: DbSnapshot::default(),
            meta_store,
        }
    }

    pub async fn build(&mut self) -> BackupResult<()> {
        todo!()
    }

    pub fn finish(self) -> BackupResult<DbSnapshot> {
        // TODO #6482 sanity check, e.g. any required field is not set.
        Ok(self.db_snapshot)
    }

    pub fn set_id(&mut self, id: DbSnapshotId) {
        self.db_snapshot.id = id;
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MetadataSnapshot {
    pub hummock_version: HummockVersion,
    pub version_stats: HummockVersionStats,
    pub compaction_groups: Vec<CompactionGroup>,
}

impl MetadataSnapshot {
    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        Self::encode_prost_message(&self.hummock_version, buf);
        Self::encode_prost_message(&self.version_stats, buf);
        Self::encode_prost_message_list(&self.compaction_groups, buf);
    }

    pub fn decode(mut buf: &[u8]) -> BackupResult<Self> {
        let hummock_version = Self::decode_prost_message(&mut buf)?;
        let version_stats = Self::decode_prost_message(&mut buf)?;
        let compaction_groups: Vec<CompactionGroup> = Self::decode_prost_message_list(&mut buf)?;
        Ok(Self {
            hummock_version,
            version_stats,
            compaction_groups,
        })
    }

    fn encode_prost_message(message: &impl prost::Message, buf: &mut Vec<u8>) {
        let encoded_message = message.encode_to_vec();
        buf.put_u32_le(encoded_message.len() as u32);
        buf.put_slice(&encoded_message);
    }

    fn decode_prost_message<T>(buf: &mut &[u8]) -> BackupResult<T>
    where
        T: prost::Message + Default,
    {
        let len = buf.get_u32_le() as usize;
        let v = buf[..len].to_vec();
        buf.advance(len);
        T::decode(v.as_slice()).map_err(|e| BackupError::Decoding(e.into()))
    }

    fn encode_prost_message_list(messages: &[impl prost::Message], buf: &mut Vec<u8>) {
        buf.put_u32_le(messages.len() as u32);
        for message in messages {
            Self::encode_prost_message(message, buf);
        }
    }

    fn decode_prost_message_list<T>(buf: &mut &[u8]) -> BackupResult<Vec<T>>
    where
        T: prost::Message + Default,
    {
        let vec_len = buf.get_u32_le() as usize;
        let mut result = vec![];
        for _ in 0..vec_len {
            let v: T = Self::decode_prost_message(buf)?;
            result.push(v);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::hummock::{CompactionGroup, TableStats};

    use crate::backup_restore::db_snapshot::{DbSnapshot, MetadataSnapshot};

    #[test]
    fn test_db_snapshot_encoding_decoding() {
        let mut metadata = MetadataSnapshot::default();
        metadata.hummock_version.id = 321;
        let raw = DbSnapshot { id: 123, metadata };
        let encoded = raw.encode();
        let decoded = DbSnapshot::decode(&encoded).unwrap();
        assert_eq!(raw, decoded);
    }

    #[test]
    fn test_metadata_snapshot_encoding_decoding() {
        let mut buf = vec![];
        let mut raw = MetadataSnapshot::default();
        raw.hummock_version.id = 1;
        raw.version_stats.hummock_version_id = 10;
        raw.version_stats.table_stats.insert(
            200,
            TableStats {
                total_key_count: 1000,
                ..Default::default()
            },
        );
        raw.compaction_groups.push(CompactionGroup {
            id: 3000,
            ..Default::default()
        });
        raw.encode_to(&mut buf);
        let decoded = MetadataSnapshot::decode(buf.as_slice()).unwrap();
        assert_eq!(raw, decoded);
    }
}
