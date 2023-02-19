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

use std::collections::HashMap;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table, View};
use risingwave_pb::hummock::{CompactionGroup, HummockVersion, HummockVersionStats};
use risingwave_pb::meta::TableFragments;
use risingwave_pb::user::UserInfo;

use crate::error::{BackupError, BackupResult};
use crate::{xxhash64_checksum, xxhash64_verify, MetaSnapshotId};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MetaSnapshot {
    pub id: MetaSnapshotId,
    /// Snapshot of meta store.
    pub metadata: ClusterMetadata,
}

impl MetaSnapshot {
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
        let metadata = ClusterMetadata::decode(buf)?;
        Ok(Self { id, metadata })
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ClusterMetadata {
    /// Unlike other metadata that has implemented `MetadataModel`,
    /// DEFAULT_COLUMN_FAMILY stores various single row metadata, e.g. id offset and epoch offset.
    /// So we use `default_cf` stores raw KVs for them.
    pub default_cf: HashMap<Vec<u8>, Vec<u8>>,

    /// Hummock metadata
    pub hummock_version: HummockVersion,
    pub version_stats: HummockVersionStats,
    pub compaction_groups: Vec<CompactionGroup>,

    /// Catalog metadata
    pub database: Vec<Database>,
    pub schema: Vec<Schema>,
    pub table: Vec<Table>,
    pub index: Vec<Index>,
    pub sink: Vec<Sink>,
    pub source: Vec<Source>,
    pub view: Vec<View>,

    pub table_fragments: Vec<TableFragments>,
    pub user_info: Vec<UserInfo>,
}

impl ClusterMetadata {
    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        let default_cf_keys = self.default_cf.keys().collect_vec();
        let default_cf_values = self.default_cf.values().collect_vec();
        Self::encode_prost_message_list(&default_cf_keys, buf);
        Self::encode_prost_message_list(&default_cf_values, buf);
        Self::encode_prost_message(&self.hummock_version, buf);
        Self::encode_prost_message(&self.version_stats, buf);
        Self::encode_prost_message_list(&self.compaction_groups.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.table_fragments.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.user_info.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.database.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.schema.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.table.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.index.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.sink.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.source.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.view.iter().collect_vec(), buf);
    }

    pub fn decode(mut buf: &[u8]) -> BackupResult<Self> {
        let default_cf_keys: Vec<Vec<u8>> = Self::decode_prost_message_list(&mut buf)?;
        let default_cf_values: Vec<Vec<u8>> = Self::decode_prost_message_list(&mut buf)?;
        let default_cf = default_cf_keys
            .into_iter()
            .zip_eq_fast(default_cf_values.into_iter())
            .collect();
        let hummock_version = Self::decode_prost_message(&mut buf)?;
        let version_stats = Self::decode_prost_message(&mut buf)?;
        let compaction_groups: Vec<CompactionGroup> = Self::decode_prost_message_list(&mut buf)?;
        let table_fragments: Vec<TableFragments> = Self::decode_prost_message_list(&mut buf)?;
        let user_info: Vec<UserInfo> = Self::decode_prost_message_list(&mut buf)?;
        let database: Vec<Database> = Self::decode_prost_message_list(&mut buf)?;
        let schema: Vec<Schema> = Self::decode_prost_message_list(&mut buf)?;
        let table: Vec<Table> = Self::decode_prost_message_list(&mut buf)?;
        let index: Vec<Index> = Self::decode_prost_message_list(&mut buf)?;
        let sink: Vec<Sink> = Self::decode_prost_message_list(&mut buf)?;
        let source: Vec<Source> = Self::decode_prost_message_list(&mut buf)?;
        let view: Vec<View> = Self::decode_prost_message_list(&mut buf)?;

        Ok(Self {
            default_cf,
            hummock_version,
            version_stats,
            compaction_groups,
            database,
            schema,
            table,
            index,
            sink,
            source,
            view,
            table_fragments,
            user_info,
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

    fn encode_prost_message_list(messages: &[&impl prost::Message], buf: &mut Vec<u8>) {
        buf.put_u32_le(messages.len() as u32);
        for message in messages {
            Self::encode_prost_message(*message, buf);
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

    use crate::meta_snapshot::{ClusterMetadata, MetaSnapshot};

    #[test]
    fn test_snapshot_encoding_decoding() {
        let mut metadata = ClusterMetadata::default();
        metadata.hummock_version.id = 321;
        let raw = MetaSnapshot { id: 123, metadata };
        let encoded = raw.encode();
        let decoded = MetaSnapshot::decode(&encoded).unwrap();
        assert_eq!(raw, decoded);
    }

    #[test]
    fn test_metadata_encoding_decoding() {
        let mut buf = vec![];
        let mut raw = ClusterMetadata::default();
        raw.default_cf.insert(vec![0, 1, 2], vec![3, 4, 5]);
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
        let decoded = ClusterMetadata::decode(buf.as_slice()).unwrap();
        assert_eq!(raw, decoded);
    }
}
