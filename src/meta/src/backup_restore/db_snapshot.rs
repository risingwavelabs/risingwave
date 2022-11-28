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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table, View};
use risingwave_pb::hummock::{
    CompactionGroup, HummockVersion, HummockVersionDelta, HummockVersionStats,
};
use risingwave_pb::meta::TableFragments;
use risingwave_pb::user::UserInfo;

use crate::backup_restore::error::{BackupError, BackupResult};
use crate::backup_restore::utils::{xxhash64_checksum, xxhash64_verify};
use crate::backup_restore::DbSnapshotId;
use crate::model::MetadataModel;
use crate::storage::{MetaStore, Snapshot, DEFAULT_COLUMN_FAMILY};

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

    pub async fn build(&mut self, id: DbSnapshotId) -> BackupResult<()> {
        self.db_snapshot.id = id;
        // Caveat: snapshot impl of etcd meta store doesn't prevent it from expiration.
        // So expired snapshot read may return error. If that happens,
        // tune auto-compaction-mode and auto-compaction-retention on demand.
        let meta_store_snapshot = self.meta_store.snapshot().await;
        let default_cf = self.build_default_cf(&meta_store_snapshot).await?;
        // hummock_version and version_stats is guaranteed to exist in a initialized cluster.
        let hummock_version = {
            let mut redo_state = HummockVersion::list_at_snapshot::<S>(&meta_store_snapshot)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("hummock version checkpoint not found in meta store"))?;
            let hummock_version_deltas =
                HummockVersionDelta::list_at_snapshot::<S>(&meta_store_snapshot).await?;
            for version_delta in &hummock_version_deltas {
                if version_delta.prev_id == redo_state.id {
                    redo_state.apply_version_delta(version_delta);
                }
            }
            redo_state
        };
        let version_stats = HummockVersionStats::list_at_snapshot::<S>(&meta_store_snapshot)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("hummock version stats not found in meta store"))?;
        let compaction_groups =
            crate::hummock::compaction_group::CompactionGroup::list_at_snapshot::<S>(
                &meta_store_snapshot,
            )
            .await?
            .iter()
            .map(MetadataModel::to_protobuf)
            .collect();
        let table_fragments =
            crate::model::TableFragments::list_at_snapshot::<S>(&meta_store_snapshot)
                .await?
                .iter()
                .map(MetadataModel::to_protobuf)
                .collect();
        let user_info = UserInfo::list_at_snapshot::<S>(&meta_store_snapshot).await?;

        let database = Database::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let schema = Schema::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let table = Table::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let index = Index::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let sink = Sink::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let source = Source::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let view = View::list_at_snapshot::<S>(&meta_store_snapshot).await?;

        self.db_snapshot.metadata = MetadataSnapshot {
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
        };
        Ok(())
    }

    pub fn finish(self) -> BackupResult<DbSnapshot> {
        // TODO #6482 sanity check, e.g. any required field is not set.
        Ok(self.db_snapshot)
    }

    async fn build_default_cf(
        &self,
        snapshot: &S::Snapshot,
    ) -> BackupResult<HashMap<Vec<u8>, Vec<u8>>> {
        // It's fine any lazy initialized value is not found in meta store.
        let default_cf =
            HashMap::from_iter(snapshot.list_cf(DEFAULT_COLUMN_FAMILY).await?.into_iter());
        Ok(default_cf)
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MetadataSnapshot {
    /// Unlike other metadata that has implemented `MetadataModel`,
    /// DEFAULT_COLUMN_FAMILY stores various single row metadata not implemented `MetadataModel`.
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

impl MetadataSnapshot {
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
            .zip_eq(default_cf_values.into_iter())
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
    use std::ops::Deref;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::error::ToErrorStr;
    use risingwave_pb::hummock::{
        CompactionGroup, HummockVersion, HummockVersionStats, TableStats,
    };

    use crate::backup_restore::db_snapshot::{DbSnapshot, DbSnapshotBuilder, MetadataSnapshot};
    use crate::backup_restore::error::BackupError;
    use crate::model::{MetadataModel, BARRIER_MANAGER_STATE_KEY};
    use crate::storage::{MemStore, MetaStore, DEFAULT_COLUMN_FAMILY};

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
        let decoded = MetadataSnapshot::decode(buf.as_slice()).unwrap();
        assert_eq!(raw, decoded);
    }

    #[tokio::test]
    async fn test_db_snapshot_builder() {
        let meta_store = Arc::new(MemStore::new());

        let mut builder = DbSnapshotBuilder::new(meta_store.clone());
        let err = builder.build(1).await.unwrap_err();
        let err = assert_matches!(err, BackupError::Other(e) => e);
        assert_eq!(
            "hummock version checkpoint not found in meta store",
            err.to_error_str()
        );

        let hummock_version = HummockVersion {
            id: 1,
            ..Default::default()
        };
        hummock_version.insert(meta_store.deref()).await.unwrap();
        let mut builder = DbSnapshotBuilder::new(meta_store.clone());
        let err = builder.build(1).await.unwrap_err();
        let err = assert_matches!(err, BackupError::Other(e) => e);
        assert_eq!(
            "hummock version stats not found in meta store",
            err.to_error_str()
        );

        let hummock_version_stats = HummockVersionStats {
            hummock_version_id: hummock_version.id,
            ..Default::default()
        };
        hummock_version_stats
            .insert(meta_store.deref())
            .await
            .unwrap();
        let mut builder = DbSnapshotBuilder::new(meta_store.clone());
        builder.build(1).await.unwrap();

        let mut builder = DbSnapshotBuilder::new(meta_store.clone());
        meta_store
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                BARRIER_MANAGER_STATE_KEY.to_vec(),
                vec![100],
            )
            .await
            .unwrap();
        builder.build(1).await.unwrap();
        let db_snapshot = builder.finish().unwrap();
        let encoded = db_snapshot.encode();
        let decoded = DbSnapshot::decode(&encoded).unwrap();
        assert_eq!(db_snapshot, decoded);
        assert_eq!(db_snapshot.id, 1);
        assert_eq!(
            db_snapshot
                .metadata
                .default_cf
                .keys()
                .cloned()
                .collect_vec(),
            vec![BARRIER_MANAGER_STATE_KEY.to_vec()]
        );
        assert_eq!(
            db_snapshot
                .metadata
                .default_cf
                .values()
                .cloned()
                .collect_vec(),
            vec![vec![100]]
        );
        assert_eq!(db_snapshot.metadata.hummock_version.id, 1);
        assert_eq!(db_snapshot.metadata.version_stats.hummock_version_id, 1);
    }
}
