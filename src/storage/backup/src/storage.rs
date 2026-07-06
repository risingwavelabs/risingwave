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
use std::hash::Hasher;
use std::mem::size_of;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, MonitoredObjectStore, MonitoredStreamingReader, ObjectError, ObjectStoreImpl,
    ObjectStoreRef, ObjectStreamingUploader,
};
use tokio::sync::RwLock;

use crate::meta_snapshot::{MetaSnapshot, Metadata};
use crate::{
    BackupError, BackupResult, MetaSnapshotId, MetaSnapshotManifest, MetaSnapshotMetadata,
};

pub type MetaSnapshotStorageRef = Arc<ObjectStoreMetaSnapshotStorage>;

pub struct MetaSnapshotStreamingUploader {
    inner: ObjectStreamingUploader,
    checksum: twox_hash::XxHash64,
}

pub struct MetaSnapshotStreamingReader {
    inner: MonitoredStreamingReader,
    buffered: BytesMut,
    checksum: twox_hash::XxHash64,
    expected_checksum: u64,
}

impl MetaSnapshotStreamingUploader {
    fn new(inner: ObjectStreamingUploader) -> Self {
        Self {
            inner,
            checksum: twox_hash::XxHash64::with_seed(0),
        }
    }

    pub async fn write_bytes(&mut self, data: Bytes) -> BackupResult<()> {
        self.checksum.write(&data);
        self.inner.write_bytes(data).await?;
        Ok(())
    }

    pub async fn write_u32_le(&mut self, value: u32) -> BackupResult<()> {
        let mut buf = Vec::with_capacity(size_of::<u32>());
        buf.put_u32_le(value);
        self.write_bytes(buf.into()).await
    }

    pub async fn write_u64_le(&mut self, value: u64) -> BackupResult<()> {
        let mut buf = Vec::with_capacity(size_of::<u64>());
        buf.put_u64_le(value);
        self.write_bytes(buf.into()).await
    }

    pub async fn finish(mut self) -> BackupResult<()> {
        let checksum = self.checksum.finish();
        let mut buf = Vec::with_capacity(size_of::<u64>());
        buf.put_u64_le(checksum);
        self.inner.write_bytes(buf.into()).await?;
        self.inner.finish().await?;
        Ok(())
    }
}

impl MetaSnapshotStreamingReader {
    fn new(inner: MonitoredStreamingReader, expected_checksum: u64) -> Self {
        Self {
            inner,
            buffered: BytesMut::new(),
            checksum: twox_hash::XxHash64::with_seed(0),
            expected_checksum,
        }
    }

    pub async fn read_bytes(&mut self, len: usize) -> BackupResult<Bytes> {
        while self.buffered.len() < len {
            let bytes = match self.inner.read_bytes().await {
                Some(Ok(bytes)) => bytes,
                Some(Err(e)) => return Err(e.into()),
                None => {
                    return Err(BackupError::Decoding(
                        anyhow!(
                            "unexpected end of metadata snapshot: need {} bytes, buffered {}",
                            len,
                            self.buffered.len()
                        )
                        .into(),
                    ));
                }
            };
            self.checksum.write(&bytes);
            self.buffered.extend_from_slice(&bytes);
        }
        Ok(self.buffered.split_to(len).freeze())
    }

    pub async fn read_u32_le(&mut self) -> BackupResult<u32> {
        Ok(self.read_bytes(size_of::<u32>()).await?.get_u32_le())
    }

    pub async fn read_u64_le(&mut self) -> BackupResult<u64> {
        Ok(self.read_bytes(size_of::<u64>()).await?.get_u64_le())
    }

    pub async fn finish(mut self) -> BackupResult<()> {
        if !self.buffered.is_empty() {
            return Err(BackupError::Decoding(
                anyhow!(
                    "metadata snapshot has {} trailing bytes before checksum",
                    self.buffered.len()
                )
                .into(),
            ));
        }

        if self.inner.read_bytes().await.transpose()?.is_some() {
            return Err(BackupError::Decoding(
                anyhow!("metadata snapshot has trailing bytes before checksum").into(),
            ));
        }

        let found = self.checksum.finish();
        if found != self.expected_checksum {
            return Err(BackupError::ChecksumMismatch {
                expected: self.expected_checksum,
                found,
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait MetaSnapshotStorage: 'static + Sync + Send {
    /// Creates a snapshot.
    async fn create<S: Metadata>(
        &self,
        snapshot: &MetaSnapshot<S>,
        remarks: Option<String>,
    ) -> BackupResult<()>;

    /// Gets a snapshot by id.
    async fn get<S: Metadata>(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>>;

    /// Gets local snapshot manifest.
    async fn manifest(&self) -> Arc<MetaSnapshotManifest>;

    /// Refreshes local snapshot manifest.
    async fn refresh_manifest(&self) -> BackupResult<()>;

    /// Deletes snapshots by ids.
    async fn delete(&self, ids: &[MetaSnapshotId]) -> BackupResult<()>;
}

#[derive(Clone)]
pub struct ObjectStoreMetaSnapshotStorage {
    path: String,
    store: ObjectStoreRef,
    manifest: Arc<RwLock<Arc<MetaSnapshotManifest>>>,
}

// TODO #6482: purge stale snapshots that is not in manifest.
impl ObjectStoreMetaSnapshotStorage {
    pub async fn new(path: &str, store: ObjectStoreRef) -> BackupResult<Self> {
        let instance = Self {
            path: path.to_owned(),
            store,
            manifest: Default::default(),
        };
        instance.refresh_manifest().await?;
        Ok(instance)
    }

    async fn update_manifest(
        &self,
        update: impl FnOnce(MetaSnapshotManifest) -> MetaSnapshotManifest,
    ) -> BackupResult<()> {
        let mut guard = self.manifest.write().await;
        let new_manifest = update((**guard).clone());
        let bytes =
            serde_json::to_vec(&new_manifest).map_err(|e| BackupError::Encoding(e.into()))?;
        self.store
            .upload(&self.get_manifest_path(), bytes.into())
            .await?;
        *guard = Arc::new(new_manifest);
        Ok(())
    }

    async fn get_manifest(&self) -> BackupResult<Option<MetaSnapshotManifest>> {
        let manifest_path = self.get_manifest_path();
        let bytes = match self.store.read(&manifest_path, ..).await {
            Ok(bytes) => bytes,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let manifest: MetaSnapshotManifest =
            serde_json::from_slice(&bytes).map_err(|e| BackupError::Encoding(e.into()))?;
        Ok(Some(manifest))
    }

    fn get_manifest_path(&self) -> String {
        format!("{}/manifest.json", self.path)
    }

    fn get_snapshot_path(&self, id: MetaSnapshotId) -> String {
        format!("{}/{}.snapshot", self.path, id)
    }

    #[expect(dead_code)]
    fn get_snapshot_id_from_path(path: &str) -> MetaSnapshotId {
        let split = path.split(&['/', '.']).collect_vec();
        debug_assert!(split.len() > 2);
        debug_assert!(split[split.len() - 1] == "snapshot");
        split[split.len() - 2]
            .parse::<MetaSnapshotId>()
            .expect("valid meta snapshot id")
    }

    pub async fn begin_snapshot_upload(
        &self,
        id: MetaSnapshotId,
        format_version: u32,
    ) -> BackupResult<MetaSnapshotStreamingUploader> {
        let path = self.get_snapshot_path(id);
        let mut uploader =
            MetaSnapshotStreamingUploader::new(self.store.streaming_upload(&path).await?);
        uploader.write_u32_le(format_version).await?;
        uploader.write_u64_le(id).await?;
        Ok(uploader)
    }

    pub async fn begin_snapshot_read(
        &self,
        id: MetaSnapshotId,
    ) -> BackupResult<MetaSnapshotStreamingReader> {
        let path = self.get_snapshot_path(id);
        let object_metadata = self.store.metadata(&path).await?;
        if object_metadata.total_size < size_of::<u32>() + size_of::<u64>() + size_of::<u64>() {
            return Err(BackupError::Decoding(
                anyhow!(
                    "metadata snapshot {} is too small: {} bytes",
                    id,
                    object_metadata.total_size
                )
                .into(),
            ));
        }

        let checksum_offset = object_metadata.total_size - size_of::<u64>();
        let mut checksum_bytes = self
            .store
            .read(&path, checksum_offset..object_metadata.total_size)
            .await?;
        let expected_checksum = checksum_bytes.get_u64_le();
        let reader = self.store.streaming_read(&path, 0..checksum_offset).await?;
        Ok(MetaSnapshotStreamingReader::new(reader, expected_checksum))
    }

    pub async fn commit_snapshot_metadata(
        &self,
        id: MetaSnapshotId,
        hummock_version: &HummockVersion,
        format_version: u32,
        remarks: Option<String>,
        table_change_log_object_ids: impl Iterator<Item = HummockRawObjectId>,
    ) -> BackupResult<()> {
        self.update_manifest(|mut manifest: MetaSnapshotManifest| {
            manifest.manifest_id += 1;
            manifest.snapshot_metadata.push(MetaSnapshotMetadata::new(
                id,
                hummock_version,
                format_version,
                remarks,
                table_change_log_object_ids,
            ));
            manifest
        })
        .await
    }
}

#[async_trait::async_trait]
impl MetaSnapshotStorage for ObjectStoreMetaSnapshotStorage {
    async fn create<S: Metadata>(
        &self,
        snapshot: &MetaSnapshot<S>,
        remarks: Option<String>,
    ) -> BackupResult<()> {
        let path = self.get_snapshot_path(snapshot.id);
        self.store.upload(&path, snapshot.encode()?.into()).await?;
        self.update_manifest(|mut manifest: MetaSnapshotManifest| {
            manifest.manifest_id += 1;
            manifest.snapshot_metadata.push(MetaSnapshotMetadata::new(
                snapshot.id,
                snapshot.metadata.hummock_version_ref(),
                snapshot.format_version,
                remarks,
                snapshot.metadata.table_change_log_object_ids().into_iter(),
            ));
            manifest
        })
        .await?;
        Ok(())
    }

    async fn get<S: Metadata>(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>> {
        let path = self.get_snapshot_path(id);
        let data = self.store.read(&path, ..).await?;
        MetaSnapshot::decode(&data)
    }

    async fn manifest(&self) -> Arc<MetaSnapshotManifest> {
        self.manifest.read().await.clone()
    }

    async fn refresh_manifest(&self) -> BackupResult<()> {
        if let Some(manifest) = self.get_manifest().await? {
            let mut guard = self.manifest.write().await;
            if manifest.manifest_id > guard.manifest_id {
                *guard = Arc::new(manifest);
            }
        }
        Ok(())
    }

    async fn delete(&self, ids: &[MetaSnapshotId]) -> BackupResult<()> {
        let to_delete: HashSet<MetaSnapshotId> = HashSet::from_iter(ids.iter().cloned());
        self.update_manifest(|mut manifest: MetaSnapshotManifest| {
            manifest.manifest_id += 1;
            manifest
                .snapshot_metadata
                .retain(|m| !to_delete.contains(&m.id));
            manifest
        })
        .await?;
        let paths = ids
            .iter()
            .map(|id| self.get_snapshot_path(*id))
            .collect_vec();
        self.store.delete_objects(&paths).await?;
        Ok(())
    }
}

impl From<ObjectError> for BackupError {
    fn from(e: ObjectError) -> Self {
        BackupError::BackupStorage(e.into())
    }
}

// #[cfg(test)]
pub async fn unused() -> ObjectStoreMetaSnapshotStorage {
    ObjectStoreMetaSnapshotStorage::new(
        "",
        Arc::new(ObjectStoreImpl::InMem(MonitoredObjectStore::new(
            InMemObjectStore::for_test(),
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ))),
    )
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta_snapshot::Metadata;
    use crate::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};

    #[tokio::test]
    async fn test_streaming_snapshot_upload_matches_legacy_encode() {
        let store = unused().await;
        let snapshot = MetaSnapshotV2 {
            format_version: 2,
            id: 42,
            metadata: MetadataV2::default(),
        };

        let mut uploader = store
            .begin_snapshot_upload(snapshot.id, snapshot.format_version)
            .await
            .unwrap();
        let mut metadata = Vec::new();
        snapshot.metadata.encode_to(&mut metadata).unwrap();
        uploader.write_bytes(metadata.into()).await.unwrap();
        uploader.finish().await.unwrap();

        let uploaded = store
            .store
            .read(&store.get_snapshot_path(snapshot.id), ..)
            .await
            .unwrap();
        assert_eq!(uploaded.as_ref(), snapshot.encode().unwrap().as_slice());
    }

    #[tokio::test]
    async fn test_streaming_snapshot_read_matches_legacy_encode() {
        let store = unused().await;
        let snapshot = MetaSnapshotV2 {
            format_version: 2,
            id: 43,
            metadata: MetadataV2::default(),
        };
        store.create(&snapshot, None).await.unwrap();

        let encoded = snapshot.encode().unwrap();
        let mut reader = store.begin_snapshot_read(snapshot.id).await.unwrap();
        assert_eq!(reader.read_u32_le().await.unwrap(), snapshot.format_version);
        assert_eq!(reader.read_u64_le().await.unwrap(), snapshot.id);
        let metadata_len = encoded.len() - size_of::<u32>() - size_of::<u64>() - size_of::<u64>();
        assert_eq!(
            reader.read_bytes(metadata_len).await.unwrap().as_ref(),
            &encoded[size_of::<u32>() + size_of::<u64>()..encoded.len() - size_of::<u64>()],
        );
        reader.finish().await.unwrap();
    }
}
