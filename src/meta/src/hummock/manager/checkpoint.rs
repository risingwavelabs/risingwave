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

use std::collections::{HashMap, HashSet};
use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

use bytes::BytesMut;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::object_size_map;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockObjectId, HummockVersionId, get_stale_object_ids};
use risingwave_pb::hummock::hummock_version_checkpoint::{PbStaleObjects, StaleObjects};
use risingwave_pb::hummock::{
    CheckpointCompressionAlgorithm, PbHummockVersion, PbHummockVersionArchive,
    PbHummockVersionCheckpoint, PbHummockVersionCheckpointEnvelope, PbVectorIndexObject,
    PbVectorIndexObjectType,
};
use thiserror_ext::AsReport;
use tracing::warn;

use crate::hummock::HummockManager;
use crate::hummock::error::Result;
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::metrics_utils::{trigger_gc_stat, trigger_split_stat};

#[derive(Default)]
pub struct HummockVersionCheckpoint {
    pub version: HummockVersion,

    /// stale objects of versions before the current checkpoint.
    ///
    /// Previously we stored the stale object of each single version.
    /// Currently we will merge the stale object between two checkpoints, and only the
    /// id of the checkpointed hummock version are included in the map.
    pub stale_objects: HashMap<HummockVersionId, PbStaleObjects>,
}

impl HummockVersionCheckpoint {
    pub fn from_protobuf(checkpoint: &PbHummockVersionCheckpoint) -> Self {
        Self {
            version: HummockVersion::from_persisted_protobuf(checkpoint.version.as_ref().unwrap()),
            stale_objects: checkpoint
                .stale_objects
                .iter()
                .map(|(version_id, objects)| (*version_id, objects.clone()))
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionCheckpoint {
        PbHummockVersionCheckpoint {
            version: Some(PbHummockVersion::from(&self.version)),
            stale_objects: self
                .stale_objects
                .iter()
                .map(|(version_id, objects)| (*version_id, objects.clone()))
                .collect(),
        }
    }
}

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl HummockManager {
    /// Returns Ok(None) if not found.
    ///
    /// Uses streaming read to avoid global read timeout issues for large checkpoints.
    /// Supports both compressed (envelope) and uncompressed (legacy) checkpoint formats.
    pub async fn try_read_checkpoint(&self) -> Result<Option<HummockVersionCheckpoint>> {
        // 1. Get file metadata to learn the size (needed for streaming_read range).
        let object_metadata = match self
            .object_store
            .metadata(&self.version_checkpoint_path)
            .await
        {
            Ok(metadata) => metadata,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let total_size = object_metadata.total_size;
        if total_size == 0 {
            return Ok(None);
        }

        // 2. Streaming read: per-chunk timeout instead of whole-file timeout.
        let download_start = std::time::Instant::now();
        let mut reader = self
            .object_store
            .streaming_read(&self.version_checkpoint_path, 0..total_size)
            .await?;
        let mut buf = BytesMut::with_capacity(total_size);
        while let Some(chunk) = reader.read_bytes().await {
            let chunk = chunk?;
            buf.extend_from_slice(&chunk);
        }
        let data = buf.freeze();
        let download_duration = download_start.elapsed();

        // 3. Decode: try envelope format first, fallback to legacy format.
        let decode_start = std::time::Instant::now();
        let ckpt = Self::decode_checkpoint_data(&data)?;
        let decode_duration = decode_start.elapsed();

        tracing::info!(
            total_size,
            download_ms = download_duration.as_millis() as u64,
            decode_ms = decode_duration.as_millis() as u64,
            "checkpoint read complete"
        );

        Ok(Some(HummockVersionCheckpoint::from_protobuf(&ckpt)))
    }

    /// Decodes checkpoint data, supporting both envelope (compressed) and legacy (raw) formats.
    ///
    /// Format detection: tries `HummockVersionCheckpointEnvelope` first. The two formats are
    /// distinguishable because field 1 of the envelope is a VARINT (enum), while field 1 of
    /// the legacy `HummockVersionCheckpoint` is a LEN (message). If envelope decoding succeeds
    /// and the payload is non-empty, we treat it as the new format.
    fn decode_checkpoint_data(
        data: &bytes::Bytes,
    ) -> std::result::Result<PbHummockVersionCheckpoint, anyhow::Error> {
        use anyhow::Context;
        use prost::Message;

        // Try envelope format
        if let Ok(envelope) = PbHummockVersionCheckpointEnvelope::decode(data.clone())
            && !envelope.payload.is_empty()
        {
            let algo = CheckpointCompressionAlgorithm::try_from(envelope.compression_algorithm)
                .unwrap_or(CheckpointCompressionAlgorithm::CheckpointCompressionUnspecified);
            let decompressed = Self::decompress_payload(algo, &envelope.payload)?;
            let ckpt = PbHummockVersionCheckpoint::decode(decompressed.as_ref())
                .context("failed to decode checkpoint payload")?;
            tracing::info!(
                compression = ?algo,
                compressed_size = envelope.payload.len(),
                decompressed_size = decompressed.len(),
                compression_ratio =
                    format!("{:.2}x", decompressed.len() as f64 / envelope.payload.len().max(1) as f64),
                "decoded compressed checkpoint"
            );
            return Ok(ckpt);
        }

        // Fallback: legacy raw PbHummockVersionCheckpoint
        tracing::info!(
            data_size = data.len(),
            "decoding checkpoint in legacy uncompressed format"
        );
        PbHummockVersionCheckpoint::decode(data.clone())
            .context("failed to decode legacy checkpoint")
    }

    /// Decompresses payload bytes according to the specified algorithm.
    fn decompress_payload(
        algo: CheckpointCompressionAlgorithm,
        payload: &[u8],
    ) -> std::result::Result<Vec<u8>, anyhow::Error> {
        use anyhow::Context;

        match algo {
            CheckpointCompressionAlgorithm::CheckpointCompressionUnspecified => {
                Ok(payload.to_vec())
            }
            CheckpointCompressionAlgorithm::CheckpointCompressionZstd => {
                zstd::stream::decode_all(payload).context("zstd decompression failed")
            }
            CheckpointCompressionAlgorithm::CheckpointCompressionLz4 => {
                let mut decoder = lz4::Decoder::new(payload).context("lz4 decoder init failed")?;
                let mut decompressed = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut decompressed)
                    .context("lz4 decompression failed")?;
                Ok(decompressed)
            }
        }
    }

    /// Compresses payload bytes according to the specified algorithm.
    fn compress_payload(
        algo: CheckpointCompressionAlgorithm,
        data: &[u8],
    ) -> std::result::Result<Vec<u8>, anyhow::Error> {
        use anyhow::Context;

        match algo {
            CheckpointCompressionAlgorithm::CheckpointCompressionUnspecified => Ok(data.to_vec()),
            CheckpointCompressionAlgorithm::CheckpointCompressionZstd => {
                // Level 3: good balance between compression ratio and speed
                zstd::stream::encode_all(data, 3).context("zstd compression failed")
            }
            CheckpointCompressionAlgorithm::CheckpointCompressionLz4 => {
                let mut compressed = Vec::new();
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(&mut compressed)
                    .context("lz4 encoder init failed")?;
                std::io::Write::write_all(&mut encoder, data)
                    .context("lz4 compression write failed")?;
                let (_writer, result) = encoder.finish();
                result.context("lz4 compression finish failed")?;
                Ok(compressed)
            }
        }
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let raw_bytes = checkpoint.to_protobuf().encode_to_vec();
        let raw_size = raw_bytes.len();

        // Compress with zstd by default
        let algo = CheckpointCompressionAlgorithm::CheckpointCompressionZstd;
        let compressed = Self::compress_payload(algo, &raw_bytes)?;

        tracing::info!(
            raw_size,
            compressed_size = compressed.len(),
            compression_ratio =
                format!("{:.2}x", raw_size as f64 / compressed.len().max(1) as f64),
            compression = ?algo,
            "writing compressed checkpoint"
        );

        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: algo as i32,
            payload: compressed,
        };
        let buf = envelope.encode_to_vec();
        self.object_store
            .upload(&self.version_checkpoint_path, buf.into())
            .await?;
        Ok(())
    }

    pub(super) async fn write_version_archive(
        &self,
        archive: &PbHummockVersionArchive,
    ) -> Result<()> {
        use prost::Message;
        let buf = archive.encode_to_vec();
        let archive_path = format!(
            "{}/{}",
            self.version_archive_dir,
            archive.version.as_ref().unwrap().id
        );
        self.object_store.upload(&archive_path, buf.into()).await?;
        Ok(())
    }

    /// Creates a hummock version checkpoint.
    /// Returns the diff between new and old checkpoint id.
    /// Note that this method must not be called concurrently, because internally it doesn't hold
    /// lock throughout the method.
    pub async fn create_version_checkpoint(&self, min_delta_log_num: u64) -> Result<u64> {
        let timer = self.metrics.version_checkpoint_latency.start_timer();
        // 1. hold read lock and create new checkpoint
        let versioning_guard = self.versioning.read().await;
        let versioning: &Versioning = versioning_guard.deref();
        let current_version: &HummockVersion = &versioning.current_version;
        let old_checkpoint: &HummockVersionCheckpoint = &versioning.checkpoint;
        let new_checkpoint_id = current_version.id;
        let old_checkpoint_id = old_checkpoint.version.id;
        if new_checkpoint_id < old_checkpoint_id + min_delta_log_num {
            return Ok(0);
        }
        if cfg!(test) && new_checkpoint_id == old_checkpoint_id {
            drop(versioning_guard);
            let versioning = self.versioning.read().await;
            let context_info = self.context_info.read().await;
            let min_pinned_version_id = context_info.min_pinned_version_id();
            trigger_gc_stat(&self.metrics, &versioning.checkpoint, min_pinned_version_id);
            return Ok(0);
        }
        assert!(new_checkpoint_id > old_checkpoint_id);
        let mut archive: Option<PbHummockVersionArchive> = None;
        let mut stale_objects = old_checkpoint.stale_objects.clone();
        // `object_sizes` is used to calculate size of stale objects.
        let mut object_sizes = object_size_map(&old_checkpoint.version);
        // The set of object ids that once exist in any hummock version
        let mut versions_object_ids: HashSet<_> =
            old_checkpoint.version.get_object_ids(false).collect();
        for (_, version_delta) in versioning
            .hummock_version_deltas
            .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
        {
            // DO NOT REMOVE THIS LINE
            // This is to ensure that when adding new variant to `HummockObjectId`,
            // the compiler will warn us if we forget to handle it here.
            match HummockObjectId::Sstable(0.into()) {
                HummockObjectId::Sstable(_) => {}
                HummockObjectId::VectorFile(_) => {}
                HummockObjectId::HnswGraphFile(_) => {}
            };
            for (object_id, file_size) in version_delta
                .newly_added_sst_infos(false)
                .map(|sst| (HummockObjectId::Sstable(sst.object_id), sst.file_size))
                .chain(
                    version_delta
                        .vector_index_delta
                        .values()
                        .flat_map(|delta| delta.newly_added_objects()),
                )
            {
                object_sizes.insert(object_id, file_size);
                versions_object_ids.insert(object_id);
            }
        }

        // Object ids that once exist in any hummock version but not exist in the latest hummock version
        let removed_object_ids =
            &versions_object_ids - &current_version.get_object_ids(false).collect();
        let total_file_size = removed_object_ids
            .iter()
            .map(|t| {
                object_sizes.get(t).copied().unwrap_or_else(|| {
                    warn!(object_id = ?t, "unable to get size of removed object id");
                    0
                })
            })
            .sum::<u64>();
        stale_objects.insert(current_version.id, {
            let mut sst_ids = vec![];
            let mut vector_files = vec![];
            for object_id in removed_object_ids {
                match object_id {
                    HummockObjectId::Sstable(sst_id) => sst_ids.push(sst_id),
                    HummockObjectId::VectorFile(vector_file_id) => {
                        vector_files.push(PbVectorIndexObject {
                            id: vector_file_id.as_raw(),
                            object_type: PbVectorIndexObjectType::VectorIndexObjectVector as _,
                        })
                    }
                    HummockObjectId::HnswGraphFile(graph_file_id) => {
                        vector_files.push(PbVectorIndexObject {
                            id: graph_file_id.as_raw(),
                            object_type: PbVectorIndexObjectType::VectorIndexObjectHnswGraph as _,
                        });
                    }
                }
            }
            StaleObjects {
                id: sst_ids,
                total_file_size,
                vector_files,
            }
        });
        if self.env.opts.enable_hummock_data_archive {
            archive = Some(PbHummockVersionArchive {
                version: Some(PbHummockVersion::from(&old_checkpoint.version)),
                version_deltas: versioning
                    .hummock_version_deltas
                    .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
                    .map(|(_, version_delta)| version_delta.into())
                    .collect(),
            });
        }
        let min_pinned_version_id = self.context_info.read().await.min_pinned_version_id();
        let may_delete_object = stale_objects
            .iter()
            .filter_map(|(version_id, object_ids)| {
                if *version_id >= min_pinned_version_id {
                    return None;
                }
                Some(get_stale_object_ids(object_ids))
            })
            .flatten();
        self.gc_manager.add_may_delete_object_ids(may_delete_object);
        stale_objects.retain(|version_id, _| *version_id >= min_pinned_version_id);
        let new_checkpoint = HummockVersionCheckpoint {
            version: current_version.clone(),
            stale_objects,
        };
        drop(versioning_guard);
        // 2. persist the new checkpoint without holding lock
        self.write_checkpoint(&new_checkpoint).await?;
        if let Some(archive) = archive
            && let Err(e) = self.write_version_archive(&archive).await
        {
            tracing::warn!(
                error = %e.as_report(),
                "failed to write version archive {}",
                archive.version.as_ref().unwrap().id
            );
        }
        // 3. hold write lock and update in memory state
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        assert!(new_checkpoint.version.id > versioning.checkpoint.version.id);
        versioning.checkpoint = new_checkpoint;
        let min_pinned_version_id = self.context_info.read().await.min_pinned_version_id();
        trigger_gc_stat(&self.metrics, &versioning.checkpoint, min_pinned_version_id);
        trigger_split_stat(&self.metrics, &versioning.current_version);
        drop(versioning_guard);
        timer.observe_duration();
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id.as_i64_id());

        Ok(new_checkpoint_id - old_checkpoint_id)
    }

    pub fn pause_version_checkpoint(&self) {
        self.pause_version_checkpoint.store(true, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is paused.");
    }

    pub fn resume_version_checkpoint(&self) {
        self.pause_version_checkpoint
            .store(false, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is resumed.");
    }

    pub fn is_version_checkpoint_paused(&self) -> bool {
        self.pause_version_checkpoint.load(Ordering::Relaxed)
    }

    pub async fn get_checkpoint_version(&self) -> HummockVersion {
        let versioning_guard = self.versioning.read().await;
        versioning_guard.checkpoint.version.clone()
    }
}
