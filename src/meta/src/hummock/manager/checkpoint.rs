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

/// Computes xxhash64 checksum of the given data, using seed 0.
/// This matches the xxhash64 used in block checksum (see `sstable/utils.rs`).
pub(crate) fn xxhash64_checksum(data: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish()
}

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

/// Decodes checkpoint data, supporting both envelope (compressed) and legacy (raw) formats.
///
/// Format detection: tries `HummockVersionCheckpointEnvelope` first.
///
/// Note: decoding legacy `HummockVersionCheckpoint` bytes as envelope can *succeed* because:
/// - field 1 has a wire-type mismatch and is skipped as unknown;
/// - field 2 in legacy is `stale_objects` (LEN), which would be parsed as envelope `payload`.
///
/// Therefore, we disambiguate by attempting to decode the envelope payload into
/// `PbHummockVersionCheckpoint` and requiring the decoded checkpoint to contain `version`.
/// If `version` is missing, we treat it as legacy bytes that happened to be decodable as an
/// envelope and fall back to legacy decoding.
fn decode_checkpoint_data(
    data: bytes::Bytes,
) -> std::result::Result<PbHummockVersionCheckpoint, anyhow::Error> {
    use anyhow::Context;
    use prost::Message;

    let data_size = data.len();
    let envelope = match PbHummockVersionCheckpointEnvelope::decode(data.clone()) {
        Ok(envelope) => envelope,
        Err(_) => {
            tracing::info!(
                data_size,
                "decoding checkpoint in legacy uncompressed format"
            );
            return PbHummockVersionCheckpoint::decode(data)
                .context("failed to decode legacy checkpoint");
        }
    };

    // If `payload` is empty, it's unlikely to be a valid envelope. Fall back to legacy format.
    if envelope.payload.is_empty() {
        tracing::info!(
            data_size,
            "decoding checkpoint in legacy uncompressed format"
        );
        return PbHummockVersionCheckpoint::decode(data)
            .context("failed to decode legacy checkpoint");
    }

    // If checksum is present or a non-default compression algorithm is set, treat it as an envelope
    // and avoid keeping the original full `data` buffer alive during decompression.
    let should_fallback_to_legacy =
        envelope.checksum.is_none() && envelope.compression_algorithm == 0;
    let mut legacy_data = if should_fallback_to_legacy {
        Some(data)
    } else {
        drop(data);
        None
    };

    // Verify checksum if present.
    if let Some(expected) = envelope.checksum {
        let actual = xxhash64_checksum(&envelope.payload);
        if actual != expected {
            return Err(anyhow::anyhow!(
                "checkpoint checksum mismatch: expected {:#x}, got {:#x}",
                expected,
                actual
            ));
        }
    }

    let algo = CheckpointCompressionAlgorithm::try_from(envelope.compression_algorithm)
        .with_context(|| {
            format!(
                "unknown checkpoint compression algorithm: {}",
                envelope.compression_algorithm
            )
        })?;

    let decompressed = decompress_payload(algo, &envelope.payload)?;
    let ckpt = match PbHummockVersionCheckpoint::decode(decompressed.as_ref()) {
        Ok(ckpt) => ckpt,
        Err(err) => {
            if should_fallback_to_legacy {
                tracing::info!(
                    data_size,
                    "decoding checkpoint in legacy uncompressed format"
                );
                return PbHummockVersionCheckpoint::decode(
                    legacy_data
                        .take()
                        .expect("legacy_data must exist when falling back to legacy format"),
                )
                .context("failed to decode legacy checkpoint")
                .or_else(|_| Err(err).context("failed to decode checkpoint envelope payload"));
            }
            return Err(err).context("failed to decode checkpoint envelope payload");
        }
    };

    if ckpt.version.is_some() {
        let checksum = envelope
            .checksum
            .map(|c| format!("{c:#x}"))
            .unwrap_or_else(|| "none".to_owned());
        tracing::info!(
            compression = ?algo,
            compressed_size = envelope.payload.len(),
            decompressed_size = decompressed.len(),
            compression_ratio =
                format!("{:.2}x", decompressed.len() as f64 / envelope.payload.len().max(1) as f64),
            checksum,
            "decoded compressed checkpoint"
        );
        return Ok(ckpt);
    }

    // If the decoded checkpoint doesn't contain `version`, treat it as legacy checkpoint bytes
    // that happened to be decodable as an envelope. Fall back to legacy format only when the
    // envelope isn't unambiguous.
    if should_fallback_to_legacy {
        tracing::info!(
            data_size,
            "decoding checkpoint in legacy uncompressed format"
        );
        PbHummockVersionCheckpoint::decode(
            legacy_data
                .take()
                .expect("legacy_data must exist when falling back to legacy format"),
        )
        .context("failed to decode legacy checkpoint")
    } else {
        Err(anyhow::anyhow!(
            "checkpoint envelope payload missing required field `version`"
        ))
    }
}

/// Decompresses payload bytes according to the specified algorithm.
fn decompress_payload(
    algo: CheckpointCompressionAlgorithm,
    payload: &[u8],
) -> std::result::Result<std::borrow::Cow<'_, [u8]>, anyhow::Error> {
    use anyhow::Context;

    match algo {
        CheckpointCompressionAlgorithm::CheckpointCompressionUnspecified => Ok(payload.into()),
        CheckpointCompressionAlgorithm::CheckpointCompressionZstd => {
            zstd::stream::decode_all(payload)
                .map(std::borrow::Cow::Owned)
                .context("zstd decompression failed")
        }
        CheckpointCompressionAlgorithm::CheckpointCompressionLz4 => {
            let mut decoder = lz4::Decoder::new(payload).context("lz4 decoder init failed")?;
            let mut decompressed = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)
                .context("lz4 decompression failed")?;
            Ok(decompressed.into())
        }
    }
}

/// Compresses payload bytes according to the specified algorithm.
fn compress_payload(
    algo: risingwave_common::config::CheckpointCompression,
    data: &[u8],
) -> std::result::Result<Vec<u8>, anyhow::Error> {
    use anyhow::Context;
    use risingwave_common::config::CheckpointCompression;

    match algo {
        CheckpointCompression::None => Ok(data.to_vec()),
        CheckpointCompression::Zstd => {
            // Level 3: good balance between compression ratio and speed
            zstd::stream::encode_all(data, 3).context("zstd compression failed")
        }
        CheckpointCompression::Lz4 => {
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

async fn read_bytes_in_chunks<F, Fut>(
    total_size: usize,
    chunk_size: usize,
    max_in_flight_chunks: usize,
    mut read_range: F,
) -> anyhow::Result<bytes::Bytes>
where
    F: FnMut(std::ops::Range<usize>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bytes::Bytes>>,
{
    use anyhow::Context;
    use futures::StreamExt;

    let num_chunks = total_size.div_ceil(chunk_size);
    let mut buf = BytesMut::with_capacity(total_size);

    let mut chunk_stream = futures::stream::iter((0..total_size).step_by(chunk_size))
        .enumerate()
        .map(|(chunk_idx, offset)| {
            let end = std::cmp::min(offset + chunk_size, total_size);
            let range = offset..end;
            let fut = read_range(range.clone());
            async move {
                fut.await.with_context(|| {
                    format!(
                        "read checkpoint chunk {}/{} range {}..{}",
                        chunk_idx + 1,
                        num_chunks,
                        range.start,
                        range.end
                    )
                })
            }
        })
        .buffered(max_in_flight_chunks);

    while let Some(chunk) = chunk_stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
    }

    Ok(buf.freeze())
}

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl HummockManager {
    /// Returns Ok(None) if not found.
    ///
    /// Reads large checkpoints using bounded parallel chunked reads (128MB per chunk) to avoid
    /// single-request timeout issues.
    /// Supports both compressed (envelope) and uncompressed (legacy) checkpoint formats.
    pub async fn try_read_checkpoint(&self) -> Result<Option<HummockVersionCheckpoint>> {
        const CHUNK_SIZE: usize = 128 * 1024 * 1024; // 128MB
        const MAX_IN_FLIGHT_CHUNKS: usize = 4;

        // 1. Get file metadata to learn the total size.
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

        // 2. Read data: single read for small files, parallel chunked reads for large files.
        let download_start = std::time::Instant::now();
        let data = if total_size <= CHUNK_SIZE {
            self.object_store
                .read(&self.version_checkpoint_path, 0..total_size)
                .await?
        } else {
            let num_chunks = total_size.div_ceil(CHUNK_SIZE);
            let data = read_bytes_in_chunks(
                total_size,
                CHUNK_SIZE,
                MAX_IN_FLIGHT_CHUNKS,
                |range| async {
                    Ok(self
                        .object_store
                        .read(&self.version_checkpoint_path, range)
                        .await?)
                },
            )
            .await?;

            tracing::info!(
                total_size,
                num_chunks,
                chunk_size = CHUNK_SIZE,
                max_in_flight_chunks = MAX_IN_FLIGHT_CHUNKS,
                "chunked read complete"
            );
            data
        };
        let download_duration = download_start.elapsed();

        // 3. Decode: try envelope format first, fallback to legacy format.
        let decode_start = std::time::Instant::now();
        let ckpt = decode_checkpoint_data(data)?;
        let decode_duration = decode_start.elapsed();

        tracing::info!(
            total_size,
            download_ms = download_duration.as_millis() as u64,
            decode_ms = decode_duration.as_millis() as u64,
            "checkpoint read complete"
        );

        Ok(Some(HummockVersionCheckpoint::from_protobuf(&ckpt)))
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let raw_bytes = checkpoint.to_protobuf().encode_to_vec();
        let raw_size = raw_bytes.len();

        // Use compression algorithm from config
        let compression = self.env.opts.checkpoint_compression_algorithm;
        let compressed = compress_payload(compression, &raw_bytes)?;
        let checksum = xxhash64_checksum(&compressed);

        tracing::info!(
            raw_size,
            compressed_size = compressed.len(),
            compression_ratio =
                format!("{:.2}x", raw_size as f64 / compressed.len().max(1) as f64),
            compression = ?compression,
            checksum = format!("{:#x}", checksum),
            "writing compressed checkpoint"
        );

        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: compression as i32,
            payload: compressed,
            checksum: Some(checksum),
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;
    use risingwave_common::config::CheckpointCompression;
    use risingwave_pb::hummock::hummock_version_checkpoint::StaleObjects;
    use risingwave_pb::hummock::{
        PbHummockVersion, PbHummockVersionCheckpoint, PbHummockVersionCheckpointEnvelope,
    };

    use super::{
        compress_payload, decode_checkpoint_data, read_bytes_in_chunks, xxhash64_checksum,
    };

    #[allow(deprecated)]
    fn make_version(id: u64) -> PbHummockVersion {
        PbHummockVersion {
            id: id.into(),
            levels: Default::default(),
            max_committed_epoch: 0,
            table_watermarks: Default::default(),
            table_change_logs: Default::default(),
            state_table_info: Default::default(),
            vector_indexes: Default::default(),
        }
    }

    fn make_checkpoint(version_id: u64) -> PbHummockVersionCheckpoint {
        let stale = StaleObjects {
            id: vec![1u64.into(), 2u64.into(), 3u64.into()],
            total_file_size: 123,
            vector_files: vec![],
        };

        PbHummockVersionCheckpoint {
            version: Some(make_version(version_id)),
            stale_objects: [(1u64.into(), stale)].into_iter().collect(),
        }
    }

    fn make_envelope_bytes(
        checkpoint: &PbHummockVersionCheckpoint,
        compression: CheckpointCompression,
        checksum: Option<u64>,
    ) -> Bytes {
        let raw = checkpoint.encode_to_vec();
        let payload = compress_payload(compression, &raw)
            .expect("compress checkpoint payload should succeed");
        let checksum = checksum.unwrap_or_else(|| xxhash64_checksum(&payload));
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: compression as i32,
            payload,
            checksum: Some(checksum),
        };
        Bytes::from(envelope.encode_to_vec())
    }

    #[test]
    fn decode_checkpoint_data_falls_back_to_legacy_format() {
        let checkpoint = make_checkpoint(42);
        let raw = Bytes::from(checkpoint.encode_to_vec());
        let decoded = decode_checkpoint_data(raw).expect("legacy checkpoint should decode");
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn decode_checkpoint_data_roundtrips_envelope_with_checksum() {
        let checkpoint = make_checkpoint(42);
        for compression in [
            CheckpointCompression::None,
            CheckpointCompression::Zstd,
            CheckpointCompression::Lz4,
        ] {
            let data = make_envelope_bytes(&checkpoint, compression, None);
            let decoded = decode_checkpoint_data(data).expect("envelope checkpoint should decode");
            assert_eq!(decoded, checkpoint);
        }
    }

    #[test]
    fn decode_checkpoint_data_skips_checksum_verification_if_missing() {
        let checkpoint = make_checkpoint(42);
        let raw = checkpoint.encode_to_vec();
        let payload = compress_payload(CheckpointCompression::Zstd, &raw)
            .expect("compress checkpoint payload should succeed");
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: CheckpointCompression::Zstd as i32,
            payload,
            checksum: None,
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let decoded =
            decode_checkpoint_data(data).expect("envelope without checksum should decode");
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn decode_checkpoint_data_decodes_envelope_without_checksum_and_no_compression() {
        let checkpoint = make_checkpoint(42);
        let payload = checkpoint.encode_to_vec();
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: CheckpointCompression::None as i32,
            payload,
            checksum: None,
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let decoded =
            decode_checkpoint_data(data).expect("envelope without checksum should decode");
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_checksum_mismatch() {
        let checkpoint = make_checkpoint(42);
        let raw = checkpoint.encode_to_vec();
        let mut payload = compress_payload(CheckpointCompression::Zstd, &raw)
            .expect("compress checkpoint payload should succeed");
        let expected = xxhash64_checksum(&payload);
        payload[0] ^= 0x01;
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: CheckpointCompression::Zstd as i32,
            payload,
            checksum: Some(expected),
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let err = decode_checkpoint_data(data).expect_err("checksum mismatch should error");
        assert!(err.to_string().contains("checksum mismatch"), "{err:?}");
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_unknown_compression_algorithm() {
        let checkpoint = make_checkpoint(42);
        let payload = checkpoint.encode_to_vec();
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: 123,
            payload,
            checksum: None,
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let err =
            decode_checkpoint_data(data).expect_err("unknown compression algorithm should error");
        assert!(
            err.to_string()
                .contains("unknown checkpoint compression algorithm"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn read_bytes_in_chunks_respects_concurrency_limit_and_reassembles() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use tokio::time::{Duration, sleep};

        let total_size = 100usize;
        let chunk_size = 10usize;
        let max_in_flight = 3usize;

        let data: Arc<Vec<u8>> = Arc::new((0..total_size).map(|i| (i % 256) as u8).collect());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let out = read_bytes_in_chunks(total_size, chunk_size, max_in_flight, {
            let data = data.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            move |range: std::ops::Range<usize>| {
                let data = data.clone();
                let in_flight = in_flight.clone();
                let max_seen = max_seen.clone();
                async move {
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(cur, Ordering::SeqCst);

                    // Add a small delay to keep multiple reads in-flight concurrently.
                    sleep(Duration::from_millis(30)).await;

                    let bytes = Bytes::copy_from_slice(&data[range]);
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(bytes)
                }
            }
        })
        .await
        .expect("chunked read should succeed");

        assert_eq!(out.as_ref(), data.as_slice());
        let max_seen = max_seen.load(Ordering::SeqCst);
        assert!(max_seen <= max_in_flight, "max_seen={max_seen}");
        assert!(
            max_seen > 1,
            "expected some concurrency, max_seen={max_seen}"
        );
    }

    #[tokio::test]
    async fn read_bytes_in_chunks_adds_range_context_on_error() {
        let total_size = 30usize;
        let chunk_size = 10usize;
        let max_in_flight = 2usize;

        let err = read_bytes_in_chunks(total_size, chunk_size, max_in_flight, |range| async move {
            if range.start == 10 {
                anyhow::bail!("boom");
            }
            Ok(Bytes::copy_from_slice(&vec![0u8; range.len()]))
        })
        .await
        .expect_err("should fail");

        let msg = err.to_string();
        assert!(
            msg.contains("read checkpoint chunk 2/3 range 10..20"),
            "unexpected error message: {msg}"
        );
        let msg_with_chain = format!("{err:#}");
        assert!(
            msg_with_chain.contains("boom"),
            "unexpected error message: {msg_with_chain}"
        );
    }
}
