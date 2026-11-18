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

//! Object-store access for full-text index segments, delete files and deletion vectors.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use risingwave_full_text::{
    BuiltSegment, FULL_TEXT_FORMAT_VERSION, FullTextResult, OpenedSegment, decode_delete_file,
    encode_delete_file,
};
use risingwave_hummock_sdk::full_text_index::{
    FullTextDeleteFileInfo, FullTextDeletionVectorInfo, FullTextIndex, FullTextSegmentInfo,
};
use risingwave_hummock_sdk::{HummockEpoch, HummockObjectId};

use crate::hummock::store::object_index_writer::ObjectIndexFileIdManagerRef;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};

pub struct CachedFullTextSegment {
    pub segment: OpenedSegment,
    pub memory_size: usize,
}

pub type CachedFullTextSegmentRef = Arc<CachedFullTextSegment>;

async fn spawn_blocking<T: Send + 'static>(
    f: impl FnOnce() -> FullTextResult<T> + Send + 'static,
) -> HummockResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| HummockError::other(format!("full-text blocking task failed: {e}")))?
        .map_err(Into::into)
}

/// Opens a segment together with its current deletion vector.
///
/// The cache is keyed by the deletion vector object id when present, so that a segment opened
/// with an outdated deletion vector is never returned. Object ids are globally unique.
pub async fn open_segment(
    sstable_store: &SstableStoreRef,
    info: &FullTextSegmentInfo,
) -> HummockResult<CachedFullTextSegmentRef> {
    let cache_key = match &info.deletion_vector {
        Some(dv) => dv.object_id.as_raw(),
        None => info.object_id.as_raw(),
    };
    let store = sstable_store.store();
    let segment_path =
        sstable_store.get_object_data_path(HummockObjectId::FullTextSegment(info.object_id));
    let dv_path = info.deletion_vector.as_ref().map(|dv| {
        sstable_store.get_object_data_path(HummockObjectId::FullTextDeletionVector(dv.object_id))
    });
    let segment_meta = info.segment_meta.clone();
    let entry = sstable_store
        .full_text_segment_cache
        .get_or_fetch(&cache_key, || async move {
            let bundle = store.read(&segment_path, ..).await?;
            let deletion_vector = match dv_path {
                Some(path) => Some(store.read(&path, ..).await?),
                None => None,
            };
            let bundle_size = bundle.len();
            let segment =
                spawn_blocking(move || OpenedSegment::open(&segment_meta, bundle, deletion_vector))
                    .await?;
            let memory_size =
                bundle_size + segment.pk_directory_heap_size() + segment.deletion_vector_size();
            Ok::<_, anyhow::Error>(Arc::new(CachedFullTextSegment {
                segment,
                memory_size,
            }))
        })
        .await
        .map_err(HummockError::foyer_error)?;
    Ok(entry.value().clone())
}

/// Returns the de-duplicated keys of alive documents that contain all `tokens`, at most `limit`
/// of them.
///
/// Delete files that have not been resolved into deletion vectors are applied here, so the
/// result is exact at the version of `index`.
pub async fn search(
    sstable_store: &SstableStoreRef,
    index: &FullTextIndex,
    tokens: Vec<String>,
    limit: Option<usize>,
) -> HummockResult<Vec<Bytes>> {
    if index.info.format_version != FULL_TEXT_FORMAT_VERSION {
        return Err(HummockError::invalid_format_version(
            index.info.format_version,
        ));
    }
    if tokens.is_empty() {
        return Err(HummockError::other(
            "full-text search requires at least one token",
        ));
    }
    if limit == Some(0) {
        return Ok(vec![]);
    }
    const READ_CONCURRENCY: usize = 8;
    let delete_files: Vec<_> = stream::iter(&index.pending_delete_files)
        .map(|info| async move {
            let file = read_delete_file(sstable_store, info).await?;
            Ok::<_, HummockError>((info, file))
        })
        .buffered(READ_CONCURRENCY)
        .try_collect()
        .await?;
    let tokens = Arc::new(tokens);
    let futures = index
        .segments
        .iter()
        .filter(|segment| segment.num_alive() > 0)
        .map(|segment| {
            // A marker only deletes documents written before its epoch.
            let delete_files: Vec<DecodedDeleteFileRef> = delete_files
                .iter()
                .filter(|(info, _)| {
                    info.epoch > segment.epoch
                        && info.min_key <= segment.max_key
                        && segment.min_key <= info.max_key
                })
                .map(|(_, file)| file.clone())
                .collect();
            let tokens = tokens.clone();
            async move {
                let segment = open_segment(sstable_store, segment).await?;
                spawn_blocking(move || {
                    if delete_files.is_empty() {
                        return segment.segment.search_filtered(&tokens, None, limit);
                    }
                    let is_deleted =
                        |key: &[u8]| delete_files.iter().any(|file| file.contains(key));
                    segment
                        .segment
                        .search_filtered(&tokens, Some(&is_deleted), limit)
                })
                .await
            }
        });
    // An unordered LIMIT can stop before opening later segments. Do not prefetch them: a
    // whole bundle may be large, and dropping spawn_blocking does not cancel running work.
    let mut results =
        stream::iter(futures).buffered(if limit.is_some() { 1 } else { READ_CONCURRENCY });
    let mut keys = BTreeSet::new();
    while let Some(batch) = results.try_next().await? {
        keys.extend(batch);
        if limit.is_some_and(|limit| keys.len() >= limit) {
            break;
        }
    }
    Ok(keys.into_iter().take(limit.unwrap_or(usize::MAX)).collect())
}

/// A decoded delete file. Its keys are sorted and de-duplicated.
pub struct DecodedDeleteFile {
    /// Markers in this file delete keys in segments with a strictly older epoch.
    pub epoch: u64,
    pub keys: Vec<Bytes>,
    pub memory_size: usize,
}

impl DecodedDeleteFile {
    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys
            .binary_search_by(|probe| probe.as_ref().cmp(key))
            .is_ok()
    }
}

pub type DecodedDeleteFileRef = Arc<DecodedDeleteFile>;

pub async fn read_delete_file(
    sstable_store: &SstableStoreRef,
    info: &FullTextDeleteFileInfo,
) -> HummockResult<DecodedDeleteFileRef> {
    let path =
        sstable_store.get_object_data_path(HummockObjectId::FullTextDeleteFile(info.object_id));
    let store = sstable_store.store();
    let entry = sstable_store
        .full_text_delete_file_cache
        .get_or_fetch(&info.object_id.as_raw(), || async move {
            let data = store.read(&path, ..).await?;
            let (epoch, keys) = decode_delete_file(&data)?;
            // The keys are slices of one buffer of the file's size.
            let memory_size = data.len() + keys.len() * size_of::<Bytes>();
            Ok::<_, anyhow::Error>(Arc::new(DecodedDeleteFile {
                epoch,
                keys,
                memory_size,
            }))
        })
        .await
        .map_err(HummockError::foyer_error)?;
    let file = entry.value();
    if file.epoch != info.epoch {
        return Err(HummockError::decode_error(
            "full-text delete file epoch does not match the manifest",
        ));
    }
    Ok(file.clone())
}

async fn upload(
    sstable_store: &SstableStoreRef,
    object_id: HummockObjectId,
    data: Bytes,
) -> HummockResult<()> {
    let path = sstable_store.get_object_data_path(object_id);
    sstable_store.store().upload(&path, data).await?;
    Ok(())
}

async fn upload_chunks(
    sstable_store: &SstableStoreRef,
    object_id: HummockObjectId,
    chunks: impl Iterator<Item = Bytes> + Send,
) -> HummockResult<u64> {
    // Tantivy components and individual keys can be much larger than an upload block.
    const MAX_WRITE_SIZE: usize = 8 << 20;
    let path = sstable_store.get_object_data_path(object_id);
    let mut uploader = sstable_store.create_streaming_uploader(&path).await?;
    let mut file_size = 0;
    for mut chunk in chunks {
        file_size += chunk.len() as u64;
        while chunk.len() > MAX_WRITE_SIZE {
            uploader.write_bytes(chunk.split_to(MAX_WRITE_SIZE)).await?;
        }
        uploader.write_bytes(chunk).await?;
    }
    uploader.finish().await?;
    Ok(file_size)
}

pub async fn upload_deletion_vector(
    sstable_store: &SstableStoreRef,
    object_id_manager: &ObjectIndexFileIdManagerRef,
    data: Bytes,
    num_deleted: u64,
) -> HummockResult<FullTextDeletionVectorInfo> {
    let object_id = object_id_manager.get_new_file_object_id().await?.into();
    let file_size = data.len() as u64;
    upload(
        sstable_store,
        HummockObjectId::FullTextDeletionVector(object_id),
        data,
    )
    .await?;
    Ok(FullTextDeletionVectorInfo {
        object_id,
        file_size,
        num_deleted,
    })
}

pub async fn upload_segment(
    sstable_store: &SstableStoreRef,
    object_id_manager: &ObjectIndexFileIdManagerRef,
    epoch: HummockEpoch,
    segment: BuiltSegment,
) -> HummockResult<FullTextSegmentInfo> {
    let object_id = object_id_manager.get_new_file_object_id().await?.into();
    let file_size = upload_chunks(
        sstable_store,
        HummockObjectId::FullTextSegment(object_id),
        segment.bundle.into_chunks(),
    )
    .await?;
    let deletion_vector = match segment.deletion_vector {
        Some((data, num_deleted)) => {
            Some(upload_deletion_vector(sstable_store, object_id_manager, data, num_deleted).await?)
        }
        None => None,
    };
    Ok(FullTextSegmentInfo {
        object_id,
        file_size,
        segment_meta: segment.segment_meta,
        num_docs: segment.num_docs,
        epoch,
        min_key: segment.min_key,
        max_key: segment.max_key,
        deletion_vector,
    })
}

pub async fn upload_delete_file(
    sstable_store: &SstableStoreRef,
    object_id_manager: &ObjectIndexFileIdManagerRef,
    epoch: u64,
    keys: &[Bytes],
) -> HummockResult<FullTextDeleteFileInfo> {
    assert!(!keys.is_empty());
    let object_id = object_id_manager.get_new_file_object_id().await?.into();
    let file_size = upload_chunks(
        sstable_store,
        HummockObjectId::FullTextDeleteFile(object_id),
        encode_delete_file(epoch, keys),
    )
    .await?;
    Ok(FullTextDeleteFileInfo {
        object_id,
        file_size,
        epoch,
        num_keys: keys.len() as u64,
        min_key: keys.first().unwrap().clone(),
        max_key: keys.last().unwrap().clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use risingwave_common::config::ObjectStoreConfig;
    use risingwave_full_text::SegmentBuilder;
    use risingwave_hummock_sdk::HummockRawObjectId;
    use risingwave_object_store::object::build_remote_object_store;
    use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
    use crate::hummock::store::object_index_writer::ObjectIndexFileIdManager;

    #[derive(Default)]
    struct TestFileIds(AtomicU64);

    #[async_trait::async_trait]
    impl ObjectIndexFileIdManager for TestFileIds {
        async fn get_new_file_object_id(&self) -> HummockResult<HummockRawObjectId> {
            Ok(HummockRawObjectId::new(
                self.0.fetch_add(1, Ordering::Relaxed),
            ))
        }

        fn discard_unsealed_file(&self, _: HummockRawObjectId) {}

        fn new_upload_scope(self: Arc<Self>) -> ObjectIndexFileIdManagerRef {
            self
        }
    }

    #[tokio::test]
    async fn test_streaming_full_text_objects() {
        let temp = tempfile::tempdir().unwrap();
        let store = build_remote_object_store(
            &format!("fs://{}", temp.path().display()),
            Arc::new(ObjectStoreMetrics::unused()),
            "full_text_test",
            Arc::new(ObjectStoreConfig::default()),
        )
        .await;
        let store = mock_sstable_store_with_object_store(Arc::new(store)).await;
        let ids: ObjectIndexFileIdManagerRef = Arc::new(TestFileIds::default());
        // Exceed both the encoding block size and the maximum bytes sent in a single write.
        let large_key = Bytes::from(vec![b'y'; 9 << 20]);
        let mut builder = SegmentBuilder::new().unwrap();
        builder.add(&large_key, &mut |emit| emit("term")).unwrap();
        let deleted = builder.add(b"z", &mut |emit| emit("term")).unwrap();
        builder.delete(deleted);
        let built = builder.finish().unwrap().unwrap();
        let info = upload_segment(&store, &ids, 42, built).await.unwrap();
        let path = store.get_object_data_path(HummockObjectId::FullTextSegment(info.object_id));
        assert_eq!(
            store.store().metadata(&path).await.unwrap().total_size as u64,
            info.file_size
        );
        let segment = open_segment(&store, &info).await.unwrap();
        assert_eq!(
            segment.segment.search(&["term".to_owned()]).unwrap(),
            [large_key.clone()]
        );

        let keys: Vec<_> = (0..2048)
            .map(|i| Bytes::from(format!("{i:04}{}", "a".repeat(256))))
            .chain([large_key, Bytes::from_static(b"z")])
            .collect();
        let info = upload_delete_file(&store, &ids, 43, &keys).await.unwrap();
        let path = store.get_object_data_path(HummockObjectId::FullTextDeleteFile(info.object_id));
        assert_eq!(
            store.store().metadata(&path).await.unwrap().total_size as u64,
            info.file_size
        );
        let decoded = read_delete_file(&store, &info).await.unwrap();
        assert_eq!(decoded.epoch, 43);
        assert_eq!(decoded.keys, keys);
    }
}
