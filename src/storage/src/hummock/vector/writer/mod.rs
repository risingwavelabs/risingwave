// Copyright 2025 RisingWave Labs
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

mod hnsw;
use std::mem::take;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use hnsw::HnswFlatIndexWriter;
use risingwave_common::array::VectorRef;
use risingwave_common::vector::distance::DistanceMeasurement;
use risingwave_hummock_sdk::vector_index::{
    FlatIndex, FlatIndexAdd, VectorFileInfo, VectorIndex, VectorIndexAdd, VectorIndexImpl,
    VectorStoreInfoDelta,
};
use risingwave_hummock_sdk::{HummockObjectId, HummockRawObjectId};

use crate::hummock::vector::file::VectorFileBuilder;
use crate::hummock::{HummockResult, ObjectIdManager, SstableStoreRef};
use crate::opts::StorageOpts;

#[async_trait::async_trait]
pub trait VectorObjectIdManager: Send + Sync {
    async fn get_new_vector_object_id(&self) -> HummockResult<HummockRawObjectId>;
}

pub type VectorObjectIdManagerRef = Arc<dyn VectorObjectIdManager>;

#[async_trait::async_trait]
impl VectorObjectIdManager for ObjectIdManager {
    async fn get_new_vector_object_id(&self) -> HummockResult<HummockRawObjectId> {
        self.get_new_object_id().await
    }
}

pub(crate) fn new_vector_file_builder(
    dimension: usize,
    next_vector_id: usize,
    sstable_store: SstableStoreRef,
    object_id_manager: VectorObjectIdManagerRef,
    storage_opts: &StorageOpts,
) -> VectorFileBuilder {
    VectorFileBuilder::new(
        dimension,
        Box::new(move || {
            let object_id_manager = object_id_manager.clone();
            let sstable_store = sstable_store.clone();
            async move {
                let object_id = object_id_manager.get_new_vector_object_id().await?.into();
                let path =
                    sstable_store.get_object_data_path(HummockObjectId::VectorFile(object_id));
                let uploader = sstable_store.create_streaming_uploader(&path).await?;
                Ok((object_id, uploader))
            }
            .boxed()
        }),
        next_vector_id,
        storage_opts.vector_file_block_size_kb * 1024,
    )
}

pub(crate) enum VectorWriterImpl {
    Flat(FlatIndexWriter),
    HnswFlat(HnswFlatIndexWriter),
}

impl VectorWriterImpl {
    pub(crate) async fn new(
        index: &VectorIndex,
        sstable_store: SstableStoreRef,
        object_id_manager: VectorObjectIdManagerRef,
        storage_opts: &StorageOpts,
    ) -> HummockResult<Self> {
        Ok(match &index.inner {
            VectorIndexImpl::Flat(flat) => VectorWriterImpl::Flat(FlatIndexWriter::new(
                flat,
                index.dimension,
                sstable_store,
                object_id_manager,
                storage_opts,
            )),
            VectorIndexImpl::HnswFlat(hnsw_flat) => VectorWriterImpl::HnswFlat(
                HnswFlatIndexWriter::new(
                    hnsw_flat,
                    index.dimension,
                    DistanceMeasurement::from(index.distance_type),
                    sstable_store,
                    object_id_manager,
                    storage_opts,
                )
                .await?,
            ),
        })
    }

    pub(crate) fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> HummockResult<()> {
        match self {
            VectorWriterImpl::Flat(writer) => writer.insert(vec, info),
            VectorWriterImpl::HnswFlat(writer) => writer.insert(vec, info),
        }
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<VectorIndexAdd> {
        match self {
            VectorWriterImpl::Flat(writer) => writer.seal_current_epoch(),
            VectorWriterImpl::HnswFlat(writer) => {
                writer.seal_current_epoch().map(VectorIndexAdd::HnswFlat)
            }
        }
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        match self {
            VectorWriterImpl::Flat(writer) => writer.flush().await,
            VectorWriterImpl::HnswFlat(writer) => writer.flush().await,
        }
    }

    pub(crate) async fn try_flush(&mut self) -> HummockResult<()> {
        match self {
            VectorWriterImpl::Flat(writer) => writer.try_flush().await,
            VectorWriterImpl::HnswFlat(writer) => writer.try_flush().await,
        }
    }
}

pub(crate) struct FlatIndexWriter {
    flushed_vector_files: Vec<VectorFileInfo>,
    sstable_store: SstableStoreRef,
    vector_file_builder: VectorFileBuilder,
}

impl FlatIndexWriter {
    pub(crate) fn new(
        index: &FlatIndex,
        dimension: usize,
        sstable_store: SstableStoreRef,
        object_id_manager: VectorObjectIdManagerRef,
        storage_opts: &StorageOpts,
    ) -> Self {
        Self {
            flushed_vector_files: vec![],
            sstable_store: sstable_store.clone(),
            vector_file_builder: new_vector_file_builder(
                dimension,
                index.vector_store_info.next_vector_id,
                sstable_store,
                object_id_manager,
                storage_opts,
            ),
        }
    }

    pub(crate) fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> HummockResult<()> {
        self.vector_file_builder.add(vec, info.as_ref());
        Ok(())
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<VectorIndexAdd> {
        assert!(self.vector_file_builder.is_empty());
        if self.flushed_vector_files.is_empty() {
            return None;
        }
        Some(VectorIndexAdd::Flat(FlatIndexAdd {
            vector_store_info_delta: VectorStoreInfoDelta {
                next_vector_id: self.vector_file_builder.next_vector_id(),
                added_vector_files: take(&mut self.flushed_vector_files),
            },
        }))
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        if let Some((file_info, blocks, meta)) = self.vector_file_builder.finish().await? {
            self.sstable_store
                .insert_vector_cache(file_info.object_id, meta, blocks);
            let size = file_info.file_size as _;
            self.flushed_vector_files.push(file_info);
            Ok(size)
        } else {
            Ok(0)
        }
    }

    pub(crate) async fn try_flush(&mut self) -> HummockResult<()> {
        self.vector_file_builder.try_flush().await
    }
}
