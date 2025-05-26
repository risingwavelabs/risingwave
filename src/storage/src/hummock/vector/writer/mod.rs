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

use bytes::Bytes;
use risingwave_hummock_sdk::vector_index::{
    FlatIndex, FlatIndexAdd, VectorFileInfo, VectorIndex, VectorIndexAdd, VectorIndexImpl,
    VectorStoreDelta,
};

use crate::hummock::vector::block::VectorBlockBuilder;
use crate::hummock::vector::writer::hnsw::HnswFlatIndexWriter;
use crate::hummock::{HummockResult, ObjectIdManagerRef, SstableStoreRef};
use crate::vector::{DistanceMeasurement, Vector};

pub enum VectorWriterImpl {
    Flat(FlatIndexWriter),
    HnswFlat(HnswFlatIndexWriter),
}

impl VectorWriterImpl {
    pub(crate) async fn new(
        index: &VectorIndex,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
    ) -> HummockResult<Self> {
        Ok(match &index.inner {
            VectorIndexImpl::Flat(flat) => VectorWriterImpl::Flat(FlatIndexWriter::new(
                flat,
                index.dimension,
                sstable_store.clone(),
                object_id_manager.clone(),
            )),
            VectorIndexImpl::HnswFlat(hnsw_flat) => VectorWriterImpl::HnswFlat(
                HnswFlatIndexWriter::new(
                    hnsw_flat,
                    index.dimension,
                    DistanceMeasurement::from(index.distance_type),
                    sstable_store.clone(),
                    object_id_manager.clone(),
                )
                .await?,
            ),
        })
    }

    pub(crate) fn insert(&mut self, vec: Vector, info: Bytes) -> HummockResult<()> {
        match self {
            VectorWriterImpl::Flat(writer) => writer.insert(vec, info),
            VectorWriterImpl::HnswFlat(writer) => writer.insert(vec, info),
        }
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<VectorIndexAdd> {
        match self {
            VectorWriterImpl::Flat(writer) => writer.seal_current_epoch(),
            VectorWriterImpl::HnswFlat(writer) => writer.seal_current_epoch(),
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
    sstable_store: SstableStoreRef,
    object_id_manager: ObjectIdManagerRef,
    dimension: usize,

    next_vector_id: usize,
    flushed_vector_files: Vec<VectorFileInfo>,
    block_builder: Option<VectorBlockBuilder>,
}

impl FlatIndexWriter {
    pub(crate) fn new(
        index: &FlatIndex,
        dimension: usize,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
    ) -> Self {
        Self {
            sstable_store,
            object_id_manager,
            dimension,
            next_vector_id: index.vector_store.next_vector_id,
            flushed_vector_files: vec![],
            block_builder: None,
        }
    }

    pub(crate) fn insert(&mut self, vec: Vector, info: Bytes) -> HummockResult<()> {
        let builder = self
            .block_builder
            .get_or_insert_with(|| VectorBlockBuilder::new(self.dimension));
        builder.add(vec.to_ref(), &info);
        Ok(())
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<VectorIndexAdd> {
        assert!(self.block_builder.is_none());
        if self.flushed_vector_files.is_empty() {
            return None;
        }
        Some(VectorIndexAdd::Flat(FlatIndexAdd {
            vector_store_delta: VectorStoreDelta {
                next_vector_id: self.next_vector_id,
                added_vector_files: take(&mut self.flushed_vector_files),
            },
        }))
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        if let Some(builder) = self.block_builder.take()
            && let Some(block) = builder.finish()
        {
            let vector_count = block.count();
            let object_id = self.object_id_manager.get_new_object_id().await?;
            let size = self
                .sstable_store
                .put_vector_block(object_id, &block)
                .await?;

            self.flushed_vector_files.push(VectorFileInfo {
                object_id,
                vector_count: vector_count as _,
                file_size: size as _,
                start_vector_id: self.next_vector_id,
            });
            self.next_vector_id += vector_count;
            Ok(size)
        } else {
            Ok(0)
        }
    }

    #[expect(clippy::unused_async)]
    pub(crate) async fn try_flush(&mut self) -> HummockResult<()> {
        // TODO: flush when the buffer is full
        Ok(())
    }
}
