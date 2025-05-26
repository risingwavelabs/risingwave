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

use std::mem::take;

use bytes::{Bytes, BytesMut};
use prost::Message;
use rand::SeedableRng;
use rand::rngs::StdRng;
use risingwave_hummock_sdk::HummockObjectId;
use risingwave_hummock_sdk::vector_index::{
    HnswFlatIndex, HnswFlatIndexAdd, HnswGraphFileInfo, VectorFileInfo, VectorIndexAdd,
    VectorStoreInfoDelta,
};
use risingwave_pb::hummock::PbHnswGraph;

use crate::dispatch_measurement;
use crate::hummock::vector::file::VectorFileBuilder;
use crate::hummock::vector::writer::new_vector_file_builder;
use crate::hummock::vector::{EnumVectorAccessor, get_vector_block};
use crate::hummock::{HummockError, HummockResult, ObjectIdManagerRef, SstableStoreRef};
use crate::opts::StorageOpts;
use crate::store::Vector;
use crate::vector::DistanceMeasurement;
use crate::vector::hnsw::{
    HnswBuilderOptions, HnswGraphBuilder, VectorAccessor, VectorStore, insert_graph, new_node,
};

struct HnswVectorStore {
    sstable_store: SstableStoreRef,

    committed_vector_files: Vec<VectorFileInfo>,
    committed_next_vector_id: usize,
    sealed_vector_files: Vec<VectorFileInfo>,
    sealed_next_vector_id: usize,
    flushed_vector_files: Vec<VectorFileInfo>,
    flushed_next_vector_id: usize,
    building_vectors: VectorFileBuilder,
}

impl HnswVectorStore {
    fn new(
        index: &HnswFlatIndex,
        dimension: usize,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
        storage_opts: &StorageOpts,
    ) -> Self {
        let next_vector_id = index.vector_store_info.next_vector_id;
        Self {
            sstable_store: sstable_store.clone(),
            committed_vector_files: index.vector_store_info.vector_files.clone(),
            committed_next_vector_id: next_vector_id,
            sealed_vector_files: vec![],
            sealed_next_vector_id: next_vector_id,
            flushed_vector_files: vec![],
            flushed_next_vector_id: next_vector_id,
            building_vectors: new_vector_file_builder(
                dimension,
                next_vector_id,
                sstable_store,
                object_id_manager,
                storage_opts,
            ),
        }
    }

    async fn flush(&mut self) -> HummockResult<usize> {
        if let Some((vector_file, blocks, meta)) = self.building_vectors.finish().await? {
            self.sstable_store
                .insert_vector_cache(vector_file.object_id, meta, blocks);
            let file_size = vector_file.file_size as usize;
            self.flushed_vector_files.push(vector_file);
            self.flushed_next_vector_id = self.building_vectors.next_vector_id();
            Ok(file_size)
        } else {
            Ok(0)
        }
    }
}

impl VectorStore for HnswVectorStore {
    type Accessor<'a>
        = EnumVectorAccessor<'a>
    where
        Self: 'a;

    async fn get_vector(&self, idx: usize) -> HummockResult<Self::Accessor<'_>> {
        if idx < self.committed_next_vector_id {
            Ok(EnumVectorAccessor::BloclHolder(
                get_vector_block(&self.sstable_store, &self.committed_vector_files, idx).await?,
            ))
        } else if idx < self.sealed_next_vector_id {
            Ok(EnumVectorAccessor::BloclHolder(
                get_vector_block(&self.sstable_store, &self.sealed_vector_files, idx).await?,
            ))
        } else if idx < self.flushed_next_vector_id {
            Ok(EnumVectorAccessor::BloclHolder(
                get_vector_block(&self.sstable_store, &self.flushed_vector_files, idx).await?,
            ))
        } else if idx < self.building_vectors.next_vector_id() {
            self.building_vectors.get_vector(idx)
        } else {
            Err(HummockError::other(format!(
                "Index {} out of bounds for all vector {}",
                idx,
                self.building_vectors.next_vector_id()
            )))
        }
    }
}

pub(crate) struct HnswFlatIndexWriter {
    measure: DistanceMeasurement,
    options: HnswBuilderOptions,
    sstable_store: SstableStoreRef,
    object_id_manager: ObjectIdManagerRef,

    vector_store: HnswVectorStore,
    next_pending_vector_id: usize,
    graph_builder: Option<HnswGraphBuilder>,
    flushed_graph_file: Option<HnswGraphFileInfo>,
    rng: StdRng,
}

impl HnswFlatIndexWriter {
    pub(crate) async fn new(
        index: &HnswFlatIndex,
        dimension: usize,
        measure: DistanceMeasurement,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
        storage_opts: &StorageOpts,
    ) -> HummockResult<Self> {
        let graph_builder = if let Some(graph_file) = &index.graph_file {
            Some(HnswGraphBuilder::from_protobuf(
                &sstable_store.get_hnsw_graph(graph_file).await?,
            ))
        } else {
            None
        };
        Ok(Self {
            measure,
            options: HnswBuilderOptions {
                m: index.config.m.try_into().unwrap(),
                ef_construction: index.config.ef_construction.try_into().unwrap(),
                max_level: index.config.max_level.try_into().unwrap(),
            },
            vector_store: HnswVectorStore::new(
                index,
                dimension,
                sstable_store.clone(),
                object_id_manager.clone(),
                storage_opts,
            ),
            sstable_store,
            object_id_manager,
            graph_builder,
            flushed_graph_file: None,
            rng: StdRng::from_os_rng(),
            next_pending_vector_id: index.vector_store_info.next_vector_id,
        })
    }

    pub(crate) fn insert(&mut self, vec: Vector, info: Bytes) -> HummockResult<()> {
        self.vector_store.building_vectors.add(vec.to_ref(), &info);
        Ok(())
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<VectorIndexAdd> {
        assert!(self.vector_store.building_vectors.is_empty());
        if self.vector_store.flushed_vector_files.is_empty() {
            assert_eq!(self.flushed_graph_file, None);
            return None;
        }
        let flushed_vector_files = take(&mut self.vector_store.flushed_vector_files);
        self.vector_store.sealed_next_vector_id = self.vector_store.flushed_next_vector_id;
        self.vector_store
            .sealed_vector_files
            .extend(flushed_vector_files.iter().cloned());
        let new_graph_info = self
            .flushed_graph_file
            .take()
            .expect("should have new graph info when having new data");
        Some(VectorIndexAdd::HnswFlat(HnswFlatIndexAdd {
            vector_store_info_delta: VectorStoreInfoDelta {
                next_vector_id: self.vector_store.building_vectors.next_vector_id(),
                added_vector_files: flushed_vector_files,
            },
            graph_file: new_graph_info,
        }))
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        self.add_pending_vectors_to_graph().await?;
        let size = self.vector_store.flush().await?;
        if !self.vector_store.flushed_vector_files.is_empty() {
            let graph_builder = self
                .graph_builder
                .as_ref()
                .expect("builder should exist when having newly flushed data");
            let pb_graph = graph_builder.to_protobuf();
            let mut buffer = BytesMut::with_capacity(pb_graph.encoded_len());
            PbHnswGraph::encode(&pb_graph, &mut buffer).unwrap();
            let encoded_graph = buffer.freeze();
            let size = encoded_graph.len();
            let object_id = self.object_id_manager.get_new_object_id().await?;
            let path = self
                .sstable_store
                .get_object_data_path(HummockObjectId::HnswGraphFile(object_id));
            self.sstable_store
                .store()
                .upload(&path, encoded_graph)
                .await?;
            self.flushed_graph_file = Some(HnswGraphFileInfo {
                object_id,
                file_size: size as _,
            });
        }
        Ok(size)
    }

    pub(crate) async fn try_flush(&mut self) -> HummockResult<()> {
        self.vector_store.building_vectors.try_flush().await?;
        self.add_pending_vectors_to_graph().await
    }

    async fn add_pending_vectors_to_graph(&mut self) -> HummockResult<()> {
        for i in self.next_pending_vector_id..self.vector_store.building_vectors.next_vector_id() {
            let node = new_node(&self.options, &mut self.rng);
            if let Some(graph_builder) = &mut self.graph_builder {
                dispatch_measurement!(&self.measure, M, {
                    insert_graph::<M>(
                        &self.vector_store,
                        graph_builder,
                        node,
                        self.vector_store.building_vectors.get_vector(i)?.vec_ref(),
                        self.options.ef_construction,
                    )
                    .await?;
                });
            } else {
                self.graph_builder = Some(HnswGraphBuilder::first(node));
            }
        }
        self.next_pending_vector_id = self.vector_store.building_vectors.next_vector_id();
        Ok(())
    }
}
