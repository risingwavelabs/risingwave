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
    VectorStoreDelta,
};
use risingwave_pb::hummock::PbHnswGraph;

use crate::dispatch_measurement;
use crate::hummock::vector::block::VectorBlockBuilder;
use crate::hummock::vector::{VectorBlockAccessor, search_vector_files};
use crate::hummock::{HummockError, HummockResult, ObjectIdManagerRef, SstableStoreRef};
use crate::store::Vector;
use crate::vector::hnsw::{
    HnswBuilderOptions, HnswGraphBuilder, VectorAccessor, VectorStore, insert_graph, new_node,
};
use crate::vector::{DistanceMeasurement, VectorRef};

enum HnswVectorAccessor<'a> {
    Block(&'a VectorBlockBuilder, usize),
    File(VectorBlockAccessor),
}

impl VectorAccessor for HnswVectorAccessor<'_> {
    fn vec_ref(&self) -> VectorRef<'_> {
        match self {
            HnswVectorAccessor::Block(builder, idx) => builder.vec_ref(*idx),
            HnswVectorAccessor::File(accessor) => accessor.vec_ref(),
        }
    }

    fn info(&self) -> &[u8] {
        match self {
            HnswVectorAccessor::Block(builder, idx) => builder.info(*idx),
            HnswVectorAccessor::File(accessor) => accessor.info(),
        }
    }
}

struct HnswVectorStore {
    sstable_store: SstableStoreRef,
    object_id_manager: ObjectIdManagerRef,

    committed_vector_files: Vec<VectorFileInfo>,
    committed_next_vector_id: usize,
    sealed_vector_files: Vec<VectorFileInfo>,
    sealed_next_vector_id: usize,
    flushed_vector_files: Vec<VectorFileInfo>,
    flushed_next_vector_id: usize,
    building_vectors: Option<VectorBlockBuilder>,
    next_vector_id: usize,
}

impl HnswVectorStore {
    fn new(
        index: &HnswFlatIndex,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
    ) -> Self {
        let next_vector_id = index.vector_store.next_vector_id;
        Self {
            sstable_store,
            object_id_manager,
            committed_vector_files: index.vector_store.vector_files.clone(),
            committed_next_vector_id: next_vector_id,
            sealed_vector_files: vec![],
            sealed_next_vector_id: next_vector_id,
            flushed_vector_files: vec![],
            flushed_next_vector_id: next_vector_id,
            building_vectors: None,
            next_vector_id,
        }
    }

    async fn flush(&mut self) -> HummockResult<usize> {
        if let Some(builder) = self.building_vectors.take()
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
                vector_count,
                file_size: size as _,
                start_vector_id: self.flushed_next_vector_id,
            });
            self.flushed_next_vector_id = self.next_vector_id;
            Ok(size)
        } else {
            Ok(0)
        }
    }
}

impl VectorStore for HnswVectorStore {
    type Accessor<'a>
        = HnswVectorAccessor<'a>
    where
        Self: 'a;

    async fn get_vector(&self, idx: usize) -> HummockResult<Self::Accessor<'_>> {
        if idx < self.committed_next_vector_id {
            let (vector_file, offset) = search_vector_files(&self.committed_vector_files, idx)?;
            let block = self.sstable_store.get_vector_block(vector_file).await?;
            Ok(HnswVectorAccessor::File(VectorBlockAccessor {
                block,
                idx: offset,
            }))
        } else if idx < self.sealed_next_vector_id {
            let (vector_file, offset) = search_vector_files(&self.sealed_vector_files, idx)?;
            let block = self.sstable_store.get_vector_block(vector_file).await?;
            Ok(HnswVectorAccessor::File(VectorBlockAccessor {
                block,
                idx: offset,
            }))
        } else if idx < self.flushed_next_vector_id {
            let (vector_file, offset) = search_vector_files(&self.flushed_vector_files, idx)?;
            let block = self.sstable_store.get_vector_block(vector_file).await?;
            Ok(HnswVectorAccessor::File(VectorBlockAccessor {
                block,
                idx: offset,
            }))
        } else if idx < self.next_vector_id {
            let builder = self
                .building_vectors
                .as_ref()
                .expect("should have building vectors");
            Ok(HnswVectorAccessor::Block(
                builder,
                idx - self.flushed_next_vector_id,
            ))
        } else {
            Err(HummockError::other(format!(
                "Index {} out of bounds for all vector {}",
                idx, self.next_vector_id
            )))
        }
    }
}

pub(crate) struct HnswFlatIndexWriter {
    dimension: usize,
    measure: DistanceMeasurement,
    options: HnswBuilderOptions,

    vector_store: HnswVectorStore,
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
    ) -> HummockResult<Self> {
        let graph_builder = if let Some(graph_file) = &index.graph_file {
            Some(HnswGraphBuilder::from_protobuf(
                &sstable_store.get_hnsw_graph(graph_file).await?,
            ))
        } else {
            None
        };
        Ok(Self {
            dimension,
            measure,
            options: HnswBuilderOptions {
                m: index.config.m.try_into().unwrap(),
                ef_construction: index.config.ef_construction.try_into().unwrap(),
                max_level: index.config.max_level.try_into().unwrap(),
            },
            vector_store: HnswVectorStore::new(index, sstable_store, object_id_manager),
            graph_builder,
            flushed_graph_file: None,
            rng: StdRng::from_os_rng(),
        })
    }

    pub(crate) fn insert(&mut self, vec: Vector, info: Bytes) -> HummockResult<()> {
        let builder = self
            .vector_store
            .building_vectors
            .get_or_insert_with(|| VectorBlockBuilder::new(self.dimension));
        builder.add(vec.to_ref(), &info);
        self.vector_store.next_vector_id += 1;
        Ok(())
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<VectorIndexAdd> {
        assert!(self.vector_store.building_vectors.is_none());
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
            vector_store_delta: VectorStoreDelta {
                next_vector_id: self.vector_store.next_vector_id,
                added_vector_files: flushed_vector_files,
            },
            graph_file: new_graph_info,
        }))
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        if let Some(building_vectors) = &self.vector_store.building_vectors
            && building_vectors.count() > 0
        {
            for i in 0..building_vectors.count() {
                let node = new_node(&self.options, &mut self.rng);
                if let Some(graph_builder) = &mut self.graph_builder {
                    dispatch_measurement!(&self.measure, M, {
                        insert_graph::<M>(
                            &self.vector_store,
                            graph_builder,
                            node,
                            building_vectors.vec_ref(i),
                            self.options.ef_construction,
                        )
                        .await?;
                    });
                } else {
                    self.graph_builder = Some(HnswGraphBuilder::first(node));
                }
            }
        }
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
            let object_id = self
                .vector_store
                .object_id_manager
                .get_new_object_id()
                .await?;
            let path = self
                .vector_store
                .sstable_store
                .get_object_data_path(HummockObjectId::HnswGraphFile(object_id));
            self.vector_store
                .sstable_store
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

    #[expect(clippy::unused_async)]
    pub(crate) async fn try_flush(&mut self) -> HummockResult<()> {
        Ok(())
    }
}
