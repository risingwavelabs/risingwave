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
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use prost::Message;
use rand::SeedableRng;
use rand::rngs::StdRng;
use risingwave_common::array::VectorRef;
use risingwave_common::catalog::TableId;
use risingwave_common::dispatch_distance_measurement;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::vector::distance::DistanceMeasurement;
use risingwave_hummock_sdk::HummockObjectId;
use risingwave_hummock_sdk::vector_index::{
    HnswFlatIndex, HnswFlatIndexAdd, HnswGraphFileInfo, VectorFileInfo, VectorStoreInfoDelta,
};
use risingwave_pb::hummock::PbHnswGraph;

use crate::hummock::vector::file::{FileVectorStore, FileVectorStoreCtx};
use crate::hummock::vector::monitor::report_hnsw_stat;
use crate::hummock::vector::writer::VectorObjectIdManagerRef;
use crate::hummock::{HummockResult, SstableStoreRef};
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;
use crate::vector::hnsw::{
    HnswBuilderOptions, HnswGraphBuilder, VectorAccessor, insert_graph, new_node,
};

pub(crate) struct HnswFlatIndexWriter {
    measure: DistanceMeasurement,
    options: HnswBuilderOptions,
    sstable_store: SstableStoreRef,
    object_id_manager: VectorObjectIdManagerRef,
    stats: HnswFlatIndexWriterStats,

    vector_store: FileVectorStore,
    ctx: FileVectorStoreCtx,
    next_pending_vector_id: usize,
    graph_builder: Option<HnswGraphBuilder>,
    unseal_vector_files: Vec<VectorFileInfo>,
    flushed_graph_file: Option<HnswGraphFileInfo>,
    rng: StdRng,
}

impl HnswFlatIndexWriter {
    pub(crate) async fn new(
        table_id: TableId,
        index: &HnswFlatIndex,
        dimension: usize,
        measure: DistanceMeasurement,
        sstable_store: SstableStoreRef,
        object_id_manager: VectorObjectIdManagerRef,
        stats: Arc<HummockStateStoreMetrics>,
        storage_opts: &StorageOpts,
    ) -> HummockResult<Self> {
        let stats = HnswFlatIndexWriterStats::new(table_id, stats);
        let mut ctx = FileVectorStoreCtx::default();
        let graph_builder = if let Some(graph_file) = &index.graph_file {
            stats.hnsw_file_size.set(graph_file.file_size as _);
            Some(HnswGraphBuilder::from_protobuf(
                &*sstable_store
                    .get_hnsw_graph(graph_file, &mut ctx.stats)
                    .await?,
                index.config.m as _,
            ))
        } else {
            None
        };

        let mut writer = Self {
            measure,
            options: HnswBuilderOptions {
                m: index.config.m.try_into().unwrap(),
                ef_construction: index.config.ef_construction.try_into().unwrap(),
                max_level: index.config.max_level.try_into().unwrap(),
            },
            vector_store: FileVectorStore::new_for_writer(
                index,
                dimension,
                sstable_store.clone(),
                object_id_manager.clone(),
                storage_opts,
            ),
            ctx,
            sstable_store,
            object_id_manager,
            graph_builder,
            unseal_vector_files: vec![],
            flushed_graph_file: None,
            rng: StdRng::from_os_rng(),
            next_pending_vector_id: index.vector_store_info.next_vector_id,
            stats,
        };

        writer.report_index_stats();

        Ok(writer)
    }

    pub(crate) fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> HummockResult<()> {
        self.vector_store
            .building_vectors
            .as_mut()
            .expect("for write")
            .file_builder
            .add(vec, &info);
        Ok(())
    }

    pub(crate) fn seal_current_epoch(&mut self) -> Option<HnswFlatIndexAdd> {
        let building_vectors = &mut self
            .vector_store
            .building_vectors
            .as_mut()
            .expect("for write");
        assert!(building_vectors.file_builder.is_empty());
        let added_vector_files = take(&mut self.unseal_vector_files);
        if added_vector_files.is_empty() {
            assert_eq!(self.flushed_graph_file, None);
            return None;
        }
        let next_vector_id = building_vectors.file_builder.next_vector_id();
        self.report_index_stats();
        let new_graph_info = self
            .flushed_graph_file
            .take()
            .expect("should have new graph info when having new data");
        Some(HnswFlatIndexAdd {
            vector_store_info_delta: VectorStoreInfoDelta {
                next_vector_id,
                added_vector_files,
            },
            graph_file: new_graph_info,
        })
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        self.add_pending_vectors_to_graph().await?;
        let new_file = self.vector_store.flush().await?;
        if let Some(new_file) = new_file {
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
                .object_id_manager
                .get_new_vector_object_id()
                .await?
                .into();
            let path = self
                .sstable_store
                .get_object_data_path(HummockObjectId::HnswGraphFile(object_id));
            self.sstable_store
                .store()
                .upload(&path, encoded_graph)
                .await?;
            self.sstable_store
                .insert_hnsw_graph_cache(object_id, pb_graph);
            self.flushed_graph_file = Some(HnswGraphFileInfo {
                object_id,
                file_size: size as _,
            });
            let file_size = new_file.file_size as _;
            self.unseal_vector_files.push(new_file);
            Ok(file_size)
        } else {
            Ok(0)
        }
    }

    pub(crate) async fn try_flush(&mut self) -> HummockResult<()> {
        self.vector_store
            .building_vectors
            .as_mut()
            .expect("for write")
            .file_builder
            .try_flush()
            .await?;
        self.add_pending_vectors_to_graph().await
    }

    async fn add_pending_vectors_to_graph(&mut self) -> HummockResult<()> {
        let building_vectors = self
            .vector_store
            .building_vectors
            .as_ref()
            .expect("for write");
        let mut stats = Vec::with_capacity(
            building_vectors.file_builder.next_vector_id() - self.next_pending_vector_id,
        );
        for i in self.next_pending_vector_id..building_vectors.file_builder.next_vector_id() {
            let node = new_node(&self.options, &mut self.rng);
            if let Some(graph_builder) = &mut self.graph_builder {
                dispatch_distance_measurement!(&self.measure, M, {
                    let stat = insert_graph::<M, _>(
                        &self.vector_store,
                        &mut self.ctx,
                        graph_builder,
                        node,
                        building_vectors.file_builder.get_vector(i).vec_ref(),
                        self.options.ef_construction,
                    )
                    .await?;
                    stats.push(stat);
                });
            } else {
                self.graph_builder = Some(HnswGraphBuilder::first(node));
            }
        }
        take(&mut self.ctx.stats).report(self.stats.table_id, "hnsw_write", &self.stats.stats);
        report_hnsw_stat(
            &self.stats.stats,
            self.stats.table_id,
            "hnsw_write",
            self.options.m,
            self.options.ef_construction,
            stats,
        );
        self.next_pending_vector_id = building_vectors.file_builder.next_vector_id();
        Ok(())
    }

    fn report_index_stats(&mut self) {
        if let Some(graph) = &self.graph_builder {
            for new_level_idx in self.stats.level_node_count.len()..graph.level_node_count().len() {
                self.stats
                    .level_node_count
                    .push(self.stats.new_level_node_gauge(new_level_idx));
            }
            for (level_idx, node_count) in graph.level_node_count().iter().enumerate() {
                self.stats.level_node_count[level_idx].set(*node_count as _);
            }
            let total_vector_file_data_size: usize = self
                .vector_store
                .flushed_vector_files()
                .iter()
                .map(|file| file.file_size as usize)
                .sum();
            let total_vector_file_meta_size: usize = self
                .vector_store
                .flushed_vector_files()
                .iter()
                .map(|file| file.file_size as usize - file.meta_offset)
                .sum();
            self.stats
                .vector_file_count
                .set(self.vector_store.flushed_vector_files().len() as _);
            self.stats
                .vector_file_data_size
                .set(total_vector_file_data_size as _);
            self.stats
                .vector_file_meta_size
                .set(total_vector_file_meta_size as _);
            if let Some(graph_info) = &self.flushed_graph_file {
                self.stats.hnsw_file_size.set(graph_info.file_size as _);
            }
        }
    }
}

struct HnswFlatIndexWriterStats {
    table_id: TableId,
    stats: Arc<HummockStateStoreMetrics>,
    level_node_count: Vec<LabelGuardedIntGauge>,
    vector_file_count: LabelGuardedIntGauge,
    vector_file_data_size: LabelGuardedIntGauge,
    vector_file_meta_size: LabelGuardedIntGauge,
    hnsw_file_size: LabelGuardedIntGauge,
}

impl HnswFlatIndexWriterStats {
    fn new_level_node_gauge(&self, level_idx: usize) -> LabelGuardedIntGauge {
        let table_id_label = format!("{}", self.table_id);
        self.stats
            .vector_hnsw_graph_level_node_count
            .with_guarded_label_values(&[
                table_id_label.as_str(),
                format!("{}", level_idx).as_str(),
            ])
    }

    fn new(table_id: TableId, stats: Arc<HummockStateStoreMetrics>) -> Self {
        let table_id_label = format!("{}", table_id);
        let vector_file_count = stats
            .vector_index_file_count
            .with_guarded_label_values(&[table_id_label.as_str()]);
        let vector_file_data_size = stats
            .vector_index_file_size
            .with_guarded_label_values(&[table_id_label.as_str(), "vector_file_data"]);
        let vector_file_meta_size = stats
            .vector_index_file_size
            .with_guarded_label_values(&[table_id_label.as_str(), "vector_file_meta"]);
        let hnsw_file_size = stats
            .vector_index_file_size
            .with_guarded_label_values(&[table_id_label.as_str(), "hnsw_graph_file"]);

        Self {
            table_id,
            stats,
            level_node_count: vec![],
            vector_file_count,
            vector_file_data_size,
            vector_file_meta_size,
            hnsw_file_size,
        }
    }
}
