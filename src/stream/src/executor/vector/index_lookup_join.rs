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

use std::sync::Arc;

use anyhow::anyhow;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, DataChunk, ListArrayBuilder, ListValue, Op, StreamChunk,
    StructArrayBuilder, StructValue,
};
use risingwave_common::catalog::TableId;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl, ScalarRef, StructType};
use risingwave_common::util::value_encoding::{
    BasicDeserializer, BasicSerializer, ValueRowSerializer,
};
use risingwave_common::vector::distance::DistanceMeasurement;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    InitOptions, NewReadSnapshotOptions, NewVectorWriterOptions, SealCurrentEpochOptions,
    StateStoreReadVector, StateStoreWriteEpochControl, StateStoreWriteVector, VectorNearestOptions,
};

use crate::executor::prelude::try_stream;
use crate::executor::{
    BoxedMessageStream, Execute, Executor, Message, StreamExecutorError, StreamExecutorResult,
    expect_first_barrier,
};

pub struct VectorIndexLookupJoinExecutor<S: StateStore> {
    input: Executor,
    store: S,
    vector_info_struct_type: StructType,

    info_output_indices: Vec<usize>,
    include_distance: bool,

    table_id: TableId,
    vector_column_idx: usize,
    top_n: usize,
    measure: DistanceMeasurement,
    deserializer: BasicDeserializer,

    hnsw_ef_search: usize,
}

impl<S: StateStore> Execute for VectorIndexLookupJoinExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        Box::pin(self.execute_inner())
    }
}

impl<S: StateStore> VectorIndexLookupJoinExecutor<S> {
    pub fn new(
        input: Executor,
        store: S,
        vector_info_struct_type: StructType,
        info_output_indices: Vec<usize>,
        include_distance: bool,

        table_id: TableId,
        vector_column_idx: usize,
        top_n: usize,
        measure: DistanceMeasurement,
        deserializer: BasicDeserializer,

        hnsw_ef_search: usize,
    ) -> Self {
        Self {
            input,
            store,
            vector_info_struct_type,
            info_output_indices,
            include_distance,
            table_id,
            vector_column_idx,
            top_n,
            measure,
            deserializer,
            hnsw_ef_search,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(mut self) {
        let Self {
            input,
            store,
            vector_info_struct_type,
            info_output_indices,
            include_distance,
            table_id,
            vector_column_idx,
            top_n,
            measure,
            deserializer,
            hnsw_ef_search,
        } = self;
        let deserializer = Arc::new(deserializer);
        let sqrt_distance = match &self.measure {
            DistanceMeasurement::L2Sqr => true,
            DistanceMeasurement::L1
            | DistanceMeasurement::Cosine
            | DistanceMeasurement::InnerProduct => false,
        };

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);

        let todo = 0;
        let mut read_snapshot: S::ReadSnapshot = store
            .new_read_snapshot(
                HummockReadEpoch::Committed(first_epoch.prev),
                NewReadSnapshotOptions { table_id },
            )
            .await?;

        while let Some(msg) = input.try_next().await? {
            match msg {
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
                Message::Chunk(chunk) => {
                    let (chunk, ops) = chunk.into_parts();
                    let mut vector_info_columns_builder = ListArrayBuilder::with_type(
                        chunk.cardinality(),
                        DataType::list(DataType::Struct(vector_info_struct_type.clone())),
                    );
                    let (mut columns, vis) = chunk.into_parts();
                    let vector_column = columns[vector_column_idx].as_vector();
                    for (idx, vis) in vis.iter().enumerate() {
                        if vis && let Some(vector) = vector_column.value_at(idx) {
                            let deserializer = deserializer.clone();
                            let info_output_indices = info_output_indices.clone();
                            let struct_len = vector_info_struct_type.len();
                            let row_results: Vec<StreamExecutorResult<StructValue>> = read_snapshot
                                .nearest(
                                    vector.to_owned_scalar(),
                                    VectorNearestOptions {
                                        top_n,
                                        measure,
                                        hnsw_ef_search,
                                    },
                                    move |_vec, distance, value| {
                                        let mut values =
                                            Vec::with_capacity(deserializer.data_types().len());
                                        deserializer.deserialize_to(value, &mut values)?;
                                        let mut info = Vec::with_capacity(struct_len);
                                        for idx in &*info_output_indices {
                                            info.push(values[*idx].clone());
                                        }
                                        if include_distance {
                                            let distance = if sqrt_distance {
                                                distance.sqrt()
                                            } else {
                                                distance
                                            };
                                            info.push(Some(ScalarImpl::Float64(distance.into())));
                                        }
                                        Ok(StructValue::new(info))
                                    },
                                )
                                .await?;
                            let mut struct_array_builder = StructArrayBuilder::with_type(
                                row_results.len(),
                                DataType::Struct(vector_info_struct_type.clone()),
                            );
                            for row in row_results {
                                let row = row?;
                                struct_array_builder.append_owned(Some(row));
                            }
                            let struct_array = struct_array_builder.finish();
                            vector_info_columns_builder.append_owned(Some(ListValue::new(
                                ArrayImpl::Struct(struct_array),
                            )));
                        } else {
                            vector_info_columns_builder.append_null();
                        }
                    }
                    columns.push(ArrayImpl::List(vector_info_columns_builder.finish()).into());

                    yield Message::Chunk(StreamChunk::new(ops, columns));
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}
