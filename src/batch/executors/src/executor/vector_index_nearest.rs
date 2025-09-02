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

use futures::pin_mut;
use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use futures_util::TryStreamExt;
use itertools::Itertools;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, DataChunk, ListArrayBuilder, ListValue, StructArrayBuilder,
    StructValue,
};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::row::RowDeserializer;
use risingwave_common::types::{DataType, ScalarImpl, ScalarRef, StructType};
use risingwave_common::util::value_encoding::BasicDeserializer;
use risingwave_common::vector::distance::DistanceMeasurement;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::{BatchQueryEpoch, PbDistanceType};
use risingwave_storage::store::{
    NewReadSnapshotOptions, StateStoreReadVector, VectorNearestOptions,
};
use risingwave_storage::{StateStore, dispatch_state_store};

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::error::{BatchError, Result};

pub struct VectorIndexNearestExecutor<S: StateStore> {
    identity: String,
    schema: Schema,
    vector_info_struct_type: StructType,

    input: BoxedExecutor,

    state_store: S,
    table_id: TableId,
    epoch: BatchQueryEpoch,
    vector_column_idx: usize,
    top_n: usize,
    measure: DistanceMeasurement,
    deserializer: BasicDeserializer,
}

pub struct VectorIndexNearestExecutorBuilder {}

impl BoxedExecutorBuilder for VectorIndexNearestExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 1,
            "VectorIndexNearest should have an input executor!"
        );
        let [input]: [_; 1] = inputs.try_into().unwrap();
        let vector_index_nearest_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::VectorIndexNearest
        )?;

        let deserializer = RowDeserializer::new(
            vector_index_nearest_node
                .info_column_desc
                .iter()
                .map(|col| DataType::from(col.column_type.clone().unwrap()))
                .collect_vec(),
        );

        let vector_info_struct_type = StructType::new(
            vector_index_nearest_node
                .info_column_desc
                .iter()
                .map(|col| {
                    (
                        col.name.clone(),
                        DataType::from(col.column_type.clone().unwrap()),
                    )
                })
                .chain([("__distance".to_owned(), DataType::Float64)]),
        );

        let mut schema = input.schema().clone();
        schema.fields.push(Field::new(
            "vector_info",
            DataType::List(DataType::Struct(vector_info_struct_type.clone()).into()),
        ));

        let epoch = source.epoch();
        dispatch_state_store!(source.context().state_store(), state_store, {
            Ok(Box::new(VectorIndexNearestExecutor {
                identity: source.plan_node().get_identity().clone(),
                schema,
                vector_info_struct_type,
                input,
                state_store,
                table_id: vector_index_nearest_node.table_id.into(),
                epoch,
                vector_column_idx: vector_index_nearest_node.vector_column_idx as usize,
                top_n: vector_index_nearest_node.top_n as usize,
                measure: PbDistanceType::try_from(vector_index_nearest_node.distance_type)
                    .unwrap()
                    .into(),
                deserializer,
            }))
        })
    }
}
impl<S: StateStore> Executor for VectorIndexNearestExecutor<S> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl<S: StateStore> VectorIndexNearestExecutor<S> {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            state_store,
            table_id,
            epoch,
            vector_info_struct_type,
            input,
            vector_column_idx,
            top_n,
            measure,
            deserializer,
            ..
        } = *self;

        let input = input.execute();
        pin_mut!(input);

        let read_snapshot: S::ReadSnapshot = state_store
            .new_read_snapshot(epoch.into(), NewReadSnapshotOptions { table_id })
            .await?;

        let deserializer = Arc::new(deserializer);
        let sqrt_distance = match &self.measure {
            DistanceMeasurement::L2Sqr => true,
            DistanceMeasurement::L1
            | DistanceMeasurement::Cosine
            | DistanceMeasurement::InnerProduct => false,
        };

        while let Some(chunk) = input.try_next().await? {
            let mut vector_info_columns_builder = ListArrayBuilder::with_type(
                chunk.cardinality(),
                DataType::List(DataType::Struct(vector_info_struct_type.clone()).into()),
            );
            let (mut columns, vis) = chunk.into_parts();
            let vector_column = columns[vector_column_idx].as_vector();
            for (idx, vis) in vis.iter().enumerate() {
                if vis && let Some(vector) = vector_column.value_at(idx) {
                    let deserializer = deserializer.clone();
                    let row_results: Vec<Result<StructValue>> = read_snapshot
                        .nearest(
                            vector.to_owned_scalar(),
                            VectorNearestOptions { top_n, measure },
                            move |_vec, distance, value| {
                                let mut values =
                                    Vec::with_capacity(deserializer.data_types().len() + 1);
                                deserializer.deserialize_to(value, &mut values)?;
                                let distance = if sqrt_distance {
                                    distance.sqrt()
                                } else {
                                    distance
                                };
                                values.push(Some(ScalarImpl::Float64(distance.into())));
                                Ok(StructValue::new(values))
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
                    vector_info_columns_builder
                        .append_owned(Some(ListValue::new(ArrayImpl::Struct(struct_array))));
                } else {
                    vector_info_columns_builder.append_null();
                }
            }
            columns.push(ArrayImpl::List(vector_info_columns_builder.finish()).into());

            yield DataChunk::new(columns, vis);
        }
    }
}
