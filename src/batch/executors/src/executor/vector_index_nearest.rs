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

use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::row::{Row, RowDeserializer, RowExt};
use risingwave_common::types::{DataType, Scalar, ScalarImpl, VectorVal};
use risingwave_common::util::value_encoding::BasicDeserializer;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::{BatchQueryEpoch, PbDistanceType};
use risingwave_storage::store::{
    NewReadSnapshotOptions, StateStoreReadVector, VectorNearestOptions,
};
use risingwave_storage::table::collect_data_chunk;
use risingwave_storage::vector::{DistanceMeasurement, Vector, VectorRef};
use risingwave_storage::{StateStore, dispatch_state_store};

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::error::{BatchError, Result};

pub struct VectorIndexNearestExecutor<S: StateStore> {
    chunk_size: usize,
    identity: String,
    schema: Schema,

    state_store: S,
    table_id: TableId,
    epoch: BatchQueryEpoch,
    vector: VectorVal,
    top_n: usize,
    measure: DistanceMeasurement,
    deserializer: BasicDeserializer,
    include_vector_col: bool,
    include_distance_col: bool,
}

pub struct VectorIndexNearestExecutorBuilder {}

impl BoxedExecutorBuilder for VectorIndexNearestExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "VectorIndexNearest should not have input executor!"
        );
        let vector_index_nearest_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::VectorIndexNearest
        )?;

        let mut fields = vector_index_nearest_node
            .info_column_desc
            .iter()
            .map(|col| Field {
                data_type: DataType::from(col.column_type.clone().unwrap()),
                name: col.name.clone(),
            })
            .collect_vec();

        let deserializer = RowDeserializer::new(
            fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect_vec(),
        );

        if vector_index_nearest_node.include_vector_col {
            fields.push(Field::new(
                "__vector",
                DataType::Vector(vector_index_nearest_node.query_vector.len()),
            ));
        }

        if vector_index_nearest_node.include_distance_col {
            fields.push(Field::new("__distance", DataType::Float64));
        }

        let schema = Schema::new(fields);

        let epoch = source.epoch();
        let chunk_size = source.context().get_config().developer.chunk_size;
        dispatch_state_store!(source.context().state_store(), state_store, {
            Ok(Box::new(VectorIndexNearestExecutor {
                chunk_size,
                identity: source.plan_node().get_identity().clone(),
                schema,
                state_store,
                table_id: vector_index_nearest_node.table_id.into(),
                epoch,
                vector: VectorVal::from_iter(
                    vector_index_nearest_node.query_vector.iter().cloned(),
                ),
                top_n: vector_index_nearest_node.top_n as usize,
                measure: PbDistanceType::try_from(vector_index_nearest_node.distance_type)
                    .unwrap()
                    .into(),
                deserializer,
                include_vector_col: vector_index_nearest_node.include_vector_col,
                include_distance_col: vector_index_nearest_node.include_distance_col,
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
            chunk_size,
            state_store,
            table_id,
            epoch,
            vector,
            top_n,
            measure,
            schema,
            deserializer,
            include_vector_col,
            include_distance_col,
            ..
        } = *self;
        let read_snapshot: S::ReadSnapshot = state_store
            .new_read_snapshot(epoch.into(), NewReadSnapshotOptions { table_id })
            .await?;
        let rows = read_snapshot
            .nearest(
                Vector::new(vector.as_scalar_ref().into_slice()),
                VectorNearestOptions { top_n, measure },
                move |vec: VectorRef<'_>, distance, value| {
                    deserializer.deserialize(value).map(|row| {
                        // TODO: optimize without repeated to_owned_row
                        let row = if include_vector_col {
                            row.chain([Some(ScalarImpl::Vector(VectorVal::from_iter(
                                vec.as_slice().iter().cloned(),
                            )))])
                            .to_owned_row()
                        } else {
                            row
                        };
                        if include_distance_col {
                            row.chain([Some(ScalarImpl::Float64((distance as f64).into()))])
                                .to_owned_row()
                        } else {
                            row
                        }
                    })
                },
            )
            .await?;
        let mut stream = futures::stream::iter(rows);
        loop {
            let chunk = collect_data_chunk(&mut stream, &schema, Some(chunk_size)).await?;

            if let Some(chunk) = chunk {
                yield chunk
            } else {
                break;
            }
        }
    }
}
