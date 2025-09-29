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

use anyhow::anyhow;
use futures::pin_mut;
use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use futures_util::TryStreamExt;
use risingwave_common::array::{Array, ArrayBuilder, ArrayImpl, DataChunk, ListArrayBuilder};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_storage::table::batch_table::VectorIndexReader;
use risingwave_storage::{StateStore, dispatch_state_store};

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::error::{BatchError, Result};

pub struct VectorIndexNearestExecutor<S: StateStore> {
    identity: String,
    schema: Schema,

    input: BoxedExecutor,
    query_epoch: BatchQueryEpoch,
    vector_column_idx: usize,

    reader: VectorIndexReader<S>,
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

        dispatch_state_store!(source.context().state_store(), state_store, {
            let reader = VectorIndexReader::new(
                vector_index_nearest_node.reader_desc.as_ref().unwrap(),
                state_store,
            );

            let mut schema = input.schema().clone();
            schema.fields.push(Field::new(
                "vector_info",
                DataType::list(DataType::Struct(reader.info_struct_type().clone())),
            ));

            Ok(Box::new(VectorIndexNearestExecutor {
                identity: source.plan_node().get_identity().clone(),
                schema,
                query_epoch: vector_index_nearest_node.query_epoch.ok_or_else(|| {
                    anyhow!("vector_index_query not set in distributed lookup join")
                })?,
                vector_column_idx: vector_index_nearest_node.vector_column_idx as usize,
                input,
                reader,
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
            query_epoch,
            input,
            vector_column_idx,
            ..
        } = *self;

        let input = input.execute();
        pin_mut!(input);

        let read_snapshot = self.reader.new_snapshot(query_epoch.into()).await?;

        while let Some(chunk) = input.try_next().await? {
            let mut vector_info_columns_builder = ListArrayBuilder::with_type(
                chunk.cardinality(),
                DataType::list(DataType::Struct(self.reader.info_struct_type().clone())),
            );
            let (mut columns, vis) = chunk.into_parts();
            let vector_column = columns[vector_column_idx].as_vector();
            for (idx, vis) in vis.iter().enumerate() {
                if vis && let Some(vector) = vector_column.value_at(idx) {
                    let value = read_snapshot.query(vector).await?;
                    vector_info_columns_builder.append_owned(Some(value));
                } else {
                    vector_info_columns_builder.append_null();
                }
            }
            columns.push(ArrayImpl::List(vector_info_columns_builder.finish()).into());

            yield DataChunk::new(columns, vis);
        }
    }
}
