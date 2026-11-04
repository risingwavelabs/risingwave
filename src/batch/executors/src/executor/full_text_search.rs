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

use anyhow::anyhow;
use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema, TableId, TableOption};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_storage::store::{NewReadSnapshotOptions, StateStoreReadFullText};
use risingwave_storage::{StateStore, dispatch_state_store};

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::error::{BatchError, Result};

/// Searches a full-text index and outputs the distinct stream keys of rows whose document
/// matches, exactly as of the query epoch.
pub struct FullTextSearchExecutor<S: StateStore> {
    identity: String,
    schema: Schema,
    chunk_size: usize,

    state_store: S,
    table_id: TableId,
    tokens: Vec<String>,
    query_epoch: BatchQueryEpoch,
    limit: Option<usize>,
    key_serde: OrderedRowSerde,
}

pub struct FullTextSearchExecutorBuilder {}

impl BoxedExecutorBuilder for FullTextSearchExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.is_empty(), "FullTextSearch should have no input");
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::FullTextSearch
        )?;
        let key_types: Vec<DataType> = node.key_types.iter().map(DataType::from).collect();
        let schema = Schema::new(
            key_types
                .iter()
                .enumerate()
                .map(|(i, data_type)| Field::new(format!("key_{i}"), data_type.clone()))
                .collect(),
        );
        let key_serde = OrderedRowSerde::new(
            key_types.clone(),
            vec![OrderType::ascending(); key_types.len()],
        );
        let query_epoch = node
            .query_epoch
            .ok_or_else(|| anyhow!("query_epoch not set in full-text search"))?;

        dispatch_state_store!(source.context().state_store(), state_store, {
            Ok(Box::new(FullTextSearchExecutor {
                identity: source.plan_node().get_identity().clone(),
                schema,
                chunk_size: source.context().get_config().developer.chunk_size,
                state_store,
                table_id: node.table_id.into(),
                tokens: node.tokens.clone(),
                query_epoch,
                limit: node.limit.map(|limit| limit as usize),
                key_serde,
            }))
        })
    }
}

impl<S: StateStore> Executor for FullTextSearchExecutor<S> {
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

impl<S: StateStore> FullTextSearchExecutor<S> {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let snapshot = self
            .state_store
            .new_read_snapshot(
                self.query_epoch.into(),
                NewReadSnapshotOptions {
                    table_id: self.table_id,
                    table_option: TableOption::default(),
                },
            )
            .await?;
        let keys = snapshot
            .full_text_search(self.tokens.clone(), self.limit)
            .await?;

        let mut builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
        for key in keys {
            let row = self
                .key_serde
                .deserialize(&key)
                .map_err(|e| anyhow!("failed to decode full-text index key: {e}"))?;
            if let Some(chunk) = builder.append_one_row(row) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
    }
}
