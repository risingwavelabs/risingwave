// Copyright 2023 RisingWave Labs
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

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::window_function::{
    create_window_state, StateKey, WindowFuncCall, WindowStates,
};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::task::{BatchTaskContext, ShutdownToken};

/// [`SortOverWindowExecutor`] accepts input chunks sorted by partition key and order key, and
/// outputs chunks with window function result columns.
pub struct SortOverWindowExecutor {
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
    shutdown_rx: ShutdownToken,

    inner: ExecutorInner,
}

struct ExecutorInner {
    calls: Vec<WindowFuncCall>,
    partition_key_indices: Vec<usize>,
    order_key_indices: Vec<usize>,
    order_key_order_types: Vec<OrderType>,
    chunk_size: usize,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for SortOverWindowExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let node = try_match_expand!(
            source.plan_node().get_node_body()?,
            NodeBody::SortOverWindow
        )?;

        let calls: Vec<_> = node
            .get_calls()
            .iter()
            .map(WindowFuncCall::from_protobuf)
            .try_collect()?;
        let partition_key_indices = node
            .get_partition_by()
            .iter()
            .map(|i| *i as usize)
            .collect();
        let (order_key_indices, order_key_order_types) = node
            .get_order_by()
            .iter()
            .map(ColumnOrder::from_protobuf)
            .map(|o| (o.column_index, o.order_type))
            .unzip();

        let mut schema = child.schema().clone();
        calls.iter().for_each(|call| {
            schema.fields.push(Field::unnamed(call.return_type.clone()));
        });

        Ok(Box::new(Self {
            child,
            schema,
            identity: source.plan_node().get_identity().clone(),
            shutdown_rx: source.shutdown_rx.clone(),

            inner: ExecutorInner {
                calls,
                partition_key_indices,
                order_key_indices,
                order_key_order_types,
                chunk_size: source.context.get_config().developer.chunk_size,
            },
        }))
    }
}

impl Executor for SortOverWindowExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl ExecutorInner {
    fn get_partition_key(&self, full_row: impl Row) -> OwnedRow {
        full_row
            .project(&self.partition_key_indices)
            .into_owned_row()
    }

    fn encode_order_key(&self, full_row: impl Row) -> Result<MemcmpEncoded> {
        Ok(memcmp_encoding::encode_row(
            full_row.project(&self.order_key_indices),
            &self.order_key_order_types,
        )?)
    }

    fn row_to_state_key(&self, full_row: impl Row) -> Result<StateKey> {
        Ok(StateKey {
            order_key: self.encode_order_key(full_row)?,
            pk: OwnedRow::empty().into(), // we don't rely on the pk part in `WindowStates`
        })
    }
}

impl SortOverWindowExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            child,
            schema,
            shutdown_rx,
            inner: this,
            ..
        } = *self;

        let mut chunk_builder = DataChunkBuilder::new(schema.data_types(), this.chunk_size);
        let mut curr_part_key = None;
        let mut curr_part_rows: Vec<OwnedRow> = Vec::new();

        #[for_await]
        for chunk in child.execute() {
            shutdown_rx.check()?;

            let chunk = chunk?;
            for row in chunk.rows() {
                let part_key = this.get_partition_key(row);
                if Some(&part_key) != curr_part_key.as_ref() {
                    if !curr_part_rows.is_empty() {
                        #[for_await]
                        for output_chunk in Self::gen_output_for_partition(
                            &this,
                            &mut curr_part_rows,
                            &mut chunk_builder,
                        ) {
                            yield output_chunk?;
                        }
                        assert!(curr_part_rows.is_empty(), "all rows should be consumed");
                    }
                    curr_part_key = Some(part_key);
                }
                curr_part_rows.push(row.to_owned_row());
            }
        }
        if !curr_part_rows.is_empty() {
            #[for_await]
            for output_chunk in
                Self::gen_output_for_partition(&this, &mut curr_part_rows, &mut chunk_builder)
            {
                yield output_chunk?;
            }
            assert!(curr_part_rows.is_empty(), "all rows should be consumed");
        }
        if let Some(output_chunk) = chunk_builder.consume_all() {
            yield output_chunk;
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn gen_output_for_partition<'a>(
        this: &'a ExecutorInner,
        rows: &'a mut Vec<OwnedRow>,
        chunk_builder: &'a mut DataChunkBuilder,
    ) {
        let mut states =
            WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?);
        for row in &*rows {
            for (call, state) in this.calls.iter().zip_eq_fast(states.iter_mut()) {
                // TODO(rc): batch appending
                state.append(
                    this.row_to_state_key(row)?,
                    row.project(call.args.val_indices())
                        .into_owned_row()
                        .as_inner()
                        .into(),
                );
            }
        }
        for row in rows.drain(..) {
            if let Some(chunk) = chunk_builder
                .append_one_row(row.chain(OwnedRow::new(states.slide_no_evict_hint()?)))
            {
                yield chunk;
            }
        }
    }
}
