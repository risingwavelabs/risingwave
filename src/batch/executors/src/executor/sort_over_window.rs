// Copyright 2024 RisingWave Labs
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

use futures::future::{BoxFuture, FutureExt};
use futures_async_stream::{for_await, try_stream};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::window_function::{
    StateKey, WindowFuncCall, WindowStates, create_window_state,
};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    PushContext, PushSink, PushStatus,
};
use crate::error::{BatchError, Result};
use crate::task::ShutdownToken;

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

impl BoxedExecutorBuilder for SortOverWindowExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
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
            shutdown_rx: source.shutdown_rx().clone(),

            inner: ExecutorInner {
                calls,
                partition_key_indices,
                order_key_indices,
                order_key_order_types,
                chunk_size: source.context().get_config().developer.chunk_size,
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

    fn execute_push<'a>(
        self: Box<Self>,
        context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let SortOverWindowExecutor {
                child,
                schema,
                identity: _,
                shutdown_rx,
                inner,
            } = *self;

            let mut state = SortOverWindowPushState::new(schema, shutdown_rx, inner);
            let mut child_sink = SortOverWindowChildSink {
                state: &mut state,
                sink,
            };
            child.execute_push(context, &mut child_sink).await?;

            state.finish(sink).await
        }
        .boxed()
    }
}

struct SortOverWindowPushState {
    shutdown_rx: ShutdownToken,
    inner: ExecutorInner,
    chunk_builder: DataChunkBuilder,
    curr_part_key: Option<OwnedRow>,
    curr_part_rows: Vec<OwnedRow>,
    downstream_finished: bool,
}

impl SortOverWindowPushState {
    fn new(schema: Schema, shutdown_rx: ShutdownToken, inner: ExecutorInner) -> Self {
        let chunk_builder = DataChunkBuilder::new(schema.data_types(), inner.chunk_size);
        Self {
            shutdown_rx,
            inner,
            chunk_builder,
            curr_part_key: None,
            curr_part_rows: Vec::new(),
            downstream_finished: false,
        }
    }

    async fn flush_partition(&mut self, sink: &mut dyn PushSink) -> Result<PushStatus> {
        if self.downstream_finished || self.curr_part_rows.is_empty() {
            return Ok(if self.downstream_finished {
                PushStatus::Finished
            } else {
                PushStatus::NeedMoreInput
            });
        }

        #[for_await]
        for output_chunk in SortOverWindowExecutor::gen_output_for_partition(
            &self.inner,
            &mut self.curr_part_rows,
            &mut self.chunk_builder,
        ) {
            let output_chunk = output_chunk?;
            let status = sink.push(output_chunk).await?;
            self.downstream_finished |= status.is_finished();
            if self.downstream_finished {
                return Ok(PushStatus::Finished);
            }
        }
        assert!(
            self.curr_part_rows.is_empty(),
            "all rows should be consumed"
        );
        Ok(PushStatus::NeedMoreInput)
    }

    async fn consume_input_chunk(
        &mut self,
        chunk: DataChunk,
        sink: &mut dyn PushSink,
    ) -> Result<PushStatus> {
        if self.downstream_finished {
            return Ok(PushStatus::Finished);
        }

        self.shutdown_rx.check()?;
        for row in chunk.rows() {
            let part_key = self.inner.get_partition_key(row);
            if Some(&part_key) != self.curr_part_key.as_ref() {
                if self.flush_partition(sink).await?.is_finished() {
                    return Ok(PushStatus::Finished);
                }
                self.curr_part_key = Some(part_key);
            }
            self.curr_part_rows.push(row.to_owned_row());
        }
        Ok(PushStatus::NeedMoreInput)
    }

    async fn finish(mut self, sink: &mut dyn PushSink) -> Result<PushStatus> {
        if !self.downstream_finished {
            self.flush_partition(sink).await?;
        }
        if !self.downstream_finished
            && let Some(output_chunk) = self.chunk_builder.consume_all()
        {
            self.downstream_finished |= sink.push(output_chunk).await?.is_finished();
        }
        sink.finish().await
    }
}

struct SortOverWindowChildSink<'a, 's> {
    state: &'a mut SortOverWindowPushState,
    sink: &'s mut dyn PushSink,
}

impl PushSink for SortOverWindowChildSink<'_, '_> {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move { self.state.consume_input_chunk(chunk, self.sink).await }.boxed()
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
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
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

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
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
