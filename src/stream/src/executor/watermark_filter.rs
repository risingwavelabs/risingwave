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

use std::cmp;

use futures::future::join_all;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{bail, row};
use risingwave_expr::expr::{
    build, BoxedExpression, Expression, InputRefExpression, LiteralExpression,
};
use risingwave_expr::Result as ExprResult;
use risingwave_pb::expr::expr_node::Type;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::filter::FilterExecutor;
use super::{
    ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message, StreamExecutorResult,
};
use crate::common::table::state_table::StateTable;
use crate::executor::{expect_first_barrier, Watermark};

/// The executor will generate a `Watermark` after each chunk.
/// This will also guarantee all later rows with event time **less than** the watermark will be
/// filtered.
pub struct WatermarkFilterExecutor<S: StateStore> {
    input: BoxedExecutor,
    /// The expression used to calculate the watermark value.
    watermark_expr: BoxedExpression,
    /// The column we should generate watermark and filter on.
    event_time_col_idx: usize,
    ctx: ActorContextRef,
    info: ExecutorInfo,
    table: StateTable<S>,
}

impl<S: StateStore> WatermarkFilterExecutor<S> {
    pub fn new(
        input: BoxedExecutor,
        watermark_expr: BoxedExpression,
        event_time_col_idx: usize,
        ctx: ActorContextRef,
        table: StateTable<S>,
    ) -> Self {
        let info = input.info();

        Self {
            input,
            watermark_expr,
            event_time_col_idx,
            ctx,
            info,
            table,
        }
    }
}

impl<S: StateStore> Executor for WatermarkFilterExecutor<S> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn info(&self) -> ExecutorInfo {
        self.info.clone()
    }
}

impl<S: StateStore> WatermarkFilterExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            input,
            event_time_col_idx,
            watermark_expr,
            ctx,
            info,
            mut table,
        } = *self;

        let watermark_type = watermark_expr.return_type();
        assert_eq!(
            watermark_type,
            input.schema().data_types()[event_time_col_idx]
        );
        let mut input = input.execute();

        let first_barrier = expect_first_barrier(&mut input).await?;
        table.init_epoch(first_barrier.epoch);
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        // Initiate and yield the first watermark.
        let mut current_watermark =
            Self::get_global_max_watermark(&table, watermark_type.clone()).await?;

        let mut last_checkpoint_watermark = watermark_type.min();

        yield Message::Watermark(Watermark::new(
            event_time_col_idx,
            watermark_type.clone(),
            current_watermark.clone(),
        ));

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact();

                    // Empty chunk should not be processed.
                    if chunk.cardinality() == 0 {
                        continue;
                    }

                    let watermark_array = watermark_expr
                        .eval_infallible(chunk.data_chunk(), |err| {
                            ctx.on_compute_error(err, &info.identity)
                        })
                        .await;

                    // Build the expression to calculate watermark filter.
                    let watermark_filter_expr = Self::build_watermark_filter_expr(
                        watermark_type.clone(),
                        event_time_col_idx,
                        current_watermark.clone(),
                    )?;

                    // NULL watermark should not be considered.
                    let max_watermark = watermark_array.iter().flatten().max();

                    if let Some(max_watermark) = max_watermark {
                        // Assign a new watermark.
                        current_watermark =
                            cmp::max(current_watermark, max_watermark.into_scalar_impl());
                    }

                    let pred_output = watermark_filter_expr
                        .eval_infallible(chunk.data_chunk(), |err| {
                            ctx.on_compute_error(err, &info.identity)
                        })
                        .await;

                    if let Some(output_chunk) = FilterExecutor::filter(chunk, pred_output)? {
                        yield Message::Chunk(output_chunk);
                    };

                    yield Message::Watermark(Watermark::new(
                        event_time_col_idx,
                        watermark_type.clone(),
                        current_watermark.clone(),
                    ));
                }
                Message::Watermark(watermark) => {
                    if watermark.col_idx == event_time_col_idx {
                        tracing::warn!("WatermarkFilterExecutor received a watermark on the event it is filtering.");
                        let watermark = watermark.val;
                        if watermark > current_watermark {
                            current_watermark = watermark;
                            yield Message::Watermark(Watermark::new(
                                event_time_col_idx,
                                watermark_type.clone(),
                                current_watermark.clone(),
                            ));
                        }
                    } else {
                        yield Message::Watermark(watermark)
                    }
                }
                Message::Barrier(barrier) => {
                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(ctx.id) {
                        let (previous_vnode_bitmap, _cache_may_stale) =
                            table.update_vnode_bitmap(vnode_bitmap.clone());

                        // Take the global max watermark when scaling happens.
                        if previous_vnode_bitmap != vnode_bitmap {
                            current_watermark =
                                Self::get_global_max_watermark(&table, watermark_type.clone())
                                    .await?;
                        }
                    }

                    if barrier.checkpoint && last_checkpoint_watermark != current_watermark {
                        last_checkpoint_watermark = current_watermark.clone();
                        // Persist the watermark when checkpoint arrives.
                        let vnodes = table.get_vnodes();
                        for vnode in vnodes.iter_vnodes() {
                            let pk = Some(ScalarImpl::Int16(vnode.to_scalar()));
                            let row = [pk, Some(current_watermark.clone())];
                            // FIXME(yuhao): use upsert.
                            table.insert(row);
                        }
                        table.commit(barrier.epoch).await?;
                    } else {
                        table.commit_no_data_expected(barrier.epoch);
                    }

                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    fn build_watermark_filter_expr(
        watermark_type: DataType,
        event_time_col_idx: usize,
        watermark: ScalarImpl,
    ) -> ExprResult<BoxedExpression> {
        build(
            Type::GreaterThanOrEqual,
            DataType::Boolean,
            vec![
                InputRefExpression::new(watermark_type.clone(), event_time_col_idx).boxed(),
                LiteralExpression::new(watermark_type, Some(watermark)).boxed(),
            ],
        )
    }

    async fn get_global_max_watermark(
        table: &StateTable<S>,
        watermark_type: DataType,
    ) -> StreamExecutorResult<ScalarImpl> {
        let watermark_iter_futures = (0..VirtualNode::COUNT).map(|vnode| async move {
            let pk = row::once(Some(ScalarImpl::Int16(vnode as _)));
            let watermark_row: Option<OwnedRow> = table.get_row(pk).await?;
            match watermark_row {
                Some(row) => {
                    if row.len() == 1 {
                        Ok::<_, StreamExecutorError>(row[0].to_owned())
                    } else {
                        bail!("The watermark row should only contains 1 datum");
                    }
                }
                _ => Ok(None),
            }
        });
        let watermarks: Vec<_> = join_all(watermark_iter_futures)
            .await
            .into_iter()
            .try_collect()?;

        // Return the minimal value if the remote max watermark is Null.
        let watermark = watermarks
            .into_iter()
            .flatten()
            .max()
            .unwrap_or_else(|| watermark_type.min());

        Ok(watermark)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::NaiveDateWrapper;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::build_from_pretty;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::Distribution;

    use super::*;
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::ActorContext;

    const WATERMARK_TYPE: DataType = DataType::Timestamp;

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        val_indices: &[usize],
        table_id: u32,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();

        // TODO: use consistent operations for watermark filter after we have upsert.
        StateTable::new_with_distribution_inconsistent_op(
            mem_state,
            TableId::new(table_id),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
            Distribution::all_vnodes(vec![0]),
            Some(val_indices.to_vec()),
        )
        .await
    }

    async fn create_watermark_filter_executor(
        mem_state: MemoryStateStore,
    ) -> (BoxedExecutor, MessageSender) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int16),        // pk
                Field::unnamed(WATERMARK_TYPE.clone()), // watermark column
            ],
        };

        let watermark_expr = build_from_pretty("(subtract:timestamp $1:timestamp 1day:interval)");

        let table = create_in_memory_state_table(
            mem_state,
            &[DataType::Int16, WATERMARK_TYPE],
            &[OrderType::ascending()],
            &[0],
            &[1],
            0,
        )
        .await;

        let (tx, source) = MockSource::channel(schema, vec![0]);

        (
            WatermarkFilterExecutor::new(
                source.boxed(),
                watermark_expr,
                1,
                ActorContext::create(123),
                table,
            )
            .boxed(),
            tx,
        )
    }

    #[tokio::test]
    async fn test_watermark_filter() {
        let chunk1 = StreamChunk::from_pretty(
            "  I TS
             + 1 2022-11-07T00:00:00
             + 2 2022-11-08T00:00:00
             + 3 2022-11-06T00:00:00",
        );
        let chunk2 = StreamChunk::from_pretty(
            "  I TS
             + 4 2022-11-07T00:00:00
             + 5 2022-11-06T00:00:00
             + 6 2022-11-10T00:00:00",
        );
        let chunk3 = StreamChunk::from_pretty(
            "  I TS
             + 7 2022-11-14T00:00:00
             + 8 2022-11-09T00:00:00
             + 9 2022-11-08T00:00:00",
        );

        let mem_state = MemoryStateStore::new();

        let (executor, mut tx) = create_watermark_filter_executor(mem_state.clone()).await;
        let mut executor = executor.execute();

        // push the init barrier
        tx.push_barrier(1, false);
        executor.next().await.unwrap().unwrap();

        macro_rules! watermark {
            ($scalar:expr) => {
                Watermark::new(1, WATERMARK_TYPE.clone(), $scalar)
            };
        }

        // Init watermark
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(WATERMARK_TYPE.min()),
        );

        // push the 1st chunk
        tx.push_chunk(chunk1);
        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                "  I TS
                 + 1 2022-11-07T00:00:00
                 + 2 2022-11-08T00:00:00
                 + 3 2022-11-06T00:00:00",
            )
        );
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                NaiveDateWrapper::from_ymd_uncheck(2022, 11, 7).and_hms_uncheck(0, 0, 0)
            ))
        );

        // push the 2nd barrier
        tx.push_barrier(2, false);
        executor.next().await.unwrap().unwrap();

        // push the 2nd chunk
        tx.push_chunk(chunk2);
        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                "  I TS
                 + 4 2022-11-07T00:00:00
                 + 6 2022-11-10T00:00:00",
            )
        );
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                NaiveDateWrapper::from_ymd_uncheck(2022, 11, 9).and_hms_uncheck(0, 0, 0)
            ))
        );

        // push the 3nd barrier
        tx.push_barrier(3, false);
        executor.next().await.unwrap().unwrap();

        // Drop executor
        drop(executor);

        // Build new executor
        let (executor, mut tx) = create_watermark_filter_executor(mem_state.clone()).await;
        let mut executor = executor.execute();

        // push the 1st barrier after failover
        tx.push_barrier(4, false);
        executor.next().await.unwrap().unwrap();

        // Init watermark after failover
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                NaiveDateWrapper::from_ymd_uncheck(2022, 11, 9).and_hms_uncheck(0, 0, 0)
            ))
        );

        // push the 3rd chunk
        tx.push_chunk(chunk3);
        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                "  I TS
                 + 7 2022-11-14T00:00:00
                 + 8 2022-11-09T00:00:00",
            )
        );
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                NaiveDateWrapper::from_ymd_uncheck(2022, 11, 13).and_hms_uncheck(0, 0, 0)
            ))
        );
    }
}
