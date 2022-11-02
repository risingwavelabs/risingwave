// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::types::{to_datum_ref, DataType, Datum, ToOwnedDatum};
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{BoxedExpression, Expression, InputRefExpression, LiteralExpression};
use risingwave_expr::Result as ExprResult;
use risingwave_pb::expr::expr_node::Type;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::filter::SimpleFilterExecutor;
use super::{ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message};
use crate::common::InfallibleExpression;
use crate::executor::{expect_first_barrier, Watermark};

pub struct WatermarkFilterExecutor<S: StateStore> {
    input: BoxedExecutor,
    watermark_expr: BoxedExpression,
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
            info,
            ctx,
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
        let mut current_watermark = None;

        
        yield Message::Watermark(Watermark::new(event_time_col_idx, current_watermark.clone()));

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
                        .eval_infallible(&chunk.data_chunk(), |err| {
                            ctx.on_compute_error(err, &info.identity)
                        });

                    // Build the expression to calculate watermark filter.
                    let cardinality = watermark_array.len();
                    let watermark_filter_expr = Self::build_watermark_filter_expr(
                        watermark_type.clone(),
                        current_watermark.clone(),
                    )?;

                    // Assign a new watermark.
                    for watermark in watermark_array.iter() {
                        // Ignore the Null watermark
                        if watermark.is_some() && to_datum_ref(&current_watermark) < watermark {
                            current_watermark = watermark.to_owned_datum();
                        }
                    }

                    // Build the watermark filter.
                    let watermark_chunk =
                        DataChunk::new(vec![Column::new(watermark_array)], cardinality);
                    let pred_output = watermark_filter_expr
                        .eval_infallible(&watermark_chunk, |err| {
                            ctx.on_compute_error(err, &info.identity)
                        });

                    if let Some(output_chunk) = SimpleFilterExecutor::filter(chunk, pred_output)? {
                        yield Message::Chunk(output_chunk);
                    };

                    yield Message::Watermark(Watermark::new(event_time_col_idx, current_watermark.clone()));
                }
                Message::Watermark(watermark) => {
                    if watermark.col_idx != event_time_col_idx {
                        yield Message::Watermark(watermark)
                    } else {
                        bail!("WatermarkFilterExecutor should not receive a watermark on the event it is filtering.")
                    }
                }
                Message::Barrier(barrier) => {
                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(ctx.id) {
                        let previous_vnode_bitmap = table.update_vnode_bitmap(vnode_bitmap.clone());
                        
                        if previous_vnode_bitmap != vnode_bitmap {
                            
                        }
                    }
                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    fn build_watermark_filter_expr(
        watermark_type: DataType,
        watermark: Datum,
    ) -> ExprResult<BoxedExpression> {
        new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            InputRefExpression::new(watermark_type.clone(), 0).boxed(),
            LiteralExpression::new(watermark_type, watermark).boxed(),
        )
    }
}

#[cfg(test)]
mod tests {}
