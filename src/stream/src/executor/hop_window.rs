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

use std::num::NonZeroUsize;

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, StreamChunk, Vis};
use risingwave_common::types::Interval;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_expr::expr::BoxedExpression;
use risingwave_expr::ExprError;

use super::error::StreamExecutorError;
use super::{ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message};

pub struct HopWindowExecutor {
    ctx: ActorContextRef,
    pub input: BoxedExecutor,
    pub info: ExecutorInfo,
    pub time_col_idx: usize,
    pub window_slide: Interval,
    pub window_size: Interval,
    window_start_exprs: Vec<BoxedExpression>,
    window_end_exprs: Vec<BoxedExpression>,
    pub output_indices: Vec<usize>,
    chunk_size: usize,
}

impl HopWindowExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: BoxedExecutor,
        info: ExecutorInfo,
        time_col_idx: usize,
        window_slide: Interval,
        window_size: Interval,
        window_start_exprs: Vec<BoxedExpression>,
        window_end_exprs: Vec<BoxedExpression>,
        output_indices: Vec<usize>,
        chunk_size: usize,
    ) -> Self {
        HopWindowExecutor {
            ctx,
            input,
            info,
            time_col_idx,
            window_slide,
            window_size,
            window_start_exprs,
            window_end_exprs,
            output_indices,
            chunk_size,
        }
    }
}

impl Executor for HopWindowExecutor {
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

impl HopWindowExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            ctx,
            input,

            window_slide,
            window_size,
            output_indices,
            info,
            time_col_idx,

            chunk_size,
            ..
        } = *self;
        let units = window_size
            .exact_div(&window_slide)
            .and_then(|x| NonZeroUsize::new(usize::try_from(x).ok()?))
            .ok_or_else(|| ExprError::InvalidParam {
                name: "window",
                reason: format!(
                    "window_size {} cannot be divided by window_slide {}",
                    window_size, window_slide
                ),
            })?
            .get();

        // The following indices are the output indices as if the downstream needs all input + hop
        // window columns.
        let logical_window_start_col_idx = input.schema().len();
        let logical_window_end_col_idx = input.schema().len() + 1;

        // The following indices are the real output column indices. `None` means we don't need to
        // output that column.
        let out_window_start_col_idx = output_indices
            .iter()
            .position(|&idx| idx == logical_window_start_col_idx);
        let out_window_end_col_idx = output_indices
            .iter()
            .position(|&idx| idx == logical_window_end_col_idx);

        #[for_await]
        for msg in input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    if units == 0 {
                        continue;
                    }

                    // TODO: compact may be not necessary here.
                    let chunk = chunk.compact();
                    let (data_chunk, ops) = chunk.into_parts();
                    // SAFETY: Already compacted.
                    assert!(matches!(data_chunk.vis(), Vis::Compact(_)));
                    let len = data_chunk.cardinality();

                    // Collect each window's data into a chunk.
                    let mut chunks = Vec::with_capacity(units);

                    for i in 0..units {
                        let window_start_col = if out_window_start_col_idx.is_some() {
                            Some(
                                self.window_start_exprs[i]
                                    .eval_infallible(&data_chunk, |err| {
                                        ctx.on_compute_error(err, &info.identity)
                                    })
                                    .await,
                            )
                        } else {
                            None
                        };
                        let window_end_col = if out_window_end_col_idx.is_some() {
                            Some(
                                self.window_end_exprs[i]
                                    .eval_infallible(&data_chunk, |err| {
                                        ctx.on_compute_error(err, &info.identity)
                                    })
                                    .await,
                            )
                        } else {
                            None
                        };
                        let new_cols = output_indices
                            .iter()
                            .filter_map(|&idx| {
                                if idx < logical_window_start_col_idx {
                                    Some(data_chunk.column_at(idx).clone())
                                } else if idx == logical_window_start_col_idx {
                                    Some(window_start_col.clone().unwrap())
                                } else if idx == logical_window_end_col_idx {
                                    Some(window_end_col.clone().unwrap())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        chunks.push(DataChunk::new(new_cols, len));
                    }

                    // Reorganize the output rows from the same input row together.
                    let mut row_iters = chunks.iter().map(|c| c.rows()).collect_vec();

                    let data_types = chunks[0].data_types();
                    let mut chunk_builder = DataChunkBuilder::new(data_types, chunk_size);
                    let mut op_builder = Vec::with_capacity(chunk_size);

                    for &op in &ops {
                        // Since there could be multiple rows for the same input row, we need to
                        // transform the `U-`/`U+` into `-`/`+` and then duplicate it.
                        let op = match op {
                            Op::Insert | Op::UpdateInsert => Op::Insert,
                            Op::Delete | Op::UpdateDelete => Op::Delete,
                        };
                        for row_iter in &mut row_iters {
                            op_builder.push(op);
                            if let Some(chunk) =
                                chunk_builder.append_one_row(row_iter.next().unwrap())
                            {
                                let ops = op_builder.drain(..).collect_vec();
                                let chunk = StreamChunk::from_data_chunk(ops, chunk);
                                yield Message::Chunk(chunk);
                            }
                        }
                    }

                    if let Some(chunk) = chunk_builder.consume_all() {
                        let ops = op_builder.drain(..).collect_vec();
                        let chunk = StreamChunk::from_data_chunk(ops, chunk);
                        yield Message::Chunk(chunk);
                    }

                    // All builders should be exhausted.
                    debug_assert!(op_builder.is_empty());
                    debug_assert!(row_iters.into_iter().all(|mut it| it.next().is_none()));
                }
                Message::Barrier(b) => {
                    yield Message::Barrier(b);
                }
                Message::Watermark(w) => {
                    if w.col_idx == time_col_idx {
                        if let (Some(out_start_idx), Some(start_expr)) =
                            (out_window_start_col_idx, self.window_start_exprs.get(0))
                        {
                            let w = w
                                .clone()
                                .transform_with_expr(start_expr, out_start_idx, |err| {
                                    ctx.on_compute_error(err, &info.identity)
                                })
                                .await;
                            if let Some(w) = w {
                                yield Message::Watermark(w);
                            }
                        }
                        if let (Some(out_end_idx), Some(end_expr)) =
                            (out_window_end_col_idx, self.window_end_exprs.get(0))
                        {
                            let w = w
                                .transform_with_expr(end_expr, out_end_idx, |err| {
                                    ctx.on_compute_error(err, &info.identity)
                                })
                                .await;
                            if let Some(w) = w {
                                yield Message::Watermark(w);
                            }
                        }
                    } else if let Some(out_idx) =
                        output_indices.iter().position(|&idx| idx == w.col_idx)
                    {
                        yield Message::Watermark(w.with_idx(out_idx));
                    }
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{DataType, Interval};
    use risingwave_expr::expr::test_utils::make_hop_window_expression;

    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, Executor, ExecutorInfo, StreamChunk};

    const CHUNK_SIZE: usize = 256;

    fn create_executor(output_indices: Vec<usize>) -> Box<dyn Executor> {
        let field1 = Field::unnamed(DataType::Int64);
        let field2 = Field::unnamed(DataType::Int64);
        let field3 = Field::with_name(DataType::Timestamp, "created_at");
        let schema = Schema::new(vec![field1, field2, field3]);
        let pk_indices = vec![0];

        let chunk = StreamChunk::from_pretty(
            &"I I TS
            + 1 1 ^10:00:00
            + 2 3 ^10:05:00
            - 3 2 ^10:14:00
            + 4 1 ^10:22:00
           U- 5 2 ^10:33:00
           U+ 6 2 ^10:42:00
            - 7 1 ^10:51:00
            + 8 3 ^11:02:00"
                .replace('^', "2022-2-2T"),
        );
        let input =
            MockSource::with_chunks(schema.clone(), pk_indices.clone(), vec![chunk]).boxed();
        let window_slide = Interval::from_minutes(15);
        let window_size = Interval::from_minutes(30);
        let window_offset = Interval::from_minutes(0);
        let (window_start_exprs, window_end_exprs) = make_hop_window_expression(
            DataType::Timestamp,
            2,
            window_size,
            window_slide,
            window_offset,
        )
        .unwrap();

        super::HopWindowExecutor::new(
            ActorContext::create(123),
            input,
            ExecutorInfo {
                // TODO: the schema is incorrect, but it seems useless here.
                schema,
                pk_indices,
                identity: "test".to_string(),
            },
            2,
            window_slide,
            window_size,
            window_start_exprs,
            window_end_exprs,
            output_indices,
            CHUNK_SIZE,
        )
        .boxed()
    }
    #[tokio::test]
    async fn test_execute() {
        let default_indices: Vec<_> = (0..5).collect();
        let executor = create_executor(default_indices);

        let mut stream = executor.execute();
        // TODO: add more test infra to reduce the duplicated codes below.

        let chunk = stream.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                &"I I TS        TS        TS
                + 1 1 ^10:00:00 ^09:45:00 ^10:15:00
                + 1 1 ^10:00:00 ^10:00:00 ^10:30:00
                + 2 3 ^10:05:00 ^09:45:00 ^10:15:00
                + 2 3 ^10:05:00 ^10:00:00 ^10:30:00
                - 3 2 ^10:14:00 ^09:45:00 ^10:15:00
                - 3 2 ^10:14:00 ^10:00:00 ^10:30:00
                + 4 1 ^10:22:00 ^10:00:00 ^10:30:00
                + 4 1 ^10:22:00 ^10:15:00 ^10:45:00
                - 5 2 ^10:33:00 ^10:15:00 ^10:45:00
                - 5 2 ^10:33:00 ^10:30:00 ^11:00:00
                + 6 2 ^10:42:00 ^10:15:00 ^10:45:00
                + 6 2 ^10:42:00 ^10:30:00 ^11:00:00
                - 7 1 ^10:51:00 ^10:30:00 ^11:00:00
                - 7 1 ^10:51:00 ^10:45:00 ^11:15:00
                + 8 3 ^11:02:00 ^10:45:00 ^11:15:00
                + 8 3 ^11:02:00 ^11:00:00 ^11:30:00"
                    .replace('^', "2022-2-2T"),
            )
        );
    }

    #[tokio::test]
    async fn test_output_indices() {
        let executor = create_executor(vec![4, 1, 0, 2]);

        let mut stream = executor.execute();
        // TODO: add more test infra to reduce the duplicated codes below.

        let chunk = stream.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                &"TS        I I TS
                + ^10:15:00 1 1 ^10:00:00
                + ^10:30:00 1 1 ^10:00:00
                + ^10:15:00 3 2 ^10:05:00
                + ^10:30:00 3 2 ^10:05:00
                - ^10:15:00 2 3 ^10:14:00
                - ^10:30:00 2 3 ^10:14:00
                + ^10:30:00 1 4 ^10:22:00
                + ^10:45:00 1 4 ^10:22:00
                - ^10:45:00 2 5 ^10:33:00
                - ^11:00:00 2 5 ^10:33:00
                + ^10:45:00 2 6 ^10:42:00
                + ^11:00:00 2 6 ^10:42:00
                - ^11:00:00 1 7 ^10:51:00
                - ^11:15:00 1 7 ^10:51:00
                + ^11:15:00 3 8 ^11:02:00
                + ^11:30:00 3 8 ^11:02:00"
                    .replace('^', "2022-2-2T"),
            )
        );
    }
}
