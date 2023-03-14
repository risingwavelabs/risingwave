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
use risingwave_common::array::column::Column;
use risingwave_common::array::{StreamChunk, Vis};
use risingwave_common::types::IntervalUnit;
use risingwave_expr::expr::BoxedExpression;
use risingwave_expr::ExprError;

use super::error::StreamExecutorError;
use super::{ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message};

pub struct HopWindowExecutor {
    ctx: ActorContextRef,
    pub input: BoxedExecutor,
    pub info: ExecutorInfo,

    pub time_col_idx: usize,
    pub window_slide: IntervalUnit,
    pub window_size: IntervalUnit,
    window_start_exprs: Vec<BoxedExpression>,
    window_end_exprs: Vec<BoxedExpression>,
    pub output_indices: Vec<usize>,
}

impl HopWindowExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: BoxedExecutor,
        info: ExecutorInfo,
        time_col_idx: usize,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
        window_start_exprs: Vec<BoxedExpression>,
        window_end_exprs: Vec<BoxedExpression>,
        output_indices: Vec<usize>,
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
    fn derive_watermarks(
        input_len: usize,
        time_col_idx: usize,
        output_indices: &[usize],
    ) -> Vec<Vec<usize>> {
        let mut watermark_derivations = vec![vec![]; input_len];
        for (out_i, in_i) in output_indices.iter().enumerate() {
            let in_i = *in_i;
            if in_i >= input_len {
                watermark_derivations[time_col_idx].push(out_i);
            } else {
                watermark_derivations[in_i].push(out_i);
            }
        }
        watermark_derivations
    }

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

        let window_start_col_index = input.schema().len();
        let window_end_col_index = input.schema().len() + 1;
        let watermark_derivations =
            Self::derive_watermarks(input.schema().len(), time_col_idx, &output_indices);
        #[for_await]
        for msg in input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    // TODO: compact may be not necessary here.
                    let chunk = chunk.compact();
                    let (data_chunk, ops) = chunk.into_parts();
                    // SAFETY: Already compacted.
                    assert!(matches!(data_chunk.vis(), Vis::Compact(_)));
                    let _len = data_chunk.cardinality();
                    for i in 0..units {
                        let window_start_col = if output_indices.contains(&window_start_col_index) {
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
                        let window_end_col = if output_indices.contains(&window_end_col_index) {
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
                                if idx < window_start_col_index {
                                    Some(data_chunk.column_at(idx).clone())
                                } else if idx == window_start_col_index {
                                    Some(Column::new(window_start_col.clone().unwrap()))
                                } else if idx == window_end_col_index {
                                    Some(Column::new(window_end_col.clone().unwrap()))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        let new_chunk = StreamChunk::new(ops.clone(), new_cols, None);
                        yield Message::Chunk(new_chunk);
                    }
                }
                Message::Barrier(b) => {
                    yield Message::Barrier(b);
                }
                Message::Watermark(w) => {
                    for i in &watermark_derivations[w.col_idx] {
                        yield Message::Watermark(w.clone().with_idx(*i));
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
    use risingwave_common::types::test_utils::IntervalUnitTestExt;
    use risingwave_common::types::{DataType, IntervalUnit};
    use risingwave_expr::expr::test_utils::make_hop_window_expression;

    use super::super::*;
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::{ActorContext, Executor, ExecutorInfo, ScalarImpl, StreamChunk};
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
            - 5 3 ^10:33:00
            + 6 2 ^10:42:00
            - 7 1 ^10:51:00
            + 8 3 ^11:02:00"
                .replace('^', "2022-2-2T"),
        );
        let input =
            MockSource::with_chunks(schema.clone(), pk_indices.clone(), vec![chunk]).boxed();
        let window_slide = IntervalUnit::from_minutes(15);
        let window_size = IntervalUnit::from_minutes(30);
        let (window_start_exprs, window_end_exprs) =
            make_hop_window_expression(DataType::Timestamp, 2, window_size, window_slide).unwrap();

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
                + 2 3 ^10:05:00 ^09:45:00 ^10:15:00
                - 3 2 ^10:14:00 ^09:45:00 ^10:15:00
                + 4 1 ^10:22:00 ^10:00:00 ^10:30:00
                - 5 3 ^10:33:00 ^10:15:00 ^10:45:00
                + 6 2 ^10:42:00 ^10:15:00 ^10:45:00
                - 7 1 ^10:51:00 ^10:30:00 ^11:00:00
                + 8 3 ^11:02:00 ^10:45:00 ^11:15:00"
                    .replace('^', "2022-2-2T"),
            )
        );

        let chunk = stream.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                &"I I TS        TS        TS
                + 1 1 ^10:00:00 ^10:00:00 ^10:30:00
                + 2 3 ^10:05:00 ^10:00:00 ^10:30:00
                - 3 2 ^10:14:00 ^10:00:00 ^10:30:00
                + 4 1 ^10:22:00 ^10:15:00 ^10:45:00
                - 5 3 ^10:33:00 ^10:30:00 ^11:00:00
                + 6 2 ^10:42:00 ^10:30:00 ^11:00:00
                - 7 1 ^10:51:00 ^10:45:00 ^11:15:00
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
                + ^10:15:00 3 2 ^10:05:00
                - ^10:15:00 2 3 ^10:14:00
                + ^10:30:00 1 4 ^10:22:00
                - ^10:45:00 3 5 ^10:33:00
                + ^10:45:00 2 6 ^10:42:00
                - ^11:00:00 1 7 ^10:51:00
                + ^11:15:00 3 8 ^11:02:00"
                    .replace('^', "2022-2-2T"),
            )
        );

        let chunk = stream.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                &"TS        I I TS 
                + ^10:30:00 1 1 ^10:00:00 
                + ^10:30:00 3 2 ^10:05:00 
                - ^10:30:00 2 3 ^10:14:00 
                + ^10:45:00 1 4 ^10:22:00 
                - ^11:00:00 3 5 ^10:33:00 
                + ^11:00:00 2 6 ^10:42:00 
                - ^11:15:00 1 7 ^10:51:00 
                + ^11:30:00 3 8 ^11:02:00"
                    .replace('^', "2022-2-2T"),
            )
        );
    }

    fn create_executor2(output_indices: Vec<usize>) -> (MessageSender, Box<dyn Executor>) {
        let field1 = Field::unnamed(DataType::Int64);
        let field2 = Field::unnamed(DataType::Int64);
        let field3 = Field::with_name(DataType::Timestamp, "created_at");
        let schema = Schema::new(vec![field1, field2, field3]);
        let pk_indices = vec![0];
        let (tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());

        let window_slide = IntervalUnit::from_minutes(15);
        let window_size = IntervalUnit::from_minutes(30);
        let (window_start_exprs, window_end_exprs) =
            make_hop_window_expression(DataType::Timestamp, 2, window_size, window_slide).unwrap();

        (
            tx,
            super::HopWindowExecutor::new(
                ActorContext::create(123),
                Box::new(source),
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
            )
            .boxed(),
        )
    }

    #[tokio::test]
    async fn test_watermark_full_output() {
        let (mut tx, hop) = create_executor2((0..5).collect());
        let mut hop = hop.execute();

        // TODO: the datatype is incorrect, but it seems useless here.
        tx.push_int64_watermark(0, 100);
        tx.push_int64_watermark(1, 100);
        tx.push_int64_watermark(2, 100);

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 1,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 3,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 4,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );
    }

    #[tokio::test]
    async fn test_watermark_output_indices1() {
        let (mut tx, hop) = create_executor2(vec![4, 1, 0, 2]);
        let mut hop = hop.execute();

        // TODO: the datatype is incorrect, but it seems useless here.
        tx.push_int64_watermark(0, 100);
        tx.push_int64_watermark(1, 100);
        tx.push_int64_watermark(2, 100);

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 1,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 3,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );
    }

    #[tokio::test]
    async fn test_watermark_output_indices2() {
        let (mut tx, hop) = create_executor2(vec![4, 1, 5, 0, 2]);
        let mut hop = hop.execute();

        // TODO: the datatype is incorrect, but it seems useless here.
        tx.push_int64_watermark(0, 100);
        tx.push_int64_watermark(1, 100);
        tx.push_int64_watermark(2, 100);

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 3,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 1,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        let w = hop.next().await.unwrap().unwrap();
        let w = w.as_watermark().unwrap();
        assert_eq!(
            w,
            &Watermark {
                col_idx: 4,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );
    }
}
