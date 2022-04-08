use std::num::NonZeroUsize;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, StreamChunk};

use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{Expression, InputRefExpression, LiteralExpression};
use risingwave_pb::expr::{expr_node};

use super::error::{StreamExecutorError, TracedStreamExecutorError};
use super::{BoxedExecutor, Executor, ExecutorInfo, Message};

#[allow(unused)]
pub struct HopWindowExecutor {
    pub(super) input: BoxedExecutor,
    pub(super) info: ExecutorInfo,

    pub(super) time_col_idx: usize,
    pub(super) window_slide: IntervalUnit,
    pub(super) window_size: IntervalUnit,
}

impl Executor for HopWindowExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        todo!()
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        todo!()
    }

    fn identity(&self) -> &str {
        todo!()
    }
}

impl HopWindowExecutor {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let input = self.input.execute();
        let units = self
            .window_size
            .exact_div(&self.window_slide)
            .and_then(|x| NonZeroUsize::new(usize::try_from(x).ok()?))
            .ok_or_else(|| {
                StreamExecutorError::InvalidArgument(format!(
                    "window_size {} cannot be divided by window_slide {}",
                    self.window_size, self.window_slide
                ))
            })?
            .get();

        let schema = self.info.schema;
        let time_col_data_type = schema.fields()[self.time_col_idx].data_type();
        let time_col_ref = InputRefExpression::new(time_col_data_type, self.time_col_idx).boxed();

        let window_size_expr = LiteralExpression::new(
            DataType::Interval,
            Some(ScalarImpl::Interval(self.window_size)),
        )
        .boxed();
        let tumble_start = new_binary_expr(
            expr_node::Type::TumbleStart,
            risingwave_common::types::DataType::Timestamp,
            time_col_ref,
            window_size_expr,
        );

        #[for_await]
        for msg in input {
            let msg = msg?;
            let Message::Chunk(chunk) = msg else {
                // TODO: syn has not supported `let_else`, we desugar here manually.
                yield std::task::Poll::Ready(msg);
                continue;
            };
            // TODO: compact may be not necessary here.
            let chunk = chunk.compact().map_err(StreamExecutorError::ExecutorV1)?;
            let (data_chunk, ops) = chunk.into_parts();
            let tumble_start = tumble_start
                .eval(&data_chunk)
                .map_err(StreamExecutorError::EvalError)?;
            let tumble_start_chunk = DataChunk::new(vec![Column::new(tumble_start)], None);
            let (origin_cols, visibility) = data_chunk.into_parts();
            assert!(visibility.is_none()); // Already compacted.
            for i in 0..units {
                let window_start_offset =
                    self.window_slide.checked_mul_int(i).ok_or_else(|| {
                        StreamExecutorError::InvalidArgument(format!(
                            "window_slide {} cannot be multiplied by {}",
                            self.window_slide, i
                        ))
                    })?;
                let window_start_offset_expr = LiteralExpression::new(
                    DataType::Interval,
                    Some(ScalarImpl::Interval(window_start_offset)),
                )
                .boxed();
                let window_end_offset =
                    self.window_slide
                        .checked_mul_int(i + units)
                        .ok_or_else(|| {
                            StreamExecutorError::InvalidArgument(format!(
                                "window_slide {} cannot be multiplied by {}",
                                self.window_slide, i
                            ))
                        })?;
                let window_end_offset_expr = LiteralExpression::new(
                    DataType::Interval,
                    Some(ScalarImpl::Interval(window_end_offset)),
                )
                .boxed();
                let window_start_expr = new_binary_expr(
                    expr_node::Type::Add,
                    DataType::Timestamp,
                    InputRefExpression::new(DataType::Timestamp, 0).boxed(),
                    window_start_offset_expr,
                );
                let window_start_col = window_start_expr
                    .eval(&tumble_start_chunk)
                    .map_err(StreamExecutorError::EvalError)?;
                let window_end_expr = new_binary_expr(
                    expr_node::Type::Add,
                    DataType::Timestamp,
                    InputRefExpression::new(DataType::Timestamp, 0).boxed(),
                    window_end_offset_expr,
                );
                let window_end_col = window_end_expr
                    .eval(&tumble_start_chunk)
                    .map_err(StreamExecutorError::EvalError)?;
                let mut new_cols = origin_cols.clone();
                new_cols.extend_from_slice(&[
                    Column::new(window_start_col),
                    Column::new(window_end_col),
                ]);
                let new_chunk = StreamChunk::new(ops.clone(), new_cols, None);
                yield Message::Chunk(new_chunk);
            }
        }
    }
}
