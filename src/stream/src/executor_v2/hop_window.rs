use std::num::NonZeroUsize;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{Expression, InputRefExpression, LiteralExpression};
use risingwave_pb::expr::expr_node;

use super::error::{StreamExecutorError, TracedStreamExecutorError};
use super::{BoxedExecutor, Executor, ExecutorInfo, Message};

#[allow(unused)]
pub struct HopWindowExecutor {
    pub input: BoxedExecutor,
    pub info: ExecutorInfo,

    pub time_col_idx: usize,
    pub window_slide: IntervalUnit,
    pub window_size: IntervalUnit,
}

impl HopWindowExecutor {
    pub fn new(
        input: BoxedExecutor,
        info: ExecutorInfo,
        time_col_idx: usize,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
    ) -> Self {
        HopWindowExecutor {
            input,
            info,
            time_col_idx,
            window_slide,
            window_size,
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

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
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
            println!("{}", tumble_start_chunk.to_pretty_string());
            let (origin_cols, visibility) = data_chunk.into_parts();
            // SAFETY: Already compacted.
            assert!(visibility.is_none());
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
                println!("{} {}", window_start_offset, window_end_offset);
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

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Op, Row};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{
        DataType, IntervalUnit, NaiveDateTimeWrapper, ScalarImpl, ToOwnedDatum,
    };

    use crate::executor::Message;
    use crate::executor_v2::test_utils::MockSource;
    use crate::executor_v2::{Executor, ExecutorInfo, StreamChunk};

    #[tokio::test]
    async fn test_execute() {
        let field1 = Field::unnamed(DataType::Int32);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::with_name(DataType::Timestamp, "created_at");
        let schema = Schema::new(vec![field1, field2, field3]);
        let pk_indices = vec![0];

        let t = |hours, minutes| {
            let date = NaiveDate::from_ymd(2022, 2, 2);
            let time = NaiveTime::from_hms(hours, minutes, 0);
            let dt = NaiveDateTime::new(date, time);
            NaiveDateTimeWrapper(dt)
        };

        #[allow(clippy::zero_prefixed_literal)]
        let rows = [
            ('+', 1, 1, t(10, 00)),
            ('+', 2, 3, t(10, 05)),
            ('-', 3, 2, t(10, 14)),
            ('+', 4, 1, t(10, 22)),
            ('-', 5, 3, t(10, 33)),
            ('+', 6, 2, t(10, 42)),
            ('-', 7, 1, t(10, 51)),
            ('+', 8, 3, t(11, 02)),
        ];
        let rows = rows
            .into_iter()
            .map(|(op, f1, f2, f3)| {
                let op = if op == '+' { Op::Insert } else { Op::Delete };
                let row = Row(vec![
                    Some(ScalarImpl::Int32(f1)),
                    Some(ScalarImpl::Int32(f2)),
                    Some(ScalarImpl::NaiveDateTime(f3)),
                ]);
                (op, row)
            })
            .collect_vec();

        let chunk = StreamChunk::from_rows(&rows, &schema.data_types()).unwrap();

        let input =
            MockSource::with_chunks(schema.clone(), pk_indices.clone(), vec![chunk]).boxed();

        let window_slide = IntervalUnit::from_minutes(15);
        let window_size = IntervalUnit::from_minutes(30);

        let executor = super::HopWindowExecutor::new(
            input,
            ExecutorInfo {
                // TODO: the schema is incorrect, but it seems useless here.
                schema: schema.clone(),
                pk_indices,
                identity: "test".to_string(),
            },
            2,
            window_slide,
            window_size,
        )
        .boxed();

        let mut stream = executor.execute();
        let Message::Chunk(chunk) = stream.next().await.unwrap().unwrap() else {
            unreachable!();
        };
        let rows = chunk
            .rows()
            .map(|r| {
                (
                    r.op,
                    Row::new(
                        r.values
                            .into_iter()
                            .map(ToOwnedDatum::to_owned_datum)
                            .collect_vec(),
                    ),
                )
            })
            .collect_vec();
        assert_eq!(rows.len(), 8);

        #[allow(clippy::zero_prefixed_literal)]
        let expected_rows = [
            ('+', 1, 1, t(10, 00), t(09, 45), t(10, 15)),
            ('+', 2, 3, t(10, 05), t(09, 45), t(10, 15)),
            ('-', 3, 2, t(10, 14), t(09, 45), t(10, 15)),
            ('+', 4, 1, t(10, 22), t(10, 00), t(10, 30)),
            ('-', 5, 3, t(10, 33), t(10, 15), t(10, 45)),
            ('+', 6, 2, t(10, 42), t(10, 15), t(10, 45)),
            ('-', 7, 1, t(10, 51), t(10, 30), t(11, 00)),
            ('+', 8, 3, t(11, 02), t(10, 45), t(11, 15)),
        ];
        let expected_rows = expected_rows
            .into_iter()
            .map(|(op, f1, f2, f3, f4, f5)| {
                let op = if op == '+' { Op::Insert } else { Op::Delete };
                let row = Row(vec![
                    Some(ScalarImpl::Int32(f1)),
                    Some(ScalarImpl::Int32(f2)),
                    Some(ScalarImpl::NaiveDateTime(f3)),
                    Some(ScalarImpl::NaiveDateTime(f4)),
                    Some(ScalarImpl::NaiveDateTime(f5)),
                ]);
                (op, row)
            })
            .collect_vec();
        let Message::Chunk(chunk) = stream.next().await.unwrap().unwrap() else {
            unreachable!();
        };
        let rows = chunk
            .rows()
            .map(|r| {
                (
                    r.op,
                    Row::new(
                        r.values
                            .into_iter()
                            .map(ToOwnedDatum::to_owned_datum)
                            .collect_vec(),
                    ),
                )
            })
            .collect_vec();
        for (idx, (actual, expected)) in rows.into_iter().zip_eq(expected_rows).enumerate() {
            assert_eq!(actual, expected, "on {}-th row", idx);
        }
    }
}
