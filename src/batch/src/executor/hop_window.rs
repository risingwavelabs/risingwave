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

use std::num::NonZeroUsize;

use futures_async_stream::try_stream;
use num_traits::CheckedSub;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Vis};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{Expression, InputRefExpression, LiteralExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::expr::expr_node;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

pub struct HopWindowExecutor {
    child: BoxedExecutor,
    identity: String,
    schema: Schema,
    time_col_idx: usize,
    window_slide: IntervalUnit,
    window_size: IntervalUnit,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for HopWindowExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
    ) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 1);
        let hop_window_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HopWindow
        )?;
        let time_col = hop_window_node.get_time_col()?.column_idx as usize;
        let window_slide = hop_window_node.get_window_slide()?.into();
        let window_size = hop_window_node.get_window_size()?.into();
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child = source.clone_for_plan(child_plan).build().await?;
            let schema = child
                .schema()
                .clone()
                .into_fields()
                .into_iter()
                .chain([
                    Field::with_name(DataType::Timestamp, "window_start"),
                    Field::with_name(DataType::Timestamp, "window_end"),
                ])
                .collect();
            return Ok(Box::new(HopWindowExecutor::new(
                child,
                schema,
                time_col,
                window_slide,
                window_size,
                source.plan_node().get_identity().clone(),
            )));
        }
        Err(ErrorCode::InternalError("HopWindow must have one child".to_string()).into())
    }
}

impl HopWindowExecutor {
    fn new(
        child: BoxedExecutor,
        schema: Schema,
        time_col_idx: usize,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
        identity: String,
    ) -> Self {
        Self {
            child,
            identity,
            schema,
            time_col_idx,
            window_slide,
            window_size,
        }
    }
}

impl Executor for HopWindowExecutor {
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

impl HopWindowExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            child,
            time_col_idx,
            window_slide,
            window_size,
            ..
        } = *self;
        let units = window_size
            .exact_div(&window_slide)
            .and_then(|x| NonZeroUsize::new(usize::try_from(x).ok()?))
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "window_size {} cannot be divided by window_slide {}",
                    window_size, window_slide
                )))
            })?
            .get();

        let schema = self.schema;
        let time_col_data_type = schema.fields()[time_col_idx].data_type();
        let time_col_ref = InputRefExpression::new(time_col_data_type, self.time_col_idx).boxed();

        let window_slide_expr =
            LiteralExpression::new(DataType::Interval, Some(ScalarImpl::Interval(window_slide)))
                .boxed();

        // The first window_start of hop window should be:
        // tumble_start(`time_col` - (`window_size` - `window_slide`), `window_slide`).
        // Let's pre calculate (`window_size` - `window_slide`).
        let window_size_sub_slide = window_size.checked_sub(&window_slide).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(format!(
                "window_size {} cannot be subtracted by window_slide {}",
                window_size, window_slide
            )))
        })?;
        let window_size_sub_slide_expr = LiteralExpression::new(
            DataType::Interval,
            Some(ScalarImpl::Interval(window_size_sub_slide)),
        )
        .boxed();
        let hop_expr = new_binary_expr(
            expr_node::Type::TumbleStart,
            risingwave_common::types::DataType::Timestamp,
            new_binary_expr(
                expr_node::Type::Subtract,
                DataType::Timestamp,
                time_col_ref,
                window_size_sub_slide_expr,
            ),
            window_slide_expr,
        );

        let mut window_start_exprs = Vec::with_capacity(units);
        let mut window_end_exprs = Vec::with_capacity(units);

        for i in 0..units {
            let window_start_offset = window_slide.checked_mul_int(i).ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "window_slide {} cannot be multiplied by {}",
                    window_slide, i
                )))
            })?;
            let window_start_offset_expr = LiteralExpression::new(
                DataType::Interval,
                Some(ScalarImpl::Interval(window_start_offset)),
            )
            .boxed();
            let window_end_offset = window_slide.checked_mul_int(i + units).ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "window_slide {} cannot be multiplied by {}",
                    window_slide, i
                )))
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
            window_start_exprs.push(window_start_expr);
            let window_end_expr = new_binary_expr(
                expr_node::Type::Add,
                DataType::Timestamp,
                InputRefExpression::new(DataType::Timestamp, 0).boxed(),
                window_end_offset_expr,
            );
            window_end_exprs.push(window_end_expr);
        }

        #[for_await]
        for data_chunk in child.execute() {
            let data_chunk = data_chunk?;
            let hop_start = hop_expr.eval(&data_chunk)?;
            let vis = Vis::Compact(hop_start.len());
            let hop_start_chunk = DataChunk::nex(vec![Column::new(hop_start)], vis);
            let (origin_cols, visibility) = data_chunk.into_partx();
            // SAFETY: Already compacted.
            assert!(matches!(visibility, Vis::Compact(_)));
            for i in 0..units {
                let window_start_col = window_start_exprs[i].eval(&hop_start_chunk)?;
                let window_end_col = window_end_exprs[i].eval(&hop_start_chunk)?;
                let vis = Vis::Compact(window_start_col.len());
                let mut new_cols = origin_cols.clone();
                new_cols.extend_from_slice(&[
                    Column::new(window_start_col),
                    Column::new(window_end_col),
                ]);
                let new_chunk = DataChunk::nex(new_cols, vis);
                yield new_chunk;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    #[tokio::test]
    async fn test_execute() {
        let field1 = Field::unnamed(DataType::Int64);
        let field2 = Field::unnamed(DataType::Int64);
        let field3 = Field::with_name(DataType::Timestamp, "created_at");
        let schema = Schema::new(vec![field1, field2, field3]);

        let chunk = DataChunk::from_pretty(
            &"I I TS
            1 1 ^10:00:00
            2 3 ^10:05:00
            3 2 ^10:14:00
            4 1 ^10:22:00
            5 3 ^10:33:00
            6 2 ^10:42:00
            7 1 ^10:51:00
            8 3 ^11:02:00"
                .replace('^', "2022-2-2T"),
        );

        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(chunk);

        let window_slide = IntervalUnit::from_minutes(15);
        let window_size = IntervalUnit::from_minutes(30);
        let executor = Box::new(HopWindowExecutor::new(
            Box::new(mock_executor),
            schema,
            2,
            window_slide,
            window_size,
            "test".to_string(),
        ));

        let mut stream = executor.execute();
        // TODO: add more test infra to reduce the duplicated codes below.

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                &"I I TS        TS        TS
                1 1 ^10:00:00 ^09:45:00 ^10:15:00
                2 3 ^10:05:00 ^09:45:00 ^10:15:00
                3 2 ^10:14:00 ^09:45:00 ^10:15:00
                4 1 ^10:22:00 ^10:00:00 ^10:30:00
                5 3 ^10:33:00 ^10:15:00 ^10:45:00
                6 2 ^10:42:00 ^10:15:00 ^10:45:00
                7 1 ^10:51:00 ^10:30:00 ^11:00:00
                8 3 ^11:02:00 ^10:45:00 ^11:15:00"
                    .replace('^', "2022-2-2T"),
            )
        );

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                &"I I TS        TS        TS
                1 1 ^10:00:00 ^10:00:00 ^10:30:00
                2 3 ^10:05:00 ^10:00:00 ^10:30:00
                3 2 ^10:14:00 ^10:00:00 ^10:30:00
                4 1 ^10:22:00 ^10:15:00 ^10:45:00
                5 3 ^10:33:00 ^10:30:00 ^11:00:00
                6 2 ^10:42:00 ^10:30:00 ^11:00:00
                7 1 ^10:51:00 ^10:45:00 ^11:15:00
                8 3 ^11:02:00 ^11:00:00 ^11:30:00"
                    .replace('^', "2022-2-2T"),
            )
        );
    }
}
