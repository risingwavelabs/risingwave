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

use anyhow::anyhow;
use futures_async_stream::try_stream;
use itertools::Itertools;
use num_traits::CheckedSub;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Vis};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{Expression, InputRefExpression, LiteralExpression};
use risingwave_expr::ExprError;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::expr::expr_node;

use crate::error::BatchError;
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
    output_indices: Vec<usize>,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for HopWindowExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let hop_window_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HopWindow
        )?;
        let time_col = hop_window_node.get_time_col()?.column_idx as usize;
        let window_slide = hop_window_node.get_window_slide()?.into();
        let window_size = hop_window_node.get_window_size()?.into();
        let output_indices = hop_window_node
            .get_output_indices()
            .iter()
            .copied()
            .map(|x| x as usize)
            .collect_vec();

        let original_schema: Schema = child
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(DataType::Timestamp, "window_start"),
                Field::with_name(DataType::Timestamp, "window_end"),
            ])
            .collect();
        let output_indices_schema: Schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();
        Ok(Box::new(HopWindowExecutor::new(
            child,
            output_indices_schema,
            time_col,
            window_slide,
            window_size,
            source.plan_node().get_identity().clone(),
            output_indices,
        )))
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
        output_indices: Vec<usize>,
    ) -> Self {
        Self {
            child,
            identity,
            schema,
            time_col_idx,
            window_slide,
            window_size,
            output_indices,
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
            output_indices,
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

        let time_col_data_type = child.schema().fields()[time_col_idx].data_type();
        let time_col_ref = InputRefExpression::new(time_col_data_type, self.time_col_idx).boxed();

        let window_slide_expr =
            LiteralExpression::new(DataType::Interval, Some(ScalarImpl::Interval(window_slide)))
                .boxed();

        // The first window_start of hop window should be:
        // tumble_start(`time_col` - (`window_size` - `window_slide`), `window_slide`).
        // Let's pre calculate (`window_size` - `window_slide`).
        let window_size_sub_slide = window_size.checked_sub(&window_slide).ok_or_else(|| {
            BatchError::Internal(anyhow!(format!(
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
                BatchError::Internal(anyhow!(format!(
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
                BatchError::Internal(anyhow!(format!(
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
        let window_start_col_index = child.schema().len();
        let window_end_col_index = child.schema().len() + 1;
        let contains_window_start = output_indices.contains(&window_start_col_index);
        let contains_window_end = output_indices.contains(&window_end_col_index);
        #[for_await]
        for data_chunk in child.execute() {
            let data_chunk = data_chunk?;
            let hop_start = hop_expr.eval(&data_chunk)?;
            let len = hop_start.len();
            let hop_start_chunk = DataChunk::new(vec![Column::new(hop_start)], len);
            let (origin_cols, visibility) = data_chunk.into_parts();
            // SAFETY: Already compacted.
            assert!(matches!(visibility, Vis::Compact(_)));
            for i in 0..units {
                let window_start_col = if contains_window_start {
                    Some(window_start_exprs[i].eval(&hop_start_chunk)?)
                } else {
                    None
                };
                let window_end_col = if contains_window_end {
                    Some(window_end_exprs[i].eval(&hop_start_chunk)?)
                } else {
                    None
                };
                let new_cols = output_indices
                    .iter()
                    .filter_map(|&idx| {
                        if idx < window_start_col_index {
                            Some(origin_cols[idx].clone())
                        } else if idx == window_start_col_index {
                            Some(Column::new(window_start_col.clone().unwrap()))
                        } else if idx == window_end_col_index {
                            Some(Column::new(window_end_col.clone().unwrap()))
                        } else {
                            None
                        }
                    })
                    .collect_vec();
                let len = {
                    if let Some(col) = &window_start_col {
                        col.len()
                    } else if let Some(col) = &window_end_col {
                        col.len()
                    } else {
                        // SAFETY: Either window_start or window_end is in output indices.
                        unreachable!();
                    }
                };
                let new_chunk = DataChunk::new(new_cols, len);
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
        let default_indices = (0..schema.len() + 2).collect_vec();
        let executor = Box::new(HopWindowExecutor::new(
            Box::new(mock_executor),
            schema,
            2,
            window_slide,
            window_size,
            "test".to_string(),
            default_indices,
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
    #[tokio::test]
    async fn test_output_indices() {
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
            vec![1, 3, 4, 2],
        ));

        let mut stream = executor.execute();
        // TODO: add more test infra to reduce the duplicated codes below.

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                &" I TS        TS        TS        
                   1 ^09:45:00 ^10:15:00 ^10:00:00 
                   3 ^09:45:00 ^10:15:00 ^10:05:00 
                   2 ^09:45:00 ^10:15:00 ^10:14:00 
                   1 ^10:00:00 ^10:30:00 ^10:22:00 
                   3 ^10:15:00 ^10:45:00 ^10:33:00 
                   2 ^10:15:00 ^10:45:00 ^10:42:00 
                   1 ^10:30:00 ^11:00:00 ^10:51:00 
                   3 ^10:45:00 ^11:15:00 ^11:02:00"
                    .replace('^', "2022-2-2T"),
            )
        );

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                &"I TS        TS        TS       
                  1 ^10:00:00 ^10:30:00 ^10:00:00
                  3 ^10:00:00 ^10:30:00 ^10:05:00
                  2 ^10:00:00 ^10:30:00 ^10:14:00
                  1 ^10:15:00 ^10:45:00 ^10:22:00
                  3 ^10:30:00 ^11:00:00 ^10:33:00
                  2 ^10:30:00 ^11:00:00 ^10:42:00
                  1 ^10:45:00 ^11:15:00 ^10:51:00
                  3 ^11:00:00 ^11:30:00 ^11:02:00"
                    .replace('^', "2022-2-2T"),
            )
        );
    }
}
