// Copyright 2025 RisingWave Labs
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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, Interval};
use risingwave_expr::ExprError;
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

pub struct HopWindowExecutor {
    child: BoxedExecutor,
    identity: String,
    schema: Schema,
    window_slide: Interval,
    window_size: Interval,
    window_start_exprs: Vec<BoxedExpression>,
    window_end_exprs: Vec<BoxedExpression>,
    output_indices: Vec<usize>,
}

impl BoxedExecutorBuilder for HopWindowExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let hop_window_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HopWindow
        )?;
        let window_slide = hop_window_node.get_window_slide()?.into();
        let window_size = hop_window_node.get_window_size()?.into();
        let output_indices = hop_window_node
            .get_output_indices()
            .iter()
            .cloned()
            .map(|x| x as usize)
            .collect_vec();

        let window_start_exprs: Vec<_> = hop_window_node
            .get_window_start_exprs()
            .iter()
            .map(build_from_prost)
            .try_collect()?;
        let window_end_exprs: Vec<_> = hop_window_node
            .get_window_end_exprs()
            .iter()
            .map(build_from_prost)
            .try_collect()?;
        assert_eq!(window_start_exprs.len(), window_end_exprs.len());

        let time_col = hop_window_node.get_time_col() as usize;
        let time_col_data_type = child.schema().fields()[time_col].data_type();
        let output_type = DataType::window_of(&time_col_data_type).unwrap();
        let original_schema: Schema = child
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(output_type.clone(), "window_start"),
                Field::with_name(output_type, "window_end"),
            ])
            .collect();
        let output_indices_schema: Schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();
        Ok(Box::new(HopWindowExecutor::new(
            child,
            output_indices_schema,
            window_slide,
            window_size,
            source.plan_node().get_identity().clone(),
            window_start_exprs,
            window_end_exprs,
            output_indices,
        )))
    }
}

impl HopWindowExecutor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        child: BoxedExecutor,
        schema: Schema,
        window_slide: Interval,
        window_size: Interval,
        identity: String,
        window_start_exprs: Vec<BoxedExpression>,
        window_end_exprs: Vec<BoxedExpression>,
        output_indices: Vec<usize>,
    ) -> Self {
        Self {
            child,
            identity,
            schema,
            window_slide,
            window_size,
            window_start_exprs,
            window_end_exprs,
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
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            child,
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
                )
                .into(),
            })?
            .get();

        let window_start_col_index = child.schema().len();
        let window_end_col_index = child.schema().len() + 1;
        #[for_await]
        for data_chunk in child.execute() {
            let data_chunk = data_chunk?;
            assert!(data_chunk.is_compacted());
            let len = data_chunk.cardinality();
            for i in 0..units {
                let window_start_col = if output_indices.contains(&window_start_col_index) {
                    Some(self.window_start_exprs[i].eval(&data_chunk).await?)
                } else {
                    None
                };
                let window_end_col = if output_indices.contains(&window_end_col_index) {
                    Some(self.window_end_exprs[i].eval(&data_chunk).await?)
                } else {
                    None
                };
                let new_cols = output_indices
                    .iter()
                    .filter_map(|&idx| {
                        if idx < window_start_col_index {
                            Some(data_chunk.column_at(idx).clone())
                        } else if idx == window_start_col_index {
                            Some(window_start_col.clone().unwrap())
                        } else if idx == window_end_col_index {
                            Some(window_end_col.clone().unwrap())
                        } else {
                            None
                        }
                    })
                    .collect_vec();
                yield DataChunk::new(new_cols, len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_expr::expr::test_utils::make_hop_window_expression;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn create_executor(
        output_indices: Vec<usize>,
        window_slide: Interval,
        window_size: Interval,
        window_offset: Interval,
    ) -> Box<HopWindowExecutor> {
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
                .replace('^', "2022-02-02T"),
        );
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(chunk);

        let (window_start_exprs, window_end_exprs) = make_hop_window_expression(
            DataType::Timestamp,
            2,
            window_size,
            window_slide,
            window_offset,
        )
        .unwrap();

        Box::new(HopWindowExecutor::new(
            Box::new(mock_executor),
            schema,
            window_slide,
            window_size,
            "test".to_owned(),
            window_start_exprs,
            window_end_exprs,
            output_indices,
        ))
    }

    #[tokio::test]
    async fn test_window_offset() {
        async fn test_window_offset_helper(window_offset: Interval) -> DataChunk {
            let default_indices = (0..3 + 2).collect_vec();
            let window_slide = Interval::from_minutes(15);
            let window_size = Interval::from_minutes(30);
            let executor =
                create_executor(default_indices, window_slide, window_size, window_offset);
            let mut stream = executor.execute();
            stream.next().await.unwrap().unwrap()
        }

        let window_size = 30;
        for offset in 0..window_size {
            for coefficient in -5..0 {
                assert_eq!(
                    test_window_offset_helper(Interval::from_minutes(
                        coefficient * window_size + offset
                    ))
                    .await,
                    test_window_offset_helper(Interval::from_minutes(
                        (coefficient - 1) * window_size + offset
                    ))
                    .await
                );
            }
        }
        for offset in 0..window_size {
            for coefficient in 0..5 {
                assert_eq!(
                    test_window_offset_helper(Interval::from_minutes(
                        coefficient * window_size + offset
                    ))
                    .await,
                    test_window_offset_helper(Interval::from_minutes(
                        (coefficient + 1) * window_size + offset
                    ))
                    .await
                );
            }
        }
        for offset in -window_size..window_size {
            assert_eq!(
                test_window_offset_helper(Interval::from_minutes(window_size + offset)).await,
                test_window_offset_helper(Interval::from_minutes(-window_size + offset)).await
            );
        }

        assert_eq!(
            test_window_offset_helper(Interval::from_minutes(-31)).await,
            DataChunk::from_pretty(
                &"I I TS        TS        TS
                1 1 ^10:00:00 ^09:44:00 ^10:14:00
                2 3 ^10:05:00 ^09:44:00 ^10:14:00
                3 2 ^10:14:00 ^09:59:00 ^10:29:00
                4 1 ^10:22:00 ^09:59:00 ^10:29:00
                5 3 ^10:33:00 ^10:14:00 ^10:44:00
                6 2 ^10:42:00 ^10:14:00 ^10:44:00
                7 1 ^10:51:00 ^10:29:00 ^10:59:00
                8 3 ^11:02:00 ^10:44:00 ^11:14:00"
                    .replace('^', "2022-02-02T"),
            )
        );
        assert_eq!(
            test_window_offset_helper(Interval::from_minutes(29)).await,
            DataChunk::from_pretty(
                &"I I TS        TS        TS
                1 1 ^10:00:00 ^09:44:00 ^10:14:00
                2 3 ^10:05:00 ^09:44:00 ^10:14:00
                3 2 ^10:14:00 ^09:59:00 ^10:29:00
                4 1 ^10:22:00 ^09:59:00 ^10:29:00
                5 3 ^10:33:00 ^10:14:00 ^10:44:00
                6 2 ^10:42:00 ^10:14:00 ^10:44:00
                7 1 ^10:51:00 ^10:29:00 ^10:59:00
                8 3 ^11:02:00 ^10:44:00 ^11:14:00"
                    .replace('^', "2022-02-02T"),
            )
        );
    }

    #[tokio::test]
    async fn test_execute() {
        let default_indices = (0..3 + 2).collect_vec();

        let window_slide = Interval::from_minutes(15);
        let window_size = Interval::from_minutes(30);
        let window_offset = Interval::from_minutes(0);
        let executor = create_executor(default_indices, window_slide, window_size, window_offset);

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
                    .replace('^', "2022-02-02T"),
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
                    .replace('^', "2022-02-02T"),
            )
        );
    }
    #[tokio::test]
    async fn test_output_indices() {
        let window_slide = Interval::from_minutes(15);
        let window_size = Interval::from_minutes(30);
        let window_offset = Interval::from_minutes(0);
        let executor = create_executor(vec![1, 3, 4, 2], window_slide, window_size, window_offset);

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
                    .replace('^', "2022-02-02T"),
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
                    .replace('^', "2022-02-02T"),
            )
        );
    }
}
