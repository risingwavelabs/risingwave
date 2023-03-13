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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Vis};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, IntervalUnit};
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_expr::ExprError;
use risingwave_pb::batch_plan::plan_node::NodeBody;

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
    window_offset: IntervalUnit,
    window_start_exprs: Vec<BoxedExpression>,
    window_end_exprs: Vec<BoxedExpression>,
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
        let time_col = hop_window_node.get_time_col() as usize;
        let window_slide = hop_window_node.get_window_slide()?.into();
        let window_size = hop_window_node.get_window_size()?.into();
        let window_offset = hop_window_node.get_window_offset()?.into();
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
            time_col,
            window_slide,
            window_size,
            window_offset,
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
        time_col_idx: usize,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
        window_offset: IntervalUnit,
        identity: String,
        window_start_exprs: Vec<BoxedExpression>,
        window_end_exprs: Vec<BoxedExpression>,
        output_indices: Vec<usize>,
    ) -> Self {
        Self {
            child,
            identity,
            schema,
            time_col_idx,
            window_slide,
            window_size,
            window_offset,
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
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
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
                ),
            })?
            .get();

        let window_start_col_index = child.schema().len();
        let window_end_col_index = child.schema().len() + 1;
        #[for_await]
        for data_chunk in child.execute() {
            let data_chunk = data_chunk?;
            assert!(matches!(data_chunk.vis(), Vis::Compact(_)));
            let len = data_chunk.cardinality();
            for i in 0..units {
                let window_start_col = if output_indices.contains(&window_start_col_index) {
                    Some(self.window_start_exprs[i].eval(&data_chunk)?)
                } else {
                    None
                };
                let window_end_col = if output_indices.contains(&window_end_col_index) {
                    Some(self.window_end_exprs[i].eval(&data_chunk)?)
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
                    .collect_vec();
                yield DataChunk::new(new_cols, len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::test_utils::IntervalUnitTestExt;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::test_utils::make_hop_window_expression;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn create_executor(output_indices: Vec<usize>) -> Box<HopWindowExecutor> {
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
        // print!("{}", chunk.to_pretty_string());
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(chunk);

        let window_slide = IntervalUnit::from_minutes(15);
        let window_size = IntervalUnit::from_minutes(30);
        let window_offset = IntervalUnit::from_minutes(0);
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
            2,
            window_slide,
            window_size,
            window_offset,
            "test".to_string(),
            window_start_exprs,
            window_end_exprs,
            output_indices,
        ))
    }

    #[tokio::test]
    async fn test_size_and_slide() {
        let default_indices = (0..3 + 2).collect_vec();
        let executor = create_executor(default_indices);
        let mut stream = executor.execute();
        // let chunk = stream.next().await.unwrap().unwrap();
        // print!("{}", chunk.to_pretty_string());

        let default_indices = (0..3 + 2).collect_vec();
        let executor = create_executor(default_indices);
        let mut stream = executor.execute();
        // let chunk = stream.next().await.unwrap().unwrap();
        // print!("{}", chunk.to_pretty_string());
    }

    #[tokio::test]
    async fn test_execute() {
        let default_indices = (0..3 + 2).collect_vec();
        // print!("{:?}", default_indices);
        let executor = create_executor(default_indices);

        let mut stream = executor.execute();
        // TODO: add more test infra to reduce the duplicated codes below.

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

        print!("{}", chunk.to_pretty_string());
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
        let executor = create_executor(vec![1, 3, 4, 2]);

        let mut stream = executor.execute();
        // TODO: add more test infra to reduce the duplicated codes below.

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
    }
}
