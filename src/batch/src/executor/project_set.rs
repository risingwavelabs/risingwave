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

use either::Either;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, DatumRef};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::table_function::ProjectSetSelectItem;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

pub struct ProjectSetExecutor {
    select_list: Vec<ProjectSetSelectItem>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
    chunk_size: usize,
}

impl Executor for ProjectSetExecutor {
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

impl ProjectSetExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        assert!(!self.select_list.is_empty());

        // First column will be `projected_row_id`, which represents the index in the
        // output table
        let mut builder = DataChunkBuilder::new(
            std::iter::once(DataType::Int64)
                .chain(self.select_list.iter().map(|i| i.return_type()))
                .collect(),
            self.chunk_size,
        );
        // a temporary row buffer
        let mut row = vec![None as DatumRef<'_>; builder.num_columns()];

        #[for_await]
        for input in self.child.execute() {
            let input = input?;

            let mut results = Vec::with_capacity(self.select_list.len());
            for select_item in &self.select_list {
                let result = select_item.eval(&input).await?;
                results.push(result);
            }

            // for each input row
            for row_idx in 0..input.capacity() {
                // for each output row
                for projected_row_id in 0i64.. {
                    // SAFETY:
                    // We use `row` as a buffer and don't read elements from the previous loop.
                    // The `transmute` is used for bypassing the borrow checker.
                    let row: &mut [DatumRef<'_>] =
                        unsafe { std::mem::transmute(row.as_mut_slice()) };
                    row[0] = Some(projected_row_id.into());
                    // if any of the set columns has a value
                    let mut valid = false;
                    // for each column
                    for (item, value) in results.iter_mut().zip_eq_fast(&mut row[1..]) {
                        *value = match item {
                            Either::Left(state) => if let Some((i, value)) = state.peek() && i == row_idx {
                                valid = true;
                                value
                            } else {
                                None
                            }
                            Either::Right(array) => array.value_at(row_idx),
                        };
                    }
                    if !valid {
                        // no more output rows for the input row
                        break;
                    }
                    if let Some(chunk) = builder.append_one_row(&*row) {
                        yield chunk;
                    }
                    // move to the next row
                    for item in &mut results {
                        if let Either::Left(state) = item && matches!(state.peek(), Some((i, _)) if i == row_idx) {
                            state.next().await?;
                        }
                    }
                }
            }
            if let Some(chunk) = builder.consume_all() {
                yield chunk;
            }
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for ProjectSetExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let project_set_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::ProjectSet
        )?;

        let select_list: Vec<_> = project_set_node
            .get_select_list()
            .iter()
            .map(|proto| {
                ProjectSetSelectItem::from_prost(
                    proto,
                    source.context.get_config().developer.chunk_size,
                )
            })
            .try_collect()?;

        let mut fields = vec![Field::with_name(DataType::Int64, "projected_row_id")];
        fields.extend(
            select_list
                .iter()
                .map(|expr| Field::unnamed(expr.return_type())),
        );

        Ok(Box::new(Self {
            select_list,
            child,
            schema: Schema { fields },
            identity: source.plan_node().get_identity().clone(),
            chunk_size: source.context.get_config().developer.chunk_size,
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use futures_async_stream::for_await;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::{Expression, InputRefExpression, LiteralExpression};
    use risingwave_expr::table_function::repeat;

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{Executor, ValuesExecutor};
    use crate::*;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_project_set_executor() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "i     i
             1     7
             2     8
             33333 66666
             4     4
             5     3",
        );

        let expr1 = InputRefExpression::new(DataType::Int32, 0);
        let expr2 = repeat(
            LiteralExpression::new(DataType::Int32, Some(1_i32.into())).boxed(),
            2,
        );
        let expr3 = repeat(
            LiteralExpression::new(DataType::Int32, Some(2_i32.into())).boxed(),
            3,
        );
        let select_list: Vec<ProjectSetSelectItem> =
            vec![expr1.boxed().into(), expr2.into(), expr3.into()];

        let schema = schema_unnamed! { DataType::Int32, DataType::Int32 };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(chunk);

        let fields = select_list
            .iter()
            .map(|expr| Field::unnamed(expr.return_type()))
            .collect::<Vec<Field>>();

        let proj_executor = Box::new(ProjectSetExecutor {
            select_list,
            child: Box::new(mock_executor),
            schema: Schema { fields },
            identity: "ProjectSetExecutor".to_string(),
            chunk_size: CHUNK_SIZE,
        });

        let fields = &proj_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);

        let expected = vec![DataChunk::from_pretty(
            "I i     i i
             0 1     1 2
             1 1     1 2
             2 1     . 2
             0 2     1 2
             1 2     1 2
             2 2     . 2
             0 33333 1 2
             1 33333 1 2
             2 33333 . 2
             0 4     1 2
             1 4     1 2
             2 4     . 2
             0 5     1 2
             1 5     1 2
             2 5     . 2",
        )];

        #[for_await]
        for (i, result_chunk) in proj_executor.execute().enumerate() {
            let result_chunk = result_chunk?;
            assert_eq!(result_chunk, expected[i]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_project_set_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1_i32.into()));
        let tf = repeat(
            LiteralExpression::new(DataType::Int32, Some(2_i32.into())).boxed(),
            2,
        );

        let values_executor2: Box<dyn Executor> = Box::new(ValuesExecutor::new(
            vec![vec![]], // One single row with no column.
            Schema::default(),
            "ValuesExecutor".to_string(),
            CHUNK_SIZE,
        ));

        let proj_executor = Box::new(ProjectSetExecutor {
            select_list: vec![literal.boxed().into(), tf.into()],
            child: values_executor2,
            schema: schema_unnamed!(DataType::Int32, DataType::Int32),
            identity: "ProjectSetExecutor2".to_string(),
            chunk_size: CHUNK_SIZE,
        });
        let mut stream = proj_executor.execute();
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                "I i i
                 0 1 2
                 1 1 2",
            ),
        );
    }
}
