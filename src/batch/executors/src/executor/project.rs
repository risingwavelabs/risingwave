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

use std::sync::Arc;

use futures::{Stream, StreamExt};
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_expr::expr::{BoxedExpression, Expression, build_batch_expr_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

pub struct ProjectExecutor {
    expr: Vec<BoxedExpression>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
}

impl Executor for ProjectExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        (*self).do_execute().boxed()
    }
}

impl ProjectExecutor {
    fn do_execute(self) -> impl Stream<Item = Result<DataChunk>> + 'static {
        let Self { expr, child, .. } = self;
        let expr: Arc<[Box<dyn Expression>]> = expr.into();
        child
            .execute()
            .map(move |data_chunk| {
                let expr = expr.clone();
                async move {
                    let data_chunk = data_chunk?;
                    let arrays = {
                        let expr_futs = expr.iter().map(|expr| expr.eval(&data_chunk));
                        futures::future::join_all(expr_futs)
                            .await
                            .into_iter()
                            .try_collect()?
                    };
                    let (_, vis) = data_chunk.into_parts();
                    Ok::<_, BatchError>(DataChunk::new(arrays, vis))
                }
            })
            .buffered(16)
    }
}

impl BoxedExecutorBuilder for ProjectExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let project_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Project
        )?;

        let project_exprs: Vec<_> = project_node
            .get_select_list()
            .iter()
            .map(build_batch_expr_from_prost)
            .try_collect()?;

        let fields = project_exprs
            .iter()
            .map(|expr| Field::unnamed(expr.return_type()))
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            expr: project_exprs,
            child,
            schema: Schema { fields },
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, I32Array};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::{InputRefExpression, LiteralExpression};

    use super::*;
    use crate::executor::ValuesExecutor;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_project_executor() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "
            i     i
            1     7
            2     8
            33333 66666
            4     4
            5     3
        ",
        );

        let expr1 = InputRefExpression::new(DataType::Int32, 0);
        let expr_vec = vec![Box::new(expr1) as BoxedExpression];

        let schema = schema_unnamed! { DataType::Int32, DataType::Int32 };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(chunk);

        let fields = expr_vec
            .iter()
            .map(|expr| Field::unnamed(expr.return_type()))
            .collect::<Vec<Field>>();

        let proj_executor = Box::new(ProjectExecutor {
            expr: expr_vec,
            child: Box::new(mock_executor),
            schema: Schema { fields },
            identity: "ProjectExecutor".to_owned(),
        });

        let fields = &proj_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);

        let mut stream = proj_executor.execute();
        let result_chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(result_chunk.dimension(), 1);
        assert_eq!(
            result_chunk
                .column_at(0)
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(33333), Some(4), Some(5)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_project_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1_i32.into()));

        let values_executor2: Box<dyn Executor> = Box::new(ValuesExecutor::new(
            vec![vec![]], // One single row with no column.
            Schema::default(),
            "ValuesExecutor".to_owned(),
            CHUNK_SIZE,
        ));

        let proj_executor = Box::new(ProjectExecutor {
            expr: vec![Box::new(literal)],
            child: values_executor2,
            schema: schema_unnamed!(DataType::Int32),
            identity: "ProjectExecutor2".to_owned(),
        });
        let mut stream = proj_executor.execute();
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(*chunk.column_at(0), I32Array::from_iter([1]).into_ref());
    }
}
