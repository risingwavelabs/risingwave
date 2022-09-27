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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

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
        self.do_execute()
    }
}

impl ProjectExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?;
            // let data_chunk = data_chunk.compact()?;
            let arrays: Vec<Column> = self
                .expr
                .iter_mut()
                .map(|expr| expr.eval(&data_chunk).map(Column::new))
                .try_collect()?;
            let (_, vis) = data_chunk.into_parts();
            let ret = DataChunk::new(arrays, vis);
            yield ret
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for ProjectExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
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
            .map(build_from_prost)
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
    use futures::stream::StreamExt;
    use risingwave_common::array::{Array, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::{InputRefExpression, LiteralExpression};

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{Executor, ValuesExecutor};
    use crate::*;

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
            identity: "ProjectExecutor".to_string(),
        });

        let fields = &proj_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);

        let mut stream = proj_executor.execute();
        let result_chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(result_chunk.dimension(), 1);
        assert_eq!(
            result_chunk
                .column_at(0)
                .array()
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
            "ValuesExecutor".to_string(),
            1024,
        ));

        let proj_executor = Box::new(ProjectExecutor {
            expr: vec![Box::new(literal)],
            child: values_executor2,
            schema: schema_unnamed!(DataType::Int32),
            identity: "ProjectExecutor2".to_string(),
        });
        let mut stream = proj_executor.execute();
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(
            *chunk.column_at(0).array(),
            array_nonnull!(I32Array, [1]).into()
        );
    }
}
