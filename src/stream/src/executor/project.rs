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

use std::fmt::{Debug, Formatter};

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_expr::expr::BoxedExpression;

use super::{
    ActorContextRef, Executor, ExecutorInfo, PkIndices, PkIndicesRef, SimpleExecutor,
    SimpleExecutorWrapper, StreamExecutorResult,
};
use crate::common::InfallibleExpression;

pub type ProjectExecutor = SimpleExecutorWrapper<SimpleProjectExecutor>;

impl ProjectExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        pk_indices: PkIndices,
        exprs: Vec<BoxedExpression>,
        execuotr_id: u64,
    ) -> Self {
        let info = ExecutorInfo {
            schema: input.schema().to_owned(),
            pk_indices,
            identity: "Project".to_owned(),
        };
        SimpleExecutorWrapper {
            input,
            inner: SimpleProjectExecutor::new(ctx, info, exprs, execuotr_id),
        }
    }
}

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct SimpleProjectExecutor {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Expressions of the current projection.
    exprs: Vec<BoxedExpression>,
}

impl SimpleProjectExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input_info: ExecutorInfo,
        exprs: Vec<BoxedExpression>,
        executor_id: u64,
    ) -> Self {
        let schema = Schema {
            fields: exprs
                .iter()
                .map(|e| Field::unnamed(e.return_type()))
                .collect_vec(),
        };
        Self {
            ctx,
            info: ExecutorInfo {
                schema,
                pk_indices: input_info.pk_indices,
                identity: format!("ProjectExecutor {:X}", executor_id),
            },
            exprs,
        }
    }
}

impl Debug for SimpleProjectExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectExecutor")
            .field("exprs", &self.exprs)
            .finish()
    }
}

impl SimpleExecutor for SimpleProjectExecutor {
    fn map_filter_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let chunk = chunk.compact();

        let (data_chunk, ops) = chunk.into_parts();

        let projected_columns = self
            .exprs
            .iter_mut()
            .map(|expr| {
                Column::new(expr.eval_infallible(&data_chunk, |err| {
                    self.ctx.on_compute_error(err, &self.info.identity)
                }))
            })
            .collect();

        let new_chunk = StreamChunk::new(ops, projected_columns, None);
        Ok(Some(new_chunk))
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::InputRefExpression;
    use risingwave_pb::expr::expr_node::Type;

    use super::super::test_utils::MockSource;
    use super::super::*;
    use super::*;

    #[tokio::test]
    async fn test_projection() {
        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I
            + 7 8
            - 3 6",
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let source = MockSource::with_chunks(schema, PkIndices::new(), vec![chunk1, chunk2]);

        let left_expr = InputRefExpression::new(DataType::Int64, 0);
        let right_expr = InputRefExpression::new(DataType::Int64, 1);
        let test_expr = new_binary_expr(
            Type::Add,
            DataType::Int64,
            Box::new(left_expr),
            Box::new(right_expr),
        );

        let project = Box::new(ProjectExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            vec![],
            vec![test_expr],
            1,
        ));
        let mut project = project.execute();

        let msg = project.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 5
                + 7
                + 9"
            )
        );

        let msg = project.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I
                + 15
                -  9"
            )
        );

        assert!(project.next().await.unwrap().unwrap().is_stop());
    }
}
