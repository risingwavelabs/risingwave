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

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::{Executor, Message, PkIndicesRef, SimpleExecutor, StreamChunk};
use crate::executor::{ExecutorBuilder, PkIndices};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectExecutor {
    schema: Schema,
    pk_indices: PkIndices,

    /// The input of the current operator
    input: Box<dyn Executor>,
    /// Expressions of the current projection.
    exprs: Vec<BoxedExpression>,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,
}

impl std::fmt::Debug for ProjectExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectExecutor")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .field("input", &self.input)
            .field("exprs", &self.exprs)
            .finish()
    }
}

pub struct ProjectExecutorBuilder {}

impl ExecutorBuilder for ProjectExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::ProjectNode)?;
        let project_exprs = node
            .get_select_list()
            .iter()
            .map(build_from_prost)
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(ProjectExecutor::new(
            params.input.remove(0),
            params.pk_indices,
            project_exprs,
            params.executor_id,
            params.op_info,
        )))
    }
}

impl ProjectExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        pk_indices: PkIndices,
        exprs: Vec<BoxedExpression>,
        executor_id: u64,
        op_info: String,
    ) -> Self {
        let schema = Schema {
            fields: exprs
                .iter()
                .map(|e| Field::unnamed(e.return_type()))
                .collect_vec(),
        };
        Self {
            schema,
            pk_indices,
            input,
            exprs,
            identity: format!("ProjectExecutor {:X}", executor_id),
            op_info,
        }
    }
}

#[async_trait]
impl Executor for ProjectExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

impl SimpleExecutor for ProjectExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let chunk = chunk.compact()?;

        let (ops, columns, visibility) = chunk.into_inner();

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(columns);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        let projected_columns = self
            .exprs
            .iter_mut()
            .map(|expr| expr.eval(&data_chunk).map(Column::new))
            .collect::<Result<Vec<Column>>>()?;

        drop(data_chunk);

        let new_chunk = StreamChunk::new(ops, projected_columns, None);
        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{I64Array, *};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::InputRefExpression;
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::test_utils::MockSource;
    use crate::executor::{Executor, Message, PkIndices, ProjectExecutor};

    #[tokio::test]
    async fn test_projection() {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [7, 3] },
                column_nonnull! { I64Array, [8, 6] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
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

        let mut project = ProjectExecutor::new(
            Box::new(source),
            vec![],
            vec![test_expr],
            1,
            "ProjectExecutor".to_string(),
        );

        if let Message::Chunk(chunk) = project.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns().len(), 1);
            assert_eq!(
                chunk
                    .column_at(0)
                    .array_ref()
                    .as_int64()
                    .iter()
                    .collect_vec(),
                vec![Some(5), Some(7), Some(9)]
            );
        } else {
            unreachable!();
        }

        if let Message::Chunk(chunk) = project.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 1);
            assert_eq!(
                chunk
                    .column_at(0)
                    .array_ref()
                    .as_int64()
                    .iter()
                    .collect_vec(),
                vec![Some(15), Some(9)]
            );
        } else {
            unreachable!();
        }

        assert!(project.next().await.unwrap().is_stop());
    }
}
