use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::expr::BoxedExpression;

use super::{Executor, Message, PkIndicesRef, SimpleExecutor, StreamChunk};
use crate::executor::PkIndices;

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
}

impl ProjectExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        pk_indices: PkIndices,
        exprs: Vec<BoxedExpression>,
    ) -> Self {
        let schema = Schema {
            fields: exprs
                .iter()
                .map(|e| Field {
                    data_type: e.return_type_ref(),
                })
                .collect_vec(),
        };
        Self {
            schema,
            pk_indices,
            input,
            exprs,
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
}

impl SimpleExecutor for ProjectExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let chunk = chunk.compact()?;

        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

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

        let new_chunk = StreamChunk {
            ops,
            columns: projected_columns,
            visibility: None,
        };

        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{I64Array, *};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::Int64Type;
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::test_utils::MockSource;
    use crate::executor::{Executor, Message, PkIndices, ProjectExecutor};
    use crate::*;

    #[tokio::test]
    async fn test_projection() {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, [7, 3] },
                column_nonnull! { I64Array, [8, 6] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };
        let source = MockSource::with_chunks(schema, PkIndices::new(), vec![chunk1, chunk2]);

        let left_type = Int64Type::create(false);
        let left_expr = InputRefExpression::new(left_type, 0);
        let right_type = Int64Type::create(false);
        let right_expr = InputRefExpression::new(right_type, 1);
        let test_expr = new_binary_expr(
            Type::Add,
            Int64Type::create(false),
            Box::new(left_expr),
            Box::new(right_expr),
        );

        let mut project = ProjectExecutor::new(Box::new(source), vec![], vec![test_expr]);

        if let Message::Chunk(chunk) = project.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns.len(), 1);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(7), Some(9)]
            );
        } else {
            unreachable!();
        }

        if let Message::Chunk(chunk) = project.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns.len(), 1);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(15), Some(9)]
            );
        } else {
            unreachable!();
        }

        assert!(project.next().await.unwrap().is_terminate());
    }
}
