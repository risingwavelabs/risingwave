use async_trait::async_trait;

use risingwave_common::array::{Array, ArrayImpl, DataChunk};
use risingwave_common::error::Result;
use risingwave_common::{catalog::Schema, expr::BoxedExpression};

use super::{Executor, Message, Op, SimpleExecutor, StreamChunk};

/// `FilterExecutor` filters data with the `expr`. The `expr` takes a chunk of data,
/// and returns a boolean array on whether each item should be retained. And then,
/// `FilterExecutor` will insert, delete or update element into next executor according
/// to the result of the expression.
pub struct FilterExecutor {
    /// The input of the current executor
    input: Box<dyn Executor>,
    /// Expression of the current filter, note that the filter must always have the same output for
    /// the same input.
    expr: BoxedExpression,
}

impl FilterExecutor {
    pub fn new(input: Box<dyn Executor>, expr: BoxedExpression) -> Self {
        Self { input, expr }
    }
}

#[async_trait]
impl Executor for FilterExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }
}

impl SimpleExecutor for FilterExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(arrays);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        // FIXME: unnecessary compact.
        // See https://github.com/singularity-data/risingwave/issues/704
        let data_chunk = data_chunk.compact()?;

        let pred_output = self.expr.eval(&data_chunk)?;

        let (arrays, visibility) = data_chunk.into_parts();

        let n = ops.len();

        // TODO: Can we update ops and visibility inplace?
        let mut new_ops = Vec::with_capacity(n);
        let mut new_visibility = Vec::with_capacity(n);
        let mut last_res = false;

        assert!(match visibility {
            None => true,
            Some(ref m) => m.len() == n,
        });
        assert!(matches!(&*pred_output, ArrayImpl::Bool(_)));

        if let ArrayImpl::Bool(bool_array) = &*pred_output {
            for (op, res) in ops.into_iter().zip(bool_array.iter()) {
                // SAFETY: ops.len() == pred_output.len() == visibility.len()
                let res = res.unwrap_or(false);
                match op {
                    Op::Insert | Op::Delete => {
                        new_ops.push(op);
                        if res {
                            new_visibility.push(true);
                        } else {
                            new_visibility.push(false);
                        }
                    }
                    Op::UpdateDelete => {
                        last_res = res;
                    }
                    Op::UpdateInsert => match (last_res, res) {
                        (true, false) => {
                            new_ops.push(Op::Delete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(true);
                            new_visibility.push(false);
                        }
                        (false, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::Insert);
                            new_visibility.push(false);
                            new_visibility.push(true);
                        }
                        (true, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(true);
                            new_visibility.push(true);
                        }
                        (false, false) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(false);
                            new_visibility.push(false);
                        }
                    },
                }
            }
        } else {
            panic!("unmatched type: filter expr returns a non-null array");
        }

        let new_chunk = StreamChunk {
            columns: arrays,
            visibility: Some((new_visibility).try_into()?),
            ops: new_ops,
        };

        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use risingwave_common::array::I64Array;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_pb::expr::expr_node::Type as ProstExprType;

    use crate::stream_op::test_utils::MockSource;
    use crate::stream_op::{Executor, FilterExecutor, Message, Op, StreamChunk};
    use crate::*;
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::binary_expr::new_binary_expr;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::{BoolType, Int64Type};

    #[tokio::test]
    async fn test_filter() {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 5, 6, 7] },
                column_nonnull! { I64Array, Int64Type, [4, 2, 6, 5] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![
                Op::UpdateDelete, // true -> true
                Op::UpdateInsert, // expect UpdateDelete, UpdateInsert
                Op::UpdateDelete, // true -> false
                Op::UpdateInsert, // expect Delete
                Op::UpdateDelete, // false -> true
                Op::UpdateInsert, // expect Insert
                Op::UpdateDelete, // false -> false
                Op::UpdateInsert, // expect nothing
            ],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [5, 7, 5, 3, 3, 5, 3, 4] },
                column_nonnull! { I64Array, Int64Type, [3, 5, 3, 5, 5, 3, 5, 6] },
            ],
            visibility: None,
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
        let source = MockSource::with_chunks(schema, vec![chunk1, chunk2]);

        let left_type = Int64Type::create(false);
        let left_expr = InputRefExpression::new(left_type, 0);
        let right_type = Int64Type::create(false);
        let right_expr = InputRefExpression::new(right_type, 1);
        let test_expr = new_binary_expr(
            ProstExprType::GreaterThan,
            BoolType::create(false),
            Box::new(left_expr),
            Box::new(right_expr),
        );
        let mut filter = FilterExecutor::new(Box::new(source), test_expr);

        if let Message::Chunk(chunk) = filter.next().await.unwrap() {
            assert_eq!(
                chunk.ops,
                vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete]
            );
            assert_eq!(chunk.columns.len(), 2);
            assert_eq!(
                chunk.visibility.unwrap().iter().collect_vec(),
                vec![false, true, false, true]
            );
        } else {
            unreachable!();
        }

        if let Message::Chunk(chunk) = filter.next().await.unwrap() {
            assert_eq!(chunk.columns.len(), 2);
            assert_eq!(
                chunk.visibility.unwrap().iter().collect_vec(),
                vec![true, true, true, false, false, true, false, false]
            );
            assert_eq!(
                chunk.ops,
                vec![
                    Op::UpdateDelete,
                    Op::UpdateInsert,
                    Op::Delete,
                    Op::UpdateInsert,
                    Op::UpdateDelete,
                    Op::Insert,
                    Op::UpdateDelete,
                    Op::UpdateInsert,
                ]
            );
        } else {
            unreachable!();
        }

        matches!(
            filter.next().await.unwrap(),
            Message::Barrier {
                epoch: _,
                stop: true
            }
        );
    }
}
