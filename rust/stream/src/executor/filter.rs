use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Array, ArrayImpl, DataChunk, Op};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::expr::{build_from_prost, BoxedExpression};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::{Executor, Message, PkIndicesRef, SimpleExecutor, StreamChunk};
use crate::executor::ExecutorBuilder;
use crate::task::{ExecutorParams, StreamManagerCore};

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

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,
}

pub struct FilterExecutorBuilder {}

impl ExecutorBuilder for FilterExecutorBuilder {
    fn build(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::FilterNode)?;
        let search_condition = build_from_prost(node.get_search_condition()?)?;
        Ok(Box::new(FilterExecutor::new(
            params.input.remove(0),
            search_condition,
            params.executor_id,
            params.op_info,
        )))
    }
}

impl FilterExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        expr: BoxedExpression,
        executor_id: u64,
        op_info: String,
    ) -> Self {
        Self {
            input,
            expr,
            identity: format!("FilterExecutor {:X}", executor_id),
            op_info,
        }
    }
}

impl Debug for FilterExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("input", &self.input)
            .field("expr", &self.expr)
            .finish()
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

    fn pk_indices(&self) -> PkIndicesRef {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

impl SimpleExecutor for FilterExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let chunk = chunk.compact()?;

        let (ops, columns, _visibility) = chunk.into_inner();
        let data_chunk = DataChunk::builder().columns(columns).build();

        let pred_output = self.expr.eval(&data_chunk)?;

        let (columns, visibility) = data_chunk.into_parts();

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
            for (op, res) in ops.into_iter().zip_eq(bool_array.iter()) {
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

        let visibility = (new_visibility).try_into()?;
        let new_chunk = StreamChunk::new(new_ops, columns, Some(visibility));
        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{I64Array, Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::test_utils::MockSource;
    use crate::executor::{Executor, FilterExecutor, Message, PkIndices};

    #[tokio::test]
    async fn test_filter() {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [1, 5, 6, 7] },
                column_nonnull! { I64Array, [4, 2, 6, 5] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![
                Op::UpdateDelete, // true -> true
                Op::UpdateInsert, // expect UpdateDelete, UpdateInsert
                Op::UpdateDelete, // true -> false
                Op::UpdateInsert, // expect Delete
                Op::UpdateDelete, // false -> true
                Op::UpdateInsert, // expect Insert
                Op::UpdateDelete, // false -> false
                Op::UpdateInsert, // expect nothing
            ],
            vec![
                column_nonnull! { I64Array, [5, 7, 5, 3, 3, 5, 3, 4] },
                column_nonnull! { I64Array, [3, 5, 3, 5, 5, 3, 5, 6] },
            ],
            None,
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
            Type::GreaterThan,
            DataType::Boolean,
            Box::new(left_expr),
            Box::new(right_expr),
        );
        let mut filter =
            FilterExecutor::new(Box::new(source), test_expr, 1, "FilterExecutor".to_string());

        if let Message::Chunk(chunk) = filter.next().await.unwrap() {
            assert_eq!(
                chunk.ops(),
                vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete]
            );
            assert_eq!(chunk.columns().len(), 2);
            assert_eq!(
                chunk.visibility().as_ref().unwrap().iter().collect_vec(),
                vec![false, true, false, true]
            );
        } else {
            unreachable!();
        }

        if let Message::Chunk(chunk) = filter.next().await.unwrap() {
            assert_eq!(chunk.columns().len(), 2);
            assert_eq!(
                chunk.visibility().as_ref().unwrap().iter().collect_vec(),
                vec![true, true, true, false, false, true, false, false]
            );
            assert_eq!(
                chunk.ops(),
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

        assert!(filter.next().await.unwrap().is_terminate());
    }
}
