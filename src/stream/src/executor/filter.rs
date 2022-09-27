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
use risingwave_common::array::{Array, ArrayImpl, Op, StreamChunk, Vis};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_expr::expr::BoxedExpression;

use super::{
    ActorContextRef, Executor, ExecutorInfo, PkIndicesRef, SimpleExecutor, SimpleExecutorWrapper,
    StreamExecutorResult,
};
use crate::common::InfallibleExpression;

pub type FilterExecutor = SimpleExecutorWrapper<SimpleFilterExecutor>;

impl FilterExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        expr: BoxedExpression,
        executor_id: u64,
    ) -> Self {
        let info = input.info();

        SimpleExecutorWrapper {
            input,
            inner: SimpleFilterExecutor::new(ctx, info, expr, executor_id),
        }
    }
}

/// `FilterExecutor` filters data with the `expr`. The `expr` takes a chunk of data,
/// and returns a boolean array on whether each item should be retained. And then,
/// `FilterExecutor` will insert, delete or update element into next executor according
/// to the result of the expression.
pub struct SimpleFilterExecutor {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Expression of the current filter, note that the filter must always have the same output for
    /// the same input.
    expr: BoxedExpression,
}

impl SimpleFilterExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input_info: ExecutorInfo,
        expr: BoxedExpression,
        executor_id: u64,
    ) -> Self {
        Self {
            ctx,
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("FilterExecutor {:X}", executor_id),
            },
            expr,
        }
    }
}

impl Debug for SimpleFilterExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("expr", &self.expr)
            .finish()
    }
}

impl SimpleExecutor for SimpleFilterExecutor {
    fn map_filter_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let chunk = chunk.compact()?;

        let (data_chunk, ops) = chunk.into_parts();

        let pred_output = self.expr.eval_infallible(&data_chunk, |err| {
            self.ctx.on_compute_error(err, self.identity())
        });

        let (columns, vis) = data_chunk.into_parts();

        let n = ops.len();

        // TODO: Can we update ops and visibility inplace?
        let mut new_ops = Vec::with_capacity(n);
        let mut new_visibility = BitmapBuilder::with_capacity(n);
        let mut last_res = false;

        assert!(match vis {
            Vis::Compact(c) => c == n,
            Vis::Bitmap(ref m) => m.len() == n,
        });

        if let ArrayImpl::Bool(bool_array) = &*pred_output {
            for (op, res) in ops.into_iter().zip_eq(bool_array.iter()) {
                // SAFETY: ops.len() == pred_output.len() == visibility.len()
                let res = res.unwrap_or(false);
                match op {
                    Op::Insert | Op::Delete => {
                        new_ops.push(op);
                        if res {
                            new_visibility.append(true);
                        } else {
                            new_visibility.append(false);
                        }
                    }
                    Op::UpdateDelete => {
                        last_res = res;
                    }
                    Op::UpdateInsert => match (last_res, res) {
                        (true, false) => {
                            new_ops.push(Op::Delete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.append(true);
                            new_visibility.append(false);
                        }
                        (false, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::Insert);
                            new_visibility.append(false);
                            new_visibility.append(true);
                        }
                        (true, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.append(true);
                            new_visibility.append(true);
                        }
                        (false, false) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.append(false);
                            new_visibility.append(false);
                        }
                    },
                }
            }
        } else {
            panic!("unmatched type: filter expr returns a non-null array");
        }

        let new_visibility = new_visibility.finish();

        Ok(if new_visibility.num_high_bits() > 0 {
            let new_chunk = StreamChunk::new(new_ops, columns, Some(new_visibility));
            Some(new_chunk)
        } else {
            None
        })
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
    async fn test_filter() {
        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 4
            + 5 2
            + 6 6
            - 7 5",
        );
        let chunk2 = StreamChunk::from_pretty(
            "  I I
            U- 5 3  // true -> true
            U+ 7 5  // expect UpdateDelete, UpdateInsert
            U- 5 3  // true -> false
            U+ 3 5  // expect Delete
            U- 3 5  // false -> true
            U+ 5 3  // expect Insert
            U- 3 5  // false -> false
            U+ 4 6  // expect nothing",
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
        let filter = Box::new(FilterExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            test_expr,
            1,
        ));
        let mut filter = filter.execute();

        let chunk = filter.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4 D
                + 5 2
                + 6 6 D
                - 7 5",
            )
        );

        let chunk = filter.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I
                U- 5 3
                U+ 7 5
                -  5 3
                U+ 3 5 D
                U- 3 5 D
                +  5 3
                U- 3 5 D
                U+ 4 6 D",
            )
        );

        assert!(filter.next().await.unwrap().unwrap().is_stop());
    }
}
