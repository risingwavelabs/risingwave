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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use risingwave_common::array::{Array, ArrayImpl, Op, StreamChunk, Vis};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::BoxedExpression;

use super::*;

/// `FilterExecutor` filters data with the `expr`. The `expr` takes a chunk of data,
/// and returns a boolean array on whether each item should be retained. And then,
/// `FilterExecutor` will insert, delete or update element into next executor according
/// to the result of the expression.
pub struct FilterExecutor {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    input: BoxedExecutor,

    /// Expression of the current filter, note that the filter must always have the same output for
    /// the same input.
    expr: BoxedExpression,
}

impl FilterExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        expr: BoxedExpression,
        executor_id: u64,
    ) -> Self {
        let input_info = input.info();
        Self {
            ctx,
            input,
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("FilterExecutor {:X}", executor_id),
            },
            expr,
        }
    }

    pub(super) fn filter(
        chunk: StreamChunk,
        filter: Arc<ArrayImpl>,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let (data_chunk, ops) = chunk.into_parts();

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

        if let ArrayImpl::Bool(bool_array) = &*filter {
            for (op, res) in ops.into_iter().zip_eq_fast(bool_array.iter()) {
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

        Ok(if new_visibility.count_ones() > 0 {
            let new_chunk = StreamChunk::new(new_ops, columns, Some(new_visibility));
            Some(new_chunk)
        } else {
            None
        })
    }
}

impl Debug for FilterExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("expr", &self.expr)
            .finish()
    }
}

impl Executor for FilterExecutor {
    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl FilterExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let input = self.input.execute();
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(w) => yield Message::Watermark(w),
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact();

                    let pred_output = self
                        .expr
                        .eval_infallible(chunk.data_chunk(), |err| {
                            self.ctx.on_compute_error(err, &self.info.identity)
                        })
                        .await;

                    match Self::filter(chunk, pred_output)? {
                        Some(new_chunk) => yield Message::Chunk(new_chunk),
                        None => continue,
                    }
                }
                m => yield m,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::build_from_pretty;

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

        let test_expr = build_from_pretty("(greater_than:boolean $0:int8 $1:int8)");

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
