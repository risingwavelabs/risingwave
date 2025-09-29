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

use risingwave_common::array::{Array, ArrayImpl, Op};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::NonStrictExpression;

use crate::executor::prelude::*;

/// `FilterExecutor` filters data with the `expr`. The `expr` takes a chunk of data,
/// and returns a boolean array on whether each item should be retained. And then,
/// `FilterExecutor` will insert, delete or update element into next executor according
/// to the result of the expression.
///
/// # Upsert mode
///
/// When `UPSERT` is specified, the filter executor will...
/// - always yield `Delete` records on `Delete` (or `UpdateDelete`) input, regardless of the filter result
/// - always yield `Delete` records on `Insert` (or `UpdateInsert`) input, if the filter result is `false`
///
/// This is to guarantee that the filtered-out data is correctly cleaned up, even if we
/// don't have information about the previous data under the same primary key. Since an
/// upsert stream is expected as the output of the filter executor in this case, we can
/// safely yield a `Delete` record no matter if it used to exist or not.
pub struct FilterExecutorInner<const UPSERT: bool> {
    _ctx: ActorContextRef,
    input: Executor,

    /// Expression of the current filter, note that the filter must always have the same output for
    /// the same input.
    expr: NonStrictExpression,
}

pub type FilterExecutor = FilterExecutorInner<false>;
pub type UpsertFilterExecutor = FilterExecutorInner<true>;

impl<const UPSERT: bool> FilterExecutorInner<UPSERT> {
    pub fn new(ctx: ActorContextRef, input: Executor, expr: NonStrictExpression) -> Self {
        Self {
            _ctx: ctx,
            input,
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

        assert_eq!(vis.len(), n);

        let ArrayImpl::Bool(bool_array) = &*filter else {
            panic!("unmatched type: filter expr returns a non-null array");
        };
        for (&op, res) in ops.iter().zip_eq_fast(bool_array.iter()) {
            // SAFETY: ops.len() == pred_output.len() == visibility.len()
            let res = res.unwrap_or(false);

            if UPSERT {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if res {
                            // Always emit an `Insert`.
                            // - if the row didn't exist before, this is a new row
                            // - if the row existed before, this is an update
                            new_ops.push(Op::Insert);
                            new_visibility.append(true);
                        } else {
                            // Always emit a `Delete`.
                            // - if the row didn't exist before, this is a no-op
                            // - if the row existed before, this is a deletion
                            new_ops.push(Op::Delete);
                            new_visibility.append(true);
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        // Always emit a `Delete` no matter what the filter result is.
                        // - if the row didn't exist before, this is a no-op
                        // - if the row existed before, this is a deletion
                        new_ops.push(Op::Delete);
                        new_visibility.append(true);
                    }
                }
            } else {
                match op {
                    Op::Insert | Op::Delete => {
                        new_ops.push(op);
                        new_visibility.append(res);
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
        }

        let new_visibility = new_visibility.finish();

        Ok(if new_visibility.count_ones() > 0 {
            let new_chunk = StreamChunk::with_visibility(new_ops, columns, new_visibility);
            Some(new_chunk)
        } else {
            None
        })
    }
}

impl<const UPSERT: bool> Debug for FilterExecutorInner<UPSERT> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("expr", &self.expr)
            .field("upsert", &UPSERT)
            .finish()
    }
}

impl<const UPSERT: bool> Execute for FilterExecutorInner<UPSERT> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<const UPSERT: bool> FilterExecutorInner<UPSERT> {
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

                    let pred_output = self.expr.eval_infallible(chunk.data_chunk()).await;

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
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;

    use super::super::test_utils::MockSource;
    use super::super::test_utils::expr::build_from_pretty;
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
        let pk_indices = PkIndices::new();
        let source = MockSource::with_chunks(vec![chunk1, chunk2])
            .into_executor(schema.clone(), pk_indices.clone());

        let test_expr = build_from_pretty("(greater_than:boolean $0:int8 $1:int8)");

        let mut filter = FilterExecutor::new(ActorContext::for_test(123), source, test_expr)
            .boxed()
            .execute();

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

    #[tokio::test]
    async fn test_upsert_filter() {
        let chunk = StreamChunk::from_pretty(
            " I  I
            + 10 14
            + 20 5
            + 10 7
            + 20 16
            + 20 18
            - 10 .
            - 20 .
            - 30 .
            ",
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let pk_indices = vec![0];
        let source =
            MockSource::with_chunks(vec![chunk]).into_executor(schema.clone(), pk_indices.clone());
        let test_expr = build_from_pretty("(greater_than:boolean $1:int8 10:int8)");
        let mut filter = UpsertFilterExecutor::new(ActorContext::for_test(123), source, test_expr)
            .boxed()
            .execute();
        let chunk = filter.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I  I
                + 10 14
                - 20 5
                - 10 7
                + 20 16
                + 20 18
                - 10 .
                - 20 .
                - 30 .
                ",
            )
        );
    }
}
