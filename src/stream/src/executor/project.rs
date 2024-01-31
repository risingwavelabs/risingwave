// Copyright 2024 RisingWave Labs
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

use auto_enums::auto_enum;
use futures::stream::Stream;
use multimap::MultiMap;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::{RwFutureExt, RwTryStreamExt};
use risingwave_expr::expr::NonStrictExpression;

use super::*;

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectExecutor {
    input: BoxedExecutor,
    inner: Inner,
    /// The mutable parts of inner fields.
    vars: ExecutionVars,
}

struct Inner {
    _ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Expressions of the current projection.
    exprs: Vec<NonStrictExpression>,
    /// All the watermark derivations, (input_column_index, output_column_index). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: MultiMap<usize, usize>,
    /// Indices of nondecreasing expressions in the expression list.
    nondecreasing_expr_indices: Vec<usize>,

    /// the selectivity threshold which should be in `[0,1]`. for the chunk with selectivity less
    /// than the threshold, the Project executor will construct a new chunk before expr evaluation,
    materialize_selectivity_threshold: f64,
}

struct ExecutionVars {
    /// Last seen values of nondecreasing expressions, buffered to periodically produce watermarks.
    last_nondec_expr_values: Vec<Option<ScalarImpl>>,
}

impl ProjectExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input: Box<dyn Executor>,
        exprs: Vec<NonStrictExpression>,
        watermark_derivations: MultiMap<usize, usize>,
        nondecreasing_expr_indices: Vec<usize>,
        materialize_selectivity_threshold: f64,
    ) -> Self {
        let n_nondecreasing_exprs = nondecreasing_expr_indices.len();
        Self {
            input,
            inner: Inner {
                _ctx: ctx,
                info,
                exprs,
                watermark_derivations,
                nondecreasing_expr_indices,
                materialize_selectivity_threshold,
            },
            vars: ExecutionVars {
                last_nondec_expr_values: vec![None; n_nondecreasing_exprs],
            },
        }
    }
}

impl Debug for ProjectExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectExecutor")
            .field("exprs", &self.inner.exprs)
            .finish()
    }
}

impl Executor for ProjectExecutor {
    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }

    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input, self.vars).boxed()
    }
}

impl Inner {
    async fn map_filter_chunk(
        &self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let chunk = if chunk.selectivity() <= self.materialize_selectivity_threshold {
            chunk.compact()
        } else {
            chunk
        };
        let (data_chunk, ops) = chunk.into_parts();
        let mut projected_columns = Vec::new();

        for expr in &self.exprs {
            let evaluated_expr = expr.eval_infallible(&data_chunk).await;
            projected_columns.push(evaluated_expr);
        }
        let (_, vis) = data_chunk.into_parts();
        let new_chunk = StreamChunk::with_visibility(ops, projected_columns, vis);
        Ok(Some(new_chunk))
    }

    async fn handle_watermark(&self, watermark: Watermark) -> StreamExecutorResult<Vec<Watermark>> {
        let out_col_indices = match self.watermark_derivations.get_vec(&watermark.col_idx) {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut ret = vec![];
        for out_col_idx in out_col_indices {
            let out_col_idx = *out_col_idx;
            let derived_watermark = watermark
                .clone()
                .transform_with_expr(&self.exprs[out_col_idx], out_col_idx)
                .await;
            if let Some(derived_watermark) = derived_watermark {
                ret.push(derived_watermark);
            } else {
                warn!(
                    "{} derive a NULL watermark with the expression {}!",
                    self.info.identity, out_col_idx
                );
            }
        }
        Ok(ret)
    }

    fn execute(
        self,
        input: BoxedExecutor,
        mut vars: ExecutionVars,
    ) -> impl Stream<Item = MessageStreamItem> {
        let return_types: Vec<_> = self.exprs.iter().map(|expr| expr.return_type()).collect();

        // Phase 1: only evaluating the expression, which can be concurrent.

        enum Phase1Item {
            Chunk(Option<StreamChunk>),
            Barrier(Barrier),
            Watermark(Vec<Watermark>),
        }

        let this = Arc::new(self);

        let this2 = this.clone();

        let st = input.execute().map(move |msg| {
            let this = this.clone();
            let msg = msg?;
            let is_fence: bool;
            #[auto_enum(Future)]
            let fut = match msg {
                Message::Chunk(chunk) => {
                    is_fence = false;
                    async move {
                        let new_chunk = this.map_filter_chunk(chunk).await?;
                        Ok(Phase1Item::Chunk(new_chunk)) as StreamExecutorResult<_>
                    }
                }
                Message::Watermark(watermark) => {
                    is_fence = false;
                    async move {
                        let watermarks = this.handle_watermark(watermark).await?;
                        Ok(Phase1Item::Watermark(watermarks))
                    }
                }
                Message::Barrier(barrier) => {
                    is_fence = true;
                    async { Ok(Phase1Item::Barrier(barrier)) }
                }
            };

            let fut = fut.with_fence(is_fence);

            Ok(fut) as StreamExecutorResult<_>
        });

        // Make the phase 1 concurrent.
        let st = st.try_buffered_with_fence(16);

        let this = this2;

        // Phase 2: Handle the watermark related logicals, and output them all. The phase is executed one by one.
        #[try_stream]
        async move {
            #[for_await]
            for msg in st {
                let msg = msg?;
                match msg {
                    Phase1Item::Watermark(watermarks) => {
                        for watermark in watermarks {
                            yield Message::Watermark(watermark)
                        }
                    }
                    Phase1Item::Chunk(new_chunk) => match new_chunk {
                        Some(new_chunk) => {
                            if !this.nondecreasing_expr_indices.is_empty() {
                                if let Some((_, first_visible_row)) = new_chunk.rows().next() {
                                    // it's ok to use the first row here, just one chunk delay
                                    first_visible_row
                                        .project(&this.nondecreasing_expr_indices)
                                        .iter()
                                        .enumerate()
                                        .for_each(|(idx, value)| {
                                            vars.last_nondec_expr_values[idx] =
                                                Some(value.to_owned_datum().expect(
                                                    "non-decreasing expression should never be NULL",
                                                ));
                                        });
                                }
                            }
                            yield Message::Chunk(new_chunk)
                        }
                        None => continue,
                    },
                    Phase1Item::Barrier(barrier) => {
                        for (&expr_idx, value) in this
                            .nondecreasing_expr_indices
                            .iter()
                            .zip_eq_fast(&mut vars.last_nondec_expr_values)
                        {
                            if let Some(value) = std::mem::take(value) {
                                yield Message::Watermark(Watermark::new(
                                    expr_idx,
                                    return_types[expr_idx].clone(),
                                    value,
                                ))
                            }
                        }
                        yield Message::Barrier(barrier);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{self, AtomicI64};

    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{DataChunk, StreamChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, Datum};
    use risingwave_expr::expr::{self, Expression, ValueImpl};
    use risingwave_hummock_sdk::EpochWithGap;

    use super::super::test_utils::MockSource;
    use super::super::*;
    use super::*;
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::test_utils::StreamExecutorTestExt;

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
        let pk_indices = vec![0];
        let (mut tx, source) = MockSource::channel(schema, pk_indices);

        let test_expr = build_from_pretty("(add:int8 $0:int8 $1:int8)");

        let info = ExecutorInfo {
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int64)],
            },
            pk_indices: vec![],
            identity: "ProjectExecutor".to_string(),
        };

        let project = Box::new(ProjectExecutor::new(
            ActorContext::create(123),
            info,
            Box::new(source),
            vec![test_expr],
            MultiMap::new(),
            vec![],
            0.0,
        ));
        let mut project = project.execute();

        tx.push_barrier(EpochWithGap::new_without_offset(1).as_u64_for_test(), false);
        let barrier = project.next().await.unwrap().unwrap();
        barrier.as_barrier().unwrap();

        tx.push_chunk(chunk1);
        tx.push_chunk(chunk2);

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

        tx.push_barrier(EpochWithGap::new_without_offset(2).as_u64_for_test(), true);
        assert!(project.next().await.unwrap().unwrap().is_stop());
    }

    static DUMMY_COUNTER: AtomicI64 = AtomicI64::new(0);

    #[derive(Debug)]
    struct DummyNondecreasingExpr;

    #[async_trait::async_trait]
    impl Expression for DummyNondecreasingExpr {
        fn return_type(&self) -> DataType {
            DataType::Int64
        }

        async fn eval_v2(&self, input: &DataChunk) -> expr::Result<ValueImpl> {
            let value = DUMMY_COUNTER.fetch_add(1, atomic::Ordering::SeqCst);
            Ok(ValueImpl::Scalar {
                value: Some(value.into()),
                capacity: input.capacity(),
            })
        }

        async fn eval_row(&self, _input: &OwnedRow) -> expr::Result<Datum> {
            let value = DUMMY_COUNTER.fetch_add(1, atomic::Ordering::SeqCst);
            Ok(Some(value.into()))
        }
    }

    #[tokio::test]
    async fn test_watermark_projection() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, PkIndices::new());

        let a_expr = build_from_pretty("(add:int8 $0:int8 1:int8)");
        let b_expr = build_from_pretty("(subtract:int8 $0:int8 1:int8)");
        let c_expr = NonStrictExpression::for_test(DummyNondecreasingExpr);

        let info = ExecutorInfo {
            schema: Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            },
            pk_indices: vec![],
            identity: "ProjectExecutor".to_string(),
        };

        let project = Box::new(ProjectExecutor::new(
            ActorContext::create(123),
            info,
            Box::new(source),
            vec![a_expr, b_expr, c_expr],
            MultiMap::from_iter(vec![(0, 0), (0, 1)].into_iter()),
            vec![2],
            0.0,
        ));
        let mut project = project.execute();

        tx.push_barrier(EpochWithGap::new_without_offset(1).as_u64_for_test(), false);
        tx.push_int64_watermark(0, 100);

        project.expect_barrier().await;
        let w1 = project.expect_watermark().await;
        let w2 = project.expect_watermark().await;
        let (w1, w2) = if w1.col_idx < w2.col_idx {
            (w1, w2)
        } else {
            (w2, w1)
        };

        assert_eq!(
            w1,
            Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(101)
            }
        );
        assert_eq!(
            w2,
            Watermark {
                col_idx: 1,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(99)
            }
        );

        // just push some random chunks
        tx.push_chunk(StreamChunk::from_pretty(
            "   I I
            + 120 4
            + 146 5
            + 133 6",
        ));
        project.expect_chunk().await;
        tx.push_chunk(StreamChunk::from_pretty(
            "   I I
            + 213 8
            - 133 6",
        ));
        project.expect_chunk().await;

        tx.push_barrier(EpochWithGap::new_without_offset(2).as_u64_for_test(), false);
        let w3 = project.expect_watermark().await;
        project.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            "   I I
            + 100 3
            + 104 5
            + 187 3",
        ));
        project.expect_chunk().await;

        tx.push_barrier(EpochWithGap::new_without_offset(3).as_u64_for_test(), false);
        let w4 = project.expect_watermark().await;
        project.expect_barrier().await;

        assert_eq!(w3.col_idx, w4.col_idx);
        assert!(w3.val.default_cmp(&w4.val).is_le());

        tx.push_int64_watermark(1, 100);
        tx.push_barrier(EpochWithGap::new_without_offset(4).as_u64_for_test(), true);

        assert!(project.next().await.unwrap().unwrap().is_stop());
    }
}
