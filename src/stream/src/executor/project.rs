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

use itertools::Itertools;
use multimap::MultiMap;
use risingwave_common::array::column::Column;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_expr::expr::BoxedExpression;

use super::*;

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectExecutor {
    input: BoxedExecutor,
    inner: Inner,
}

struct Inner {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Expressions of the current projection.
    exprs: Vec<BoxedExpression>,
    /// All the watermark derivations, (input_column_index, output_column_index). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: MultiMap<usize, usize>,

    /// the selectivity threshold which should be in [0,1]. for the chunk with selectivity less
    /// than the threshold, the Project executor will construct a new chunk before expr evaluation,
    materialize_selectivity_threshold: f64,
}

impl ProjectExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        pk_indices: PkIndices,
        exprs: Vec<BoxedExpression>,
        executor_id: u64,
        watermark_derivations: MultiMap<usize, usize>,
        materialize_selectivity_threshold: f64,
    ) -> Self {
        let info = ExecutorInfo {
            schema: input.schema().to_owned(),
            pk_indices,
            identity: "Project".to_owned(),
        };

        let schema = Schema {
            fields: exprs
                .iter()
                .map(|e| Field::unnamed(e.return_type()))
                .collect_vec(),
        };
        Self {
            input,
            inner: Inner {
                ctx,
                info: ExecutorInfo {
                    schema,
                    pk_indices: info.pk_indices,
                    identity: format!("ProjectExecutor {:X}", executor_id),
                },
                exprs,
                watermark_derivations,
                materialize_selectivity_threshold,
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
        self.inner.execute(self.input).boxed()
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
            let evaluated_expr = expr
                .eval_infallible(&data_chunk, |err| {
                    self.ctx.on_compute_error(err, &self.info.identity)
                })
                .await;
            let new_column = Column::new(evaluated_expr);
            projected_columns.push(new_column);
        }
        let (_, vis) = data_chunk.into_parts();
        let vis = vis.into_visibility();
        let new_chunk = StreamChunk::new(ops, projected_columns, vis);
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
                .transform_with_expr(&self.exprs[out_col_idx], out_col_idx, |err| {
                    self.ctx.on_compute_error(
                        err,
                        &(self.info.identity.to_string() + "(when computing watermark)"),
                    )
                })
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

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(self, input: BoxedExecutor) {
        #[for_await]
        for msg in input.execute() {
            let msg = msg?;
            match msg {
                Message::Watermark(w) => {
                    let watermarks = self.handle_watermark(w).await?;
                    for watermark in watermarks {
                        yield Message::Watermark(watermark)
                    }
                }
                Message::Chunk(chunk) => match self.map_filter_chunk(chunk).await? {
                    Some(new_chunk) => yield Message::Chunk(new_chunk),
                    None => continue,
                },
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
    use risingwave_expr::expr::{build, Expression, InputRefExpression, LiteralExpression};
    use risingwave_pb::expr::expr_node::PbType;

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

        let test_expr = build(
            PbType::Add,
            DataType::Int64,
            vec![
                InputRefExpression::new(DataType::Int64, 0).boxed(),
                InputRefExpression::new(DataType::Int64, 1).boxed(),
            ],
        )
        .unwrap();

        let project = Box::new(ProjectExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            vec![],
            vec![test_expr],
            1,
            MultiMap::new(),
            0.0,
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
    #[tokio::test]
    async fn test_watermark_projection() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, PkIndices::new());

        let a_expr = build(
            PbType::Add,
            DataType::Int64,
            vec![
                InputRefExpression::new(DataType::Int64, 0).boxed(),
                LiteralExpression::new(DataType::Int64, Some(ScalarImpl::Int64(1))).boxed(),
            ],
        )
        .unwrap();

        let b_expr = build(
            PbType::Subtract,
            DataType::Int64,
            vec![
                Box::new(InputRefExpression::new(DataType::Int64, 0)),
                Box::new(LiteralExpression::new(
                    DataType::Int64,
                    Some(ScalarImpl::Int64(1)),
                )),
            ],
        )
        .unwrap();

        let project = Box::new(ProjectExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            vec![],
            vec![a_expr, b_expr],
            1,
            MultiMap::from_iter(vec![(0, 0), (0, 1)].into_iter()),
            0.0,
        ));
        let mut project = project.execute();

        tx.push_int64_watermark(0, 100);

        let w1 = project.next().await.unwrap().unwrap();
        let w1 = w1.as_watermark().unwrap();
        let w2 = project.next().await.unwrap().unwrap();
        let w2 = w2.as_watermark().unwrap();
        let (w1, w2) = if w1.col_idx < w2.col_idx {
            (w1, w2)
        } else {
            (w2, w1)
        };

        assert_eq!(
            w1,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(101)
            }
        );

        assert_eq!(
            w2,
            &Watermark {
                col_idx: 1,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(99)
            }
        );
        tx.push_int64_watermark(1, 100);
        tx.push_barrier(1, true);
        assert!(project.next().await.unwrap().unwrap().is_stop());
    }
}
