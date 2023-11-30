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

use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use multimap::MultiMap;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::{DataType, Datum, DatumRef, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{LogReport, NonStrictExpression};
use risingwave_expr::table_function::ProjectSetSelectItem;

use super::error::StreamExecutorError;
use super::{
    ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message, PkIndicesRef,
    StreamExecutorResult, Watermark,
};
use crate::common::StreamChunkBuilder;

const PROJ_ROW_ID_OFFSET: usize = 1;

/// `ProjectSetExecutor` projects data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectSetExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectSetExecutor {
    input: BoxedExecutor,
    inner: Inner,
}

struct Inner {
    _ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Expressions of the current project_section.
    select_list: Vec<ProjectSetSelectItem>,
    chunk_size: usize,
    /// All the watermark derivations, (input_column_index, expr_idx). And the
    /// derivation expression is the project_set's expression itself.
    watermark_derivations: MultiMap<usize, usize>,
    /// Indices of nondecreasing expressions in the expression list.
    nondecreasing_expr_indices: Vec<usize>,
}

impl ProjectSetExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input: Box<dyn Executor>,
        select_list: Vec<ProjectSetSelectItem>,
        chunk_size: usize,
        watermark_derivations: MultiMap<usize, usize>,
        nondecreasing_expr_indices: Vec<usize>,
    ) -> Self {
        let inner = Inner {
            _ctx: ctx,
            info,
            select_list,
            chunk_size,
            watermark_derivations,
            nondecreasing_expr_indices,
        };

        Self { input, inner }
    }
}

impl Debug for ProjectSetExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectSetExecutor")
            .field("exprs", &self.inner.select_list)
            .finish()
    }
}

impl Executor for ProjectSetExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl Inner {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(self, input: BoxedExecutor) {
        assert!(!self.select_list.is_empty());
        // First column will be `projected_row_id`, which represents the index in the
        // output table
        let data_types: Vec<_> = std::iter::once(DataType::Int64)
            .chain(self.select_list.iter().map(|i| i.return_type()))
            .collect();
        // a temporary row buffer
        let mut row = vec![DatumRef::None; data_types.len()];
        let mut builder = StreamChunkBuilder::new(self.chunk_size, data_types);

        let mut last_nondec_expr_values = vec![None; self.nondecreasing_expr_indices.len()];
        #[for_await]
        for msg in input.execute() {
            match msg? {
                Message::Watermark(watermark) => {
                    let watermarks = self.handle_watermark(watermark).await?;
                    for watermark in watermarks {
                        yield Message::Watermark(watermark)
                    }
                }
                m @ Message::Barrier(_) => {
                    for (&expr_idx, value) in self
                        .nondecreasing_expr_indices
                        .iter()
                        .zip_eq_fast(&mut last_nondec_expr_values)
                    {
                        if let Some(value) = std::mem::take(value) {
                            yield Message::Watermark(Watermark::new(
                                expr_idx + PROJ_ROW_ID_OFFSET,
                                self.select_list[expr_idx].return_type(),
                                value,
                            ))
                        }
                    }
                    yield m
                }
                Message::Chunk(chunk) => {
                    let mut results = Vec::with_capacity(self.select_list.len());
                    for select_item in &self.select_list {
                        let result = select_item.eval(chunk.data_chunk()).await?;
                        results.push(result);
                    }

                    // for each input row
                    for row_idx in 0..chunk.capacity() {
                        // ProjectSet cannot preserve that U- is followed by U+,
                        // so we rewrite update to insert/delete.
                        let op = match chunk.ops()[row_idx] {
                            Op::Delete | Op::UpdateDelete => Op::Delete,
                            Op::Insert | Op::UpdateInsert => Op::Insert,
                        };
                        // for each output row
                        for projected_row_id in 0i64.. {
                            // SAFETY:
                            // We use `row` as a buffer and don't read elements from the previous
                            // loop. The `transmute` is used for bypassing the borrow checker.
                            let row: &mut [DatumRef<'_>] =
                                unsafe { std::mem::transmute(row.as_mut_slice()) };

                            row[0] = Some(projected_row_id.into());
                            // if any of the set columns has a value
                            let mut valid = false;
                            // for each column
                            for (item, value) in results.iter_mut().zip_eq_fast(&mut row[1..]) {
                                *value = match item {
                                    Either::Left(state) => {
                                        if let Some((i, value)) = state.peek()
                                            && i == row_idx
                                        {
                                            valid = true;
                                            value
                                        } else {
                                            None
                                        }
                                    }
                                    Either::Right(array) => array.value_at(row_idx),
                                };
                            }
                            if !valid {
                                // no more output rows for the input row
                                break;
                            }
                            if let Some(chunk) = builder.append_row(op, &*row) {
                                self.update_last_nondec_expr_values(
                                    &mut last_nondec_expr_values,
                                    &chunk,
                                );
                                yield Message::Chunk(chunk);
                            }
                            // move to the next row
                            for item in &mut results {
                                if let Either::Left(state) = item
                                    && matches!(state.peek(), Some((i, _)) if i == row_idx)
                                {
                                    state.next().await?;
                                }
                            }
                        }
                    }
                    if let Some(chunk) = builder.take() {
                        self.update_last_nondec_expr_values(&mut last_nondec_expr_values, &chunk);
                        yield Message::Chunk(chunk);
                    }
                }
            }
        }
    }

    fn update_last_nondec_expr_values(
        &self,
        last_nondec_expr_values: &mut [Datum],
        chunk: &StreamChunk,
    ) {
        if !self.nondecreasing_expr_indices.is_empty() {
            if let Some((_, first_visible_row)) = chunk.rows().next() {
                // it's ok to use the first row here, just one chunk delay
                first_visible_row
                    .project(&self.nondecreasing_expr_indices)
                    .iter()
                    .enumerate()
                    .for_each(|(idx, value)| {
                        last_nondec_expr_values[idx] = Some(
                            value
                                .to_owned_datum()
                                .expect("non-decreasing expression should never be NULL"),
                        );
                    });
            }
        }
    }

    async fn handle_watermark(&self, watermark: Watermark) -> StreamExecutorResult<Vec<Watermark>> {
        let expr_indices = match self.watermark_derivations.get_vec(&watermark.col_idx) {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut ret = vec![];
        for expr_idx in expr_indices {
            let expr_idx = *expr_idx;
            let derived_watermark = match &self.select_list[expr_idx] {
                ProjectSetSelectItem::Expr(expr) => {
                    watermark
                        .clone()
                        .transform_with_expr(
                            // TODO: should we build `expr` in non-strict mode?
                            &NonStrictExpression::new_topmost(expr, LogReport),
                            expr_idx + PROJ_ROW_ID_OFFSET,
                        )
                        .await
                }
                ProjectSetSelectItem::TableFunction(_) => {
                    bail!("Watermark should not be produced by a table function");
                }
            };

            if let Some(derived_watermark) = derived_watermark {
                ret.push(derived_watermark);
            } else {
                warn!(
                    "{} derive a NULL watermark with the expression {}!",
                    self.info.identity, expr_idx
                );
            }
        }
        Ok(ret)
    }
}
