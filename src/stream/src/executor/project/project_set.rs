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

use either::Either;
use multimap::MultiMap;
use risingwave_common::array::{ArrayRef, DataChunk, Op};
use risingwave_common::bail;
use risingwave_common::row::RowExt;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::ExprError;
use risingwave_expr::expr::{self, EvalErrorReport, NonStrictExpression};
use risingwave_expr::table_function::{self, BoxedTableFunction, TableFunctionOutputIter};
use risingwave_pb::expr::PbProjectSetSelectItem;
use risingwave_pb::expr::project_set_select_item::PbSelectItem;

use crate::executor::prelude::*;
use crate::task::ActorEvalErrorReport;

const PROJ_ROW_ID_OFFSET: usize = 1;

/// `ProjectSetExecutor` projects data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectSetExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectSetExecutor {
    input: Executor,
    inner: Inner,
}

struct Inner {
    _ctx: ActorContextRef,

    /// Expressions of the current `project_section`.
    select_list: Vec<ProjectSetSelectItem>,
    chunk_size: usize,
    /// All the watermark derivations, (`input_column_index`, `expr_idx`). And the
    /// derivation expression is the `project_set`'s expression itself.
    watermark_derivations: MultiMap<usize, usize>,
    /// Indices of nondecreasing expressions in the expression list.
    nondecreasing_expr_indices: Vec<usize>,
    error_report: ActorEvalErrorReport,
}

impl ProjectSetExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        select_list: Vec<ProjectSetSelectItem>,
        chunk_size: usize,
        watermark_derivations: MultiMap<usize, usize>,
        nondecreasing_expr_indices: Vec<usize>,
        error_report: ActorEvalErrorReport,
    ) -> Self {
        let inner = Inner {
            _ctx: ctx,
            select_list,
            chunk_size,
            watermark_derivations,
            nondecreasing_expr_indices,
            error_report,
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

impl Execute for ProjectSetExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }
}

impl Inner {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(self, input: Executor) {
        let mut input = input.execute();
        let first_barrier = expect_first_barrier(&mut input).await?;
        let mut is_paused = first_barrier.is_pause_on_startup();
        yield Message::Barrier(first_barrier);

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
        for msg in input {
            match msg? {
                Message::Watermark(watermark) => {
                    let watermarks = self.handle_watermark(watermark).await?;
                    for watermark in watermarks {
                        yield Message::Watermark(watermark)
                    }
                }
                Message::Barrier(barrier) => {
                    if !is_paused {
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
                    }

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause => {
                                is_paused = true;
                            }
                            Mutation::Resume => {
                                is_paused = false;
                            }
                            _ => (),
                        }
                    }

                    yield Message::Barrier(barrier);
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

                        // Whether the output corresponds to the current input row.
                        let is_current_input = |i| {
                            assert!(
                                i >= row_idx,
                                "unexpectedly operating on previous input, i: {i}, row_idx: {row_idx}",
                            );
                            i == row_idx
                        };

                        // for each output row
                        for projected_row_id in 0i64.. {
                            // SAFETY:
                            // We use `row` as a buffer and don't read elements from the previous
                            // loop. The `transmute` is used for bypassing the borrow checker.
                            let row: &mut [DatumRef<'_>] =
                                unsafe { std::mem::transmute(row.as_mut_slice()) };

                            row[0] = Some(projected_row_id.into());

                            // Whether all table functions has exhausted or has failed for current input row.
                            let mut fully_consumed = true;

                            // for each column
                            for (item, value) in results.iter_mut().zip_eq_fast(&mut row[1..]) {
                                *value = match item {
                                    Either::Left(state) => {
                                        if let Some((i, result)) = state.peek()
                                            && is_current_input(i)
                                        {
                                            match result {
                                                Ok(value) => {
                                                    fully_consumed = false;
                                                    value
                                                }
                                                Err(err) => {
                                                    self.error_report.report(err);
                                                    // When we encounter an error from one of the table functions,
                                                    //
                                                    // - if there are other successful table functions, `fully_consumed` will still be
                                                    //   set to `false`, a `NULL` will be set in the output row for the failed table function,
                                                    //   that's why we set `None` here.
                                                    //
                                                    // - if there are no other successful table functions (or we are the only table function),
                                                    //   `fully_consumed` will be set to `true`, we won't output the row at all but skip
                                                    //   the whole result set of the given row. Setting `None` here is no-op.
                                                    None
                                                }
                                            }
                                        } else {
                                            None
                                        }
                                    }
                                    Either::Right(array) => array.value_at(row_idx),
                                };
                            }

                            if fully_consumed {
                                // Skip the current input row and break the loop to handle the next input row.
                                // - If all exhausted, this is no-op.
                                // - If all failed, this skips remaining outputs of the current input row.
                                for item in &mut results {
                                    if let Either::Left(state) = item {
                                        while let Some((i, _)) = state.peek()
                                            && is_current_input(i)
                                        {
                                            state.next().await?;
                                        }
                                    }
                                }
                                break;
                            } else {
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
                                        && matches!(state.peek(), Some((i, _)) if is_current_input(i))
                                    {
                                        state.next().await?;
                                    }
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
        if !self.nondecreasing_expr_indices.is_empty()
            && let Some((_, first_visible_row)) = chunk.rows().next()
        {
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

    async fn handle_watermark(&self, watermark: Watermark) -> StreamExecutorResult<Vec<Watermark>> {
        let expr_indices = match self.watermark_derivations.get_vec(&watermark.col_idx) {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut ret = vec![];
        for expr_idx in expr_indices {
            let expr_idx = *expr_idx;
            let derived_watermark = match &self.select_list[expr_idx] {
                ProjectSetSelectItem::Scalar(expr) => {
                    watermark
                        .clone()
                        .transform_with_expr(expr, expr_idx + PROJ_ROW_ID_OFFSET)
                        .await
                }
                ProjectSetSelectItem::Set(_) => {
                    bail!("Watermark should not be produced by a table function");
                }
            };

            if let Some(derived_watermark) = derived_watermark {
                ret.push(derived_watermark);
            } else {
                warn!(
                    "a NULL watermark is derived with the expression {}!",
                    expr_idx
                );
            }
        }
        Ok(ret)
    }
}

/// Either a scalar expression or a set-returning function.
///
/// See also [`PbProjectSetSelectItem`].
///
/// A similar enum is defined in the `batch` module. The difference is that
/// we use `NonStrictExpression` instead of `BoxedExpression` here.
#[derive(Debug)]
pub enum ProjectSetSelectItem {
    Scalar(NonStrictExpression),
    Set(BoxedTableFunction),
}

impl From<BoxedTableFunction> for ProjectSetSelectItem {
    fn from(table_function: BoxedTableFunction) -> Self {
        ProjectSetSelectItem::Set(table_function)
    }
}

impl From<NonStrictExpression> for ProjectSetSelectItem {
    fn from(expr: NonStrictExpression) -> Self {
        ProjectSetSelectItem::Scalar(expr)
    }
}

impl ProjectSetSelectItem {
    pub fn from_prost(
        prost: &PbProjectSetSelectItem,
        error_report: impl EvalErrorReport + 'static,
        chunk_size: usize,
    ) -> Result<Self, ExprError> {
        match prost.select_item.as_ref().unwrap() {
            PbSelectItem::Expr(expr) => {
                expr::build_non_strict_from_prost(expr, error_report).map(Self::Scalar)
            }
            PbSelectItem::TableFunction(tf) => {
                table_function::build_from_prost(tf, chunk_size).map(Self::Set)
            }
        }
    }

    pub fn return_type(&self) -> DataType {
        match self {
            ProjectSetSelectItem::Scalar(expr) => expr.return_type(),
            ProjectSetSelectItem::Set(tf) => tf.return_type(),
        }
    }

    pub async fn eval<'a>(
        &'a self,
        input: &'a DataChunk,
    ) -> Result<Either<TableFunctionOutputIter<'a>, ArrayRef>, ExprError> {
        match self {
            Self::Scalar(expr) => Ok(Either::Right(expr.eval_infallible(input).await)),
            Self::Set(tf) => Ok(Either::Left(
                TableFunctionOutputIter::new(tf.eval(input).await).await?,
            )),
        }
    }
}
