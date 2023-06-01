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

use std::sync::Arc;

use either::Either;
use futures_async_stream::try_stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I64ArrayBuilder, StructArray,
};
use risingwave_common::types::{DataType, DataTypeName, DatumRef};
use risingwave_pb::expr::project_set_select_item::SelectItem;
use risingwave_pb::expr::table_function::PbType;
use risingwave_pb::expr::{PbProjectSetSelectItem, PbTableFunction};

use super::{ExprError, Result};
use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression};
use crate::sig::FuncSigDebug;

mod generate_series;
mod jsonb;
mod regexp_matches;
mod repeat;
mod unnest;
mod user_defined;

pub use self::repeat::*;
use self::user_defined::*;

/// Instance of a table function.
///
/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
#[async_trait::async_trait]
pub trait TableFunction: std::fmt::Debug + Sync + Send {
    fn return_type(&self) -> DataType;

    /// # Contract of the output
    ///
    /// The returned `DataChunk` contains at least two columns:
    /// - The first column is the row indexes of input chunk. It should be monotonically increasing.
    /// - The remaining columns are the output values. More than one columns are allowed, which will
    ///   be transformed into a single `STRUCT` column later.
    ///
    /// i.e., for the `i`-th input row, the output rows are `(i, output_1)`, `(i, output_2)`, ...
    ///
    /// How the output is splited into the `Stream` is arbitrary. It's usually done by a
    /// `DataChunkBuilder`.
    ///
    /// ## Example
    ///
    /// ```text
    /// select generate_series(1, x) from t(x);
    ///
    /// # input chunk     output chunks
    /// 1 --------------> 0 1
    /// 2 --------------> 1 1
    /// 3 ----┐           ---
    ///       │           1 2
    ///       └---------> 2 1
    ///                   ---
    ///                   2 2
    ///                   2 3
    ///          row idx--^ ^--values
    /// ```
    ///
    /// # Relationship with `ProjectSet` executor
    ///
    /// (You don't need to understand this section to implement a `TableFunction`)
    ///
    /// The output of the `TableFunction` is different from the output of the `ProjectSet` executor.
    /// `ProjectSet` executor uses the row indexes to stitch multiple table functions and produces
    /// `projected_row_id`.
    ///
    /// ## Example
    ///
    /// ```text
    /// select generate_series(1, x) from t(x);
    ///
    /// # input chunk     output chunks (TableFunction)  output chunks (ProjectSet)
    /// 1 --------------> 0 1 -------------------------> 0 1
    /// 2 --------------> 1 1 -------------------------> 0 1
    /// 3 ----┐           ---                            ---
    ///       │           1 2                            1 2
    ///       └---------> 2 1 -------------------------> 0 1
    ///                   ---                            ---
    ///                   2 2                            1 2
    ///                   2 3                            2 3
    ///          row idx--^ ^--values  projected_row_id--^ ^--values
    /// ```
    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>>;

    fn boxed(self) -> BoxedTableFunction
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

pub type BoxedTableFunction = Box<dyn TableFunction>;

pub fn build_from_prost(prost: &PbTableFunction, chunk_size: usize) -> Result<BoxedTableFunction> {
    use risingwave_pb::expr::table_function::Type::*;

    if prost.get_function_type().unwrap() == Udtf {
        return new_user_defined(prost, chunk_size);
    }

    build(
        prost.get_function_type().unwrap(),
        prost.get_return_type()?.into(),
        chunk_size,
        prost.args.iter().map(expr_build_from_prost).try_collect()?,
    )
}

/// Build a table function.
pub fn build(
    func: PbType,
    return_type: DataType,
    chunk_size: usize,
    children: Vec<BoxedExpression>,
) -> Result<BoxedTableFunction> {
    let args = children
        .iter()
        .map(|t| t.return_type().into())
        .collect::<Vec<DataTypeName>>();
    let desc = crate::sig::table_function::FUNC_SIG_MAP
        .get(func, &args)
        .ok_or_else(|| {
            ExprError::UnsupportedFunction(format!(
                "{:?}",
                FuncSigDebug {
                    func: func.as_str_name(),
                    inputs_type: &args,
                    ret_type: (&return_type).into(),
                    set_returning: true,
                }
            ))
        })?;
    (desc.build)(return_type, chunk_size, children)
}

/// See also [`PbProjectSetSelectItem`]
#[derive(Debug)]
pub enum ProjectSetSelectItem {
    TableFunction(BoxedTableFunction),
    Expr(BoxedExpression),
}

impl From<BoxedTableFunction> for ProjectSetSelectItem {
    fn from(table_function: BoxedTableFunction) -> Self {
        ProjectSetSelectItem::TableFunction(table_function)
    }
}

impl From<BoxedExpression> for ProjectSetSelectItem {
    fn from(expr: BoxedExpression) -> Self {
        ProjectSetSelectItem::Expr(expr)
    }
}

impl ProjectSetSelectItem {
    pub fn from_prost(prost: &PbProjectSetSelectItem, chunk_size: usize) -> Result<Self> {
        match prost.select_item.as_ref().unwrap() {
            SelectItem::Expr(expr) => expr_build_from_prost(expr).map(Into::into),
            SelectItem::TableFunction(tf) => build_from_prost(tf, chunk_size).map(Into::into),
        }
    }

    pub fn return_type(&self) -> DataType {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf.return_type(),
            ProjectSetSelectItem::Expr(expr) => expr.return_type(),
        }
    }

    pub async fn eval<'a>(
        &'a self,
        input: &'a DataChunk,
    ) -> Result<Either<TableFunctionOutputIter<'a>, ArrayRef>> {
        match self {
            Self::TableFunction(tf) => Ok(Either::Left(
                TableFunctionOutputIter::new(tf.eval(input).await).await?,
            )),
            Self::Expr(expr) => expr.eval(input).await.map(Either::Right),
        }
    }
}

/// A wrapper over the output of table function that allows iteration by rows.
///
/// If the table function returns multiple columns, the output will be struct values.
///
/// Note that to get datum reference for efficiency, this iterator doesn't follow the standard
/// `Stream` API. Instead, it provides a `peek` method to get the next row without consuming it,
/// and a `next` method to consume the next row.
///
/// ```
/// # use futures_util::StreamExt;
/// # use risingwave_common::array::{DataChunk, DataChunkTestExt};
/// # use risingwave_expr::table_function::TableFunctionOutputIter;
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let mut iter = TableFunctionOutputIter::new(
///     futures_util::stream::iter([
///         DataChunk::from_pretty(
///             "I I
///              0 0
///              1 1",
///         ),
///         DataChunk::from_pretty(
///             "I I
///              2 2
///              3 3",
///         ),
///     ])
///     .map(Ok)
///     .boxed(),
/// )
/// .await.unwrap();
///
/// for i in 0..4 {
///     let (index, value) = iter.peek().unwrap();
///     assert_eq!(index, i);
///     assert_eq!(value, Some((i as i64).into()));
///     iter.next().await.unwrap();
/// }
/// assert!(iter.peek().is_none());
/// # });
/// ```
pub struct TableFunctionOutputIter<'a> {
    stream: BoxStream<'a, Result<DataChunk>>,
    chunk: Option<(ArrayRef, ArrayRef)>,
    index: usize,
}

impl<'a> TableFunctionOutputIter<'a> {
    pub async fn new(
        stream: BoxStream<'a, Result<DataChunk>>,
    ) -> Result<TableFunctionOutputIter<'a>> {
        let mut iter = Self {
            stream,
            chunk: None,
            index: 0,
        };
        iter.pop_from_stream().await?;
        Ok(iter)
    }

    /// Gets the current row.
    pub fn peek(&'a self) -> Option<(usize, DatumRef<'a>)> {
        let (indexes, values) = self.chunk.as_ref()?;
        let index = indexes.as_int64().value_at(self.index).unwrap() as usize;
        let value = values.value_at(self.index);
        Some((index, value))
    }

    /// Moves to the next row.
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<()> {
        let Some((indexes, _)) = &self.chunk else {
            return Ok(());
        };
        if self.index + 1 == indexes.len() {
            // note: for cancellation safety, do not mutate self before await.
            self.pop_from_stream().await?;
            self.index = 0;
        } else {
            self.index += 1;
        }
        Ok(())
    }

    /// Gets the next chunk from stream.
    async fn pop_from_stream(&mut self) -> Result<()> {
        let chunk = self.stream.next().await.transpose()?;
        self.chunk = chunk.map(|c| {
            let (c1, c2) = c.split_column_at(1);
            let indexes = c1.column_at(0).clone();
            let values = if c2.columns().len() > 1 {
                Arc::new(StructArray::from(c2).into())
            } else {
                c2.column_at(0).clone()
            };
            (indexes, values)
        });
        Ok(())
    }
}
