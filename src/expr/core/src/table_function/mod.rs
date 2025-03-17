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

use futures_async_stream::try_stream;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use risingwave_common::array::{Array, ArrayBuilder, ArrayImpl, DataChunk};
use risingwave_common::types::{DataType, DatumRef};
use risingwave_pb::expr::PbTableFunction;
use risingwave_pb::expr::table_function::PbType;

use super::{ExprError, Result};
use crate::expr::{BoxedExpression, build_from_prost as expr_build_from_prost};

mod empty;
mod repeat;
mod user_defined;

pub use self::empty::*;
pub use self::repeat::*;
use self::user_defined::*;

/// A table function takes a row as input and returns multiple rows as output.
///
/// It is also known as Set-Returning Function.
#[async_trait::async_trait]
pub trait TableFunction: std::fmt::Debug + Sync + Send {
    /// The data type of the output.
    fn return_type(&self) -> DataType;

    /// # Contract of the output
    ///
    /// The returned `DataChunk` contains two or three columns:
    /// - The first column is an I32Array containing row indices of input chunk. It should be
    ///   monotonically increasing.
    /// - The second column is the output values. The data type of the column is `return_type`.
    /// - (Optional) If any error occurs, the error message is stored in the third column.
    ///
    /// i.e., for the `i`-th input row, the output rows are `(i, output_1)`, `(i, output_2)`, ...
    ///
    /// How the output is split into the `Stream` is arbitrary. It's usually done by a
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
    /// `ProjectSet` executor uses the row indices to stitch multiple table functions and produces
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
    if prost.get_function_type().unwrap() == PbType::UserDefined {
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
    use itertools::Itertools;
    let args = children.iter().map(|t| t.return_type()).collect_vec();
    let desc = crate::sig::FUNCTION_REGISTRY.get(func, &args, &return_type)?;
    desc.build_table(return_type, chunk_size, children)
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
///             "i I
///              0 0
///              1 1",
///         ),
///         DataChunk::from_pretty(
///             "i I
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
///     assert_eq!(value.unwrap(), Some((i as i64).into()));
///     iter.next().await.unwrap();
/// }
/// assert!(iter.peek().is_none());
/// # });
/// ```
pub struct TableFunctionOutputIter<'a> {
    stream: BoxStream<'a, Result<DataChunk>>,
    chunk: Option<DataChunk>,
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
    pub fn peek(&'a self) -> Option<(usize, Result<DatumRef<'a>>)> {
        let chunk = self.chunk.as_ref()?;
        let index = chunk.column_at(0).as_int32().value_at(self.index).unwrap() as usize;
        let result = if let Some(msg) = chunk
            .columns()
            .get(2)
            .and_then(|errors| errors.as_utf8().value_at(self.index))
        {
            Err(ExprError::Custom(msg.into()))
        } else {
            Ok(chunk.column_at(1).value_at(self.index))
        };
        Some((index, result))
    }

    /// Moves to the next row.
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<()> {
        let Some(chunk) = &self.chunk else {
            return Ok(());
        };
        if self.index + 1 == chunk.capacity() {
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
        self.chunk = self.stream.next().await.transpose()?;
        Ok(())
    }
}

/// Checks if the output chunk returned by `TableFunction::eval` contains any error.
pub fn check_error(chunk: &DataChunk) -> Result<()> {
    if let Some(errors) = chunk.columns().get(2) {
        if errors.null_bitmap().any() {
            return Err(ExprError::Custom(
                errors
                    .as_utf8()
                    .iter()
                    .find_map(|s| s)
                    .expect("no error message")
                    .into(),
            ));
        }
    }
    Ok(())
}
