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

use either::Either;
use futures_async_stream::try_stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I64ArrayBuilder,
};
use risingwave_common::types::{DataType, DatumRef};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::expr::project_set_select_item::SelectItem;
use risingwave_pb::expr::{PbProjectSetSelectItem, PbTableFunction};

use super::{ExprError, Result};
use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression};

mod generate_series;
mod regexp_matches;
mod unnest;
mod user_defined;

use self::generate_series::*;
use self::regexp_matches::*;
use self::unnest::*;
use self::user_defined::*;

/// Instance of a table function.
///
/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
#[async_trait::async_trait]
pub trait TableFunction: std::fmt::Debug + Sync + Send {
    fn return_type(&self) -> DataType;

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

    match prost.get_function_type().unwrap() {
        Generate => new_generate_series::<true>(prost, chunk_size),
        Unnest => new_unnest(prost, chunk_size),
        RegexpMatches => new_regexp_matches(prost, chunk_size),
        Range => new_generate_series::<false>(prost, chunk_size),
        Udtf => new_user_defined(prost, chunk_size),
        Unspecified => unreachable!(),
    }
}

/// Used for tests. Repeat an expression n times
pub fn repeat_tf(expr: BoxedExpression, n: usize) -> BoxedTableFunction {
    #[derive(Debug)]
    struct RepeatN {
        expr: BoxedExpression,
        n: usize,
    }

    #[async_trait::async_trait]
    impl TableFunction for RepeatN {
        fn return_type(&self) -> DataType {
            self.expr.return_type()
        }

        async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
            self.eval_inner(input)
        }
    }

    impl RepeatN {
        #[try_stream(boxed, ok = DataChunk, error = ExprError)]
        async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
            let array = self.expr.eval(input).await?;

            let mut index_builder = I64ArrayBuilder::new(0x100);
            let mut value_builder = self.return_type().create_array_builder(0x100);
            for (i, value) in array.iter().enumerate() {
                index_builder.append_n(self.n, Some(i as i64));
                value_builder.append_datum_n(self.n, value);
            }
            let len = index_builder.len();
            let index_array: ArrayImpl = index_builder.finish().into();
            let value_array = value_builder.finish();
            yield DataChunk::new(vec![index_array.into(), value_array.into()], len);
        }
    }

    RepeatN { expr, n }.boxed()
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
pub struct TableFunctionOutputIter<'a> {
    stream: BoxStream<'a, Result<DataChunk>>,
    chunk: Option<DataChunk>,
    index: usize,
}

impl<'a> TableFunctionOutputIter<'a> {
    pub async fn new(
        mut stream: BoxStream<'a, Result<DataChunk>>,
    ) -> Result<TableFunctionOutputIter<'a>> {
        Ok(Self {
            chunk: stream.next().await.transpose()?,
            stream,
            index: 0,
        })
    }

    pub fn peek(&'a self) -> Option<(usize, DatumRef<'a>)> {
        let chunk = self.chunk.as_ref()?;
        let index = chunk
            .column_at(0)
            .array_ref()
            .as_int64()
            .value_at(self.index)
            .unwrap() as usize;
        let value = chunk.column_at(1).array_ref().value_at(self.index);
        Some((index, value))
    }

    pub async fn next(&mut self) -> Result<()> {
        let Some(chunk) = &self.chunk else {
            return Ok(());
        };
        self.index += 1;
        if self.index == chunk.capacity() {
            self.chunk = self.stream.next().await.transpose()?;
            self.index = 0;
        }
        Ok(())
    }
}
