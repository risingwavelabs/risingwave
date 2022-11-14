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

use std::sync::Arc;

use either::Either;
use itertools::Itertools;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::types::DataType;
use risingwave_pb::expr::project_set_select_item::SelectItem::*;
use risingwave_pb::expr::{
    ProjectSetSelectItem as SelectItemProst, TableFunction as TableFunctionProst,
};

use super::Result;
use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression};

mod generate_series;
use generate_series::*;
mod unnest;
use unnest::*;
mod regexp_matches;
use regexp_matches::*;

/// Instance of a table function.
///
/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
pub trait TableFunction: std::fmt::Debug + Sync + Send {
    fn return_type(&self) -> DataType;

    fn eval(&self, input: &DataChunk) -> Result<Vec<ArrayRef>>;

    fn boxed(self) -> BoxedTableFunction
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

pub type BoxedTableFunction = Box<dyn TableFunction>;

pub fn build_from_prost(
    prost: &TableFunctionProst,
    chunk_size: usize,
) -> Result<BoxedTableFunction> {
    use risingwave_pb::expr::table_function::Type::*;

    match prost.get_function_type().unwrap() {
        Generate => new_generate_series(prost, chunk_size),
        Unnest => new_unnest(prost, chunk_size),
        RegexpMatches => new_regexp_matches(prost, chunk_size),
        Unspecified => unreachable!(),
    }
}

/// Helper function to create an empty array.
fn empty_array(data_type: DataType) -> ArrayRef {
    Arc::new(data_type.create_array_builder(0).finish())
}

/// Used for tests. Repeat an expression n times
pub fn repeat_tf(expr: BoxedExpression, n: usize) -> BoxedTableFunction {
    #[derive(Debug)]
    struct Mock {
        expr: BoxedExpression,
        n: usize,
    }

    impl TableFunction for Mock {
        fn return_type(&self) -> DataType {
            self.expr.return_type()
        }

        fn eval(&self, input: &DataChunk) -> Result<Vec<ArrayRef>> {
            let array = self.expr.eval(input)?;

            let mut res = vec![];
            for datum_ref in array.iter() {
                let mut builder = self.return_type().create_array_builder(self.n);
                for _ in 0..self.n {
                    builder.append_datum_ref(datum_ref);
                }
                res.push(Arc::new(builder.finish()));
            }

            Ok(res)
        }
    }

    Mock { expr, n }.boxed()
}

/// See also [`SelectItemProst`]
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
    pub fn from_prost(prost: &SelectItemProst, chunk_size: usize) -> Result<Self> {
        match prost.select_item.as_ref().unwrap() {
            Expr(expr) => expr_build_from_prost(expr).map(Into::into),
            TableFunction(tf) => build_from_prost(tf, chunk_size).map(Into::into),
        }
    }

    pub fn return_type(&self) -> DataType {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf.return_type(),
            ProjectSetSelectItem::Expr(expr) => expr.return_type(),
        }
    }

    pub fn eval(&self, input: &DataChunk) -> Result<Either<Vec<ArrayRef>, ArrayRef>> {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf.eval(input).map(Either::Left),
            ProjectSetSelectItem::Expr(expr) => expr.eval(input).map(Either::Right),
        }
    }
}
