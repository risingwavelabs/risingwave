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

use core::slice;
use std::iter::Cloned;
use std::sync::Arc;

use either::Either;
use itertools::Itertools;
use risingwave_common::array::{ArrayImplIterator, ArrayRef, DataChunk};
use risingwave_common::types::{DataType, DatumRef};
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

pub fn build_from_prost(prost: &TableFunctionProst) -> Result<BoxedTableFunction> {
    use risingwave_pb::expr::table_function::Type::*;

    let return_type = DataType::from(prost.get_return_type().unwrap());
    let args: Vec<_> = prost.args.iter().map(expr_build_from_prost).try_collect()?;

    match prost.get_function_type().unwrap() {
        Generate => new_generate_series(args, return_type),
        Unnest => new_unnest(args, return_type),
    }
}

/// Helper function to create an empty array.
fn empty_array(data_type: DataType) -> ArrayRef {
    Arc::new(data_type.create_array_builder(0).finish().unwrap())
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
                    builder.append_datum_ref(datum_ref)?;
                }
                let array = builder.finish()?;
                res.push(Arc::new(array));
            }

            Ok(res)
        }
    }

    Mock { expr, n }.boxed()
}

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
    pub fn from_prost(prost: &SelectItemProst) -> Result<Self> {
        match prost.select_item.as_ref().unwrap() {
            Expr(expr) => expr_build_from_prost(expr).map(Into::into),
            TableFunction(tf) => build_from_prost(tf).map(Into::into),
        }
    }

    pub fn return_type(&self) -> DataType {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf.return_type(),
            ProjectSetSelectItem::Expr(expr) => expr.return_type(),
        }
    }

    pub fn eval(&self, input: &DataChunk) -> Result<ProjectSetSelectItemResult> {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf
                .eval(input)
                .map(ProjectSetSelectItemResult::TableFunction),
            ProjectSetSelectItem::Expr(expr) => {
                expr.eval(input).map(ProjectSetSelectItemResult::Expr)
            }
        }
    }
}

pub enum ProjectSetSelectItemResult {
    TableFunction(Vec<ArrayRef>),
    Expr(ArrayRef),
}

type IterArrays<'a> = Cloned<slice::Iter<'a, ArrayRef>>;

pub struct ProjectSetSelectItemResultIter<'a>(Either<IterArrays<'a>, ArrayImplIterator<'a>>);

impl<'a> Iterator for ProjectSetSelectItemResultIter<'a> {
    type Item = Either<ArrayRef, DatumRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Either::Left(l) => Either::Left(l.next()).factor_none(),
            Either::Right(r) => Either::Right(r.next()).factor_none(),
        }
    }
}

impl ProjectSetSelectItemResult {
    pub fn iter(&self) -> ProjectSetSelectItemResultIter<'_> {
        ProjectSetSelectItemResultIter(match self {
            ProjectSetSelectItemResult::TableFunction(arrays) => {
                Either::Left(arrays.iter().cloned())
            }
            ProjectSetSelectItemResult::Expr(array) => Either::Right(array.iter()),
        })
    }
}
