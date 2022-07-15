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
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImplIterator, ArrayRef, DataChunk, I64ArrayBuilder,
};
use risingwave_common::error::RwError;
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

/// A column corresponds to the range `0..len`
fn index_array_column(len: usize) -> Column {
    let mut builder = I64ArrayBuilder::new(len);
    for value in 0..len {
        builder.append(Some(value as i64)).unwrap();
    }
    let array = builder.finish().unwrap();
    Column::new(Arc::new(array.into()))
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

    fn eval(&self, input: &DataChunk) -> Result<ProjectSetSelectItemResult> {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf
                .eval(input)
                .map(ProjectSetSelectItemResult::TableFunction),
            ProjectSetSelectItem::Expr(expr) => {
                expr.eval(input).map(ProjectSetSelectItemResult::Expr)
            }
        }
    }

    /// First column will be `projected_row_id`, which represents the index in the output table
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    pub async fn execute(
        select_list: Arc<Vec<Self>>,
        data_types: Vec<DataType>,
        input: &DataChunk,
    ) {
        assert!(!select_list.is_empty());

        let results: Vec<_> = select_list
            .iter()
            .map(|select_item| select_item.eval(input))
            .try_collect()?;

        let mut iters = results.iter().map(|r| r.iter()).collect_vec();

        // each iteration corresponds to the outputs of one input row
        loop {
            let items = iters.iter_mut().map(|iter| iter.next()).collect_vec();

            if items[0].is_none() {
                assert!(items.iter().all(|i| i.is_none()));
                break;
            }
            let items = items.into_iter().map(|i| i.unwrap()).collect_vec();

            // The maximum length of the results of table functions will be the output length.
            let max_tf_len = items
                .iter()
                .map(|i| i.as_ref().map_left(|arr| arr.len()).left_or(0))
                .max()
                .unwrap();
            let builders = data_types
                .clone()
                .into_iter()
                .map(|ty| ty.create_array_builder(max_tf_len));

            let mut columns = vec![index_array_column(max_tf_len)];

            for (item, mut builder) in items.into_iter().zip_eq(builders) {
                match item {
                    Either::Left(array_ref) => {
                        builder.append_array(&array_ref)?;
                        for _ in 0..(max_tf_len - array_ref.len()) {
                            builder.append_null()?;
                        }
                    }
                    Either::Right(datum_ref) => {
                        for _ in 0..max_tf_len {
                            builder.append_datum_ref(datum_ref)?;
                        }
                    }
                }
                let array = builder.finish()?;
                columns.push(Column::new(Arc::new(array)));
            }
            let chunk = DataChunk::new(columns, max_tf_len);

            yield chunk;
        }
    }
}

enum ProjectSetSelectItemResult {
    TableFunction(Vec<ArrayRef>),
    Expr(ArrayRef),
}

type IterArrays<'a> = Cloned<slice::Iter<'a, ArrayRef>>;

struct ProjectSetSelectItemResultIter<'a>(Either<IterArrays<'a>, ArrayImplIterator<'a>>);

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
    fn iter(&self) -> ProjectSetSelectItemResultIter<'_> {
        ProjectSetSelectItemResultIter(match self {
            ProjectSetSelectItemResult::TableFunction(arrays) => {
                Either::Left(arrays.iter().cloned())
            }
            ProjectSetSelectItemResult::Expr(array) => Either::Right(array.iter()),
        })
    }
}
