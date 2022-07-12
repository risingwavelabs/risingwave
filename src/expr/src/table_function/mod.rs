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

use itertools::Itertools;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::types::DataType;
use risingwave_pb::expr::TableFunction as TableFunctionProst;

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
