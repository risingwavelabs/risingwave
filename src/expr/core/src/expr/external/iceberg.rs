// Copyright 2024 RisingWave Labs
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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use icelake::types::{
    Bucket as BucketTransform, Day as DayTransform, Hour as HourTransform, Month as MonthTransform,
    TransformFunction, Truncate as TruncateTransform, Year as YearTransform,
};
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::ensure;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::{get_children_and_return_type_for_func_call, BoxedExpression, Build};
use risingwave_expr::Result;
use risingwave_pb::expr::ExprNode;

// Bucket
pub struct Bucket {
    child: BoxedExpression,
    n: i32,
    transform: BucketTransform,
}

impl Debug for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iceberg_Bucket({})", self.n)
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for Bucket {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        unimplemented!()
    }
}

impl Build for Bucket {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let (children, res_type) = get_children_and_return_type_for_func_call(prost)?;

        // Check expression
        // # TODO
        // Check the first child type here.
        ensure!(children.len() == 2);
        ensure!(res_type == DataType::Int32);

        // Get the second child as const param
        ensure!(DataType::Int32 == children[1].get_return_type().unwrap().into());
        let literal = build_child(&children[1])?;
        let n = *literal.eval_const()?.unwrap().as_int32();

        // Build the child
        let child = build_child(&children[0])?;
        Ok(Bucket {
            child,
            n,
            transform: BucketTransform::new(n),
        })
    }
}

// Truncate
pub struct Truncate {
    child: BoxedExpression,
    w: i32,
    transform: TruncateTransform,
}

impl Debug for Truncate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iceberg_Truncate({})", self.w)
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for Truncate {
    fn return_type(&self) -> DataType {
        self.child.return_type()
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        unimplemented!()
    }
}

impl Build for Truncate {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let (children, res_type) = get_children_and_return_type_for_func_call(prost)?;

        // Check expression
        // # TODO
        // Check the first child type here.
        ensure!(children.len() == 2);
        ensure!(res_type == children[0].get_return_type().unwrap().into());

        // Get the second child as const param
        ensure!(DataType::Int32 == children[1].get_return_type().unwrap().into());
        let literal = build_child(&children[1])?;
        let w = *literal.eval_const()?.unwrap().as_int32();

        // Build the child
        let child = build_child(&children[0])?;
        Ok(Truncate {
            child,
            w,
            transform: TruncateTransform::new(w),
        })
    }
}

// Year
pub struct Year {
    child: BoxedExpression,
    transform: YearTransform,
}

impl Debug for Year {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iceberg_Year")
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for Year {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        unimplemented!()
    }
}

impl Build for Year {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let (children, res_type) = get_children_and_return_type_for_func_call(prost)?;

        // Check expression
        // # TODO
        // Check the first child type here.
        ensure!(children.len() == 1);
        ensure!(res_type == DataType::Int32);

        // Build the child
        let child = build_child(&children[0])?;
        Ok(Year {
            child,
            transform: YearTransform {},
        })
    }
}

// Month
pub struct Month {
    child: BoxedExpression,
    transform: MonthTransform,
}

impl Debug for Month {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iceberg_Month")
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for Month {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        unimplemented!()
    }
}

impl Build for Month {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let (children, res_type) = get_children_and_return_type_for_func_call(prost)?;

        // Check expression
        // # TODO
        // Check the first child type here.
        ensure!(children.len() == 1);
        ensure!(res_type == DataType::Int32);

        // Build the child
        let child = build_child(&children[0])?;
        Ok(Month {
            child,
            transform: MonthTransform {},
        })
    }
}

// Day
pub struct Day {
    child: BoxedExpression,
    transform: DayTransform,
}

impl Debug for Day {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iceberg_Day")
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for Day {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        unimplemented!()
    }
}

impl Build for Day {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let (children, res_type) = get_children_and_return_type_for_func_call(prost)?;

        // Check expression
        // # TODO
        // Check the first child type here.
        ensure!(children.len() == 1);
        ensure!(res_type == DataType::Int32);

        // Build the child
        let child = build_child(&children[0])?;
        Ok(Day {
            child,
            transform: DayTransform {},
        })
    }
}

// Hour
pub struct Hour {
    child: BoxedExpression,
    transform: HourTransform,
}

impl Debug for Hour {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iceberg_Hour")
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for Hour {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        unimplemented!()
    }
}

impl Build for Hour {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let (children, res_type) = get_children_and_return_type_for_func_call(prost)?;

        // Check expression
        // # TODO
        // Check the first child type here.
        ensure!(children.len() == 1);
        ensure!(res_type == DataType::Int32);

        // Build the child
        let child = build_child(&children[0])?;
        Ok(Hour {
            child,
            transform: HourTransform {},
        })
    }
}
