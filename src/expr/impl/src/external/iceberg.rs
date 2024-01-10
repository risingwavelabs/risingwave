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

//! This module contains the expression for computing the iceberg partition value.
//! spec ref: <https://iceberg.apache.org/spec/#partition-transforms>
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use icelake::types::{
    BoxedTransformFunction, Bucket as BucketTransform, Day as DayTransform, Hour as HourTransform,
    Month as MonthTransform, Truncate as TruncateTransform, Year as YearTransform,
};
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::ensure;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::BoxedExpression;
use risingwave_expr::{build_function, Result};

#[derive(Debug)]
pub enum IcebergTransformType {
    Bucket = 0,
    Truncate = 1,
    Year = 2,
    Month = 3,
    Day = 4,
    Hour = 5,
}

impl From<i32> for IcebergTransformType {
    fn from(ty: i32) -> Self {
        match ty {
            0 => IcebergTransformType::Bucket,
            1 => IcebergTransformType::Truncate,
            2 => IcebergTransformType::Year,
            3 => IcebergTransformType::Month,
            4 => IcebergTransformType::Day,
            5 => IcebergTransformType::Hour,
            _ => panic!("Invalid iceberg transform type: {}", ty),
        }
    }
}

pub struct IcebergTransform {
    child: BoxedExpression,
    transform: BoxedTransformFunction,
    ty: IcebergTransformType,
}

impl std::fmt::Debug for IcebergTransform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTransform")
            .field("child", &self.child)
            .field("ty", &self.ty)
            .finish()
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for IcebergTransform {
    fn return_type(&self) -> DataType {
        match self.ty {
            IcebergTransformType::Bucket => DataType::Int32,
            IcebergTransformType::Truncate => self.child.return_type(),
            IcebergTransformType::Year => DataType::Int32,
            IcebergTransformType::Month => DataType::Int32,
            IcebergTransformType::Day => DataType::Int32,
            IcebergTransformType::Hour => DataType::Int32,
        }
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

#[build_function("iceberg_transform(int4, any, ...) -> any")]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let transform_type: IcebergTransformType =
        (*children[0].eval_const()?.unwrap().as_int32()).into();
    match transform_type {
        IcebergTransformType::Bucket => IcebergTransform::build_bucket(return_type, children),
        IcebergTransformType::Truncate => IcebergTransform::build_truncate(return_type, children),
        IcebergTransformType::Year => IcebergTransform::build_year(return_type, children),
        IcebergTransformType::Month => IcebergTransform::build_month(return_type, children),
        IcebergTransformType::Day => IcebergTransform::build_day(return_type, children),
        IcebergTransformType::Hour => IcebergTransform::build_hour(return_type, children),
    }
}

impl IcebergTransform {
    fn build_bucket(
        return_type: DataType,
        mut children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        // Check expression
        ensure!(children.len() == 3);
        ensure!(matches!(
            children[1].return_type(),
            DataType::Int32
                | DataType::Int64
                | DataType::Decimal
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::Timestamptz
                | DataType::Varchar
                | DataType::Bytea
        ));
        ensure!(DataType::Int32 == children[2].return_type());
        ensure!(return_type == DataType::Int32);

        // Get const param
        let n = *children[2].eval_const()?.unwrap().as_int32();

        Ok(Box::new(IcebergTransform {
            child: children.remove(1),
            transform: Box::new(BucketTransform::new(n)),
            ty: IcebergTransformType::Bucket,
        }))
    }

    fn build_truncate(
        return_type: DataType,
        mut children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        // Check expression
        ensure!(children.len() == 3);
        ensure!(matches!(
            children[1].return_type(),
            DataType::Int32 | DataType::Int64 | DataType::Decimal | DataType::Varchar
        ));
        ensure!(DataType::Int32 == children[2].return_type());
        ensure!(return_type == children[1].return_type());

        // Get const param
        let w = *children[2].eval_const()?.unwrap().as_int32();

        Ok(Box::new(IcebergTransform {
            child: children.remove(1),
            transform: Box::new(TruncateTransform::new(w)),
            ty: IcebergTransformType::Truncate,
        }))
    }

    fn build_year(
        return_type: DataType,
        mut children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        // Check expression
        ensure!(children.len() == 2);
        ensure!(matches!(
            children[1].return_type(),
            DataType::Date | DataType::Timestamp | DataType::Timestamptz
        ));
        ensure!(return_type == DataType::Int32);

        Ok(Box::new(IcebergTransform {
            child: children.remove(1),
            transform: Box::new(YearTransform {}),
            ty: IcebergTransformType::Year,
        }))
    }

    fn build_month(
        return_type: DataType,
        mut children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        // Check expression
        ensure!(children.len() == 2);
        ensure!(matches!(
            children[1].return_type(),
            DataType::Date | DataType::Timestamp | DataType::Timestamptz
        ));
        ensure!(return_type == DataType::Int32);

        Ok(Box::new(IcebergTransform {
            child: children.remove(1),
            transform: Box::new(MonthTransform {}),
            ty: IcebergTransformType::Month,
        }))
    }

    fn build_day(
        return_type: DataType,
        mut children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        // Check expression
        ensure!(children.len() == 2);
        ensure!(matches!(
            children[1].return_type(),
            DataType::Date | DataType::Timestamp | DataType::Timestamptz
        ));
        ensure!(return_type == DataType::Int32);

        Ok(Box::new(IcebergTransform {
            child: children.remove(1),
            transform: Box::new(DayTransform {}),
            ty: IcebergTransformType::Day,
        }))
    }

    fn build_hour(
        return_type: DataType,
        mut children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        // Check expression
        ensure!(children.len() == 2);
        ensure!(matches!(
            children[1].return_type(),
            DataType::Timestamp | DataType::Timestamptz
        ));
        ensure!(return_type == DataType::Int32);

        Ok(Box::new(IcebergTransform {
            child: children.remove(1),
            transform: Box::new(HourTransform {}),
            ty: IcebergTransformType::Hour,
        }))
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_bucket() {
        let (input, expected) = DataChunk::from_pretty(
            "i   i
             34  1373",
        )
        .split_column_at(1);
        let expr = build_from_pretty("(iceberg_transform:int4 0:int4 $0:int 2017:int4)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(0));
    }

    #[tokio::test]
    async fn test_truncate() {
        let (input, expected) = DataChunk::from_pretty(
            "T         T
            iceberg   ice
            risingwave ris
            delta     del",
        )
        .split_column_at(1);
        let expr = build_from_pretty("(iceberg_transform:varchar 1:int4 $0:varchar 3:int4)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(0));
    }

    #[tokio::test]
    async fn test_year_month_day_hour() {
        let (input, expected) = DataChunk::from_pretty(
            "TZ                                  i i i i
            1970-01-01T00:00:00.000000000+00:00  0 0 0 0
            1971-02-01T01:00:00.000000000+00:00  1 13 396 9505
            1972-03-01T02:00:00.000000000+00:00  2 26 790 18962
            1970-05-01T06:00:00.000000000+00:00  0 4 120 2886
            1970-06-01T07:00:00.000000000+00:00  0 5 151 3631",
        )
        .split_column_at(1);

        // year
        let expr = build_from_pretty("(iceberg_transform:int4 2:int4 $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(0));

        // month
        let expr = build_from_pretty("(iceberg_transform:int4 3:int4 $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(1));

        // day
        let expr = build_from_pretty("(iceberg_transform:int4 4:int4 $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(2));

        // hour
        let expr = build_from_pretty("(iceberg_transform:int4 5:int4 $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(3));
    }
}
