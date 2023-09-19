use async_trait::async_trait;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};

use crate::error::Result;
use crate::expr::{Expression, ValueImpl};

#[derive(Debug)]
pub struct CheckedExpression<E>(pub E);

#[async_trait]
impl<E: Expression> Expression for CheckedExpression<E> {
    fn return_type(&self) -> DataType {
        self.0.return_type()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let res = self.0.eval(input).await?;
        assert_eq!(res.len(), input.capacity());
        Ok(res)
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        let res = self.0.eval_v2(input).await?;
        assert_eq!(res.len(), input.capacity());
        Ok(res)
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        self.0.eval_row(input).await
    }

    fn eval_const(&self) -> Result<Datum> {
        self.0.eval_const()
    }
}
