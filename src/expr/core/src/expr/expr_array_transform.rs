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

use async_trait::async_trait;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ListValue, ScalarImpl};

use super::{BoxedExpression, Expression};
use crate::Result;

#[derive(Debug)]
pub struct ArrayTransformExpression {
    pub(super) array: BoxedExpression,
    pub(super) lambda: BoxedExpression,
}

#[async_trait]
impl Expression for ArrayTransformExpression {
    fn return_type(&self) -> DataType {
        DataType::List(Box::new(self.lambda.return_type()))
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let lambda_input = self.array.eval(input).await?;
        let lambda_input = Arc::unwrap_or_clone(lambda_input).into_list();
        let new_list = lambda_input
            .map_inner(|flatten_input| async move {
                let flatten_len = flatten_input.len();
                let chunk = DataChunk::new(vec![Arc::new(flatten_input)], flatten_len);
                self.lambda.eval(&chunk).await.map(Arc::unwrap_or_clone)
            })
            .await?;
        Ok(Arc::new(new_list.into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let lambda_input = self.array.eval_row(input).await?;
        let lambda_input = lambda_input.map(ScalarImpl::into_list);
        if let Some(lambda_input) = lambda_input {
            let mut new_vals = Vec::with_capacity(lambda_input.values().len());
            for val in lambda_input.values() {
                let row = OwnedRow::new(vec![val.clone()]);
                let res = self.lambda.eval_row(&row).await?;
                new_vals.push(res);
            }
            let new_list = ListValue::new(new_vals);
            Ok(Some(new_list.into()))
        } else {
            Ok(None)
        }
    }
}
