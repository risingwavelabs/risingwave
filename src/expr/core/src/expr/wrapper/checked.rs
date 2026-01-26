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

use async_trait::async_trait;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};

use crate::error::Result;
use crate::expr::{Expression, ValueImpl};

/// A wrapper of [`Expression`] that does extra checks after evaluation.
#[derive(Debug)]
pub(crate) struct Checked<E>(pub E);

// TODO: avoid the overhead of extra boxing.
#[async_trait]
impl<E: Expression> Expression for Checked<E> {
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

    fn input_ref_index(&self) -> Option<usize> {
        self.0.input_ref_index()
    }
}
