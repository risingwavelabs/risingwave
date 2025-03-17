// Copyright 2025 RisingWave Labs
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

use crate::ExprError;
use crate::error::Result;
use crate::expr::{Expression, ValueImpl};

/// A wrapper of [`Expression`] that only keeps the first error if multiple errors are returned.
pub(crate) struct Strict<E> {
    inner: E,
}

impl<E> std::fmt::Debug for Strict<E>
where
    E: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Strict")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<E> Strict<E>
where
    E: Expression,
{
    pub fn new(inner: E) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<E> Expression for Strict<E>
where
    E: Expression,
{
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        match self.inner.eval(input).await {
            Err(ExprError::Multiple(_, errors)) => Err(errors.into_first()),
            res => res,
        }
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        match self.inner.eval_v2(input).await {
            Err(ExprError::Multiple(_, errors)) => Err(errors.into_first()),
            res => res,
        }
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        self.inner.eval_row(input).await
    }

    fn eval_const(&self) -> Result<Datum> {
        self.inner.eval_const()
    }

    fn input_ref_index(&self) -> Option<usize> {
        self.inner.input_ref_index()
    }
}
