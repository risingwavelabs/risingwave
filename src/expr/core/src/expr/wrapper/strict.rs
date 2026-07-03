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

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};

use crate::ExprError;
use crate::error::Result;
use crate::expr::{AsyncExpression, ExpressionInfo, SyncExpression, ValueImpl};

/// A wrapper of an expression that only keeps the first error if multiple errors are returned.
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
    E: ExpressionInfo,
{
    pub fn new(inner: E) -> Self {
        Self { inner }
    }
}

impl<E> ExpressionInfo for Strict<E>
where
    E: ExpressionInfo,
{
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn input_ref_index(&self) -> Option<usize> {
        self.inner.input_ref_index()
    }
}

macro_rules! strict_eval {
    ($mode:ident, $this:expr, $input:expr, $method:ident) => {
        match forward!($mode, $this.inner, $method($input)) {
            Err(ExprError::Multiple(_, errors)) => Err(errors.into_first()),
            res => res,
        }
    };
}

impl<E> SyncExpression for Strict<E>
where
    E: SyncExpression,
{
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        strict_eval!(sync, self, input, eval)
    }

    fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        strict_eval!(sync, self, input, eval_v2)
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        self.inner.eval_row(input)
    }

    fn eval_const(&self) -> Result<Datum> {
        self.inner.eval_const()
    }
}

impl<E> AsyncExpression for Strict<E>
where
    E: AsyncExpression,
{
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        strict_eval!(async, self, input, eval)
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        strict_eval!(async, self, input, eval_v2)
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        self.inner.eval_row(input).await
    }
}
