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

use std::fmt::{Debug, Formatter};

use itertools::Itertools;
use risingwave_common::array::{Array, ArrayImpl, DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::expr::BoxedExpression;

use super::{BoxedExecutor, Executor, SimpleExecutor, SimpleExecutorWrapper};
use crate::executor::PkIndicesRef;

#[allow(dead_code)]
pub type FilterExecutor = SimpleExecutorWrapper<SimpleFilterExecutor>;

/// `FilterExecutor` filters data with the `expr`. The `expr` takes a chunk of data,
/// and returns a boolean array on whether each item should be retained. And then,
/// `FilterExecutor` will insert, delete or update element into next executor according
/// to the result of the expression.
pub struct SimpleFilterExecutor {
    /// The input of the current executor
    input: BoxedExecutor,

    /// Expression of the current filter, note that the filter must always have the same output for
    /// the same input.
    expr: BoxedExpression,

    /// Identity string
    identity: String,
}

impl SimpleFilterExecutor {
    pub fn new(input: Box<dyn Executor>, expr: BoxedExpression, executor_id: u64) -> Self {
        Self {
            input,
            expr,
            identity: format!("FilterExecutor {:X}", executor_id),
        }
    }
}

impl Debug for SimpleFilterExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("expr", &self.expr)
            .finish()
    }
}

impl SimpleExecutor for SimpleFilterExecutor {
    fn map_chunk(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        let chunk = chunk.compact()?;

        let (ops, columns, _visibility) = chunk.into_inner();
        let data_chunk = DataChunk::builder().columns(columns).build();

        let pred_output = self.expr.eval(&data_chunk)?;

        let (columns, visibility) = data_chunk.into_parts();

        let n = ops.len();

        // TODO: Can we update ops and visibility inplace?
        let mut new_ops = Vec::with_capacity(n);
        let mut new_visibility = Vec::with_capacity(n);
        let mut last_res = false;

        assert!(match visibility {
            None => true,
            Some(ref m) => m.len() == n,
        });
        assert!(matches!(&*pred_output, ArrayImpl::Bool(_)));

        if let ArrayImpl::Bool(bool_array) = &*pred_output {
            for (op, res) in ops.into_iter().zip_eq(bool_array.iter()) {
                // SAFETY: ops.len() == pred_output.len() == visibility.len()
                let res = res.unwrap_or(false);
                match op {
                    Op::Insert | Op::Delete => {
                        new_ops.push(op);
                        if res {
                            new_visibility.push(true);
                        } else {
                            new_visibility.push(false);
                        }
                    }
                    Op::UpdateDelete => {
                        last_res = res;
                    }
                    Op::UpdateInsert => match (last_res, res) {
                        (true, false) => {
                            new_ops.push(Op::Delete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(true);
                            new_visibility.push(false);
                        }
                        (false, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::Insert);
                            new_visibility.push(false);
                            new_visibility.push(true);
                        }
                        (true, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(true);
                            new_visibility.push(true);
                        }
                        (false, false) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(false);
                            new_visibility.push(false);
                        }
                    },
                }
            }
        } else {
            panic!("unmatched type: filter expr returns a non-null array");
        }

        let visibility = (new_visibility).try_into()?;
        let new_chunk = StreamChunk::new(new_ops, columns, Some(visibility));
        Ok(new_chunk)
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}
