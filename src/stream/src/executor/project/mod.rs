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

mod materialized_exprs;
mod project_scalar;
mod project_set;

pub use materialized_exprs::{MaterializedExprsArgs, MaterializedExprsExecutor};
pub use project_scalar::ProjectExecutor;
pub use project_set::{ProjectSetExecutor, ProjectSetSelectItem};
use risingwave_common::array::StreamChunk;
use risingwave_expr::expr::NonStrictExpression;

use crate::executor::StreamExecutorResult;

pub async fn apply_project_exprs(
    exprs: &[NonStrictExpression],
    chunk: StreamChunk,
) -> StreamExecutorResult<StreamChunk> {
    let (data_chunk, ops) = chunk.into_parts();
    let mut projected_columns = Vec::new();

    for expr in exprs {
        let evaluated_expr = expr.eval_infallible(&data_chunk).await;
        projected_columns.push(evaluated_expr);
    }
    let (_, vis) = data_chunk.into_parts();

    let new_chunk = StreamChunk::with_visibility(ops, projected_columns, vis);

    Ok(new_chunk)
}
