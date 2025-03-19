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

use std::future::Future;

use risingwave_expr::{Result as ExprResult, define_context};
use risingwave_pb::plan_common::ExprContext;

// For all execution mode.
define_context! {
    pub TIME_ZONE: String,
    pub FRAGMENT_ID: u32,
    pub VNODE_COUNT: usize,
    pub STRICT_MODE: bool,
}

pub fn capture_expr_context() -> ExprResult<ExprContext> {
    let time_zone = TIME_ZONE::try_with(ToOwned::to_owned)?;
    let strict_mode = STRICT_MODE::try_with(|v| *v)?;
    Ok(ExprContext {
        time_zone,
        strict_mode,
    })
}

/// Get the vnode count from the context.
///
/// Always returns `Ok` in streaming mode and `Err` in batch mode.
pub fn vnode_count() -> ExprResult<usize> {
    VNODE_COUNT::try_with(|&x| x)
}

/// Get the strict mode from expr context
///
/// The return value depends on session variable. Default is true for batch query.
///
/// Conceptually, streaming always use non-strict mode. Our implementation doesn't read this value,
/// although it's set to false as a placeholder.
pub fn strict_mode() -> ExprResult<bool> {
    STRICT_MODE::try_with(|&v| v)
}

pub async fn expr_context_scope<Fut>(expr_context: ExprContext, future: Fut) -> Fut::Output
where
    Fut: Future,
{
    TIME_ZONE::scope(
        expr_context.time_zone.to_owned(),
        STRICT_MODE::scope(expr_context.strict_mode, future),
    )
    .await
}
