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

use itertools::Itertools;
use risingwave_expr::window_function::{
    BoxedWindowState, WINDOW_STATE_BUILDERS, WindowFuncCall, WindowFuncKind,
};
use risingwave_expr::{ExprError, Result};

mod aggregate;
mod buffer;
mod range_utils;
mod rank;

#[linkme::distributed_slice(WINDOW_STATE_BUILDERS)]
fn create_window_state_impl(call: &WindowFuncCall) -> Result<BoxedWindowState> {
    assert!(call.frame.bounds.validate().is_ok());

    use WindowFuncKind::*;
    Ok(match &call.kind {
        RowNumber => Box::new(rank::RankState::<rank::RowNumber>::new(call)),
        Rank => Box::new(rank::RankState::<rank::Rank>::new(call)),
        DenseRank => Box::new(rank::RankState::<rank::DenseRank>::new(call)),
        Aggregate(_) => aggregate::new(call)?,
        kind => {
            return Err(ExprError::UnsupportedFunction(format!(
                "{}({}) -> {}",
                kind,
                call.args.arg_types().iter().format(", "),
                &call.return_type,
            )));
        }
    })
}
