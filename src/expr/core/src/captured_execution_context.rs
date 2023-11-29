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

use risingwave_expr::{define_context, Result as ExprResult};
use risingwave_pb::stream_plan::CapturedExecutionContext;

// For all execution mode.
define_context! {
    pub TIME_ZONE: String,
}

pub fn capture_context() -> ExprResult<CapturedExecutionContext> {
    let ctx = TIME_ZONE::try_with(|time_zone| CapturedExecutionContext {
        time_zone: time_zone.to_owned(),
    })?;
    Ok(ctx)
}
