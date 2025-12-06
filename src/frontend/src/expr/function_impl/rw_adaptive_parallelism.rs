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

use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_expr::{ExprError, Result, capture_context, function};

use super::context::SYSTEM_PARAMS_MANAGER;

/// Compute target parallelism using the current adaptive strategy and the provided available
/// parallelism.
#[function("rw_adaptive_parallelism(int4) -> int4", volatile)]
async fn rw_adaptive_parallelism_with_available(available_parallelism: i32) -> Result<i32> {
    if available_parallelism <= 0 {
        return Err(ExprError::InvalidParam {
            name: "available_parallelism",
            reason: "must be positive".into(),
        });
    }
    rw_adaptive_parallelism_impl_captured(available_parallelism as usize).await
}

#[capture_context(SYSTEM_PARAMS_MANAGER)]
async fn rw_adaptive_parallelism_impl(
    system_params_manager: &LocalSystemParamsManagerRef,
    available_parallelism: usize,
) -> Result<i32> {
    let strategy = system_params_manager
        .get_params()
        .load()
        .adaptive_parallelism_strategy();

    compute_target_parallelism(strategy, available_parallelism)
}

fn compute_target_parallelism(
    strategy: AdaptiveParallelismStrategy,
    available_parallelism: usize,
) -> Result<i32> {
    let target = strategy.compute_target_parallelism(available_parallelism);
    i32::try_from(target).map_err(|_| ExprError::NumericOutOfRange)
}
