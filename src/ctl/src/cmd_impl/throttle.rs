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

use risingwave_pb::common::PbThrottleType;
use risingwave_pb::meta::PbThrottleTarget;

use crate::common::CtlContext;
use crate::{ThrottleCommandArgs, ThrottleTypeArg};

pub async fn apply_throttle(
    context: &CtlContext,
    kind: PbThrottleTarget,
    params: ThrottleCommandArgs,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let throttle_type = match params.throttle_type {
        ThrottleTypeArg::Dml => PbThrottleType::Dml,
        ThrottleTypeArg::Backfill => PbThrottleType::Backfill,
        ThrottleTypeArg::Source => PbThrottleType::Source,
        ThrottleTypeArg::Sink => PbThrottleType::Sink,
    };

    meta_client
        .apply_throttle(kind, throttle_type, params.id, params.rate)
        .await?;
    Ok(())
}
