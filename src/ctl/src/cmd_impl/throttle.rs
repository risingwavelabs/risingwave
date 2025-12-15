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

use anyhow::bail;
use risingwave_pb::common::PbThrottleType;
use risingwave_pb::meta::PbThrottleTarget;

use crate::ThrottleCommandArgs;
use crate::common::CtlContext;

/// Parse throttle type from string
fn parse_throttle_type(s: &str) -> anyhow::Result<PbThrottleType> {
    if s.eq_ignore_ascii_case("dml") {
        Ok(PbThrottleType::Dml)
    } else if s.eq_ignore_ascii_case("backfill") {
        Ok(PbThrottleType::Backfill)
    } else if s.eq_ignore_ascii_case("source") {
        Ok(PbThrottleType::Source)
    } else if s.eq_ignore_ascii_case("sink") {
        Ok(PbThrottleType::Sink)
    } else {
        bail!(
            "Invalid throttle type: {}. Valid options are: dml, backfill, source, sink",
            s
        )
    }
}

pub async fn apply_throttle(
    context: &CtlContext,
    kind: PbThrottleTarget,
    params: ThrottleCommandArgs,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let throttle_type = parse_throttle_type(&params.throttle_type)?;

    meta_client
        .apply_throttle(kind, throttle_type, params.id, params.rate)
        .await?;
    Ok(())
}
