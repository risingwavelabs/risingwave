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
    match s.to_lowercase().as_str() {
        "dml" => Ok(PbThrottleType::Dml),
        "backfill" => Ok(PbThrottleType::Backfill),
        "source" => Ok(PbThrottleType::Source),
        "sink" => Ok(PbThrottleType::Sink),
        _ => bail!(
            "Invalid throttle type: {}. Valid options are: dml, backfill, source, sink",
            s
        ),
    }
}

pub async fn apply_throttle(
    context: &CtlContext,
    kind: PbThrottleTarget,
    params: ThrottleCommandArgs,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    // Use provided throttle type if specified, otherwise infer from target for backward compatibility
    let throttle_type = if let Some(ref type_str) = params.throttle_type {
        parse_throttle_type(type_str)?
    } else {
        // Infer throttle type from target for backward compatibility
        match kind {
            PbThrottleTarget::Source => PbThrottleType::Source,
            PbThrottleTarget::Mv => PbThrottleType::Backfill,
            PbThrottleTarget::Sink => PbThrottleType::Sink,
            PbThrottleTarget::Table
            | PbThrottleTarget::Fragment
            | PbThrottleTarget::Unspecified => {
                // Default to Backfill for unspecified/unsupported combinations; user should use SQL for table throttling
                PbThrottleType::Backfill
            }
        }
    };

    meta_client
        .apply_throttle(kind, throttle_type, params.id, params.rate)
        .await?;
    Ok(())
}
