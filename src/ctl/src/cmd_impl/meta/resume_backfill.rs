// Copyright 2026 RisingWave Labs
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

use anyhow::{Result, anyhow};
use risingwave_pb::ddl_service::RisectlResumeBackfillRequest;
use risingwave_pb::ddl_service::risectl_resume_backfill_request::Target;
use risingwave_pb::id::{FragmentId, JobId};

use crate::CtlContext;

pub async fn resume_backfill(
    context: &CtlContext,
    job_id: Option<JobId>,
    fragment_id: Option<FragmentId>,
) -> Result<()> {
    let target = match (job_id, fragment_id) {
        (Some(job_id), None) => Target::JobId(job_id.as_raw_id()),
        (None, Some(fragment_id)) => Target::FragmentId(fragment_id.as_raw_id()),
        _ => {
            return Err(anyhow!(
                "exactly one of job_id or fragment_id must be provided"
            ));
        }
    };

    let meta_client = context.meta_client().await?;
    meta_client
        .risectl_resume_backfill(RisectlResumeBackfillRequest {
            target: Some(target),
        })
        .await?;
    println!("Done.");
    Ok(())
}
