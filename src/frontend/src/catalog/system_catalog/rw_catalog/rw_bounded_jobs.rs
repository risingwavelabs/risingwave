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

use anyhow::anyhow;
use risingwave_common::id::{JobId, SourceId};
use risingwave_common::types::{Fields, JsonbVal, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::meta::list_bounded_jobs_response::bounded_job::PinnedInput;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(job_id)]
struct RwBoundedJob {
    job_id: JobId,
    source_id: SourceId,
    /// `LOADING` or `COMPLETED`
    status: String,
    /// The input fixed at creation, keyed by its kind, e.g. `{"iceberg_snapshot": {"snapshot_id": 1}}`
    pinned_input: JsonbVal,
    completed_at: Option<Timestamptz>,
}

#[system_catalog(table, "rw_catalog.rw_bounded_jobs")]
async fn read_rw_bounded_jobs(reader: &SysCatalogReaderImpl) -> Result<Vec<RwBoundedJob>> {
    let jobs = reader.meta_client.list_bounded_jobs().await?;
    jobs.into_iter()
        .map(|job| {
            let pinned_input = match job.pinned_input {
                Some(PinnedInput::IcebergSnapshot(snapshot)) => json!({
                    "iceberg_snapshot": { "snapshot_id": snapshot.snapshot_id }
                })
                .into(),
                None => {
                    return Err(anyhow!("bounded job {} has no pinned input", job.job_id).into());
                }
            };

            Ok(RwBoundedJob {
                job_id: job.job_id,
                source_id: job.source_id,
                status: job.status,
                pinned_input,
                completed_at: job.completed_at.and_then(Timestamptz::from_millis),
            })
        })
        .collect()
}
