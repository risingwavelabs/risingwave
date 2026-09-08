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

use risingwave_common::id::JobId;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::meta::list_cdc_progress_response::PbCdcProgress;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwCdcProgress {
    #[primary_key]
    job_id: JobId,
    split_total_count: i64,
    split_backfilled_count: i64,
    split_completed_count: i64,
    backfilled_row_count: Option<i64>,
    estimated_row_count: Option<i64>,
}

#[system_catalog(table, "rw_catalog.rw_cdc_progress")]
async fn read_rw_cdc_progress(reader: &SysCatalogReaderImpl) -> Result<Vec<RwCdcProgress>> {
    let progress = reader.meta_client.list_cdc_progress().await?;

    Ok(progress
        .into_iter()
        .map(|(job_id, p)| RwCdcProgress::from_progress(job_id, p))
        .collect())
}

impl RwCdcProgress {
    fn from_progress(job_id: JobId, p: PbCdcProgress) -> Self {
        // Missing or stale upstream statistics must degrade to rows-only progress.
        let estimated_row_count = p.estimated_row_count.filter(|&estimate| {
            estimate > 0
                && p.backfilled_row_count
                    .is_some_and(|count| count <= estimate)
        });
        Self {
            job_id,
            split_total_count: p.split_total_count as _,
            split_backfilled_count: p.split_backfilled_count as _,
            split_completed_count: p.split_completed_count as _,
            backfilled_row_count: p.backfilled_row_count.map(|count| count as _),
            estimated_row_count: estimated_row_count.and_then(|count| i64::try_from(count).ok()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_progress_estimate_fallback() {
        for (backfilled, estimate, expected) in [
            (Some(10), None, None),
            (Some(0), Some(0), None),
            (Some(10), Some(0), None),
            (Some(10), Some(9), None),
            (Some(10), Some(10), Some(10)),
            (Some(10), Some(20), Some(20)),
            (Some(0), Some(20), Some(20)),
            (None, None, None),
            (None, Some(20), None),
            (Some(10), Some(u64::MAX), None),
        ] {
            let row = RwCdcProgress::from_progress(
                JobId::new(1),
                PbCdcProgress {
                    split_total_count: 3,
                    split_backfilled_count: 2,
                    split_completed_count: 1,
                    backfilled_row_count: backfilled,
                    estimated_row_count: estimate,
                },
            );
            assert_eq!(
                row.backfilled_row_count,
                backfilled.map(|count| count as i64)
            );
            assert_eq!(
                row.estimated_row_count, expected,
                "{backfilled:?}/{estimate:?}"
            );
            assert_eq!(row.job_id, JobId::new(1));
            assert_eq!(row.split_total_count, 3);
            assert_eq!(row.split_backfilled_count, 2);
            assert_eq!(row.split_completed_count, 1);
        }
    }
}
