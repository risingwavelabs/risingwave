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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// Provides fragment level backfill progress.
#[system_catalog(
view,
"rw_catalog.rw_fragment_backfill_progress",
"select 
  progress.job_id,
  progress.fragment_id,
  concat(
    progress.current_row_count::numeric / stats.total_key_count::numeric * 100,
    '%',
    ' ',
    '(',
    progress.current_row_count,
    '/',
    stats.total_key_count,
    ')'
  ) as progress
FROM internal_backfill_progress() progress
JOIN rw_table_scan scan_info ON progress.job_id = scan_info.job_id AND progress.fragment_id = scan_info.fragment_id
JOIN rw_table_stats stats ON scan_info.backfill_target_table_id = stats.id
"
)]
#[derive(Fields)]
struct RwFragmentBackfillProgress {
    job_id: i32,
    #[primary_key]
    fragment_id: i32,
    progress: String,
}

