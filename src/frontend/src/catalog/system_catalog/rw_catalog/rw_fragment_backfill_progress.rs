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
"with 
table_backfill_progress as (select
  progress.job_id,
  progress.fragment_id,
  scan_info.backfill_target_relation_id,
  case when scan_info.backfill_type = 'SNAPSHOT_BACKFILL' AND progress.min_epoch > scan_info.backfill_epoch
  then concat('100% (', stats.total_key_count, '/', stats.total_key_count, ')')
  else
    concat(
      coalesce(progress.current_row_count::numeric / stats.total_key_count::numeric * 100, 0),
      '%',
      ' ',
      '(',
      coalesce(progress.current_row_count, 0),
      '/',
      stats.total_key_count,
      ')'
    )
  end as progress
FROM internal_backfill_progress() progress
JOIN rw_backfill_info scan_info ON progress.job_id = scan_info.job_id AND progress.fragment_id = scan_info.fragment_id
JOIN rw_table_stats stats ON scan_info.backfill_target_relation_id = stats.id
),
source_backfill_progress as (select
  source_backfill_progress.job_id,
  source_backfill_progress.fragment_id,
  scan_info.backfill_target_relation_id,
  source_backfill_progress.backfill_progress::varchar as progress
FROM internal_source_backfill_progress() source_backfill_progress
JOIN rw_backfill_info scan_info ON source_backfill_progress.job_id = scan_info.job_id AND source_backfill_progress.fragment_id = scan_info.fragment_id
),
backfill_progress as (
  select * from table_backfill_progress
  UNION ALL
  select * from source_backfill_progress
)
select
  backfill_progress.job_id,
  backfill_progress.fragment_id,
  concat(job_schemas.name, '.', job_tables.name) as job_name,
  concat(upstream_schemas.name, '.', upstream_tables.name) as upstream_table_name,
  backfill_progress.progress
FROM
  backfill_progress
  join rw_relations job_tables on backfill_progress.job_id = job_tables.id
  join rw_schemas job_schemas on job_tables.schema_id = job_schemas.id
  join rw_relations upstream_tables on backfill_progress.backfill_target_relation_id = upstream_tables.id
  join rw_schemas upstream_schemas on upstream_tables.schema_id = upstream_schemas.id
"
)]
#[derive(Fields)]
struct RwFragmentBackfillProgress {
    job_id: i32,
    #[primary_key]
    fragment_id: i32,
    job_name: String,
    upstream_table_name: String,
    progress: String,
}
