// Copyright 2024 RisingWave Labs
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

/// Provides a mapping from `fragment_id` to its ddl info.
#[system_catalog(
    view,
    "rw_catalog.rw_fragment_id_to_ddl",
    "with
   all_streaming_jobs as (
     select id as job_id, schema_id, 'mv' as ddl_type, name from rw_materialized_views
       union all select id as job_id, schema_id, 'sink' as ddl_type, name from rw_sinks
         union all select id as job_id, schema_id, 'source' as ddl_type, name from rw_sources
           union all select id as job_id, schema_id, 'table' as ddl_type, name from rw_tables
             union all select id as job_id, schema_id, 'index' as ddl_type, name from rw_indexes
   )
   select f.fragment_id, job.job_id, job.schema_id, job.ddl_type, job.name
   from all_streaming_jobs job join rw_fragments f on job.job_id = f.table_id"
)]
#[derive(Fields)]
struct RwFragmentIdToDdl {
    #[primary_key]
    fragment_id: i32,
    job_id: i32,
    schema_id: i32,
    ddl_type: String,
    name: String,
}
