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

/// Provides a mapping from `actor_id` to its ddl info.
#[system_catalog(
view,
"rw_catalog.rw_actor_id_to_ddl",
"with
   actor_to_job_id as (select actor_id, a.fragment_id, table_id from rw_fragments f join rw_actors a on f.fragment_id = a.fragment_id),
   job_id_to_mv as (select actor_id, fragment_id, d.id as job_id, schema_id, 'mv' as ddl_type, name from rw_materialized_views d join actor_to_job_id a on d.id = a.table_id),
   job_id_to_sink as (select actor_id, fragment_id, d.id as job_id, schema_id, 'sink' as ddl_type, name from rw_sinks d join actor_to_job_id a on d.id = a.table_id),
   job_id_to_source as (select actor_id, fragment_id, d.id as job_id, schema_id, 'source' as ddl_type, name from rw_sources d join actor_to_job_id a on d.id = a.table_id),
   job_id_to_table as (select actor_id, fragment_id, d.id as job_id, schema_id, 'table' as ddl_type, name from rw_tables d join actor_to_job_id a on d.id = a.table_id)
   select * from job_id_to_mv
     union all select * from job_id_to_sink
       union all select * from job_id_to_source
         union all select * from job_id_to_table"
)]
#[derive(Fields)]
struct RwActorIdToDdl {
    #[primary_key]
    actor_id: i32,
    fragment_id: i32,
    job_id: i32,
    schema_id: i32,
    ddl_type: String,
    name: String,
}
