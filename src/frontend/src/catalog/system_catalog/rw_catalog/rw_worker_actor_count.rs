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

#[system_catalog(
    view,
    "rw_catalog.rw_worker_actor_count",
    "SELECT t2.id as worker_id, parallelism, count(*) as actor_count
     FROM rw_actors t1, rw_worker_nodes t2
     where t1.worker_id = t2.id
     GROUP  BY t2.id, t2.parallelism;"
)]
#[derive(Fields)]
struct RwWorkerActorCount {
    worker_id: i32,
    parallelism: i32,
    actor_count: i64,
}
