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

use itertools::Itertools;
use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

/// `rw_worker_nodes` contains all information about the compute nodes in the cluster.
#[derive(Fields)]
struct RwWorkerNode {
    #[primary_key]
    id: i32,
    host: Option<String>,
    port: Option<String>,
    r#type: String,
    state: String,
    parallelism: i32,
    is_streaming: Option<bool>,
    is_serving: Option<bool>,
    is_unschedulable: Option<bool>,
    rw_version: Option<String>,
    system_total_memory_bytes: Option<i64>,
    system_total_cpu_cores: Option<i64>,
    started_at: Option<Timestamptz>,
}

#[system_catalog(table, "rw_catalog.rw_worker_nodes")]
async fn read_rw_worker_nodes_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwWorkerNode>> {
    let workers = reader.meta_client.list_all_nodes().await?;

    Ok(workers
        .into_iter()
        .sorted_by_key(|w| w.id)
        .map(|worker| {
            let host = worker.host.as_ref();
            let property = worker.property.as_ref();
            let resource = worker.resource.as_ref();
            RwWorkerNode {
                id: worker.id as i32,
                host: host.map(|h| h.host.clone()),
                port: host.map(|h| h.port.to_string()),
                r#type: worker.get_type().unwrap().as_str_name().into(),
                state: worker.get_state().unwrap().as_str_name().into(),
                parallelism: worker.parallel_units.len() as i32,
                is_streaming: property.map(|p| p.is_streaming),
                is_serving: property.map(|p| p.is_serving),
                is_unschedulable: property.map(|p| p.is_unschedulable),
                rw_version: resource.map(|r| r.rw_version.to_owned()),
                system_total_memory_bytes: resource.map(|r| r.total_memory_bytes as _),
                system_total_cpu_cores: resource.map(|r| r.total_cpu_cores as _),
                started_at: worker
                    .started_at
                    .map(|ts| Timestamptz::from_secs(ts as i64).unwrap()),
            }
        })
        .collect())
}
