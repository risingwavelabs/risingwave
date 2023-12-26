// Copyright 2023 RisingWave Labs
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
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl, Timestamptz};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// `rw_worker_nodes` contains all information about the compute nodes in the cluster.
pub const RW_WORKER_NODES: BuiltinTable = BuiltinTable {
    name: "rw_worker_nodes",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "id"),
        (DataType::Varchar, "host"),
        (DataType::Varchar, "port"),
        (DataType::Varchar, "type"),
        (DataType::Varchar, "state"),
        (DataType::Int32, "parallelism"),
        (DataType::Boolean, "is_streaming"),
        (DataType::Boolean, "is_serving"),
        (DataType::Boolean, "is_unschedulable"),
        (DataType::Varchar, "rw_version"),
        (DataType::Int64, "total_memory_bytes"),
        (DataType::Int64, "total_cpu_cores"),
        (DataType::Timestamptz, "started_at"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_rw_worker_nodes_info(&self) -> Result<Vec<OwnedRow>> {
        let workers = self.meta_client.list_all_nodes().await?;

        Ok(workers
            .into_iter()
            .sorted_by_key(|w| w.id)
            .map(|worker| {
                let host = worker.host.as_ref();
                let property = worker.property.as_ref();
                let resource = worker.resource.as_ref();
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(worker.id as i32)),
                    host.map(|h| ScalarImpl::Utf8(h.host.clone().into())),
                    host.map(|h| ScalarImpl::Utf8(h.port.to_string().into())),
                    Some(ScalarImpl::Utf8(
                        worker.get_type().unwrap().as_str_name().into(),
                    )),
                    Some(ScalarImpl::Utf8(
                        worker.get_state().unwrap().as_str_name().into(),
                    )),
                    Some(ScalarImpl::Int32(worker.parallel_units.len() as i32)),
                    property.map(|p| ScalarImpl::Bool(p.is_streaming)),
                    property.map(|p| ScalarImpl::Bool(p.is_serving)),
                    property.map(|p| ScalarImpl::Bool(p.is_unschedulable)),
                    resource.map(|r| ScalarImpl::Utf8(r.rw_version.to_owned().into())),
                    resource.map(|r| ScalarImpl::Int64(r.total_memory_bytes as _)),
                    resource.map(|r| ScalarImpl::Int64(r.total_cpu_cores as _)),
                    worker.started_at.map(|ts| {
                        ScalarImpl::Timestamptz(Timestamptz::from_secs(ts as i64).unwrap())
                    }),
                ])
            })
            .collect_vec())
    }
}
