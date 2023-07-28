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
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// `rw_worker_nodes` contains all information about the compute nodes in the cluster.
/// TODO: Add other type of nodes if necessary in the future.
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
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_rw_worker_nodes_info(&self) -> Result<Vec<OwnedRow>> {
        let workers = self.worker_node_manager.list_worker_nodes();

        Ok(workers
            .into_iter()
            .map(|worker| {
                let host = worker.host.as_ref().unwrap();
                let property = worker.property.as_ref().unwrap();
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(worker.id as i32)),
                    Some(ScalarImpl::Utf8(host.host.clone().into())),
                    Some(ScalarImpl::Utf8(host.port.to_string().into())),
                    Some(ScalarImpl::Utf8(
                        worker.get_type().unwrap().as_str_name().into(),
                    )),
                    Some(ScalarImpl::Utf8(
                        worker.get_state().unwrap().as_str_name().into(),
                    )),
                    Some(ScalarImpl::Int32(worker.parallel_units.len() as i32)),
                    Some(ScalarImpl::Bool(property.is_streaming)),
                    Some(ScalarImpl::Bool(property.is_serving)),
                    Some(ScalarImpl::Bool(property.is_unschedulable)),
                ])
            })
            .collect_vec())
    }
}
