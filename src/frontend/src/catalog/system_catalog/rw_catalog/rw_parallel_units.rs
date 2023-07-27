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

pub const RW_PARALLEL_UNITS: BuiltinTable = BuiltinTable {
    name: "rw_parallel_units",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[(DataType::Int32, "id"), (DataType::Int32, "worker_id")],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_rw_parallel_units_info(&self) -> Result<Vec<OwnedRow>> {
        let workers = self.worker_node_manager.list_worker_nodes();

        Ok(workers
            .into_iter()
            .flat_map(|worker| {
                worker.parallel_units.into_iter().map(|unit| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(unit.id as i32)),
                        Some(ScalarImpl::Int32(unit.worker_node_id as i32)),
                    ])
                })
            })
            .collect_vec())
    }
}
