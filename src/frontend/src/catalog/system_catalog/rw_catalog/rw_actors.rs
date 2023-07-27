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
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{SysCatalogReaderImpl, SystemCatalogColumnsDef};

pub const RW_ACTORS_TABLE_NAME: &str = "rw_actors";

pub const RW_ACTORS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "actor_id"),
    (DataType::Int32, "fragment_id"),
    (DataType::Int32, "parallel_unit_id"),
    (DataType::Varchar, "status"),
];

impl SysCatalogReaderImpl {
    pub async fn read_rw_actor_states_info(&self) -> Result<Vec<OwnedRow>> {
        let states = self.meta_client.list_actor_states().await?;

        Ok(states
            .into_iter()
            .map(|state| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(state.actor_id as i32)),
                    Some(ScalarImpl::Int32(state.fragment_id as i32)),
                    Some(ScalarImpl::Int32(state.parallel_unit_id as i32)),
                    Some(ScalarImpl::Utf8(state.state().as_str_name().into())),
                ])
            })
            .collect_vec())
    }
}
