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

use risingwave_common::bail;

use crate::manager::{ConnectionId, DatabaseManager};

pub fn refcnt_inc_connection(
    database_mgr: &mut DatabaseManager,
    connection_id: Option<ConnectionId>,
) -> anyhow::Result<()> {
    if let Some(connection_id) = connection_id {
        if let Some(_conn) = database_mgr.get_connection(connection_id) {
            // TODO(weili): wait for yezizp to refactor ref cnt
            database_mgr.increase_ref_count(connection_id);
        } else {
            bail!("connection {} not found.", connection_id);
        }
    }
    Ok(())
}

pub fn refcnt_dec_connection(
    database_mgr: &mut DatabaseManager,
    connection_id: Option<ConnectionId>,
) {
    if let Some(connection_id) = connection_id {
        // TODO: wait for yezizp to refactor ref cnt
        database_mgr.decrease_ref_count(connection_id);
    }
}
