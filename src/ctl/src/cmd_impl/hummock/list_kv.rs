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

use core::ops::Bound::Unbounded;

use risingwave_common::catalog::TableId;
use risingwave_storage::store::{ReadOptions, StateStoreReadExt};

use crate::CtlContext;

pub async fn list_kv(context: &CtlContext, epoch: u64, table_id: u32) -> anyhow::Result<()> {
    let hummock = context.hummock_store().await?;
    if epoch == u64::MAX {
        tracing::info!("using u64::MAX as epoch");
    }
    let scan_result = {
        let range = (Unbounded, Unbounded);
        hummock
            .scan(
                range,
                epoch,
                None,
                ReadOptions {
                    ignore_range_tombstone: false,
                    prefix_hint: None,
                    table_id: TableId { table_id },
                    retention_seconds: None,
                    read_version_from_backup: false,
                    exhaust_iter: true,
                },
            )
            .await?
    };
    for (k, v) in scan_result {
        let print_string = format!("[t{}]", k.user_key.table_id.table_id());
        println!("{} {:?} => {:?}", print_string, k, v)
    }
    Ok(())
}
