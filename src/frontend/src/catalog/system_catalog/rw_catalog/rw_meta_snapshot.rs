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

use risingwave_common::types::{Fields, Timestamp};
use risingwave_common::util::epoch::Epoch;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwMetaSnapshot {
    #[primary_key]
    meta_snapshot_id: i64,
    hummock_version_id: i64,
    // the smallest epoch this meta snapshot includes
    safe_epoch: i64,
    // human-readable timestamp of safe_epoch
    safe_epoch_ts: Option<Timestamp>,
    // the largest epoch this meta snapshot includes
    max_committed_epoch: i64,
    // human-readable timestamp of max_committed_epoch
    max_committed_epoch_ts: Option<Timestamp>,
    remarks: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_meta_snapshot")]
async fn read_meta_snapshot(reader: &SysCatalogReaderImpl) -> Result<Vec<RwMetaSnapshot>> {
    let try_get_date_time = |epoch: u64| {
        if epoch == 0 {
            return None;
        }
        let time_millis = Epoch::from(epoch).as_unix_millis();
        Timestamp::with_secs_nsecs(
            (time_millis / 1000) as i64,
            (time_millis % 1000 * 1_000_000) as u32,
        )
        .ok()
    };
    let meta_snapshots = reader
        .meta_client
        .list_meta_snapshots()
        .await?
        .into_iter()
        .map(|s| RwMetaSnapshot {
            meta_snapshot_id: s.id as _,
            hummock_version_id: s.hummock_version_id as _,
            safe_epoch: s.safe_epoch as _,
            safe_epoch_ts: try_get_date_time(s.safe_epoch),
            max_committed_epoch: s.max_committed_epoch as _,
            max_committed_epoch_ts: try_get_date_time(s.max_committed_epoch),
            remarks: s.remarks,
        })
        .collect();
    Ok(meta_snapshots)
}
