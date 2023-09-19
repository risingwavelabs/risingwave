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
use risingwave_common::types::{DataType, ScalarImpl, Timestamp};
use risingwave_common::util::epoch::Epoch;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_META_SNAPSHOT: BuiltinTable = BuiltinTable {
    name: "rw_meta_snapshot",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "meta_snapshot_id"),
        (DataType::Int64, "hummock_version_id"),
        // the smallest epoch this meta snapshot includes
        (DataType::Int64, "safe_epoch"),
        // human-readable timestamp of safe_epoch
        (DataType::Timestamp, "safe_epoch_ts"),
        // the largest epoch this meta snapshot includes
        (DataType::Int64, "max_committed_epoch"),
        // human-readable timestamp of max_committed_epoch
        (DataType::Timestamp, "max_committed_epoch_ts"),
    ],
    pk: &[],
};

impl SysCatalogReaderImpl {
    pub async fn read_meta_snapshot(&self) -> Result<Vec<OwnedRow>> {
        let try_get_date_time = |epoch: u64| {
            if epoch == 0 {
                return None;
            }
            let time_millis = Epoch::from(epoch).as_unix_millis();
            Timestamp::with_secs_nsecs(
                (time_millis / 1000) as i64,
                (time_millis % 1000 * 1_000_000) as u32,
            )
            .map(ScalarImpl::Timestamp)
            .ok()
        };
        let meta_snapshots = self
            .meta_client
            .list_meta_snapshots()
            .await?
            .into_iter()
            .map(|s| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(s.id as i64)),
                    Some(ScalarImpl::Int64(s.hummock_version_id as i64)),
                    Some(ScalarImpl::Int64(s.safe_epoch as i64)),
                    try_get_date_time(s.safe_epoch),
                    Some(ScalarImpl::Int64(s.max_committed_epoch as i64)),
                    try_get_date_time(s.max_committed_epoch),
                ])
            })
            .collect_vec();
        Ok(meta_snapshots)
    }
}
