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

mod cdc_backfill;
mod state;
mod upstream_table;

pub use cdc_backfill::CdcBackfillExecutor;
use risingwave_pb::stream_plan::StreamCdcScanOptions;
pub use upstream_table::external::ExternalStorageTable;

#[derive(Debug, Clone)]
pub struct CdcScanOptions {
    /// Whether to disable backfill
    pub disable_backfill: bool,
    /// Barreir interval to start a new snapshot read
    pub snapshot_interval: u32,
    /// Batch size for a snapshot read query
    pub snapshot_batch_size: u32,
}

impl Default for CdcScanOptions {
    fn default() -> Self {
        Self {
            disable_backfill: false,
            snapshot_interval: 1,
            snapshot_batch_size: 1000,
        }
    }
}

impl CdcScanOptions {
    pub fn from_proto(proto: StreamCdcScanOptions) -> Self {
        Self {
            disable_backfill: proto.disable_backfill,
            snapshot_interval: proto.snapshot_barrier_interval,
            snapshot_batch_size: proto.snapshot_batch_size,
        }
    }
}
