// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_pb::stream_service::PbBarrierCompleteResponse;

use crate::MetaResult;
use crate::barrier::info::BarrierInfo;
use crate::controller::SqlMetaStore;

pub type CdcTableBackfillTrackerRef = Arc<CdcTableBackfillTracker>;

pub struct CdcTableBackfillTracker {
    meta_store: SqlMetaStore,
}

impl CdcTableBackfillTracker {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self { meta_store }
    }

    pub(super) fn apply_collected_command(
        &self,
        barrier_info: &BarrierInfo,
        resps: impl IntoIterator<Item = &PbBarrierCompleteResponse>,
    ) -> Vec<TableId> {
        // TODO(zw): !!!
        vec![]
    }

    pub(super) async fn finish_backfill(&self, job_id: TableId) -> MetaResult<()> {
        // TODO(zw): !!!
        Ok(())
    }
}
