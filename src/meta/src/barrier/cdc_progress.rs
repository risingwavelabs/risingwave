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
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;

use risingwave_common::catalog::TableId;
use risingwave_pb::stream_service::PbBarrierCompleteResponse;

use crate::controller::SqlMetaStore;
use crate::MetaResult;

pub type CdcTableBackfillTrackerRef = Arc<CdcTableBackfillTracker>;

pub struct CdcTableBackfillTracker {
    meta_store: SqlMetaStore,
    inner: Mutex<CdcTableBackfillTrackerInner>,
    next_generation: AtomicU64,
}

impl CdcTableBackfillTracker {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self {
            meta_store,
            inner: Mutex::new(CdcTableBackfillTrackerInner::new()),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn apply_collected_command(
        &self,
        resps: impl IntoIterator<Item = &PbBarrierCompleteResponse>,
    ) -> Vec<TableId> {
        // TODO(zw): !!!
        vec![]
    }

    pub async fn finish_backfill(&self, job_id: TableId) -> MetaResult<()> {
        // TODO(zw): !!!
        Ok(())
    }

    pub fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }
}

struct CdcTableBackfillTrackerInner {}

impl CdcTableBackfillTrackerInner {
    fn new() -> Self {
        Self {}
    }
}
