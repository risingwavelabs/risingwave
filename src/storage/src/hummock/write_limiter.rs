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

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::write_limits::WriteLimit;

pub type WriteLimiterRef = Arc<WriteLimiter>;

#[derive(Default)]
pub struct WriteLimiter {
    limits: ArcSwap<HashMap<CompactionGroupId, WriteLimit>>,
    notify: tokio::sync::Notify,
}

impl WriteLimiter {
    pub fn unused() -> Arc<Self> {
        Arc::new(WriteLimiter::default())
    }

    pub fn update_write_limits(&self, limits: HashMap<CompactionGroupId, WriteLimit>) {
        self.limits.store(Arc::new(limits));
        self.notify.notify_waiters();
    }

    /// Returns the reason if write for `table_id` is blocked.
    fn try_find(&self, table_id: &TableId) -> Option<String> {
        for group in self.limits.load().values() {
            if group.table_ids.contains(&table_id.table_id) {
                return Some(group.reason.clone());
            }
        }
        None
    }

    /// Waits until write is permitted for `table_id`.
    pub async fn wait_permission(&self, table_id: TableId) {
        // Fast path.
        match self.try_find(&table_id) {
            Some(reason) => {
                tracing::warn!(
                    "write to table {} is blocked due to {}",
                    table_id.table_id,
                    reason,
                );
            }
            None => {
                return;
            }
        }
        // Slow path.
        loop {
            let notified = self.notify.notified();
            if self.try_find(&table_id).is_none() {
                break;
            }
            notified.await;
        }
        tracing::info!("write to table {} is unblocked", table_id.table_id,);
    }
}
