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
    limits: ArcSwap<(
        HashMap<CompactionGroupId, WriteLimit>,
        HashMap<TableId, CompactionGroupId>,
    )>,
    notify: tokio::sync::Notify,
}

impl WriteLimiter {
    pub fn unused() -> Arc<Self> {
        Arc::new(WriteLimiter::default())
    }

    pub fn update_write_limits(&self, limits: HashMap<CompactionGroupId, WriteLimit>) {
        let mut index: HashMap<TableId, CompactionGroupId> = HashMap::new();
        for (group_id, limit) in &limits {
            for table_id in &limit.table_ids {
                index.insert(table_id.into(), *group_id);
            }
        }
        self.limits.store(Arc::new((limits, index)));
        self.notify.notify_waiters();
    }

    /// Returns the reason if write for `table_id` is blocked.
    fn try_find(&self, table_id: &TableId) -> Option<String> {
        let limits = self.limits.load();
        let group_id = match limits.1.get(table_id) {
            None => {
                return None;
            }
            Some(group_id) => *group_id,
        };
        let reason = limits
            .0
            .get(&group_id)
            .as_ref()
            .expect("table to group index should be accurate")
            .reason
            .clone();
        Some(reason)
    }

    /// Waits until write is permitted for `table_id`.
    pub async fn wait_permission(&self, table_id: TableId) {
        // Fast path.
        if self.try_find(&table_id).is_none() {
            return;
        }
        let mut first_block_msg = true;
        // Slow path.
        loop {
            let notified = self.notify.notified();
            match self.try_find(&table_id) {
                Some(reason) => {
                    if first_block_msg {
                        first_block_msg = false;
                        tracing::warn!(
                            "write to table {} is blocked: {}",
                            table_id.table_id,
                            reason,
                        );
                    } else {
                        tracing::warn!(
                            "write limiter is updated, but write to table {} is still blocked: {}",
                            table_id.table_id,
                            reason,
                        );
                    }
                }
                None => {
                    break;
                }
            }
            notified.await;
        }
        tracing::info!("write to table {} is unblocked", table_id.table_id,);
    }
}
