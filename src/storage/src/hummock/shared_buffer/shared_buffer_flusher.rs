// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use risingwave_common::error::RwError;

use crate::hummock::shared_buffer::shared_buffer_manager::SharedBufferManager;
use crate::monitor::StateStoreMetrics;

/// Background worker for flushing hummock shared buffer to persistent storage
/// based on configured threshold.
pub struct SharedBufferFlusher {
    shared_buffer_mgr: Weak<SharedBufferManager>,
    stats: Arc<StateStoreMetrics>,
}

impl SharedBufferFlusher {
    pub fn new(
        stats: Arc<StateStoreMetrics>,
        shared_buffer_mgr: Weak<SharedBufferManager>,
    ) -> Self {
        Self {
            stats,
            shared_buffer_mgr,
        }
    }

    pub async fn run(self) {
        let mut time_interval = tokio::time::interval(Duration::from_millis(10));
        log::info!("flusher start running!");
        loop {
            if let Some(shared_buffer_mgr) = self.shared_buffer_mgr.upgrade() {
                // threshold-based sync
                let shared_buff_cur_size = self.stats.shared_buffer_cur_size.load(Ordering::SeqCst);
                if self.stats.shared_buffer_threshold_size <= shared_buff_cur_size {
                    if let Err(e) = shared_buffer_mgr.sync(None).await {
                        panic!("Failed to flush shared buffer due to {}", RwError::from(e));
                    }
                } else {
                    // wait a moment for executors to fill the shared buffer
                    time_interval.tick().await;
                }
            } else {
                break;
            }
        }
        log::info!("flusher exited!");
    }
}
