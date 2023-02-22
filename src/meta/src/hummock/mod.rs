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
pub mod compaction;
mod compaction_schedule_policy;
mod compaction_scheduler;
pub mod compactor_manager;
pub mod error;
mod manager;
pub use manager::*;

mod level_handler;
mod metrics_utils;
#[cfg(any(test, feature = "test"))]
pub mod mock_hummock_meta_client;
pub mod model;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
mod utils;
mod vacuum;

use std::time::Duration;

pub use compaction_scheduler::CompactionScheduler;
pub use compactor_manager::*;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use sync_point::sync_point;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
pub use vacuum::*;

pub use crate::hummock::compaction_scheduler::{
    CompactionRequestChannelRef, CompactionSchedulerRef,
};
use crate::storage::MetaStore;
use crate::util::GlobalEventManager;
use crate::MetaOpts;

/// Start hummock's asynchronous tasks.
pub fn start_hummock_workers<S>(
    vacuum_manager: VacuumManagerRef<S>,
    compaction_scheduler: CompactionSchedulerRef<S>,
    timer_manager: &mut GlobalEventManager,
    meta_opts: &MetaOpts,
) where
    S: MetaStore,
{
    compaction_scheduler.start(timer_manager);
    if !meta_opts.compaction_deterministic_test {
        timer_manager.register_interval_task(meta_opts.vacuum_interval_sec, move || {
            let vacuum = vacuum_manager.clone();
            async move {
                // May metadata vacuum and SST vacuum split into two tasks.
                if let Err(err) = vacuum.vacuum_metadata().await {
                    tracing::warn!("Vacuum metadata error {:#?}", err);
                }
                if let Err(err) = vacuum.vacuum_sst_data().await {
                    tracing::warn!("Vacuum SST error {:#?}", err);
                }
                sync_point!("AFTER_SCHEDULE_VACUUM");
            }
        });
    }
}

/// Starts a task to periodically vacuum hummock.
pub fn start_vacuum_scheduler<S>(
    vacuum: VacuumManagerRef<S>,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>)
where
    S: MetaStore,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = &mut shutdown_rx => {
                    tracing::info!("Vacuum is stopped");
                    return;
                }
            }
            // May metadata vacuum and SST vacuum split into two tasks.
            if let Err(err) = vacuum.vacuum_metadata().await {
                tracing::warn!("Vacuum metadata error {:#?}", err);
            }
            if let Err(err) = vacuum.vacuum_sst_data().await {
                tracing::warn!("Vacuum SST error {:#?}", err);
            }
            sync_point!("AFTER_SCHEDULE_VACUUM");
        }
    });
    (join_handle, shutdown_tx)
}
