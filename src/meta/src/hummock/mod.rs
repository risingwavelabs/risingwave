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

pub use compactor_manager::*;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
pub use vacuum::*;

use crate::MetaOpts;

/// Start hummock's asynchronous tasks.
pub fn start_hummock_workers(
    hummock_manager: HummockManagerRef,
    vacuum_manager: VacuumManagerRef,
    meta_opts: &MetaOpts,
) -> Vec<(JoinHandle<()>, Sender<()>)> {
    // These critical tasks are put in their own timer loop deliberately, to avoid long-running ones
    // from blocking others.
    let workers = vec![
        start_checkpoint_loop(
            hummock_manager,
            Duration::from_secs(meta_opts.hummock_version_checkpoint_interval_sec),
            meta_opts.min_delta_log_num_for_hummock_version_checkpoint,
        ),
        start_vacuum_metadata_loop(
            vacuum_manager.clone(),
            Duration::from_secs(meta_opts.vacuum_interval_sec),
        ),
        start_vacuum_object_loop(
            vacuum_manager,
            Duration::from_secs(meta_opts.vacuum_interval_sec),
        ),
    ];
    workers
}

/// Starts a task to periodically vacuum stale metadata.
pub fn start_vacuum_metadata_loop(
    vacuum: VacuumManagerRef,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>) {
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
                    tracing::info!("Vacuum metadata loop is stopped");
                    return;
                }
            }
            if let Err(err) = vacuum.vacuum_metadata().await {
                tracing::warn!("Vacuum metadata error {:#?}", err);
            }
        }
    });
    (join_handle, shutdown_tx)
}

/// Starts a task to periodically vacuum stale objects.
pub fn start_vacuum_object_loop(
    vacuum: VacuumManagerRef,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>) {
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
                    tracing::info!("Vacuum object loop is stopped");
                    return;
                }
            }
            if let Err(err) = vacuum.vacuum_object().await {
                tracing::warn!("Vacuum object error {:#?}", err);
            }
        }
    });
    (join_handle, shutdown_tx)
}

pub fn start_checkpoint_loop(
    hummock_manager: HummockManagerRef,
    interval: Duration,
    min_delta_log_num: u64,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown checkpoint
                _ = &mut shutdown_rx => {
                    tracing::info!("Hummock version checkpoint is stopped");
                    return;
                }
            }
            if hummock_manager.is_version_checkpoint_paused()
                || hummock_manager.env.opts.compaction_deterministic_test
            {
                continue;
            }
            if let Err(err) = hummock_manager
                .create_version_checkpoint(min_delta_log_num)
                .await
            {
                tracing::warn!("Hummock version checkpoint error {:#?}", err);
            }
        }
    });
    (join_handle, shutdown_tx)
}
