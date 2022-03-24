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

mod compaction;
mod compactor_manager;
mod hummock_manager;
#[cfg(test)]
mod hummock_manager_tests;
#[cfg(test)]
mod integration_tests;
mod level_handler;
#[cfg(test)]
mod mock_hummock_meta_client;
mod model;
#[cfg(test)]
pub mod test_utils;
mod vacuum;

use std::sync::Arc;
use std::time::Duration;

pub use compactor_manager::*;
pub use hummock_manager::*;
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
pub use vacuum::*;

use crate::storage::MetaStore;

pub fn start_hummock_workers<S>(
    hummock_manager_ref: Arc<HummockManager<S>>,
    compactor_manager_ref: Arc<CompactorManager>,
    vacuum_trigger_ref: Arc<VacuumTrigger<S>>,
) -> Vec<(JoinHandle<()>, UnboundedSender<()>)>
where
    S: MetaStore,
{
    vec![
        start_compaction_trigger(hummock_manager_ref, compactor_manager_ref),
        VacuumTrigger::start_vacuum_trigger(vacuum_trigger_ref),
    ]
}

const COMPACT_TRIGGER_INTERVAL: Duration = Duration::from_secs(10);
/// Starts a worker to conditionally trigger compaction.
/// A vacuum trigger is started here too.
pub fn start_compaction_trigger<S>(
    hummock_manager_ref: Arc<HummockManager<S>>,
    compactor_manager_ref: Arc<CompactorManager>,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        let mut min_interval = tokio::time::interval(COMPACT_TRIGGER_INTERVAL);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_interval.tick() => {},
                // Shutdown compactor
                _ = shutdown_rx.recv() => {
                    tracing::info!("compaction trigger is shutting down");
                    return;
                }
            }

            // Get a compact task and assign it to a compactor
            let compact_task = match hummock_manager_ref.get_compact_task().await {
                Ok(Some(compact_task)) => compact_task,
                Ok(None) => {
                    continue;
                }
                Err(e) => {
                    tracing::warn!("failed to get_compact_task {}", e);
                    continue;
                }
            };
            let input_ssts = compact_task
                .input_ssts
                .iter()
                .flat_map(|v| v.level.as_ref().unwrap().table_ids.clone())
                .collect_vec();
            if !compactor_manager_ref
                .try_assign_compact_task(Some(compact_task.clone()), None)
                .await
            {
                // TODO #546: Cancel a task only requires task_id. compact_task.clone() can be
                // avoided.
                if let Err(e) = hummock_manager_ref
                    .report_compact_task(compact_task, false)
                    .await
                {
                    tracing::warn!("failed to report_compact_task {}", e);
                }
                continue;
            }
            tracing::debug!("Try to compact SSTs {:?}", input_ssts);
        }
    });

    (join_handle, shutdown_tx)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::hummock::{start_compaction_trigger, CompactorManager, HummockManager};
    use crate::manager::MetaSrvEnv;
    use crate::rpc::metrics::MetaMetrics;

    #[tokio::test]
    async fn test_shutdown_compaction_trigger() {
        let env = MetaSrvEnv::for_test().await;
        let hummock_manager_ref = Arc::new(
            HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
                .await
                .unwrap(),
        );
        let compactor_manager_ref = Arc::new(CompactorManager::new());
        let (join_handle, shutdown_sender) =
            start_compaction_trigger(hummock_manager_ref, compactor_manager_ref);
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
