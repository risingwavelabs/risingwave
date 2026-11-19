// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use clap::{Args, Subcommand};
use futures::{StreamExt, stream};
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_rpc_client::MonitorClientPool;
use thiserror_ext::AsReport;

use crate::common::{CtlContext, WorkerTypeArg};

const WORKER_LOG_FILTER_TIMEOUT: Duration = Duration::from_secs(30);

/// Explicitly select workers. IDs and types cannot be mixed.
#[derive(Args, Clone, Debug)]
#[group(required = true, multiple = false)]
pub struct LogWorkerSelector {
    /// Worker IDs. Repeatable or comma-separated. Meta uses worker ID 0.
    #[arg(long = "worker-id", visible_alias = "node-id", value_delimiter = ',')]
    pub worker_ids: Vec<u32>,
    /// Select all workers of these types. Repeatable or comma-separated.
    #[arg(
        long = "worker-type",
        visible_alias = "node-type",
        value_delimiter = ','
    )]
    pub worker_types: Vec<WorkerTypeArg>,
    /// Select all frontend, compute, compactor, and meta nodes.
    #[arg(long)]
    pub all: bool,
}

impl LogWorkerSelector {
    pub fn select(&self, workers: Vec<WorkerNode>) -> anyhow::Result<Vec<WorkerNode>> {
        ensure!(
            usize::from(!self.worker_ids.is_empty())
                + usize::from(!self.worker_types.is_empty())
                + usize::from(self.all)
                == 1,
            "select worker IDs, worker types, or --all"
        );
        let ids: BTreeSet<_> = self.worker_ids.iter().copied().collect();
        let found: BTreeSet<_> = workers.iter().map(|worker| worker.id.as_raw_id()).collect();
        let missing: Vec<_> = ids.difference(&found).collect();
        ensure!(missing.is_empty(), "unknown worker IDs: {missing:?}");

        let types: Vec<_> = self
            .worker_types
            .iter()
            .copied()
            .map(WorkerType::from)
            .collect();
        let mut selected: Vec<_> = workers
            .into_iter()
            .filter(|worker| {
                matches!(
                    worker.r#type(),
                    WorkerType::Frontend
                        | WorkerType::ComputeNode
                        | WorkerType::Compactor
                        | WorkerType::Meta
                ) && (self.all
                    || ids.contains(&worker.id.as_raw_id())
                    || types.contains(&worker.r#type()))
            })
            .collect();
        ensure!(!selected.is_empty(), "no workers match the selector");
        selected.sort_by_key(|worker| worker.id);
        Ok(selected)
    }
}

#[derive(Subcommand, Clone, Debug)]
pub enum LogCommands {
    /// Show startup and effective stdout/stderr filters.
    Get {
        #[command(flatten)]
        workers: LogWorkerSelector,
    },
    /// Replace startup `RUST_LOG` and runtime directives using target/level syntax.
    ///
    /// Changes apply to the entire process and are lost on restart.
    /// Query-log files and tracing exporters retain their own filters.
    Set {
        /// For example: `info,risingwave_storage=debug`
        filter: String,
        #[command(flatten)]
        workers: LogWorkerSelector,
    },
    /// Restore startup settings, including the original `RUST_LOG` overrides.
    Reset {
        #[command(flatten)]
        workers: LogWorkerSelector,
    },
}

pub async fn run(context: &CtlContext, command: LogCommands) -> anyhow::Result<()> {
    let selector = match &command {
        LogCommands::Get { workers }
        | LogCommands::Set { workers, .. }
        | LogCommands::Reset { workers } => workers,
    };
    let workers = context.meta_client().await?.list_worker_nodes(None).await?;
    // Resolve every requested ID before modifying any worker.
    let workers = selector.select(workers)?;
    let clients = MonitorClientPool::adhoc();
    let mut results = stream::iter(workers.into_iter().map(|worker| {
        let clients = &clients;
        let command = &command;
        async move {
            let result = tokio::time::timeout(WORKER_LOG_FILTER_TIMEOUT, async {
                let client = clients.get(&worker).await?;
                let state = match command {
                    LogCommands::Get { .. } => client.get_log_filter().await?,
                    LogCommands::Set { filter, .. } => {
                        client.set_log_filter(Some(filter.clone())).await?
                    }
                    LogCommands::Reset { .. } => client.set_log_filter(None).await?,
                };
                Ok::<_, anyhow::Error>(state)
            })
            .await
            .context("timed out waiting for worker log filter response")
            .and_then(|result| result)
            .with_context(|| format!("worker {} ({:?})", worker.id, worker.r#type()));
            (worker, result)
        }
    }))
    .buffer_unordered(16)
    .collect::<Vec<_>>()
    .await;
    results.sort_by_key(|(worker, _)| worker.id);

    let mut failures = 0;
    for (worker, result) in results {
        match result {
            Ok(state) => {
                println!("worker {} ({:?})", worker.id, worker.r#type());
                println!("  startup: {}", state.startup_filter);
                println!("  current: {}", state.current_filter);
            }
            Err(error) => {
                failures += 1;
                eprintln!("{}", error.as_report());
            }
        }
    }
    if failures > 0 {
        bail!("log filter command failed on {failures} worker(s); see per-worker results");
    }
    Ok(())
}
