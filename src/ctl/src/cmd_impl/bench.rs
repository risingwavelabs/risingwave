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

use std::ops::Bound;
use std::ops::Bound::Unbounded;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use anyhow::{Result, anyhow};
use clap::Subcommand;
use futures::future::try_join_all;
use futures::{Future, StreamExt, pin_mut};
use risingwave_common::row::{self, OwnedRow};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::store::PrefetchOptions;
use size::Size;
use tokio::task::JoinHandle;

use super::table::{get_table_catalog, make_state_table};
use crate::CtlContext;
use crate::common::HummockServiceOpts;

#[derive(Subcommand)]
pub enum BenchCommands {
    /// benchmark scan state table
    Scan {
        /// name of the materialized view to operate on
        mv_name: String,
        /// number of futures doing scan
        #[clap(long, default_value_t = 1)]
        threads: usize,
        data_dir: Option<String>,
        #[clap(short, long = "use-new-object-prefix-strategy", default_value = "true")]
        use_new_object_prefix_strategy: bool,
    },
}

/// Spawn a tokio task with output of `anyhow::Result`, so that we can write dead loop in async
/// functions.
fn spawn_okk(fut: impl Future<Output = Result<()>> + Send + 'static) -> JoinHandle<Result<()>> {
    tokio::spawn(fut)
}

#[derive(Clone, Debug)]
pub struct InterestedMetrics {
    object_store_read: u64,
    object_store_write: u64,
    next_cnt: u64,
    iter_cnt: u64,
    now: Instant,
}

impl InterestedMetrics {
    pub fn report(&self, metrics: &InterestedMetrics) {
        let elapsed = self.now.duration_since(metrics.now).as_secs_f64();
        let read_rate = (self.object_store_read - metrics.object_store_read) as f64 / elapsed;
        let write_rate = (self.object_store_write - metrics.object_store_write) as f64 / elapsed;
        let next_rate = (self.next_cnt - metrics.next_cnt) as f64 / elapsed;
        let iter_rate = (self.iter_cnt - metrics.iter_cnt) as f64 / elapsed;
        println!(
            "read_rate: {}/s\nwrite_rate:{}/s\nnext_rate:{}/s\niter_rate:{}/s\n",
            Size::from_bytes(read_rate),
            Size::from_bytes(write_rate),
            next_rate,
            iter_rate
        );
    }
}

pub async fn do_bench(context: &CtlContext, cmd: BenchCommands) -> Result<()> {
    let meta = context.meta_client().await?;
    let next_cnt = Arc::new(AtomicU64::new(0));
    let iter_cnt = Arc::new(AtomicU64::new(0));
    match cmd {
        BenchCommands::Scan {
            mv_name,
            threads,
            data_dir,
            use_new_object_prefix_strategy,
        } => {
            let (hummock, metrics) = context
                .hummock_store_with_metrics(HummockServiceOpts::from_env(
                    data_dir,
                    use_new_object_prefix_strategy,
                )?)
                .await?;
            let table = get_table_catalog(meta.clone(), mv_name).await?;
            let committed_epoch = hummock
                .inner()
                .get_pinned_version()
                .table_committed_epoch(table.id)
                .ok_or_else(|| anyhow!("table id {} not exist", table.id))?;
            let mut handlers = vec![];
            for i in 0..threads {
                let table = table.clone();
                let next_cnt = next_cnt.clone();
                let iter_cnt = iter_cnt.clone();
                let hummock = hummock.clone();
                let handler = spawn_okk(async move {
                    tracing::info!(thread = i, "starting scan");
                    let state_table = {
                        let mut tb = make_state_table(hummock, &table).await;
                        tb.init_epoch(EpochPair::new(u64::MAX, committed_epoch))
                            .await?;
                        tb
                    };
                    loop {
                        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                            &(Unbounded, Unbounded);
                        let stream = state_table
                            .iter_with_prefix(
                                row::empty(),
                                sub_range,
                                PrefetchOptions::prefetch_for_large_range_scan(),
                            )
                            .await?;
                        pin_mut!(stream);
                        iter_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        while let Some(item) = stream.next().await {
                            next_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            item?;
                        }
                    }
                });
                handlers.push(handler);
            }
            let handler = spawn_okk(async move {
                tracing::info!("starting report metrics");
                let mut last_collected_metrics = None;
                loop {
                    let collected_metrics = InterestedMetrics {
                        object_store_read: metrics.object_store_metrics.read_bytes.get(),
                        object_store_write: metrics.object_store_metrics.write_bytes.get(),
                        next_cnt: next_cnt.load(std::sync::atomic::Ordering::Relaxed),
                        iter_cnt: iter_cnt.load(std::sync::atomic::Ordering::Relaxed),
                        now: Instant::now(),
                    };
                    if let Some(ref last_collected_metrics) = last_collected_metrics {
                        collected_metrics.report(last_collected_metrics);
                    }
                    last_collected_metrics = Some(collected_metrics);
                    tracing::info!("starting report metrics");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            });
            handlers.push(handler);
            for result in try_join_all(handlers).await? {
                result?;
            }
        }
    }

    Ok(())
}
