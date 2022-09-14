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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut};
use clap::Parser;
use futures::future::join_all;
use itertools::Itertools;
use prometheus::Registry;
use rand::{Rng, SeedableRng};
use risingwave_storage::hummock::file_cache::cache::{FileCache, FileCacheOptions};
use risingwave_storage::hummock::file_cache::metrics::FileCacheMetrics;
use risingwave_storage::hummock::{TieredCacheKey, TieredCacheValue};
use tokio::sync::oneshot;

use crate::analyze::{analyze, monitor, Hook, Metrics};
use crate::rate::RateLimiter;
use crate::utils::{dev_stat_path, iostat};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    path: String,
    /// (MiB)
    #[clap(long, default_value = "1024")]
    capacity: usize,
    /// (MiB)
    #[clap(long, default_value = "128")]
    total_buffer_capacity: usize,
    /// (MiB)
    #[clap(long, default_value = "512")]
    cache_file_fallocate_unit: usize,
    /// (MiB)
    #[clap(long, default_value = "16")]
    cache_meta_fallocate_unit: usize,

    /// (KiB)
    #[clap(long, default_value = "1024")]
    bs: usize,
    #[clap(long, default_value = "8")]
    concurrency: usize,
    /// (s)
    #[clap(long, default_value = "600")]
    time: u64,
    #[clap(long, default_value = "1")]
    read: usize,
    #[clap(long, default_value = "1")]
    write: usize,
    #[clap(long, default_value = "10000")]
    look_up_range: u32,
    /// (MiB/s), 0 for inf.
    #[clap(long, default_value = "0")]
    r_rate: f64,
    /// (MiB/s), 0 for inf.
    #[clap(long, default_value = "0")]
    w_rate: f64,
    /// (s)
    #[clap(long, default_value = "1")]
    report_interval: u64,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct Index {
    sst: u32,
    idx: u32,
}

impl TieredCacheKey for Index {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.sst);
        buf.put_u32(self.idx);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let sst = buf.get_u32();
        let idx = buf.get_u32();
        Self { sst, idx }
    }
}

pub async fn run() {
    let args = Args::parse();

    let metrics = Metrics::default();
    let hook = Arc::new(Hook::new(metrics.clone()));

    let options = FileCacheOptions {
        dir: args.path.clone(),
        capacity: args.capacity * 1024 * 1024,
        total_buffer_capacity: args.total_buffer_capacity * 1024 * 1024,
        cache_file_fallocate_unit: args.cache_file_fallocate_unit * 1024 * 1024,
        cache_meta_fallocate_unit: args.cache_meta_fallocate_unit * 1024 * 1024,
        flush_buffer_hooks: vec![hook],
    };

    let cache: FileCache<Index, CacheValue> =
        FileCache::open(options, Arc::new(FileCacheMetrics::new(Registry::new())))
            .await
            .unwrap();

    let iostat_path = dev_stat_path(&args.path);

    let iostat_start = iostat(&iostat_path);
    let metrics_dump_start = metrics.dump();
    let time_start = Instant::now();

    let (txs, rxs): (Vec<_>, Vec<_>) = (0..args.concurrency).map(|_| oneshot::channel()).unzip();

    let futures = rxs
        .into_iter()
        .enumerate()
        .map(|(id, rx)| {
            bench(
                id,
                args.clone(),
                cache.clone(),
                args.time,
                metrics.clone(),
                rx,
            )
        })
        .collect_vec();

    let handles = futures.into_iter().map(tokio::spawn).collect_vec();

    let (tx_monitor, rx_monitor) = oneshot::channel();
    let handle_monitor = tokio::spawn({
        let iostat_path = iostat_path.clone();
        let metrics = metrics.clone();
        async move {
            monitor(
                iostat_path,
                Duration::from_secs(args.report_interval),
                metrics,
                rx_monitor,
            )
            .await;
        }
    });

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        for tx in txs {
            let _ = tx.send(());
        }
        let _ = tx_monitor.send(());
    });

    for handle in handles {
        handle.await.unwrap();
    }
    handle_monitor.abort();

    let iostat_end = iostat(&iostat_path);
    let metrics_dump_end = metrics.dump();
    let analysis = analyze(
        time_start.elapsed(),
        &iostat_start,
        &iostat_end,
        &metrics_dump_start,
        &metrics_dump_end,
    );
    println!("\nTotal:\n{}", analysis);

    // TODO: Remove this after graceful shutdown is done.
    // Waiting for the inflight flush io to pervent files from being closed.
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[derive(PartialEq, Eq, Clone, Debug)]
struct CacheValue(Vec<u8>);

impl TieredCacheValue for CacheValue {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn encoded_len(&self) -> usize {
        self.len()
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_slice(&self.0)
    }

    fn decode(buf: Vec<u8>) -> Self {
        Self(buf)
    }
}

async fn bench(
    id: usize,
    args: Args,
    cache: FileCache<Index, CacheValue>,
    time: u64,
    metrics: Metrics,
    stop: oneshot::Receiver<()>,
) {
    let bs = args.bs * 1024;
    let w_rate = if args.w_rate == 0.0 {
        None
    } else {
        Some(args.w_rate * 1024.0 * 1024.0)
    };
    let r_rate = if args.r_rate == 0.0 {
        None
    } else {
        Some(args.r_rate * 1024.0 * 1024.0)
    };

    let index = Arc::new(AtomicU32::new(0));

    let (w_stop_tx, w_stop_rx) = oneshot::channel();
    let (r_stop_tx, r_stop_rx) = oneshot::channel();

    let handle_w = tokio::spawn(write(
        id,
        bs,
        w_rate,
        index.clone(),
        cache.clone(),
        time,
        metrics.clone(),
        w_stop_rx,
    ));
    let handle_r = tokio::spawn(read(
        id,
        bs,
        r_rate,
        index,
        cache,
        time,
        metrics,
        r_stop_rx,
        args.look_up_range,
    ));
    tokio::spawn(async move {
        if let Ok(()) = stop.await {
            let _ = w_stop_tx.send(());
            let _ = r_stop_tx.send(());
        }
    });

    join_all([handle_r, handle_w]).await;
}

async fn read(
    id: usize,
    bs: usize,
    rate: Option<f64>,
    index: Arc<AtomicU32>,
    cache: FileCache<Index, CacheValue>,
    time: u64,
    metrics: Metrics,
    mut stop: oneshot::Receiver<()>,
    look_up_range: u32,
) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let start = Instant::now();

    let sst = id as u32;
    let mut limiter = match rate {
        Some(rate) => Some(RateLimiter::new(rate)),
        None => None,
    };

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let idx = index.load(Ordering::Relaxed);
        let key = Index {
            sst,
            idx: rng.gen_range(std::cmp::max(idx, look_up_range + 1) - look_up_range..=idx),
        };

        if let Some(limiter) = &mut limiter && let Some(wait) = limiter.consume(bs as f64) {
            tokio::time::sleep(wait).await;
        }

        let start = Instant::now();
        let hit = cache.get(&key).await.unwrap().is_some();
        let lat = start.elapsed().as_micros() as u64;
        if hit {
            metrics
                .get_hit_lats
                .write()
                .record(lat)
                .expect("record out of range");
        } else {
            metrics
                .get_miss_lats
                .write()
                .record(lat)
                .expect("record out of range");
            metrics.get_miss_ios.fetch_add(1, Ordering::Relaxed);
        }
        metrics.get_ios.fetch_add(1, Ordering::Relaxed);

        tokio::task::consume_budget().await;
    }
}

async fn write(
    id: usize,
    bs: usize,
    rate: Option<f64>,
    index: Arc<AtomicU32>,
    cache: FileCache<Index, CacheValue>,
    time: u64,
    metrics: Metrics,
    mut stop: oneshot::Receiver<()>,
) {
    let start = Instant::now();

    let sst = id as u32;
    let mut limiter = match rate {
        Some(rate) => Some(RateLimiter::new(rate)),
        None => None,
    };

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let idx = index.fetch_add(1, Ordering::Relaxed);
        let key = Index { sst, idx };
        let value = CacheValue(vec![b'x'; bs]);

        if let Some(limiter) = &mut limiter && let Some(wait) = limiter.consume(bs as f64) {
            tokio::time::sleep(wait).await;
        }

        let start = Instant::now();
        cache.insert(key, value).unwrap();
        metrics
            .insert_lats
            .write()
            .record(start.elapsed().as_micros() as u64)
            .expect("record out of range");
        metrics.insert_ios.fetch_add(1, Ordering::Relaxed);
        metrics.insert_bytes.fetch_add(bs, Ordering::Relaxed);

        tokio::task::consume_budget().await;
    }
}
