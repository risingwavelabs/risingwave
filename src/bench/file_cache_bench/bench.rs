use std::path::Path;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut};
use bytesize::ByteSize;
use clap::Parser;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::{Rng, SeedableRng};
use risingwave_storage::hummock::file_cache::cache::{FileCache, FileCacheOptions};
use risingwave_storage::hummock::file_cache::coding::CacheKey;
use tokio::sync::oneshot;

use crate::utils::{dev_stat_path, iostat, IoStat};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(long, default_value = "1073741824")] // 1 GiB
    capacity: usize,
    #[clap(long, default_value = "134217728")] // 2 * 64 MiB
    total_buffer_capacity: usize,
    #[clap(long, default_value = "67108864")] // 64 MiB
    cache_file_fallocate_unit: usize,

    #[clap(long, default_value = "1048576")] // 1 MiB
    bs: usize,
    #[clap(long, default_value = "8")]
    concurrency: usize,
    #[clap(long, default_value = "268435456")] // 256 MiB/s
    throughput: usize,
    #[clap(long, default_value = "600")] // 600s
    time: u64,
    #[clap(long, default_value = "2")]
    read: usize,
    #[clap(long, default_value = "1")]
    write: usize,
    #[clap(long, default_value = "0.5")]
    miss: f64,

    #[clap(long, default_value = "1")] // (s)
    report_interval: u64,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct Index {
    sst: u32,
    idx: u32,
}

impl CacheKey for Index {
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

    let options = FileCacheOptions {
        dir: args.path.clone(),
        capacity: args.capacity,
        total_buffer_capacity: args.total_buffer_capacity,
        cache_file_fallocate_unit: args.cache_file_fallocate_unit,
        filters: vec![],
        flush_buffer_hooks: vec![],
    };

    let cache: FileCache<Index> = FileCache::open(options).await.unwrap();

    let iostat_path = dev_stat_path(&args.path);

    let iostat_start = iostat(&iostat_path);
    let time_start = Instant::now();

    let (mut txs, rxs): (Vec<_>, Vec<_>) =
        (0..args.concurrency).map(|_| oneshot::channel()).unzip();

    let futures = rxs
        .into_iter()
        .enumerate()
        .map(|(id, rx)| bench(id, args.clone(), cache.clone(), args.time, rx))
        .collect_vec();

    let mut handles = futures.into_iter().map(tokio::spawn).collect_vec();

    let (tx_monitor, rx_monitor) = oneshot::channel();
    let handle_monitor = tokio::spawn({
        let iostat_path = iostat_path.clone();
        async move {
            monitor(
                iostat_path,
                Duration::from_secs(args.report_interval),
                rx_monitor,
            )
            .await;
        }
    });
    txs.push(tx_monitor);
    handles.push(handle_monitor);

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        for tx in txs {
            let _ = tx.send(());
        }
    });

    for handle in handles {
        handle.await.unwrap();
    }

    let iostat_end = iostat(&iostat_path);
    let analysis = analyze(time_start.elapsed(), &iostat_start, &iostat_end);
    println!("Total:\n{}", analysis);
}

async fn bench(
    id: usize,
    args: Args,
    cache: FileCache<Index>,
    time: u64,
    mut stop: oneshot::Receiver<()>,
) {
    let start = Instant::now();

    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let sst = id as u32;
    let mut idx = 0;

    let nkey = Index {
        sst: u32::MAX,
        idx: u32::MAX,
    };

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let mut keys = Vec::with_capacity(args.write);
        for _ in 0..args.write {
            idx += 1;
            let key = Index { sst, idx };
            let value = vec![b'x'; args.bs];
            cache.insert(key.clone(), value).unwrap();
            keys.push(key);
        }
        for _ in 0..args.read {
            let key = if rng.gen_range(0f64..1f64) < args.miss {
                &nkey
            } else {
                keys.choose(&mut rng).unwrap()
            };
            cache.get(key).await.unwrap();
        }

        tokio::task::yield_now().await;
    }
}

async fn monitor(
    iostat_path: impl AsRef<Path>,
    interval: Duration,
    mut stop: oneshot::Receiver<()>,
) {
    let mut ios = iostat(&iostat_path);
    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }

        tokio::time::sleep(interval).await;
        let nios = iostat(&iostat_path);
        let analysis = analyze(interval, &ios, &nios);
        println!("{}", analysis);
        ios = nios;
    }
}

const SECTOR_SIZE: usize = 512;

#[derive(Clone, Copy, Debug)]
struct Analysis {
    read_iops: f64,
    read_throughput: f64,
    write_iops: f64,
    write_throughput: f64,
}

impl std::fmt::Display for Analysis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let read_throughput = ByteSize::b(self.read_throughput as u64);
        let write_throughput = ByteSize::b(self.write_throughput as u64);

        writeln!(f, "read iops: {}", self.read_iops)?;
        writeln!(
            f,
            "read throughput: {}/s",
            read_throughput.to_string_as(true)
        )?;
        writeln!(f, "write iops: {}", self.write_iops)?;
        writeln!(
            f,
            "write throughput: {}/s",
            write_throughput.to_string_as(true)
        )?;
        Ok(())
    }
}

fn analyze(duration: Duration, iostat_start: &IoStat, iostat_end: &IoStat) -> Analysis {
    let secs = duration.as_secs_f64();
    let read_iops = (iostat_end.read_ios as f64 - iostat_start.read_ios as f64) / secs;
    let read_throughput = (iostat_end.read_sectors as f64 - iostat_start.read_sectors as f64)
        * SECTOR_SIZE as f64
        / secs;
    let write_iops = (iostat_end.write_ios as f64 - iostat_start.write_ios as f64) / secs;
    let write_throughput = (iostat_end.write_sectors as f64 - iostat_start.write_sectors as f64)
        * SECTOR_SIZE as f64
        / secs;
    Analysis {
        read_iops,
        read_throughput,
        write_iops,
        write_throughput,
    }
}
