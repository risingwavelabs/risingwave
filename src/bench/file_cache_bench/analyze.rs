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

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytesize::ByteSize;
use hdrhistogram::Histogram;
use parking_lot::RwLock;
use risingwave_storage::hummock::file_cache::cache::FlushBufferHook;
use risingwave_storage::hummock::file_cache::error::Result;
use tokio::sync::oneshot;

use crate::utils::{iostat, IoStat};

const SECTOR_SIZE: usize = 512;

// latencies are measured by 'us'
#[derive(Clone, Copy, Debug)]
pub struct Analysis {
    disk_read_iops: f64,
    disk_read_throughput: f64,
    disk_write_iops: f64,
    disk_write_throughput: f64,

    insert_iops: f64,
    insert_throughput: f64,
    insert_lat_p50: u64,
    insert_lat_p90: u64,
    insert_lat_p99: u64,

    get_iops: f64,
    get_miss: f64,
    get_miss_lat_p50: u64,
    get_miss_lat_p90: u64,
    get_miss_lat_p99: u64,
    get_hit_lat_p50: u64,
    get_hit_lat_p90: u64,
    get_hit_lat_p99: u64,

    flush_iops: f64,
    flush_throughput: f64,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct MetricsDump {
    pub insert_ios: usize,
    pub insert_bytes: usize,
    pub insert_lat_p50: u64,
    pub insert_lat_p90: u64,
    pub insert_lat_p99: u64,

    pub get_ios: usize,
    pub get_miss_ios: usize,
    pub get_hit_lat_p50: u64,
    pub get_hit_lat_p90: u64,
    pub get_hit_lat_p99: u64,
    pub get_miss_lat_p50: u64,
    pub get_miss_lat_p90: u64,
    pub get_miss_lat_p99: u64,

    pub flush_ios: usize,
    pub flush_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct Metrics {
    pub insert_ios: Arc<AtomicUsize>,
    pub insert_bytes: Arc<AtomicUsize>,
    pub insert_lats: Arc<RwLock<Histogram<u64>>>,

    pub get_ios: Arc<AtomicUsize>,
    pub get_miss_ios: Arc<AtomicUsize>,
    pub get_hit_lats: Arc<RwLock<Histogram<u64>>>,
    pub get_miss_lats: Arc<RwLock<Histogram<u64>>>,

    pub flush_ios: Arc<AtomicUsize>,
    pub flush_bytes: Arc<AtomicUsize>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            insert_ios: Arc::new(AtomicUsize::new(0)),
            insert_bytes: Arc::new(AtomicUsize::new(0)),
            insert_lats: Arc::new(RwLock::new(
                Histogram::new_with_bounds(1, 10_000_000, 2).unwrap(),
            )),

            get_ios: Arc::new(AtomicUsize::new(0)),
            get_miss_ios: Arc::new(AtomicUsize::new(0)),
            get_hit_lats: Arc::new(RwLock::new(
                Histogram::new_with_bounds(1, 10_000_000, 2).unwrap(),
            )),
            get_miss_lats: Arc::new(RwLock::new(
                Histogram::new_with_bounds(1, 10_000_000, 2).unwrap(),
            )),

            flush_ios: Arc::new(AtomicUsize::new(0)),
            flush_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Metrics {
    pub fn dump(&self) -> MetricsDump {
        let insert_lats = self.insert_lats.read();
        let get_hit_lats = self.get_hit_lats.read();
        let get_miss_lats = self.get_miss_lats.read();
        MetricsDump {
            insert_ios: self.insert_ios.load(Ordering::Relaxed),
            insert_bytes: self.insert_bytes.load(Ordering::Relaxed),
            insert_lat_p50: insert_lats.value_at_quantile(0.5),
            insert_lat_p90: insert_lats.value_at_quantile(0.9),
            insert_lat_p99: insert_lats.value_at_quantile(0.99),

            get_ios: self.get_ios.load(Ordering::Relaxed),
            get_miss_ios: self.get_miss_ios.load(Ordering::Relaxed),
            get_hit_lat_p50: get_hit_lats.value_at_quantile(0.5),
            get_hit_lat_p90: get_hit_lats.value_at_quantile(0.9),
            get_hit_lat_p99: get_hit_lats.value_at_quantile(0.99),
            get_miss_lat_p50: get_miss_lats.value_at_quantile(0.5),
            get_miss_lat_p90: get_miss_lats.value_at_quantile(0.9),
            get_miss_lat_p99: get_miss_lats.value_at_quantile(0.99),

            flush_ios: self.flush_ios.load(Ordering::Relaxed),
            flush_bytes: self.flush_bytes.load(Ordering::Relaxed),
        }
    }
}

pub struct Hook {
    metrics: Metrics,
}

impl Hook {
    pub fn new(metrics: Metrics) -> Self {
        Hook { metrics }
    }
}

#[async_trait]
impl FlushBufferHook for Hook {
    async fn post_flush(&self, bytes: usize) -> Result<()> {
        self.metrics.flush_ios.fetch_add(1, Ordering::Relaxed);
        self.metrics.flush_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
}

impl std::fmt::Display for Analysis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let disk_read_throughput = ByteSize::b(self.disk_read_throughput as u64);
        let disk_write_throughput = ByteSize::b(self.disk_write_throughput as u64);
        let disk_total_throughput = disk_read_throughput + disk_write_throughput;

        // disk statics
        writeln!(
            f,
            "disk total iops: {:.1}",
            self.disk_write_iops + self.disk_read_iops
        )?;
        writeln!(
            f,
            "disk total throughput: {}/s",
            disk_total_throughput.to_string_as(true)
        )?;
        writeln!(f, "disk read iops: {:.1}", self.disk_read_iops)?;
        writeln!(
            f,
            "disk read throughput: {}/s",
            disk_read_throughput.to_string_as(true)
        )?;
        writeln!(f, "disk write iops: {:.1}", self.disk_write_iops)?;
        writeln!(
            f,
            "disk write throughput: {}/s",
            disk_write_throughput.to_string_as(true)
        )?;

        // insert statics
        let insert_throughput = ByteSize::b(self.insert_throughput as u64);
        writeln!(f, "insert iops: {:.1}/s", self.insert_iops)?;
        writeln!(
            f,
            "insert throughput: {}/s",
            insert_throughput.to_string_as(true)
        )?;
        writeln!(f, "insert lat p50: {}us", self.insert_lat_p50)?;
        writeln!(f, "insert lat p90: {}us", self.insert_lat_p90)?;
        writeln!(f, "insert lat p99: {}us", self.insert_lat_p99)?;

        // get statics
        writeln!(f, "get iops: {:.1}/s", self.get_iops)?;
        writeln!(f, "get miss: {:.2}% ", self.get_miss * 100f64)?;
        writeln!(f, "get hit lat p50: {}us", self.get_hit_lat_p50)?;
        writeln!(f, "get hit lat p90: {}us", self.get_hit_lat_p90)?;
        writeln!(f, "get hit lat p99: {}us", self.get_hit_lat_p99)?;
        writeln!(f, "get miss lat p50: {}us", self.get_miss_lat_p50)?;
        writeln!(f, "get miss lat p90: {}us", self.get_miss_lat_p90)?;
        writeln!(f, "get miss lat p99: {}us", self.get_miss_lat_p99)?;

        // flush statics
        let flush_throughput = ByteSize::b(self.flush_throughput as u64);
        writeln!(f, "flush iops: {:.1}/s", self.flush_iops)?;
        writeln!(
            f,
            "flush throughput: {}/s",
            flush_throughput.to_string_as(true)
        )?;
        Ok(())
    }
}

pub fn analyze(
    duration: Duration,
    iostat_start: &IoStat,
    iostat_end: &IoStat,
    metrics_dump_start: &MetricsDump,
    metrics_dump_end: &MetricsDump,
) -> Analysis {
    let secs = duration.as_secs_f64();
    let disk_read_iops = (iostat_end.read_ios - iostat_start.read_ios) as f64 / secs;
    let disk_read_throughput =
        (iostat_end.read_sectors - iostat_start.read_sectors) as f64 * SECTOR_SIZE as f64 / secs;
    let disk_write_iops = (iostat_end.write_ios - iostat_start.write_ios) as f64 / secs;
    let disk_write_throughput =
        (iostat_end.write_sectors - iostat_start.write_sectors) as f64 * SECTOR_SIZE as f64 / secs;

    let insert_iops = (metrics_dump_end.insert_ios - metrics_dump_start.insert_ios) as f64 / secs;
    let insert_throughput =
        (metrics_dump_end.insert_bytes - metrics_dump_start.insert_bytes) as f64 / secs;

    let get_iops = (metrics_dump_end.get_ios - metrics_dump_start.get_ios) as f64 / secs;
    let get_miss = (metrics_dump_end.get_miss_ios - metrics_dump_start.get_miss_ios) as f64
        / (metrics_dump_end.get_ios - metrics_dump_start.get_ios) as f64;

    let flush_iops = (metrics_dump_end.flush_ios - metrics_dump_start.flush_ios) as f64 / secs;
    let flush_throughput =
        (metrics_dump_end.flush_bytes - metrics_dump_start.flush_bytes) as f64 / secs;

    Analysis {
        disk_read_iops,
        disk_read_throughput,
        disk_write_iops,
        disk_write_throughput,

        insert_iops,
        insert_throughput,
        insert_lat_p50: metrics_dump_end.insert_lat_p50,
        insert_lat_p90: metrics_dump_end.insert_lat_p90,
        insert_lat_p99: metrics_dump_end.insert_lat_p99,

        get_iops,
        get_miss,
        get_hit_lat_p50: metrics_dump_end.get_hit_lat_p50,
        get_hit_lat_p90: metrics_dump_end.get_hit_lat_p90,
        get_hit_lat_p99: metrics_dump_end.get_hit_lat_p99,
        get_miss_lat_p50: metrics_dump_end.get_miss_lat_p50,
        get_miss_lat_p90: metrics_dump_end.get_miss_lat_p90,
        get_miss_lat_p99: metrics_dump_end.get_miss_lat_p99,

        flush_iops,
        flush_throughput,
    }
}

pub async fn monitor(
    iostat_path: impl AsRef<Path>,
    interval: Duration,
    metrics: Metrics,
    mut stop: oneshot::Receiver<()>,
) {
    let mut stat = iostat(&iostat_path);
    let mut metrics_dump = metrics.dump();
    loop {
        let start = Instant::now();
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }

        tokio::time::sleep(interval).await;
        let new_stat = iostat(&iostat_path);
        let new_metrics_dump = metrics.dump();
        let analysis = analyze(
            // interval may have ~ +7% error
            start.elapsed(),
            &stat,
            &new_stat,
            &metrics_dump,
            &new_metrics_dump,
        );
        println!("{}", analysis);
        stat = new_stat;
        metrics_dump = new_metrics_dump;
    }
}
