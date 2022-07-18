// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version &2.0 (the "License");
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
use std::time::Duration;

use async_trait::async_trait;
use bytesize::ByteSize;
use risingwave_storage::hummock::file_cache::cache::FlushBufferHook;
use risingwave_storage::hummock::file_cache::error::Result;
use tokio::sync::oneshot;

use crate::utils::{iostat, IoStat};

const SECTOR_SIZE: usize = 512;

#[derive(Clone, Copy, Debug)]
pub struct Analysis {
    disk_read_iops: f64,
    disk_read_throughput: f64,
    disk_write_iops: f64,
    disk_write_throughput: f64,

    insert_iops: f64,
    insert_throughput: f64,
    flush_iops: f64,
    flush_throughput: f64,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct MetricsDump {
    pub insert_ios: usize,
    pub insert_bytes: usize,
    pub flush_ios: usize,
    pub flush_bytes: usize,
}

#[derive(Default, Clone, Debug)]
pub struct Metrics {
    pub insert_ios: Arc<AtomicUsize>,
    pub insert_bytes: Arc<AtomicUsize>,
    pub flush_ios: Arc<AtomicUsize>,
    pub flush_bytes: Arc<AtomicUsize>,
}

impl Metrics {
    pub fn dump(&self) -> MetricsDump {
        MetricsDump {
            insert_ios: self.insert_ios.load(Ordering::Relaxed),
            insert_bytes: self.insert_bytes.load(Ordering::Relaxed),
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

        let insert_throughput = ByteSize::b(self.insert_throughput as u64);
        let flush_throughput = ByteSize::b(self.flush_throughput as u64);

        writeln!(f, "insert iops: {:.1}/s", self.insert_iops)?;
        writeln!(
            f,
            "insert throughput: {}/s",
            insert_throughput.to_string_as(true)
        )?;
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
    let disk_read_iops = (iostat_end.read_ios as f64 - iostat_start.read_ios as f64) / secs;
    let disk_read_throughput = (iostat_end.read_sectors as f64 - iostat_start.read_sectors as f64)
        * SECTOR_SIZE as f64
        / secs;
    let disk_write_iops = (iostat_end.write_ios as f64 - iostat_start.write_ios as f64) / secs;
    let disk_write_throughput =
        (iostat_end.write_sectors as f64 - iostat_start.write_sectors as f64) * SECTOR_SIZE as f64
            / secs;

    let foreground_write_iops =
        (metrics_dump_end.insert_ios as f64 - metrics_dump_start.insert_ios as f64) / secs;
    let foreground_write_throughput =
        (metrics_dump_end.insert_bytes as f64 - metrics_dump_start.insert_bytes as f64) / secs;
    let flush_data_iops =
        (metrics_dump_end.flush_ios as f64 - metrics_dump_start.flush_ios as f64) / secs;
    let flush_data_throughput =
        (metrics_dump_end.flush_bytes as f64 - metrics_dump_start.flush_bytes as f64) / secs;

    Analysis {
        disk_read_iops,
        disk_read_throughput,
        disk_write_iops,
        disk_write_throughput,

        insert_iops: foreground_write_iops,
        insert_throughput: foreground_write_throughput,
        flush_iops: flush_data_iops,
        flush_throughput: flush_data_throughput,
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
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }

        tokio::time::sleep(interval).await;
        let new_stat = iostat(&iostat_path);
        let new_metrics_dump = metrics.dump();
        let analysis = analyze(interval, &stat, &new_stat, &metrics_dump, &new_metrics_dump);
        println!("{}", analysis);
        stat = new_stat;
        metrics_dump = new_metrics_dump;
    }
}
