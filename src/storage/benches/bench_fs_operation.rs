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

use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::io::AsyncReadExt;

#[derive(Default)]
struct BenchStats {
    total_time_read_millis: f64,
    total_read_count: usize,
    total_time_write_millis: f64,
    total_write_count: usize,
}

impl Display for BenchStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Write {} times, avg {}ms. Read {} times, avg {}ms",
            self.total_write_count,
            self.total_time_write_millis / self.total_write_count as f64,
            self.total_read_count,
            self.total_time_read_millis / self.total_read_count as f64
        )?;
        Ok(())
    }
}

impl BenchStats {
    fn add_read_time(&mut self, time: f64) {
        self.total_time_read_millis += time;
        self.total_read_count += 1;
    }

    fn add_write_time(&mut self, time: f64) {
        self.total_time_write_millis += time;
        self.total_write_count += 1;
    }
}

const BENCH_SIZE: usize = 10;
fn gen_test_payload() -> Vec<u8> {
    let mut ret = Vec::new();
    for i in 0..1_000_000 {
        ret.extend(format!("{:08}", i).as_bytes());
    }
    ret
}

async fn run_tokio_fs(payload: &[u8], stats: &mut BenchStats) {
    use tempfile::tempdir;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let temp_dir = tempdir().unwrap();

    for i in 0..BENCH_SIZE {
        let mut path = PathBuf::from(temp_dir.path());
        path.push(format!("{}.txt", i));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .unwrap();
        let start_time = SystemTime::now();
        file.write_all(payload).await.unwrap();
        stats.add_write_time(
            SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_millis() as f64,
        );
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut buffer = Vec::with_capacity(payload.len());
        let start_time = SystemTime::now();
        file.read_to_end(&mut buffer).await.unwrap();
        stats.add_read_time(
            SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_millis() as f64,
        );
        assert_eq!(buffer, payload);
    }
}

fn run_std_fs(payload: &[u8], stats: &mut BenchStats) {
    use tempfile::tempdir;
    let temp_dir = tempdir().unwrap();

    for i in 0..BENCH_SIZE {
        let mut path = PathBuf::from(temp_dir.path());
        path.push(format!("{}.txt", i));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let start_time = SystemTime::now();
        file.write_all(payload).unwrap();
        stats.add_write_time(
            SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_millis() as f64,
        );
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut buffer = Vec::with_capacity(payload.len());
        let start_time = SystemTime::now();
        file.read_to_end(&mut buffer).unwrap();
        stats.add_read_time(
            SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_millis() as f64,
        );
        assert_eq!(buffer, payload);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let payload = gen_test_payload();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut tokio_stats = BenchStats::default();
    c.bench_with_input(
        BenchmarkId::new("fs_operation", "tokio"),
        &payload,
        |b, payload| {
            b.iter(|| runtime.block_on(run_tokio_fs(payload, &mut tokio_stats)));
        },
    );
    println!("tokio stat: {}", tokio_stats);
    let mut std_stats = BenchStats::default();
    c.bench_with_input(
        BenchmarkId::new("fs_operation", "std"),
        &payload,
        |b, payload| {
            b.iter(|| run_std_fs(payload, &mut std_stats));
        },
    );
    println!("std stat: {}", std_stats);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
